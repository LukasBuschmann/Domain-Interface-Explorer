from __future__ import annotations

import hashlib
import importlib
import json
import os
import sys
import tempfile
import threading
from dataclasses import dataclass
from importlib.machinery import PathFinder
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .config import ALPHAFOLD_API


_PYMOL_API_LOCK = threading.RLock()
_PYMOL2_MODULE = None
_PYMOL2_IMPORT_ERROR: Exception | None = None
_CACHE_FILE_LOCKS: dict[str, threading.Lock] = {}
_CACHE_FILE_LOCKS_GUARD = threading.Lock()


@dataclass(frozen=True)
class PyMOLAPIStatus:
    available: bool
    reason: str


def parse_row_key(row_key: str) -> tuple[str, str]:
    parts = row_key.split("_", maxsplit=2)
    if len(parts) < 2:
        raise ValueError(f"invalid row_key: {row_key}")
    return parts[0], parts[1]


def parse_interface_row_key(row_key: str) -> tuple[str, str, str]:
    parts = row_key.split("_", maxsplit=2)
    if len(parts) < 3:
        return parts[0] if parts else "", parts[1] if len(parts) > 1 else "", ""
    return parts[0], parts[1], parts[2]


def fragment_ranges(fragment_key: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for part in str(fragment_key or "").split(","):
        item = part.strip()
        if not item:
            continue
        start, end = item.split("-", maxsplit=1)
        ranges.append((int(start), int(end)))
    return ranges


def fragment_bounds(fragment_key: str) -> tuple[int, int]:
    ranges = fragment_ranges(fragment_key)
    if not ranges:
        raise ValueError(f"invalid fragment_key: {fragment_key}")
    return ranges[0][0], ranges[-1][1]


def json_request(url: str) -> list[dict]:
    request = Request(url, headers={"User-Agent": "domain-interface-explorer/1.0"})
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def prediction_cache_file(cache_dir: Path, accession: str) -> Path:
    return cache_dir / "alphafold" / accession / "predictions.json"


def cache_file_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _CACHE_FILE_LOCKS_GUARD:
        lock = _CACHE_FILE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _CACHE_FILE_LOCKS[key] = lock
        return lock


def load_cached_predictions(cache_dir: Path, accession: str) -> list[dict] | None:
    cache_file = prediction_cache_file(cache_dir, accession)
    if not cache_file.exists():
        return None
    try:
        with cache_file.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


def write_text_atomic(destination: Path, text: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(text)
        temp_path = Path(handle.name)
    os.replace(temp_path, destination)


def write_bytes_atomic(destination: Path, data: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "wb",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(data)
        temp_path = Path(handle.name)
    os.replace(temp_path, destination)


def save_cached_predictions(cache_dir: Path, accession: str, predictions: list[dict]) -> None:
    cache_file = prediction_cache_file(cache_dir, accession)
    write_text_atomic(cache_file, json.dumps(predictions))


def download_file(url: str, destination: Path) -> None:
    request = Request(url, headers={"User-Agent": "domain-interface-explorer/1.0"})
    with urlopen(request, timeout=60) as response:
        payload = response.read()
    write_bytes_atomic(destination, payload)


def model_file_is_usable(model_path: Path) -> bool:
    if not model_path.exists() or not model_path.is_file():
        return False
    if model_path.stat().st_size <= 0:
        return False
    if model_path.suffix.lower() == ".pdb":
        try:
            has_atom = False
            has_end = False
            with model_path.open("r", encoding="utf-8", errors="ignore") as handle:
                for line in handle:
                    if line.startswith("ATOM"):
                        has_atom = True
                    elif line.startswith("END"):
                        has_end = True
        except OSError:
            return False
        return has_atom and has_end
    return True


def choose_alphafold_prediction(cache_dir: Path, accession: str, start: int, end: int) -> dict:
    cache_file = prediction_cache_file(cache_dir, accession)
    with cache_file_lock(cache_file):
        predictions = load_cached_predictions(cache_dir, accession)
        if predictions is None:
            predictions = json_request(ALPHAFOLD_API.format(accession=accession))
            save_cached_predictions(cache_dir, accession, predictions)
    if not predictions:
        raise FileNotFoundError(f"no AlphaFold prediction found for {accession}")
    for prediction in predictions:
        try:
            pred_start = int(prediction.get("uniprotStart"))
            pred_end = int(prediction.get("uniprotEnd"))
        except (TypeError, ValueError):
            continue
        if pred_start <= start and pred_end >= end:
            return prediction
    return predictions[0]


def ensure_alphafold_model(cache_dir: Path, accession: str, start: int, end: int) -> tuple[Path, dict]:
    prediction = choose_alphafold_prediction(cache_dir, accession, start, end)
    structure_url = prediction.get("pdbUrl") or prediction.get("cifUrl")
    if not structure_url:
        raise FileNotFoundError(f"prediction for {accession} has no downloadable structure")
    filename = Path(urlparse(structure_url).path).name
    destination = cache_dir / "alphafold" / accession / filename
    with cache_file_lock(destination):
        if not model_file_is_usable(destination):
            download_file(structure_url, destination)
    return destination, prediction


def expand_fragments_to_residue_ids(fragments: list[list[int]]) -> set[int]:
    residue_ids: set[int] = set()
    for fragment in fragments:
        if len(fragment) != 2:
            continue
        start, end = int(fragment[0]), int(fragment[1])
        residue_ids.update(range(start, end + 1))
    return residue_ids


def expand_fragment_key_to_residue_ids(fragment_key: str) -> set[int]:
    residue_ids: set[int] = set()
    for start, end in fragment_ranges(fragment_key):
        residue_ids.update(range(start, end + 1))
    return residue_ids


def fragment_key_to_ranges(fragment_key: str) -> list[str]:
    return [f"{start}-{end}" for start, end in fragment_ranges(fragment_key)]


def collect_row_structure_payload(
    interface_data: dict[str, dict[str, dict]], row_key: str, partner_filter: str
) -> dict[str, object]:
    interface_residues: set[int] = set()
    surface_residues: set[int] = set()
    partner_interface_residues: set[int] = set()
    partner_surface_residues: set[int] = set()
    partner_fragment_residues: set[int] = set()
    partner_fragment_ranges: set[str] = set()
    residue_contacts: set[tuple[int, int]] = set()
    matched_partners: list[str] = []
    for partner_domain, rows in interface_data.items():
        if partner_filter != "__all__" and partner_domain != partner_filter:
            continue
        payload = rows.get(row_key)
        if payload is None:
            continue
        _protein_id, _fragment_key, partner_fragment_key = parse_interface_row_key(row_key)
        matched_partners.append(partner_domain)
        interface_residues.update(int(value) for value in payload.get("interface_residues_a", []))
        surface_residues.update(int(value) for value in payload.get("surface_residue_ids_a", []))
        partner_interface_residues.update(int(value) for value in payload.get("interface_residues_b", []))
        partner_surface_residues.update(int(value) for value in payload.get("surface_residue_ids_b", []))
        for contact in payload.get("residue_contacts", []):
            if not isinstance(contact, (list, tuple)) or len(contact) < 2:
                continue
            try:
                residue_contacts.add((int(contact[0]), int(contact[1])))
            except (TypeError, ValueError):
                continue
        partner_fragments = payload.get("fragments_b", [])
        if partner_fragments:
            partner_fragment_residues.update(expand_fragments_to_residue_ids(partner_fragments))
        elif partner_fragment_key:
            partner_fragment_residues.update(expand_fragment_key_to_residue_ids(partner_fragment_key))
            for partner_start, partner_end in fragment_ranges(partner_fragment_key):
                partner_fragment_ranges.add(f"{partner_start}-{partner_end}")
        for fragment in partner_fragments:
            if len(fragment) != 2:
                continue
            partner_fragment_ranges.add(f"{int(fragment[0])}-{int(fragment[1])}")
    return {
        "interface_residue_ids": sorted(interface_residues),
        "surface_residue_ids": sorted(surface_residues),
        "partner_interface_residue_ids": sorted(partner_interface_residues),
        "partner_surface_residue_ids": sorted(partner_surface_residues),
        "partner_fragment_residue_ids": sorted(partner_fragment_residues),
        "partner_fragment_ranges": sorted(partner_fragment_ranges),
        "residue_contacts": [list(contact) for contact in sorted(residue_contacts)],
        "matched_partners": sorted(matched_partners),
    }


def structure_cache_key(
    accession: str,
    fragment_key: str,
    partner_filter: str,
    interface_residues: list[int],
    surface_residues: list[int],
) -> str:
    payload = {
        "accession": accession,
        "fragment_key": fragment_key,
        "partner_filter": partner_filter,
        "interface_residues": interface_residues,
        "surface_residues": surface_residues,
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def aligned_model_cache_key(
    *,
    reference_accession: str,
    reference_fragment_key: str,
    mobile_accession: str,
    mobile_fragment_key: str,
) -> str:
    payload = {
        "reference_accession": reference_accession,
        "reference_fragment_key": reference_fragment_key,
        "mobile_accession": mobile_accession,
        "mobile_fragment_key": mobile_fragment_key,
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _install_imp_compat_module() -> None:
    if "imp" in sys.modules:
        return
    compat_module = ModuleType("imp")

    def find_module(name: str, path: list[str] | None = None):
        spec = PathFinder.find_spec(name, path)
        if spec is None:
            raise ImportError(name)
        if spec.submodule_search_locations:
            location = list(spec.submodule_search_locations)[0]
        else:
            location = spec.origin
        if location is None:
            raise ImportError(name)
        return None, location, ("", "", 5)

    compat_module.find_module = find_module  # type: ignore[attr-defined]
    sys.modules["imp"] = compat_module


def load_pymol2_module():
    global _PYMOL2_MODULE, _PYMOL2_IMPORT_ERROR
    if _PYMOL2_MODULE is not None:
        return _PYMOL2_MODULE
    if _PYMOL2_IMPORT_ERROR is not None:
        raise RuntimeError(f"PyMOL API unavailable: {_PYMOL2_IMPORT_ERROR}") from _PYMOL2_IMPORT_ERROR
    try:
        _install_imp_compat_module()
        _PYMOL2_MODULE = importlib.import_module("pymol2")
        return _PYMOL2_MODULE
    except Exception as exc:  # pragma: no cover - exercised indirectly in startup/runtime checks
        _PYMOL2_IMPORT_ERROR = exc
        raise RuntimeError(f"PyMOL API unavailable: {exc}") from exc


def validate_pymol_api() -> PyMOLAPIStatus:
    try:
        pymol2 = load_pymol2_module()
        with _PYMOL_API_LOCK:
            with pymol2.PyMOL() as session:
                session.cmd.reinitialize()
        return PyMOLAPIStatus(available=True, reason="PyMOL API available")
    except Exception as exc:
        return PyMOLAPIStatus(available=False, reason=str(exc))


def residue_selection_expression(object_name: str, residue_ids: list[int]) -> str:
    if not residue_ids:
        return "none"
    joined = "+".join(str(residue_id) for residue_id in residue_ids)
    return f"{object_name} and resi {joined}"


def selection_ca_residue_ids(cmd, selection: str) -> list[int]:
    residue_ids: set[int] = set()
    cmd.iterate(
        selection,
        "residue_ids.add(int(resi))",
        space={"residue_ids": residue_ids},
    )
    return sorted(residue_ids)


def selection_ca_diagnostics(cmd, object_name: str, selection: str) -> dict[str, int | None]:
    selected_residue_ids = selection_ca_residue_ids(cmd, selection)
    all_ca_residue_ids = selection_ca_residue_ids(
        cmd,
        f"{object_name} and polymer.protein and name CA",
    )
    return {
        "selected_ca_count": int(cmd.count_atoms(selection)),
        "selected_min_resi": selected_residue_ids[0] if selected_residue_ids else None,
        "selected_max_resi": selected_residue_ids[-1] if selected_residue_ids else None,
        "total_ca_count": int(cmd.count_atoms(f"{object_name} and polymer.protein and name CA")),
        "total_min_resi": all_ca_residue_ids[0] if all_ca_residue_ids else None,
        "total_max_resi": all_ca_residue_ids[-1] if all_ca_residue_ids else None,
    }


def format_alignment_diagnostics(
    *,
    reference_model_path: Path,
    reference_fragment_key: str,
    reference_selection: str,
    reference_diagnostics: dict[str, int | None],
    mobile_model_path: Path,
    mobile_fragment_key: str,
    mobile_selection: str,
    mobile_diagnostics: dict[str, int | None],
) -> str:
    return (
        f"reference_model={reference_model_path.name} "
        f"reference_fragment={reference_fragment_key} "
        f"reference_selection={reference_selection!r} "
        f"reference_selected_ca={reference_diagnostics['selected_ca_count']} "
        f"reference_selected_span={reference_diagnostics['selected_min_resi']}-"
        f"{reference_diagnostics['selected_max_resi']} "
        f"reference_total_ca={reference_diagnostics['total_ca_count']} "
        f"reference_total_span={reference_diagnostics['total_min_resi']}-"
        f"{reference_diagnostics['total_max_resi']} | "
        f"mobile_model={mobile_model_path.name} "
        f"mobile_fragment={mobile_fragment_key} "
        f"mobile_selection={mobile_selection!r} "
        f"mobile_selected_ca={mobile_diagnostics['selected_ca_count']} "
        f"mobile_selected_span={mobile_diagnostics['selected_min_resi']}-"
        f"{mobile_diagnostics['selected_max_resi']} "
        f"mobile_total_ca={mobile_diagnostics['total_ca_count']} "
        f"mobile_total_span={mobile_diagnostics['total_min_resi']}-"
        f"{mobile_diagnostics['total_max_resi']}"
    )


def render_structure_image(
    model_path: Path,
    image_path: Path,
    fragment_key_name: str,
    interface_residues: list[int],
    surface_residues: list[int],
) -> None:
    image_path.parent.mkdir(parents=True, exist_ok=True)
    fragment_start, fragment_end = fragment_bounds(fragment_key_name)
    try:
        pymol2 = load_pymol2_module()
        with _PYMOL_API_LOCK:
            with pymol2.PyMOL() as session:
                cmd = session.cmd
                cmd.reinitialize()
                object_name = "structure_model"
                cmd.load(str(model_path), object_name)
                cmd.hide("everything", "all")
                cmd.show("cartoon", object_name)
                cmd.color("gray70", object_name)
                cmd.set("cartoon_transparency", 0.72, object_name)
                fragment_selection = f"{object_name} and resi {fragment_start}-{fragment_end}"
                cmd.select("fragment_sel", fragment_selection)
                cmd.show("cartoon", "fragment_sel")
                cmd.color("gray80", "fragment_sel")
                cmd.set("cartoon_transparency", 0.0, "fragment_sel")
                if surface_residues:
                    cmd.select("surface_sel", residue_selection_expression(object_name, surface_residues))
                    cmd.color("tv_orange", "surface_sel")
                    cmd.set("cartoon_transparency", 0.0, "surface_sel")
                    cmd.show("sticks", "surface_sel")
                    cmd.set("stick_radius", 0.18, "surface_sel")
                if interface_residues:
                    cmd.select("interface_sel", residue_selection_expression(object_name, interface_residues))
                    cmd.color("firebrick", "interface_sel")
                    cmd.set("cartoon_transparency", 0.0, "interface_sel")
                    cmd.show("sticks", "interface_sel")
                    cmd.set("stick_radius", 0.24, "interface_sel")
                cmd.orient("fragment_sel")
                cmd.zoom("fragment_sel", 10)
                cmd.bg_color("white")
                cmd.set("antialias", 2)
                cmd.set("depth_cue", 0)
                cmd.png(str(image_path), width=1400, height=1050, ray=1)
    except Exception as exc:
        raise RuntimeError(str(exc) or "PyMOL render failed") from exc


def convert_model_to_pdb(model_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp.pdb",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
        pymol2 = load_pymol2_module()
        with _PYMOL_API_LOCK:
            with pymol2.PyMOL() as session:
                cmd = session.cmd
                cmd.reinitialize()
                cmd.load(str(model_path), "structure_model")
                cmd.save(str(temp_path), "structure_model")
        os.replace(temp_path, output_path)
    except Exception as exc:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        error_message = str(exc).strip() or "PyMOL model conversion failed"
        raise RuntimeError(error_message) from exc


def render_aligned_model(
    reference_model_path: Path,
    reference_fragment_key: str,
    mobile_model_path: Path,
    mobile_fragment_key: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    reference_start, reference_end = fragment_bounds(reference_fragment_key)
    mobile_start, mobile_end = fragment_bounds(mobile_fragment_key)
    reference_selection = (
        "reference_model and polymer.protein and name CA "
        f"and resi {reference_start}-{reference_end}"
    )
    mobile_selection = (
        "mobile_model and polymer.protein and name CA "
        f"and resi {mobile_start}-{mobile_end}"
    )
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp.pdb",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
        pymol2 = load_pymol2_module()
        with _PYMOL_API_LOCK:
            with pymol2.PyMOL() as session:
                cmd = session.cmd
                cmd.reinitialize()
                cmd.load(str(reference_model_path), "reference_model")
                cmd.load(str(mobile_model_path), "mobile_model")
                reference_diagnostics = selection_ca_diagnostics(
                    cmd,
                    "reference_model",
                    reference_selection,
                )
                mobile_diagnostics = selection_ca_diagnostics(
                    cmd,
                    "mobile_model",
                    mobile_selection,
                )
                alignment_diagnostics = format_alignment_diagnostics(
                    reference_model_path=reference_model_path,
                    reference_fragment_key=reference_fragment_key,
                    reference_selection=reference_selection,
                    reference_diagnostics=reference_diagnostics,
                    mobile_model_path=mobile_model_path,
                    mobile_fragment_key=mobile_fragment_key,
                    mobile_selection=mobile_selection,
                    mobile_diagnostics=mobile_diagnostics,
                )
                cmd.cealign(reference_selection, mobile_selection)
                cmd.save(str(temp_path), "mobile_model")
        os.replace(temp_path, output_path)
    except Exception as exc:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        error_message = str(exc).strip() or "PyMOL cealign failed"
        if error_message.lower() in {"error", "error:"}:
            error_message = "PyMOL cealign failed"
        if "alignment_diagnostics" in locals():
            error_message = f"{error_message} | {alignment_diagnostics}"
        raise RuntimeError(error_message) from exc
