from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .config import ALPHAFOLD_API


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


def load_cached_predictions(cache_dir: Path, accession: str) -> list[dict] | None:
    cache_file = prediction_cache_file(cache_dir, accession)
    if not cache_file.exists():
        return None
    with cache_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_cached_predictions(cache_dir: Path, accession: str, predictions: list[dict]) -> None:
    cache_file = prediction_cache_file(cache_dir, accession)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with cache_file.open("w", encoding="utf-8") as handle:
        json.dump(predictions, handle)


def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(url, headers={"User-Agent": "domain-interface-explorer/1.0"})
    with urlopen(request, timeout=60) as response, destination.open("wb") as handle:
        handle.write(response.read())


def choose_alphafold_prediction(cache_dir: Path, accession: str, start: int, end: int) -> dict:
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
    if not destination.exists():
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
) -> dict[str, list[int] | list[str]]:
    interface_residues: set[int] = set()
    surface_residues: set[int] = set()
    partner_interface_residues: set[int] = set()
    partner_surface_residues: set[int] = set()
    partner_fragment_residues: set[int] = set()
    partner_fragment_ranges: set[str] = set()
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


def pymol_residue_selection(name: str, residue_ids: list[int]) -> str:
    if not residue_ids:
        return f"select {name}, none"
    joined = "+".join(str(residue_id) for residue_id in residue_ids)
    return f"select {name}, resi {joined}"


def render_structure_image(
    pymol_bin: Path,
    model_path: Path,
    image_path: Path,
    fragment_key_name: str,
    interface_residues: list[int],
    surface_residues: list[int],
) -> None:
    if not pymol_bin.exists():
        raise FileNotFoundError(f"PyMOL binary not found at {pymol_bin}")
    image_path.parent.mkdir(parents=True, exist_ok=True)
    fragment_start, fragment_end = fragment_bounds(fragment_key_name)
    pml = f"""
reinitialize
load {model_path.as_posix()}, model
hide everything, all
show cartoon, model
color gray70, model
set cartoon_transparency, 0.72, model
select fragment_sel, resi {fragment_start}-{fragment_end}
show cartoon, fragment_sel
color gray80, fragment_sel
set cartoon_transparency, 0.0, fragment_sel
{pymol_residue_selection("surface_sel", surface_residues)}
color tv_orange, surface_sel
set cartoon_transparency, 0.0, surface_sel
show sticks, surface_sel
set stick_radius, 0.18, surface_sel
{pymol_residue_selection("interface_sel", interface_residues)}
color firebrick, interface_sel
set cartoon_transparency, 0.0, interface_sel
show sticks, interface_sel
set stick_radius, 0.24, interface_sel
orient fragment_sel
zoom fragment_sel, 10
bg_color white
set antialias, 2
set depth_cue, 0
png {image_path.as_posix()}, width=1400, height=1050, ray=1
quit
""".strip()
    with tempfile.NamedTemporaryFile("w", suffix=".pml", delete=False, encoding="utf-8") as handle:
        handle.write(pml)
        script_path = Path(handle.name)
    try:
        subprocess.run(
            [str(pymol_bin), "-cq", str(script_path)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(exc.stderr.strip() or exc.stdout.strip() or "PyMOL render failed") from exc
    finally:
        script_path.unlink(missing_ok=True)


def render_aligned_model(
    pymol_bin: Path,
    reference_model_path: Path,
    reference_fragment_key: str,
    mobile_model_path: Path,
    mobile_fragment_key: str,
    output_path: Path,
) -> None:
    if not pymol_bin.exists():
        raise FileNotFoundError(f"PyMOL binary not found at {pymol_bin}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
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
    pml = f"""
reinitialize
load {reference_model_path.as_posix()}, reference_model
load {mobile_model_path.as_posix()}, mobile_model
cealign {reference_selection}, {mobile_selection}
save {output_path.as_posix()}, mobile_model
quit
""".strip()
    with tempfile.NamedTemporaryFile("w", suffix=".pml", delete=False, encoding="utf-8") as handle:
        handle.write(pml)
        script_path = Path(handle.name)
    try:
        subprocess.run(
            [str(pymol_bin), "-cq", str(script_path)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(exc.stderr.strip() or exc.stdout.strip() or "PyMOL alignment failed") from exc
    finally:
        script_path.unlink(missing_ok=True)
