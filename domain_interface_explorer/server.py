from __future__ import annotations

import argparse
import json
import mimetypes
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

from domain_interface_explorer.serverlib.config import (
    DEFAULT_CACHE_DIR,
    DEFAULT_CACHE_WORKERS,
    DEFAULT_HOST,
    DEFAULT_INTERFACE_DIR,
    STATIC_DIR,
)
from domain_interface_explorer.serverlib.interface_files import (
    directory_interface_json_paths,
    interface_file_pfam_id,
    is_interface_json_path,
    load_interface_json,
)
from domain_interface_explorer.serverlib.interface_embedding import (
    build_interface_alignment_rows,
    compute_cluster_compare_payload,
    compute_tsne_embedding_payload,
    clustering_cache_path,
    embedding_cache_path,
    filter_interface_payload,
    load_interface_distance_data,
    load_or_compute_row_distance_matrix_payload,
    load_or_compute_clustering_payload,
    parse_clustering_settings,
    parse_distance_metric,
    parse_embedding_settings,
    parse_interface_filter_settings,
)
from domain_interface_explorer.serverlib.stats_service import (
    load_cached_pfam_option_stats,
    load_or_fetch_pfam_info,
    load_or_compute_clean_column_identity,
    start_background_pfam_metadata_refresh,
)
from domain_interface_explorer.serverlib.structure_service import (
    aligned_model_cache_key,
    cache_file_lock,
    collect_row_structure_payload,
    convert_model_to_pdb,
    ensure_alphafold_model,
    expand_fragment_key_to_residue_ids,
    fragment_bounds,
    fragment_key_to_ranges,
    model_file_is_usable,
    parse_row_key,
    render_aligned_model,
    structure_cache_key,
    validate_pymol_api,
)


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--interface-dir", type=Path, default=DEFAULT_INTERFACE_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--cache-workers", type=positive_int, default=DEFAULT_CACHE_WORKERS)
    return parser.parse_args()


def list_json_files(directory: Path) -> list[str]:
    return [path.name for path in directory_interface_json_paths(directory)]


def safe_file_path(directory: Path, filename: str) -> Path | None:
    candidate = directory / Path(filename).name
    if candidate.parent != directory:
        return None
    if not candidate.exists() or not candidate.is_file() or not is_interface_json_path(candidate):
        return None
    return candidate


class ViewerRequestHandler(BaseHTTPRequestHandler):
    interface_dir: Path
    cache_dir: Path
    cache_workers: int
    pfam_option_stats: dict[str, dict[str, object]]

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/files":
            self._handle_files()
            return
        if parsed.path == "/api/msa":
            self._handle_msa(parse_qs(parsed.query))
            return
        if parsed.path == "/api/interface":
            self._handle_interface(parse_qs(parsed.query))
            return
        if parsed.path == "/api/pfam-info":
            self._handle_pfam_info(parse_qs(parsed.query))
            return
        if parsed.path == "/api/embedding":
            self._handle_embedding(parse_qs(parsed.query))
            return
        if parsed.path == "/api/clustering":
            self._handle_clustering(parse_qs(parsed.query))
            return
        if parsed.path == "/api/distance-matrix":
            self._handle_distance_matrix(parse_qs(parsed.query))
            return
        if parsed.path == "/api/cluster-compare":
            self._handle_cluster_compare(parse_qs(parsed.query))
            return
        if parsed.path == "/api/structure-preview":
            self._handle_structure_preview(parse_qs(parsed.query))
            return
        if parsed.path.startswith("/api/alphafold-model/"):
            self._handle_alphafold_model(parsed.path.removeprefix("/api/alphafold-model/"))
            return
        if parsed.path.startswith("/api/aligned-model/"):
            self._handle_aligned_model(parsed.path.removeprefix("/api/aligned-model/"))
            return
        if parsed.path.startswith("/api/converted-model/"):
            self._handle_converted_model(parsed.path.removeprefix("/api/converted-model/"))
            return
        if parsed.path.startswith("/api/rendered-image/"):
            self._handle_rendered_image(parsed.path.removeprefix("/api/rendered-image/"))
            return
        if parsed.path == "/":
            self._serve_static("index.html")
            return
        self._serve_static(parsed.path.lstrip("/"))

    def log_message(self, format: str, *args) -> None:
        return

    def _log_structure_preview(self, message: str, **context: object) -> None:
        details = ", ".join(
            f"{key}={value}"
            for key, value in context.items()
            if value not in ("", None)
        )
        suffix = f" ({details})" if details else ""
        print(f"[structure-preview] {message}{suffix}", flush=True)

    def _handle_files(self) -> None:
        self._send_json(
            {
                "interface_dir": str(self.interface_dir),
                "interface_files": list_json_files(self.interface_dir),
                "pfam_option_stats": self.pfam_option_stats,
            }
        )

    def _resolve_interface_request(
        self,
        filename: str,
        query: dict[str, list[str]],
    ) -> tuple[
        Path,
        dict[str, dict[str, dict]],
        dict[str, dict[str, dict]],
        dict[str, object],
    ] | None:
        path = safe_file_path(self.interface_dir, filename)
        if path is None:
            self._send_json({"error": f"missing interface file {filename}"}, status=HTTPStatus.NOT_FOUND)
            return None
        try:
            interface_filter_settings = parse_interface_filter_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return None
        interface_payload = load_interface_json(path)
        filtered_payload = filter_interface_payload(interface_payload, interface_filter_settings)
        return path, interface_payload, filtered_payload, interface_filter_settings

    def _handle_msa(self, query: dict[str, list[str]]) -> None:
        self._send_json(
            {"error": "MSA files are no longer served by the viewer; use /api/interface instead."},
            status=HTTPStatus.GONE,
        )

    def _handle_interface(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved = self._resolve_interface_request(filename, query)
        if resolved is None:
            return
        path, raw_payload, filtered_payload, interface_filter_settings = resolved
        rows, alignment_length = build_interface_alignment_rows(filtered_payload)
        pfam_id = interface_file_pfam_id(path)
        self._send_json(
            {
                "file": path.name,
                "pfam_id": pfam_id,
                "filter_settings": interface_filter_settings,
                "alignment_length": alignment_length,
                "row_count": len(rows),
                "clean_column_identity": load_or_compute_clean_column_identity(path, raw_payload),
                "rows": rows,
                "data": filtered_payload,
            }
        )

    def _handle_pfam_info(self, query: dict[str, list[str]]) -> None:
        pfam_id = query.get("pfam_id", [""])[0]
        if not pfam_id:
            self._send_json({"error": "missing pfam_id"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            pfam_info = load_or_fetch_pfam_info(self.cache_dir, pfam_id)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except HTTPError as exc:
            status = (
                HTTPStatus.NOT_FOUND
                if exc.code == HTTPStatus.NOT_FOUND
                else HTTPStatus.BAD_GATEWAY
            )
            self._send_json(
                {"error": f"failed to load PFAM info for {pfam_id}: {exc.reason}"},
                status=status,
            )
            return
        except URLError as exc:
            self._send_json(
                {"error": f"failed to load PFAM info for {pfam_id}: {exc.reason}"},
                status=HTTPStatus.BAD_GATEWAY,
            )
            return
        self._send_json(pfam_info)

    def _handle_embedding(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved = self._resolve_interface_request(filename, query)
        if resolved is None:
            return
        path, _raw_payload, interface_payload, interface_filter_settings = resolved
        try:
            settings = parse_embedding_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        cache_path = embedding_cache_path(self.cache_dir, path, settings, interface_filter_settings)
        if cache_path.exists():
            with cache_path.open("r", encoding="utf-8") as handle:
                self._send_json(json.load(handle))
            return
        try:
            distance_data = load_interface_distance_data(
                self.cache_dir,
                path,
                interface_payload,
                str(settings["distance"]),
                interface_filter_settings,
                distance_scope="compressed",
                cache_workers=self.cache_workers,
            )
            embedding_payload = compute_tsne_embedding_payload(distance_data, settings)
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        response_payload = {
            "file": path.name,
            "pfam_id": interface_file_pfam_id(path),
            "filter_settings": interface_filter_settings,
            **embedding_payload,
        }
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("w", encoding="utf-8") as handle:
            json.dump(response_payload, handle)
        self._send_json(response_payload)

    def _handle_clustering(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved = self._resolve_interface_request(filename, query)
        if resolved is None:
            return
        path, _raw_payload, interface_payload, interface_filter_settings = resolved
        try:
            clustering_settings = parse_clustering_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        cache_path = clustering_cache_path(
            self.cache_dir,
            path,
            clustering_settings,
            interface_filter_settings,
        )
        if cache_path.exists():
            with cache_path.open("r", encoding="utf-8") as handle:
                self._send_json(json.load(handle))
            return
        try:
            response_payload = load_or_compute_clustering_payload(
                self.cache_dir,
                path,
                interface_payload,
                clustering_settings,
                interface_filter_settings,
                cache_workers=self.cache_workers,
            )
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected clustering error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json(response_payload)

    def _handle_distance_matrix(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved = self._resolve_interface_request(filename, query)
        if resolved is None:
            return
        path, _raw_payload, interface_payload, interface_filter_settings = resolved
        try:
            distance_metric = parse_distance_metric(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            response_payload = {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                **load_or_compute_row_distance_matrix_payload(
                    self.cache_dir,
                    path,
                    interface_payload,
                    distance_metric,
                    interface_filter_settings,
                ),
            }
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._send_json(response_payload)

    def _handle_cluster_compare(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        cluster_label_raw = query.get("cluster_label", [""])[0].strip()
        resolved = self._resolve_interface_request(filename, query)
        if resolved is None:
            return
        path, _raw_payload, interface_payload, interface_filter_settings = resolved
        if cluster_label_raw == "":
            self._send_json({"error": "cluster_label is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            cluster_label = int(cluster_label_raw)
            clustering_settings = parse_clustering_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            distance_scope = "expanded" if clustering_settings["method"] == "hdbscan" else "compressed"
            distance_data = load_interface_distance_data(
                self.cache_dir,
                path,
                interface_payload,
                str(clustering_settings["distance"]),
                interface_filter_settings,
                distance_scope=distance_scope,
                cache_workers=self.cache_workers,
            )
            clustering_payload = load_or_compute_clustering_payload(
                self.cache_dir,
                path,
                interface_payload,
                clustering_settings,
                interface_filter_settings,
                cache_workers=self.cache_workers,
            )
            response_payload = {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                **compute_cluster_compare_payload(distance_data, clustering_payload, cluster_label),
            }
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected cluster compare error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json(response_payload)

    def _handle_structure_preview(self, query: dict[str, list[str]]) -> None:
        interface_filename = query.get("interface_file", [""])[0]
        row_key = query.get("row_key", [""])[0]
        uniprot_id = query.get("uniprot_id", [""])[0]
        fragment_key_name = query.get("fragment_key", [""])[0]
        partner = query.get("partner", ["__all__"])[0]
        align_to_row_key = query.get("align_to_row_key", [""])[0]
        if not interface_filename or (not row_key and not (uniprot_id and fragment_key_name)):
            self._log_structure_preview(
                "rejected request",
                reason="missing required identifiers",
                interface_file=interface_filename,
                row_key=row_key,
                uniprot_id=uniprot_id,
                fragment_key=fragment_key_name,
                partner=partner,
                align_to_row_key=align_to_row_key,
            )
            self._send_json(
                {"error": "interface_file and either row_key or uniprot_id+fragment_key are required"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        interface_path = safe_file_path(self.interface_dir, interface_filename)
        if interface_path is None:
            self._log_structure_preview(
                "rejected request",
                reason="missing interface file",
                interface_file=interface_filename,
                row_key=row_key,
                uniprot_id=uniprot_id,
                fragment_key=fragment_key_name,
                partner=partner,
            )
            self._send_json({"error": f"missing interface file {interface_filename}"}, status=HTTPStatus.NOT_FOUND)
            return
        try:
            interface_filter_settings = parse_interface_filter_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if not uniprot_id or not fragment_key_name:
            try:
                uniprot_id, fragment_key_name = parse_row_key(row_key)
            except ValueError:
                self._log_structure_preview(
                    "failed to parse row key",
                    reason=f"invalid row_key {row_key}",
                    interface_file=interface_filename,
                    row_key=row_key,
                    partner=partner,
                    align_to_row_key=align_to_row_key,
                )
                self._send_json({"error": f"invalid row_key {row_key}"}, status=HTTPStatus.BAD_REQUEST)
                return
        if not row_key:
            row_key = f"{uniprot_id}_{fragment_key_name}"
        interface_data = filter_interface_payload(load_interface_json(interface_path), interface_filter_settings)
        row_structure = collect_row_structure_payload(interface_data, row_key, partner)
        fragment_start, fragment_end = fragment_bounds(fragment_key_name)
        fragment_residue_ids = sorted(expand_fragment_key_to_residue_ids(fragment_key_name))
        fragment_ranges = fragment_key_to_ranges(fragment_key_name)
        try:
            model_path, prediction = ensure_alphafold_model(self.cache_dir, uniprot_id, fragment_start, fragment_end)
        except (FileNotFoundError, HTTPError, URLError, RuntimeError) as exc:
            self._log_structure_preview(
                "failed to load model",
                reason=str(exc),
                interface_file=interface_filename,
                row_key=row_key,
                uniprot_id=uniprot_id,
                fragment_key=fragment_key_name,
                partner=partner,
                align_to_row_key=align_to_row_key,
            )
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_GATEWAY)
            return
        cache_key = structure_cache_key(
            uniprot_id,
            fragment_key_name,
            partner,
            row_structure["interface_residue_ids"],
            row_structure["surface_residue_ids"],
        )
        image_path = self.cache_dir / "renders" / f"{cache_key}.png"
        image_url = None
        if image_path.exists():
            image_url = f"/api/rendered-image/{image_path.name}"
        response_model_path = model_path
        response_model_format = model_path.suffix.lstrip(".").lower()
        if response_model_format in {"cif", "mmcif"}:
            converted_model_path = self.cache_dir / "converted" / f"{uniprot_id}_{model_path.stem}.pdb"
            try:
                with cache_file_lock(converted_model_path):
                    if not model_file_is_usable(converted_model_path):
                        convert_model_to_pdb(model_path, converted_model_path)
                response_model_path = converted_model_path
                response_model_format = "pdb"
            except RuntimeError as exc:
                self._log_structure_preview(
                    "cif conversion fallback",
                    reason=str(exc),
                    interface_file=interface_filename,
                    row_key=row_key,
                    uniprot_id=uniprot_id,
                    fragment_key=fragment_key_name,
                    partner=partner,
                    align_to_row_key=align_to_row_key,
                )
        alignment_reference_row_key = ""
        alignment_method = ""
        alignment_error = ""
        if align_to_row_key and align_to_row_key != row_key:
            try:
                reference_uniprot_id, reference_fragment_key = parse_row_key(align_to_row_key)
                reference_start, reference_end = fragment_bounds(reference_fragment_key)
                reference_model_path, _ = ensure_alphafold_model(
                    self.cache_dir, reference_uniprot_id, reference_start, reference_end
                )
                aligned_cache_key = aligned_model_cache_key(
                    reference_accession=reference_uniprot_id,
                    reference_fragment_key=reference_fragment_key,
                    mobile_accession=uniprot_id,
                    mobile_fragment_key=fragment_key_name,
                )
                aligned_model_path = self.cache_dir / "aligned" / f"{aligned_cache_key}.pdb"
                with cache_file_lock(aligned_model_path):
                    if not model_file_is_usable(aligned_model_path):
                        render_aligned_model(
                            reference_model_path,
                            reference_fragment_key,
                            model_path,
                            fragment_key_name,
                            aligned_model_path,
                        )
                response_model_path = aligned_model_path
                response_model_format = "pdb"
                alignment_reference_row_key = align_to_row_key
                alignment_method = "cealign"
            except (ValueError, FileNotFoundError, RuntimeError, HTTPError, URLError) as exc:
                alignment_error = str(exc)
                self._log_structure_preview(
                    "alignment fallback",
                    reason=alignment_error,
                    interface_file=interface_filename,
                    row_key=row_key,
                    uniprot_id=uniprot_id,
                    fragment_key=fragment_key_name,
                    partner=partner,
                    align_to_row_key=align_to_row_key,
                )
        self._send_json(
            {
                "row_key": row_key,
                "uniprot_id": uniprot_id,
                "fragment_key": fragment_key_name,
                "fragment_start": fragment_start,
                "fragment_end": fragment_end,
                "fragment_residue_ids": fragment_residue_ids,
                "fragment_ranges": fragment_ranges,
                "partner": partner,
                "matched_partners": row_structure["matched_partners"],
                "interface_residue_ids": row_structure["interface_residue_ids"],
                "surface_residue_ids": row_structure["surface_residue_ids"],
                "partner_interface_residue_ids": row_structure["partner_interface_residue_ids"],
                "partner_surface_residue_ids": row_structure["partner_surface_residue_ids"],
                "partner_fragment_residue_ids": row_structure["partner_fragment_residue_ids"],
                "partner_fragment_ranges": row_structure["partner_fragment_ranges"],
                "residue_contacts": row_structure["residue_contacts"],
                "model_source": prediction.get("entryId", ""),
                "model_url": (
                    f"/api/aligned-model/{Path(response_model_path).name}"
                    if response_model_path.parent == self.cache_dir / "aligned"
                    else f"/api/converted-model/{Path(response_model_path).name}"
                    if response_model_path.parent == self.cache_dir / "converted"
                    else f"/api/alphafold-model/{uniprot_id}/{Path(response_model_path).name}"
                ),
                "model_format": response_model_format,
                "image_url": image_url,
                "alignment_reference_row_key": alignment_reference_row_key,
                "alignment_method": alignment_method,
                "alignment_error": alignment_error,
            }
        )

    def _handle_alphafold_model(self, relative_path: str) -> None:
        relative = Path(relative_path)
        if len(relative.parts) != 2:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        accession, filename = relative.parts
        model_path = self.cache_dir / "alphafold" / accession / Path(filename).name
        if not model_path.exists() or not model_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        mime_type, _ = mimetypes.guess_type(str(model_path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type or "chemical/x-pdb")
        self.send_header("Content-Length", str(model_path.stat().st_size))
        self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        self.end_headers()
        try:
            with model_path.open("rb") as handle:
                self.wfile.write(handle.read())
        except (BrokenPipeError, ConnectionResetError):
            return

    def _handle_aligned_model(self, filename: str) -> None:
        model_path = self.cache_dir / "aligned" / Path(filename).name
        if not model_path.exists() or not model_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        mime_type, _ = mimetypes.guess_type(str(model_path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type or "chemical/x-pdb")
        self.send_header("Content-Length", str(model_path.stat().st_size))
        self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        self.end_headers()
        try:
            with model_path.open("rb") as handle:
                self.wfile.write(handle.read())
        except (BrokenPipeError, ConnectionResetError):
            return

    def _handle_converted_model(self, filename: str) -> None:
        model_path = self.cache_dir / "converted" / Path(filename).name
        if not model_path.exists() or not model_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        mime_type, _ = mimetypes.guess_type(str(model_path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type or "chemical/x-pdb")
        self.send_header("Content-Length", str(model_path.stat().st_size))
        self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        self.end_headers()
        try:
            with model_path.open("rb") as handle:
                self.wfile.write(handle.read())
        except (BrokenPipeError, ConnectionResetError):
            return

    def _handle_rendered_image(self, image_name: str) -> None:
        image_path = self.cache_dir / "renders" / Path(image_name).name
        if not image_path.exists() or not image_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "image/png")
        self.send_header("Content-Length", str(image_path.stat().st_size))
        self.end_headers()
        try:
            with image_path.open("rb") as handle:
                self.wfile.write(handle.read())
        except (BrokenPipeError, ConnectionResetError):
            return

    def _serve_static(self, relative_path: str) -> None:
        path = (STATIC_DIR / relative_path).resolve()
        if not str(path).startswith(str(STATIC_DIR.resolve())) or not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        mime_type, _ = mimetypes.guess_type(str(path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type or "application/octet-stream")
        self.end_headers()
        try:
            with path.open("rb") as handle:
                self.wfile.write(handle.read())
        except (BrokenPipeError, ConnectionResetError):
            return

    def _send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return


def build_handler(
    interface_dir: Path,
    cache_dir: Path,
    cache_workers: int,
    pfam_option_stats: dict[str, dict[str, object]],
):
    class ConfiguredHandler(ViewerRequestHandler):
        pass

    ConfiguredHandler.interface_dir = interface_dir
    ConfiguredHandler.cache_dir = cache_dir
    ConfiguredHandler.cache_workers = max(1, int(cache_workers))
    ConfiguredHandler.pfam_option_stats = pfam_option_stats
    return ConfiguredHandler


def main() -> None:
    args = parse_args()
    cache_workers = max(1, int(args.cache_workers))
    pymol_status = validate_pymol_api()
    if not pymol_status.available:
        print(
            "WARNING: "
            f"{pymol_status.reason}. Alignment-based structure outputs will fall back to raw models.",
            file=sys.stderr,
            flush=True,
        )
    pfam_option_stats = load_cached_pfam_option_stats(
        args.cache_dir.resolve(),
        args.interface_dir.resolve(),
        cache_workers,
    )
    handler = build_handler(
        args.interface_dir.resolve(),
        args.cache_dir.resolve(),
        cache_workers,
        pfam_option_stats,
    )
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(
        f"Serving Domain Interface Explorer at http://{args.host}:{args.port} "
        f"(interface-dir={args.interface_dir}, cache-dir={args.cache_dir}, "
        f"pymol-api={'available' if pymol_status.available else 'unavailable'})"
    )
    start_background_pfam_metadata_refresh(args.cache_dir.resolve(), pfam_option_stats)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
