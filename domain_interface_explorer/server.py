from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import threading
from collections import OrderedDict
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
from domain_interface_explorer.serverlib.interface_store import InterfaceStore
from domain_interface_explorer.serverlib.interface_embedding import (
    build_interface_alignment_rows_from_metadata,
    compute_columns_chart_payload,
    compute_cluster_compare_payload,
    compute_embedding_payload,
    clustering_cache_path,
    embedding_cache_path,
    filter_interface_payload,
    hierarchy_status_payload,
    collect_interface_alignment_row_metadata,
    interface_filter_settings_key,
    load_interface_distance_data,
    load_interface_point_data,
    load_or_compute_clustering_payload,
    parse_clustering_settings,
    parse_embedding_settings,
    parse_interface_filter_settings,
)
from domain_interface_explorer.serverlib.representative import (
    REPRESENTATIVE_METHOD_BALANCED,
    REPRESENTATIVE_METHODS,
    compute_cluster_summary_payload,
    compute_representative_payload,
    interaction_row_key as representative_interaction_row_key,
)
from domain_interface_explorer.serverlib.stats_service import (
    interface_summary_from_payload,
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
from domain_interface_explorer.serverlib.timing import log_event, timed_step


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
    parser.add_argument("--hierarchy-dir", type=Path, default=None)
    parser.add_argument(
        "--workers",
        "--cache-workers",
        dest="cache_workers",
        type=positive_int,
        default=DEFAULT_CACHE_WORKERS,
    )
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


INTERFACE_VIEW_CACHE_LIMIT = 4
INTERFACE_VIEW_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
INTERFACE_VIEW_CACHE_LOCK = threading.Lock()
REPRESENTATIVE_CACHE_LIMIT = 32
REPRESENTATIVE_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
REPRESENTATIVE_CACHE_LOCK = threading.Lock()


def interface_view_cache_key(path: Path, filter_settings: dict[str, object]) -> str:
    stat = path.stat()
    return "|".join(
        (
            str(path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            interface_filter_settings_key(filter_settings),
        )
    )


def representative_cache_key(
    path: Path,
    filter_settings: dict[str, object],
    partner_filter: str,
    scope: str,
    representative_method: str,
    cluster_label: int | None,
    clustering_settings: dict[str, object] | None,
) -> str:
    stat = path.stat()
    return "|".join(
        (
            str(path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            interface_filter_settings_key(filter_settings),
            str(partner_filter),
            str(scope),
            str(representative_method),
            "" if cluster_label is None else str(cluster_label),
            json.dumps(clustering_settings or {}, sort_keys=True),
        )
    )


def load_cached_interface_view(
    path: Path,
    filter_settings: dict[str, object],
) -> tuple[str, dict[str, object], dict[str, dict[str, dict]], dict[str, object]]:
    raw_payload = load_interface_json(path)
    cache_key = interface_view_cache_key(path, filter_settings)
    with INTERFACE_VIEW_CACHE_LOCK:
        cached = INTERFACE_VIEW_CACHE.get(cache_key)
        if cached is not None:
            INTERFACE_VIEW_CACHE.move_to_end(cache_key)
            filtered_payload = cached["filtered_payload"]
            if isinstance(filtered_payload, dict):
                log_event(
                    "json",
                    "reuse filtered interface payload",
                    file=path.name,
                    rows=sum(len(rows) for rows in filtered_payload.values() if isinstance(rows, dict)),
                )
                return cache_key, raw_payload, filtered_payload, cached
    filtered_payload = filter_interface_payload(raw_payload, filter_settings)
    cache_entry: dict[str, object] = {
        "filtered_payload": filtered_payload,
    }
    with INTERFACE_VIEW_CACHE_LOCK:
        INTERFACE_VIEW_CACHE[cache_key] = cache_entry
        INTERFACE_VIEW_CACHE.move_to_end(cache_key)
        while len(INTERFACE_VIEW_CACHE) > INTERFACE_VIEW_CACHE_LIMIT:
            INTERFACE_VIEW_CACHE.popitem(last=False)
    return cache_key, raw_payload, filtered_payload, cache_entry


def cached_alignment_metadata(
    cache_key: str,
    cache_entry: dict[str, object],
    filtered_payload: dict[str, dict[str, dict]],
) -> tuple[list[dict[str, object]], int]:
    raw_rows = cache_entry.get("alignment_raw_rows")
    alignment_length = cache_entry.get("alignment_length")
    if isinstance(raw_rows, list) and isinstance(alignment_length, int):
        log_event(
            "json",
            "reuse alignment row metadata",
            raw_rows=len(raw_rows),
            alignment_length=alignment_length,
        )
        return raw_rows, alignment_length
    raw_rows, alignment_length = collect_interface_alignment_row_metadata(filtered_payload)
    with INTERFACE_VIEW_CACHE_LOCK:
        current = INTERFACE_VIEW_CACHE.get(cache_key)
        if current is not None:
            current["alignment_raw_rows"] = raw_rows
            current["alignment_length"] = alignment_length
    cache_entry["alignment_raw_rows"] = raw_rows
    cache_entry["alignment_length"] = alignment_length
    return raw_rows, alignment_length


def query_flag(query: dict[str, list[str]], name: str, default: bool = True) -> bool:
    raw_value = query.get(name, [None])[0]
    if raw_value is None:
        return default
    normalized = str(raw_value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def query_non_negative_int(
    query: dict[str, list[str]],
    name: str,
    default: int = 0,
) -> int:
    raw_value = query.get(name, [str(default)])[0]
    if raw_value is None or str(raw_value).strip() == "":
        return default
    parsed = int(str(raw_value).strip())
    if parsed < 0:
        raise ValueError(f"{name} must be non-negative")
    return parsed


def query_positive_int_or_none(query: dict[str, list[str]], name: str) -> int | None:
    raw_value = query.get(name, [""])[0]
    if raw_value is None or str(raw_value).strip() == "":
        return None
    parsed = int(str(raw_value).strip())
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def compact_interface_payload_for_client(
    interface_payload: dict[str, dict[str, dict]],
    raw_rows: list[dict[str, object]] | None = None,
    row_offset: int = 0,
    row_limit: int | None = None,
) -> dict[str, dict[str, dict[str, object]]]:
    normalized_offset = max(0, int(row_offset or 0))
    normalized_limit = None if row_limit is None else max(0, int(row_limit))
    with timed_step(
        "json",
        "compact interface overlay payload",
        partner_domains=len(interface_payload),
        row_offset=normalized_offset,
        row_limit=normalized_limit if normalized_limit is not None else "all",
    ) as timer:
        compact_payload: dict[str, dict[str, dict[str, object]]] = {}
        row_count = 0
        if raw_rows is not None:
            selected_rows = (
                raw_rows[normalized_offset:]
                if normalized_limit is None
                else raw_rows[normalized_offset:normalized_offset + normalized_limit]
            )
            for raw_row in selected_rows:
                partner_domain = str(raw_row.get("partner_domain") or "")
                row_key = str(raw_row.get("interface_row_key") or "")
                row_payload = interface_payload.get(partner_domain, {}).get(row_key)
                if not isinstance(row_payload, dict):
                    continue
                compact_payload.setdefault(partner_domain, {})[row_key] = {
                    "interface_msa_columns_a": row_payload.get("interface_msa_columns_a", []),
                    "surface_msa_columns_a": row_payload.get("surface_msa_columns_a", []),
                }
                row_count += 1
            timer.set(rows=row_count, partner_domains=len(compact_payload))
            return compact_payload
        for partner_domain in sorted(interface_payload):
            rows = interface_payload.get(partner_domain)
            if not isinstance(rows, dict):
                continue
            compact_rows: dict[str, dict[str, object]] = {}
            for row_key, row_payload in rows.items():
                if not isinstance(row_payload, dict):
                    continue
                compact_rows[str(row_key)] = {
                    "interface_msa_columns_a": row_payload.get("interface_msa_columns_a", []),
                    "surface_msa_columns_a": row_payload.get("surface_msa_columns_a", []),
                }
                row_count += 1
            if compact_rows:
                compact_payload[str(partner_domain)] = compact_rows
        timer.set(rows=row_count, partner_domains=len(compact_payload))
        return compact_payload


class ViewerRequestHandler(BaseHTTPRequestHandler):
    interface_dir: Path
    cache_dir: Path
    hierarchy_dir: Path | None
    interface_store: InterfaceStore | None
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
        if parsed.path == "/api/hierarchy-status":
            self._handle_hierarchy_status(parse_qs(parsed.query))
            return
        if parsed.path == "/api/cluster-compare":
            self._handle_cluster_compare(parse_qs(parsed.query))
            return
        if parsed.path == "/api/representative":
            self._handle_representative(parse_qs(parsed.query))
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
        str,
        Path,
        dict[str, dict[str, dict]],
        dict[str, dict[str, dict]],
        dict[str, object],
        dict[str, object],
    ] | None:
        resolved = self._resolve_interface_file_and_filter(filename, query)
        if resolved is None:
            return None
        path, interface_filter_settings = resolved
        cache_key, interface_payload, filtered_payload, cache_entry = load_cached_interface_view(
            path,
            interface_filter_settings,
        )
        return cache_key, path, interface_payload, filtered_payload, interface_filter_settings, cache_entry

    def _resolve_interface_file_and_filter(
        self,
        filename: str,
        query: dict[str, list[str]],
    ) -> tuple[Path, dict[str, object]] | None:
        path = safe_file_path(self.interface_dir, filename)
        if path is None:
            self._send_json({"error": f"missing interface file {filename}"}, status=HTTPStatus.NOT_FOUND)
            return None
        try:
            interface_filter_settings = parse_interface_filter_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return None
        return path, interface_filter_settings

    def _handle_msa(self, query: dict[str, list[str]]) -> None:
        self._send_json(
            {"error": "MSA files are no longer served by the viewer; use /api/interface instead."},
            status=HTTPStatus.GONE,
        )

    def _handle_interface(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
        try:
            row_offset = query_non_negative_int(query, "row_offset", 0)
            row_limit = query_positive_int_or_none(query, "row_limit")
            data_offset = query_non_negative_int(query, "data_offset", row_offset)
            data_limit = query_positive_int_or_none(query, "data_limit")
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if data_limit is None:
            data_limit = row_limit
        include_rows = query_flag(query, "include_rows", True)
        include_data = query_flag(query, "include_data", True)
        include_clean_column_identity = query_flag(
            query,
            "include_clean_column_identity",
            query_flag(query, "include_clean", True),
        )
        if self.interface_store is not None:
            try:
                response_payload = self.interface_store.get_interface_page(
                    path,
                    interface_filter_settings,
                    row_offset=row_offset,
                    row_limit=row_limit,
                    include_rows=include_rows,
                    include_data=include_data,
                    data_offset=data_offset,
                    data_limit=data_limit,
                    include_clean_column_identity=include_clean_column_identity,
                )
                self._send_json(response_payload)
                return
            except Exception as exc:
                log_event("store", "interface store fallback", file=path.name, error=exc)
        cache_key, raw_payload, filtered_payload, cache_entry = load_cached_interface_view(
            path,
            interface_filter_settings,
        )
        resolved = (cache_key, path, raw_payload, filtered_payload, interface_filter_settings, cache_entry)
        if resolved is None:
            return
        cache_key, path, raw_payload, filtered_payload, interface_filter_settings, cache_entry = resolved
        with timed_step("json", "build interface endpoint payload", file=path.name) as timer:
            pfam_id = interface_file_pfam_id(path)
            clean_column_identity = (
                load_or_compute_clean_column_identity(self.cache_dir, path, raw_payload)
                if include_clean_column_identity
                else None
            )
            raw_rows, alignment_length = cached_alignment_metadata(
                cache_key,
                cache_entry,
                filtered_payload,
            )
            rows, alignment_length, total_rows = build_interface_alignment_rows_from_metadata(
                raw_rows,
                alignment_length,
                row_offset=row_offset if include_rows else 0,
                row_limit=row_limit if include_rows else 0,
                include_total=True,
            )
            compact_interface_payload = (
                compact_interface_payload_for_client(
                    filtered_payload,
                    raw_rows=raw_rows,
                    row_offset=data_offset,
                    row_limit=data_limit,
                )
                if include_data
                else None
            )
            returned_row_count = len(rows)
            rows_complete = row_offset + returned_row_count >= total_rows
            data_loaded = (
                sum(len(rows_by_partner) for rows_by_partner in compact_interface_payload.values())
                if compact_interface_payload is not None
                else 0
            )
            data_complete = data_offset + data_loaded >= total_rows
            interface_partner_counts = {
                str(partner_domain): len(rows_by_partner)
                for partner_domain, rows_by_partner in sorted(filtered_payload.items())
                if isinstance(rows_by_partner, dict)
            }
            interface_summary = interface_summary_from_payload(filtered_payload)
            response_payload = {
                "file": path.name,
                "pfam_id": pfam_id,
                "filter_settings": interface_filter_settings,
                "alignment_length": alignment_length,
                "row_count": total_rows,
                "interface_partner_domains": list(interface_partner_counts),
                "interface_partner_counts": interface_partner_counts,
                "interface_summary": interface_summary,
                "row_offset": row_offset,
                "row_limit": row_limit,
                "rows_loaded": returned_row_count,
                "rows_complete": rows_complete,
                "rows": rows,
            }
            if clean_column_identity is not None:
                response_payload["clean_column_identity"] = clean_column_identity
            if compact_interface_payload is not None:
                response_payload["data"] = compact_interface_payload
                response_payload["data_row_count"] = total_rows
                response_payload["data_offset"] = data_offset
                response_payload["data_limit"] = data_limit
                response_payload["data_loaded"] = data_loaded
                response_payload["data_complete"] = data_complete
            timer.set(
                rows=returned_row_count,
                total_rows=total_rows,
                alignment_length=alignment_length,
                clean_columns=len(clean_column_identity or []),
                overlay_rows=data_loaded,
                partner_domains=len(compact_interface_payload or {}),
                include_data=include_data,
                include_clean_column_identity=include_clean_column_identity,
                row_offset=row_offset,
                row_limit=row_limit if row_limit is not None else "all",
                data_offset=data_offset,
                data_limit=data_limit if data_limit is not None else "all",
            )
        self._send_json(response_payload)

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
        resolved = self._resolve_interface_file_and_filter(filename, query)
        if resolved is None:
            return
        path, interface_filter_settings = resolved
        try:
            settings = parse_embedding_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        cache_path = embedding_cache_path(self.cache_dir, path, settings, interface_filter_settings)
        if cache_path.exists():
            with timed_step(
                "points",
                "load cached point layout",
                file=cache_path.name,
                method=settings["method"],
                distance=settings["distance"],
            ):
                with cache_path.open("r", encoding="utf-8") as handle:
                    self._send_json(json.load(handle))
            return
        try:
            interface_payload = (
                self.interface_store.get_columns_payload(path, interface_filter_settings)
                if self.interface_store is not None
                else None
            )
        except Exception as exc:
            log_event("store", "embedding columns payload fallback", file=path.name, error=exc)
            interface_payload = None
        if interface_payload is None:
            _cache_key, _raw_payload, interface_payload, _cache_entry = load_cached_interface_view(
                path,
                interface_filter_settings,
            )
        try:
            point_data = load_interface_point_data(
                self.cache_dir,
                path,
                interface_payload,
                str(settings["distance"]),
                interface_filter_settings,
                cache_workers=self.cache_workers,
            )
            embedding_payload = compute_embedding_payload(point_data, settings, worker_count=self.cache_workers)
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        response_payload = {
            "file": path.name,
            "pfam_id": interface_file_pfam_id(path),
            "filter_settings": interface_filter_settings,
            **embedding_payload,
        }
        with timed_step(
            "points",
            "write point layout cache",
            file=cache_path.name,
            method=settings["method"],
            distance=settings["distance"],
            points=len(response_payload.get("points", [])),
        ):
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with cache_path.open("w", encoding="utf-8") as handle:
                json.dump(response_payload, handle)
        self._send_json(response_payload)

    def _load_interface_columns_payload(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
        *,
        fallback_context: str,
    ) -> dict[str, dict[str, dict[str, object]]]:
        try:
            interface_payload = (
                self.interface_store.get_columns_payload(path, interface_filter_settings)
                if self.interface_store is not None
                else None
            )
        except Exception as exc:
            log_event("store", fallback_context, file=path.name, error=exc)
            interface_payload = None
        if interface_payload is None:
            _cache_key, _raw_payload, interface_payload, _cache_entry = load_cached_interface_view(
                path,
                interface_filter_settings,
            )
        return interface_payload

    def _filtered_alignment_length(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
        interface_payload: dict[str, dict[str, dict]],
    ) -> int | None:
        if self.interface_store is not None:
            try:
                return self.interface_store.get_filtered_alignment_length(path, interface_filter_settings)
            except Exception as exc:
                log_event("store", "filtered alignment length fallback", file=path.name, error=exc)
        try:
            _raw_rows, alignment_length = collect_interface_alignment_row_metadata(interface_payload)
            return int(alignment_length)
        except Exception as exc:
            log_event("columns", "alignment length fallback failed", file=path.name, error=exc)
            return None

    def _attach_columns_chart_payload(
        self,
        response_payload: dict[str, object],
        path: Path,
        interface_filter_settings: dict[str, object],
    ) -> dict[str, object]:
        if response_payload.get("columns_chart") is not None:
            return response_payload
        interface_payload = self._load_interface_columns_payload(
            path,
            interface_filter_settings,
            fallback_context="clustering columns chart payload fallback",
        )
        alignment_length = self._filtered_alignment_length(
            path,
            interface_filter_settings,
            interface_payload,
        )
        return {
            **response_payload,
            "columns_chart": compute_columns_chart_payload(
                interface_payload,
                response_payload,
                alignment_length=alignment_length,
            ),
        }

    def _load_clustering_payload(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
        clustering_settings: dict[str, object],
    ) -> dict[str, object]:
        cache_path = clustering_cache_path(
            self.cache_dir,
            path,
            clustering_settings,
            interface_filter_settings,
        )
        if cache_path.exists():
            with timed_step(
                "clustering",
                "load cached clustering response",
                file=cache_path.name,
                method=clustering_settings["method"],
                distance=clustering_settings["distance"],
            ):
                with cache_path.open("r", encoding="utf-8") as handle:
                    return json.load(handle)
        interface_payload = self._load_interface_columns_payload(
            path,
            interface_filter_settings,
            fallback_context="clustering columns payload fallback",
        )
        return load_or_compute_clustering_payload(
            self.cache_dir,
            path,
            interface_payload,
            clustering_settings,
            interface_filter_settings,
            cache_workers=self.cache_workers,
            hierarchy_dir=self.hierarchy_dir,
        )

    def _handle_clustering(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved = self._resolve_interface_file_and_filter(filename, query)
        if resolved is None:
            return
        path, interface_filter_settings = resolved
        try:
            clustering_settings = parse_clustering_settings(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            response_payload = self._load_clustering_payload(
                path,
                interface_filter_settings,
                clustering_settings,
            )
            response_payload = self._attach_columns_chart_payload(
                response_payload,
                path,
                interface_filter_settings,
            )
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected clustering error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json(response_payload)

    def _handle_hierarchy_status(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
        try:
            interface_payload = (
                self.interface_store.get_columns_payload(path, interface_filter_settings)
                if self.interface_store is not None
                else None
            )
        except Exception as exc:
            log_event("store", "hierarchy-status columns payload fallback", file=path.name, error=exc)
            interface_payload = None
        if interface_payload is None:
            resolved = self._resolve_interface_request(filename, query)
            if resolved is None:
                return
            _cache_key, path, _raw_payload, interface_payload, interface_filter_settings, _cache_entry = resolved
        try:
            clustering_settings = parse_clustering_settings(query)
            response_payload = hierarchy_status_payload(
                self.cache_dir,
                path,
                interface_payload,
                clustering_settings,
                interface_filter_settings,
                hierarchy_dir=self.hierarchy_dir,
            )
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected hierarchy status error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json(
            {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                **response_payload,
            }
        )

    def _handle_cluster_compare(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        cluster_label_raw = query.get("cluster_label", [""])[0].strip()
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
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
            interface_payload = (
                self.interface_store.get_columns_payload(path, interface_filter_settings)
                if self.interface_store is not None
                else None
            )
        except Exception as exc:
            log_event("store", "cluster-compare columns payload fallback", file=path.name, error=exc)
            interface_payload = None
        if interface_payload is None:
            _cache_key, _raw_payload, interface_payload, _cache_entry = load_cached_interface_view(
                path,
                interface_filter_settings,
            )
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
                hierarchy_dir=self.hierarchy_dir,
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

    def _load_representative_candidates(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
    ) -> tuple[list[dict[str, object]], int]:
        if self.interface_store is not None:
            try:
                return self.interface_store.get_representative_candidates(
                    path,
                    interface_filter_settings,
                )
            except Exception as exc:
                log_event("store", "representative candidates fallback", file=path.name, error=exc)
        cache_key, _raw_payload, filtered_payload, cache_entry = load_cached_interface_view(
            path,
            interface_filter_settings,
        )
        raw_rows, alignment_length = cached_alignment_metadata(
            cache_key,
            cache_entry,
            filtered_payload,
        )
        candidates: list[dict[str, object]] = []
        with timed_step(
            "json",
            "load representative candidates",
            file=path.name,
            rows=len(raw_rows),
        ) as timer:
            for raw_row in raw_rows:
                partner_domain = str(raw_row.get("partner_domain") or "")
                row_key = str(raw_row.get("interface_row_key") or "")
                row_payload = filtered_payload.get(partner_domain, {}).get(row_key, {})
                candidates.append(
                    {
                        **raw_row,
                        "interface_msa_columns_a": (
                            row_payload.get("interface_msa_columns_a", [])
                            if isinstance(row_payload, dict)
                            else []
                        ),
                    }
                )
            timer.set(alignment_length=alignment_length)
        return candidates, alignment_length

    def _cluster_member_interaction_keys(
        self,
        clustering_payload: dict[str, object],
        cluster_label: int,
    ) -> set[str]:
        member_keys: set[str] = set()
        points = clustering_payload.get("points")
        if not isinstance(points, list):
            return member_keys
        for point in points:
            if not isinstance(point, dict):
                continue
            try:
                point_cluster_label = int(point.get("cluster_label", -1))
            except (TypeError, ValueError):
                continue
            if point_cluster_label != cluster_label:
                continue
            member_keys.add(
                representative_interaction_row_key(
                    point.get("row_key"),
                    point.get("partner_domain"),
                )
            )
        return member_keys

    def _handle_representative(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
        scope = query.get("representative_scope", query.get("scope", ["overall"]))[0].strip().lower()
        representative_method = query.get(
            "representative_method",
            [""],
        )[0].strip().lower() or REPRESENTATIVE_METHOD_BALANCED
        partner_filter = query.get("partner", ["__all__"])[0].strip() or "__all__"
        if scope not in {"overall", "cluster"}:
            self._send_json(
                {"error": "representative_scope must be either 'overall' or 'cluster'"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if representative_method not in REPRESENTATIVE_METHODS:
            self._send_json(
                {"error": "representative_method must be either 'balanced' or 'residue'"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        cluster_label: int | None = None
        if scope == "cluster":
            cluster_label_raw = query.get("cluster_label", [""])[0].strip()
            if cluster_label_raw == "":
                self._send_json({"error": "cluster_label is required"}, status=HTTPStatus.BAD_REQUEST)
                return
            try:
                cluster_label = int(cluster_label_raw)
                clustering_settings = parse_clustering_settings(query)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
        else:
            clustering_settings = None
        cache_key = representative_cache_key(
            path,
            interface_filter_settings,
            partner_filter,
            scope,
            representative_method,
            cluster_label,
            clustering_settings,
        )
        with REPRESENTATIVE_CACHE_LOCK:
            cached_response = REPRESENTATIVE_CACHE.get(cache_key)
            if cached_response is not None:
                REPRESENTATIVE_CACHE.move_to_end(cache_key)
                log_event(
                    "representative",
                    "reuse cached representative",
                    file=path.name,
                    representative_scope=scope,
                    representative_method=representative_method,
                    partner=partner_filter,
                    cluster_label=cluster_label if cluster_label is not None else "",
                    row_key=cached_response.get("representative_row_key"),
                )
                self._send_json(cached_response)
                return
        try:
            candidates, alignment_length = self._load_representative_candidates(
                path,
                interface_filter_settings,
            )
            if partner_filter != "__all__":
                candidates = [
                    candidate
                    for candidate in candidates
                    if str(candidate.get("partner_domain") or "") == partner_filter
                ]
            cluster_summaries: list[dict[str, object]] | None = None
            if scope == "cluster" and clustering_settings is not None and cluster_label is not None:
                clustering_payload = self._load_clustering_payload(
                    path,
                    interface_filter_settings,
                    clustering_settings,
                )
                cluster_summaries = compute_cluster_summary_payload(
                    candidates,
                    clustering_payload,
                )
                member_keys = self._cluster_member_interaction_keys(
                    clustering_payload,
                    cluster_label,
                )
                candidates = [
                    candidate
                    for candidate in candidates
                    if representative_interaction_row_key(
                        candidate.get("interface_row_key"),
                        candidate.get("partner_domain"),
                    )
                    in member_keys
                ]
            response_payload = {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                "partner_filter": partner_filter,
                **compute_representative_payload(
                    candidates,
                    alignment_length,
                    scope=scope,
                    cluster_label=cluster_label,
                    method=representative_method,
                ),
            }
            if cluster_summaries is not None:
                response_payload["cluster_summaries"] = cluster_summaries
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected representative error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        with REPRESENTATIVE_CACHE_LOCK:
            REPRESENTATIVE_CACHE[cache_key] = response_payload
            REPRESENTATIVE_CACHE.move_to_end(cache_key)
            while len(REPRESENTATIVE_CACHE) > REPRESENTATIVE_CACHE_LIMIT:
                REPRESENTATIVE_CACHE.popitem(last=False)
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
        try:
            interface_data = (
                self.interface_store.get_structure_interface_payload(
                    interface_path,
                    interface_filter_settings,
                    row_key,
                    partner,
                )
                if self.interface_store is not None
                else None
            )
        except Exception as exc:
            log_event("store", "structure row payload fallback", file=interface_path.name, error=exc)
            interface_data = None
        if interface_data is None:
            _cache_key, _raw_payload, interface_data, _cache_entry = load_cached_interface_view(
                interface_path,
                interface_filter_settings,
            )
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
        endpoint = self.path.split("?", maxsplit=1)[0]
        with timed_step(
            "http",
            "serialize json response",
            endpoint=endpoint,
            status=int(status),
        ) as timer:
            body = json.dumps(payload).encode("utf-8")
            timer.set(bytes=len(body))
        try:
            with timed_step(
                "http",
                "send json response",
                endpoint=endpoint,
                status=int(status),
                bytes=len(body),
            ):
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
    hierarchy_dir: Path | None,
    interface_store: InterfaceStore | None,
    cache_workers: int,
    pfam_option_stats: dict[str, dict[str, object]],
):
    class ConfiguredHandler(ViewerRequestHandler):
        pass

    ConfiguredHandler.interface_dir = interface_dir
    ConfiguredHandler.cache_dir = cache_dir
    ConfiguredHandler.hierarchy_dir = hierarchy_dir
    ConfiguredHandler.interface_store = interface_store
    ConfiguredHandler.cache_workers = max(1, int(cache_workers))
    ConfiguredHandler.pfam_option_stats = pfam_option_stats
    return ConfiguredHandler


def main() -> None:
    args = parse_args()
    cache_workers = max(1, int(args.cache_workers))
    interface_dir = args.interface_dir.resolve()
    cache_dir = args.cache_dir.resolve()
    hierarchy_dir = args.hierarchy_dir.resolve() if args.hierarchy_dir is not None else None
    interface_store = InterfaceStore(cache_dir / "interface_store.sqlite", interface_dir)
    pymol_status = validate_pymol_api()
    if not pymol_status.available:
        print(
            "WARNING: "
            f"{pymol_status.reason}. Alignment-based structure outputs will fall back to raw models.",
            file=sys.stderr,
            flush=True,
        )
    pfam_option_stats = load_cached_pfam_option_stats(
        cache_dir,
        interface_dir,
        cache_workers,
    )
    handler = build_handler(
        interface_dir,
        cache_dir,
        hierarchy_dir,
        interface_store,
        cache_workers,
        pfam_option_stats,
    )
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(
        f"Serving Domain Interface Explorer at http://{args.host}:{args.port} "
        f"(interface-dir={args.interface_dir}, cache-dir={args.cache_dir}, "
        f"interface-store={interface_store.db_path}, "
        f"hierarchy-dir={hierarchy_dir or 'none'}, "
        f"workers={cache_workers}, "
        f"pymol-api={'available' if pymol_status.available else 'unavailable'})"
    )
    interface_store.start_background_sync()
    start_background_pfam_metadata_refresh(cache_dir, pfam_option_stats)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
