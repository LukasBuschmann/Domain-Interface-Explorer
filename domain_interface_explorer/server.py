from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import sys
import threading
from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

from domain_interface_explorer.serverlib.config import (
    DEFAULT_CACHE_DIR,
    DEFAULT_CACHE_WORKERS,
    DEFAULT_HMMER_BIN_DIR,
    DEFAULT_HOST,
    DEFAULT_INTERFACE_DIR,
    DEFAULT_PFAM_HMM_PATH,
    DEFAULT_SEQUENCE_BY_DOMAIN_DIR,
    STATIC_DIR,
)
from domain_interface_explorer.serverlib.interface_files import (
    directory_interface_json_paths,
    interface_file_pfam_id,
    is_interface_json_path,
    load_interface_json,
)
from domain_interface_explorer.serverlib.hmmer_service import compute_domain_hmm_bit_scores
from domain_interface_explorer.serverlib.interface_store import InterfaceStore
from domain_interface_explorer.serverlib.interface_embedding import (
    build_interface_alignment_rows_from_metadata,
    alignment_fragment_key_for_row_payload,
    compute_columns_chart_payload,
    compute_cluster_compare_payload_from_clustering,
    compute_embedding_payload,
    clustering_cache_path,
    embedding_cache_path,
    filter_interface_payload,
    hierarchy_status_payload,
    collect_interface_alignment_row_metadata,
    domain_length_from_row_payload,
    domain_size_filter_is_active,
    domain_size_filter_key,
    entry_matches_domain_size_filter,
    interface_residue_count,
    interface_filter_settings_key,
    load_interface_point_data,
    load_or_compute_clustering_payload,
    load_or_compute_dendrogram_payload,
    mask_alignment_to_fragment_ranges,
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
    sample_representative_candidates,
)
from domain_interface_explorer.serverlib.stats_service import (
    compute_and_cache_pfam_option_stats,
    interface_summary_from_payload,
    load_available_pfam_option_stats,
    load_cached_pfam_metadata,
    load_or_fetch_pfam_info,
    load_or_compute_clean_column_identity,
    pfam_row_coverage_percent_from_payload,
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
    parse_interface_row_key,
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
        "--default-dataset",
        "--preferred-dataset",
        dest="default_dataset",
        default="",
        help="Dataset key/name to use when no dataset is specified in the request.",
    )
    parser.add_argument(
        "--pfam-hmm",
        "--pfam-hmm-path",
        dest="pfam_hmm_path",
        type=Path,
        default=DEFAULT_PFAM_HMM_PATH,
    )
    parser.add_argument(
        "--sequence-dir",
        "--sequence-by-domain-dir",
        dest="sequence_dir",
        type=Path,
        default=DEFAULT_SEQUENCE_BY_DOMAIN_DIR,
    )
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


def directory_has_interface_json(directory: Path) -> bool:
    if not directory.exists() or not directory.is_dir():
        return False
    try:
        for path in directory.iterdir():
            if path.is_file() and is_interface_json_path(path):
                return True
    except OSError:
        return False
    return False


def safe_file_path(directory: Path, filename: str) -> Path | None:
    candidate = directory / Path(filename).name
    if candidate.parent != directory:
        return None
    if not candidate.exists() or not candidate.is_file() or not is_interface_json_path(candidate):
        return None
    return candidate


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    label: str
    interface_dir: Path
    hierarchy_dir: Path | None


@dataclass
class DatasetRuntime:
    key: str
    label: str
    interface_dir: Path
    hierarchy_dir: Path | None
    interface_store: InterfaceStore | None
    pfam_option_stats: dict[str, dict[str, object]] = field(default_factory=dict)
    pfam_option_stats_status: dict[str, object] = field(
        default_factory=lambda: {
            "state": "loading",
            "cached": False,
            "refreshing": False,
            "message": "Loading PFAM selector stats cache",
        }
    )
    pfam_option_stats_loaded: bool = False
    pfam_option_stats_current: bool = False
    pfam_option_stats_signature: str = ""
    metadata_refresh_started: bool = False
    stats_refresh_started: bool = False
    store_sync_started: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


def hierarchy_dir_for_dataset(
    hierarchy_root: Path | None,
    dataset_name: str,
    *,
    interface_dir: Path | None = None,
    direct_dataset: bool = False,
) -> Path | None:
    if hierarchy_root is None:
        return None
    resolved_hierarchy_root = hierarchy_root.resolve()
    resolved_interface_dir = interface_dir.resolve() if interface_dir is not None else None
    candidates = [
        hierarchy_root / f"h_{dataset_name}",
        hierarchy_root / dataset_name,
        hierarchy_root / f"{dataset_name}_hierarchy",
    ]
    if dataset_name.startswith("h_"):
        candidates.append(hierarchy_root / dataset_name.removeprefix("h_"))
    for candidate in candidates:
        if not candidate.exists() or not candidate.is_dir():
            continue
        resolved_candidate = candidate.resolve()
        if resolved_interface_dir is not None and resolved_candidate == resolved_interface_dir:
            continue
        return resolved_candidate
    if (
        direct_dataset
        and hierarchy_root.exists()
        and hierarchy_root.is_dir()
        and resolved_hierarchy_root != resolved_interface_dir
    ):
        return resolved_hierarchy_root
    return None


def unique_dataset_key(name: str, existing_keys: set[str]) -> str:
    base = str(name or "dataset").strip() or "dataset"
    key = base
    suffix = 2
    while key in existing_keys:
        key = f"{base}_{suffix}"
        suffix += 1
    existing_keys.add(key)
    return key


def discover_dataset_configs(interface_root: Path, hierarchy_root: Path | None) -> list[DatasetConfig]:
    interface_root = interface_root.resolve()
    hierarchy_root = hierarchy_root.resolve() if hierarchy_root is not None else None
    datasets: list[DatasetConfig] = []
    keys: set[str] = set()

    if directory_has_interface_json(interface_root):
        key = unique_dataset_key(interface_root.name or "default", keys)
        datasets.append(
            DatasetConfig(
                key=key,
                label=key,
                interface_dir=interface_root,
                hierarchy_dir=hierarchy_dir_for_dataset(
                    hierarchy_root,
                    interface_root.name,
                    interface_dir=interface_root,
                    direct_dataset=True,
                ),
            )
        )

    try:
        candidate_dirs = sorted(
            (path for path in interface_root.iterdir() if path.is_dir()),
            key=lambda item: item.name,
        )
    except OSError:
        candidate_dirs = []

    for candidate_dir in candidate_dirs:
        if not directory_has_interface_json(candidate_dir):
            continue
        key = unique_dataset_key(candidate_dir.name, keys)
        datasets.append(
            DatasetConfig(
                key=key,
                label=candidate_dir.name,
                interface_dir=candidate_dir.resolve(),
                hierarchy_dir=hierarchy_dir_for_dataset(
                    hierarchy_root,
                    candidate_dir.name,
                    interface_dir=candidate_dir,
                ),
            )
        )

    if not datasets:
        key = unique_dataset_key(interface_root.name or "default", keys)
        datasets.append(
            DatasetConfig(
                key=key,
                label=key,
                interface_dir=interface_root,
                hierarchy_dir=hierarchy_dir_for_dataset(
                    hierarchy_root,
                    interface_root.name,
                    interface_dir=interface_root,
                    direct_dataset=True,
                ),
            )
        )
    return datasets


def default_dataset_key_from_configs(
    dataset_configs: list[DatasetConfig],
    requested_dataset: str | None,
) -> str:
    if not dataset_configs:
        raise SystemExit("No datasets were discovered.")
    requested = str(requested_dataset or "").strip()
    if not requested:
        return dataset_configs[0].key
    for dataset in dataset_configs:
        if requested in {dataset.key, dataset.label, dataset.interface_dir.name}:
            return dataset.key
    available = ", ".join(dataset.key for dataset in dataset_configs)
    raise SystemExit(
        f"--default-dataset {requested!r} was not found. "
        f"Available datasets: {available or 'none'}"
    )


def dataset_payload(runtime: DatasetRuntime) -> dict[str, object]:
    return {
        "key": runtime.key,
        "label": runtime.label,
        "interface_dir": str(runtime.interface_dir),
        "hierarchy_dir": str(runtime.hierarchy_dir) if runtime.hierarchy_dir is not None else None,
        "has_hierarchy": runtime.hierarchy_dir is not None,
    }


def interface_store_db_path(
    cache_dir: Path,
    dataset: DatasetConfig,
    *,
    use_legacy_path: bool = False,
) -> Path:
    if use_legacy_path:
        return cache_dir / "interface_store.sqlite"
    safe_key = "".join(
        char if char.isalnum() or char in ("-", "_", ".") else "_"
        for char in dataset.key
    ).strip("._") or "dataset"
    digest = hashlib.sha1(
        f"{dataset.key}|{dataset.interface_dir.resolve()}".encode("utf-8")
    ).hexdigest()[:16]
    return cache_dir / "interface_store" / f"{safe_key}-{digest}.sqlite"


def optional_positive_int_query(
    query: dict[str, list[str]],
    *names: str,
) -> int | None:
    for name in names:
        raw_values = query.get(name, [])
        if not raw_values:
            continue
        raw_value = str(raw_values[0] or "").strip()
        if raw_value == "":
            return None
        try:
            value = int(raw_value)
        except ValueError as exc:
            raise ValueError(f"{name} must be a positive integer") from exc
        if value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        return value
    return None


def representative_domain_size_filter_from_query(
    query: dict[str, list[str]],
) -> tuple[int | None, int | None]:
    min_size = optional_positive_int_query(
        query,
        "representative_domain_size_min",
        "shown_domain_size_min",
    )
    max_size = optional_positive_int_query(
        query,
        "representative_domain_size_max",
        "shown_domain_size_max",
    )
    if min_size is not None and max_size is not None and min_size > max_size:
        min_size, max_size = max_size, min_size
    return min_size, max_size


def candidate_matches_domain_size_filter(
    candidate: dict[str, object],
    domain_size_filter: tuple[int | None, int | None] | None,
) -> bool:
    if not domain_size_filter_is_active(domain_size_filter):
        return True
    return entry_matches_domain_size_filter(
        {
            "domain_length": domain_length_from_row_payload(
                candidate.get("interface_row_key") or candidate.get("row_key"),
                candidate,
            ),
        },
        domain_size_filter,
    )


INTERFACE_VIEW_CACHE_LIMIT = 4
INTERFACE_VIEW_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
INTERFACE_VIEW_CACHE_LOCK = threading.Lock()
REPRESENTATIVE_CACHE_LIMIT = 32
REPRESENTATIVE_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
REPRESENTATIVE_CACHE_LOCK = threading.Lock()
REPRESENTATIVE_CANDIDATES_CACHE_LIMIT = 4
REPRESENTATIVE_CANDIDATES_CACHE: OrderedDict[str, tuple[list[dict[str, object]], int]] = OrderedDict()
REPRESENTATIVE_CANDIDATES_CACHE_LOCK = threading.Lock()
COLUMNS_PAYLOAD_CACHE_LIMIT = 2
COLUMNS_PAYLOAD_CACHE: OrderedDict[str, dict[str, dict[str, dict[str, object]]]] = OrderedDict()
COLUMNS_PAYLOAD_CACHE_LOCK = threading.Lock()
COLUMNS_PAYLOAD_IN_FLIGHT: dict[str, Future[dict[str, dict[str, dict[str, object]]]]] = {}
INTERFACE_SUMMARY_CACHE_LIMIT = 32
INTERFACE_SUMMARY_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
INTERFACE_SUMMARY_CACHE_LOCK = threading.Lock()
INTERFACE_SUMMARY_IN_FLIGHT: dict[str, Future[dict[str, object]]] = {}
CLUSTER_OVERVIEW_CACHE_LIMIT = 16
CLUSTER_OVERVIEW_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
CLUSTER_OVERVIEW_CACHE_LOCK = threading.Lock()


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
    representative_domain_size_filter: tuple[int | None, int | None] | None = None,
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
            domain_size_filter_key(representative_domain_size_filter),
        )
    )


def representative_candidate_keys_cache_key(
    path: Path,
    filter_settings: dict[str, object],
    partner_filter: str,
) -> str:
    stat = path.stat()
    return "|".join(
        (
            str(path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            interface_filter_settings_key(filter_settings),
            str(partner_filter),
            "candidate_keys:v1",
        )
    )


def cluster_overview_cache_key(
    path: Path,
    filter_settings: dict[str, object],
    partner_filter: str,
    representative_method: str,
    cluster_labels: list[int],
    clustering_settings: dict[str, object],
    representative_domain_size_filter: tuple[int | None, int | None] | None = None,
) -> str:
    stat = path.stat()
    return "|".join(
        (
            str(path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            interface_filter_settings_key(filter_settings),
            str(partner_filter),
            str(representative_method),
            ",".join(str(label) for label in cluster_labels),
            json.dumps(clustering_settings or {}, sort_keys=True),
            domain_size_filter_key(representative_domain_size_filter),
        )
    )


def disk_cache_path(cache_dir: Path, namespace: str, cache_key: str) -> Path:
    digest = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
    return cache_dir / namespace / f"{digest}.json"


def read_disk_json_cache(cache_dir: Path, namespace: str, cache_key: str) -> dict[str, object] | None:
    path = disk_cache_path(cache_dir, namespace, cache_key)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        log_event("cache", "read json cache failed", namespace=namespace, file=path.name, error=exc)
        return None
    return payload if isinstance(payload, dict) else None


def write_disk_json_cache(cache_dir: Path, namespace: str, cache_key: str, payload: dict[str, object]) -> None:
    path = disk_cache_path(cache_dir, namespace, cache_key)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_suffix(f"{path.suffix}.{threading.get_ident()}.tmp")
        with temporary_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        temporary_path.replace(path)
    except OSError as exc:
        log_event("cache", "write json cache failed", namespace=namespace, file=path.name, error=exc)


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


def alignment_payload_for_structure_row(
    interface_data: dict[str, dict[str, dict]],
    row_key: str,
    partner_filter: str,
    fragment_key: str,
) -> dict[str, object]:
    aligned_sequence = ""
    matched_payload: dict | None = None
    for partner_domain, rows in interface_data.items():
        if partner_filter != "__all__" and partner_domain != partner_filter:
            continue
        if not isinstance(rows, dict):
            continue
        row_payload = rows.get(row_key)
        if not isinstance(row_payload, dict):
            continue
        aligned_sequence = str(
            row_payload.get("aligned_sequence") or row_payload.get("aligned_seq") or ""
        )
        if aligned_sequence:
            matched_payload = row_payload
            break
    if not aligned_sequence:
        return {"aligned_sequence": "", "residue_ids": []}
    alignment_key = alignment_fragment_key_for_row_payload(
        aligned_sequence,
        fragment_key,
        matched_payload or {},
    )
    masked_sequence, residue_ids = mask_alignment_to_fragment_ranges(
        aligned_sequence,
        alignment_key,
        fragment_key,
    )
    return {
        "aligned_sequence": masked_sequence,
        "residue_ids": residue_ids,
    }


class ViewerRequestHandler(BaseHTTPRequestHandler):
    interface_dir: Path
    cache_dir: Path
    hierarchy_dir: Path | None
    pfam_hmm_path: Path
    sequence_dir: Path
    interface_store: InterfaceStore | None
    cache_workers: int
    dataset_runtimes: OrderedDict[str, DatasetRuntime]
    default_dataset_key: str
    pfam_option_stats: dict[str, dict[str, object]]
    pfam_option_stats_lock: threading.Lock
    pfam_option_stats_status: dict[str, object]
    dataset_start_lock: threading.Lock

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        dataset_scoped_paths = {
            "/api/files",
            "/api/pfam-selector-metadata",
            "/api/msa",
            "/api/interface",
            "/api/interface-summary",
            "/api/pfam-info",
            "/api/histogram-targets",
            "/api/embedding",
            "/api/clustering",
            "/api/columns-chart",
            "/api/dendrogram",
            "/api/hierarchy-status",
            "/api/cluster-compare",
            "/api/cluster-overview",
            "/api/representative",
            "/api/structure-preview",
            "/api/hmm-bit-scores",
        }
        if parsed.path in dataset_scoped_paths and not self._apply_dataset_from_query(query):
            return
        if parsed.path == "/api/files":
            self._handle_files()
            return
        if parsed.path == "/api/pfam-selector-metadata":
            self._handle_pfam_selector_metadata()
            return
        if parsed.path == "/api/msa":
            self._handle_msa(query)
            return
        if parsed.path == "/api/interface":
            self._handle_interface(query)
            return
        if parsed.path == "/api/interface-summary":
            self._handle_interface_summary(query)
            return
        if parsed.path == "/api/pfam-info":
            self._handle_pfam_info(query)
            return
        if parsed.path == "/api/histogram-targets":
            self._handle_histogram_targets(query)
            return
        if parsed.path == "/api/embedding":
            self._handle_embedding(query)
            return
        if parsed.path == "/api/clustering":
            self._handle_clustering(query)
            return
        if parsed.path == "/api/columns-chart":
            self._handle_columns_chart(query)
            return
        if parsed.path == "/api/dendrogram":
            self._handle_dendrogram(query)
            return
        if parsed.path == "/api/hierarchy-status":
            self._handle_hierarchy_status(query)
            return
        if parsed.path == "/api/cluster-compare":
            self._handle_cluster_compare(query)
            return
        if parsed.path == "/api/cluster-overview":
            self._handle_cluster_overview(query)
            return
        if parsed.path == "/api/representative":
            self._handle_representative(query)
            return
        if parsed.path == "/api/structure-preview":
            self._handle_structure_preview(query)
            return
        if parsed.path == "/api/hmm-bit-scores":
            self._handle_hmm_bit_scores(query)
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

    def _runtime_from_query(self, query: dict[str, list[str]]) -> DatasetRuntime | None:
        requested_key = str(query.get("dataset", [""])[0] or "").strip()
        key = requested_key or self.default_dataset_key
        runtime = self.dataset_runtimes.get(key)
        if runtime is None:
            self._send_json(
                {"error": f"unknown dataset {requested_key or key}"},
                status=HTTPStatus.NOT_FOUND,
            )
            return None
        return runtime

    def _load_dataset_stats(self, runtime: DatasetRuntime) -> None:
        with runtime.lock:
            if runtime.pfam_option_stats_loaded:
                return
            stats, current, signature = load_available_pfam_option_stats(
                self.cache_dir,
                runtime.interface_dir,
            )
            with self.pfam_option_stats_lock:
                runtime.pfam_option_stats.clear()
                runtime.pfam_option_stats.update(stats)
                runtime.pfam_option_stats_status.clear()
                runtime.pfam_option_stats_status.update(
                    {
                        "state": "ready" if current else "refreshing",
                        "cached": bool(stats),
                        "refreshing": not current,
                        "message": "" if current else "Refreshing PFAM selector stats cache",
                    }
                )
            runtime.pfam_option_stats_loaded = True
            runtime.pfam_option_stats_current = current
            runtime.pfam_option_stats_signature = signature
            if not current:
                cached_label = "stale cached stats" if stats else "no cached stats"
                print(
                    f"PFAM selector stats cache for dataset {runtime.key} is stale or missing "
                    f"({cached_label}); serving available files while refreshing in the background.",
                    flush=True,
                )

    def _start_dataset_background_tasks(self, runtime: DatasetRuntime) -> None:
        with self.dataset_start_lock:
            if runtime.interface_store is not None and not runtime.store_sync_started:
                runtime.interface_store.start_background_sync()
                runtime.store_sync_started = True
            if runtime.pfam_option_stats_loaded and not runtime.metadata_refresh_started:
                start_background_pfam_metadata_refresh(
                    self.cache_dir,
                    runtime.pfam_option_stats,
                    self.pfam_option_stats_lock,
                )
                runtime.metadata_refresh_started = True
            if (
                runtime.pfam_option_stats_loaded
                and not runtime.pfam_option_stats_current
                and not runtime.stats_refresh_started
            ):
                start_background_pfam_option_stats_refresh(
                    self.cache_dir,
                    runtime.interface_dir,
                    self.cache_workers,
                    runtime.pfam_option_stats,
                    self.pfam_option_stats_lock,
                    runtime.pfam_option_stats_status,
                    runtime.pfam_option_stats_signature,
                )
                runtime.stats_refresh_started = True

    def _apply_dataset_from_query(self, query: dict[str, list[str]]) -> bool:
        runtime = self._runtime_from_query(query)
        if runtime is None:
            return False
        self._load_dataset_stats(runtime)
        self._start_dataset_background_tasks(runtime)
        self.active_dataset_runtime = runtime
        self.interface_dir = runtime.interface_dir
        self.hierarchy_dir = runtime.hierarchy_dir
        self.interface_store = runtime.interface_store
        self.pfam_option_stats = runtime.pfam_option_stats
        self.pfam_option_stats_status = runtime.pfam_option_stats_status
        return True

    def _handle_files(self) -> None:
        runtime = getattr(self, "active_dataset_runtime", None)
        with self.pfam_option_stats_lock:
            pfam_option_stats = {
                str(pfam_id): dict(stats)
                for pfam_id, stats in self.pfam_option_stats.items()
                if isinstance(stats, dict)
            }
            pfam_option_stats_status = dict(self.pfam_option_stats_status)
        self._send_json(
            {
                "dataset": runtime.key if runtime is not None else self.default_dataset_key,
                "datasets": [
                    dataset_payload(candidate_runtime)
                    for candidate_runtime in self.dataset_runtimes.values()
                ],
                "interface_dir": str(self.interface_dir),
                "hierarchy_dir": str(self.hierarchy_dir) if self.hierarchy_dir is not None else None,
                "interface_files": list_json_files(self.interface_dir),
                "pfam_option_stats": pfam_option_stats,
                "pfam_option_stats_status": pfam_option_stats_status,
            }
        )

    def _handle_pfam_selector_metadata(self) -> None:
        pfam_ids = sorted(
            {
                interface_file_pfam_id(path)
                for path in directory_interface_json_paths(self.interface_dir)
            }
        )
        metadata = load_cached_pfam_metadata(self.cache_dir, pfam_ids)
        pfam_metadata = {
            pfam_id: str(entry.get("display_name", "")).strip()
            for pfam_id, entry in metadata.items()
            if str(entry.get("display_name", "")).strip()
        }
        with self.pfam_option_stats_lock:
            pfam_option_stats_status = dict(self.pfam_option_stats_status)
        self._send_json(
            {
                "pfam_metadata": pfam_metadata,
                "pfam_option_stats_status": pfam_option_stats_status,
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
        include_summary = query_flag(query, "include_summary", True)
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
                    include_summary=include_summary,
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
            interface_summary = interface_summary_from_payload(filtered_payload) if include_summary else None
            response_payload = {
                "file": path.name,
                "pfam_id": pfam_id,
                "filter_settings": interface_filter_settings,
                "alignment_length": alignment_length,
                "row_count": total_rows,
                "interface_partner_domains": list(interface_partner_counts),
                "interface_partner_counts": interface_partner_counts,
                "row_offset": row_offset,
                "row_limit": row_limit,
                "rows_loaded": returned_row_count,
                "rows_complete": rows_complete,
                "rows": rows,
            }
            if interface_summary is not None:
                response_payload["interface_summary"] = interface_summary
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
                include_summary=include_summary,
                row_offset=row_offset,
                row_limit=row_limit if row_limit is not None else "all",
                data_offset=data_offset,
                data_limit=data_limit if data_limit is not None else "all",
            )
        self._send_json(response_payload)

    def _load_interface_summary_payload(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
    ) -> dict[str, object]:
        cache_key = interface_view_cache_key(path, interface_filter_settings)
        with INTERFACE_SUMMARY_CACHE_LOCK:
            cached_payload = INTERFACE_SUMMARY_CACHE.get(cache_key)
            if cached_payload is not None:
                INTERFACE_SUMMARY_CACHE.move_to_end(cache_key)
                log_event("summary", "reuse cached interface summary", file=path.name)
                return cached_payload
        disk_payload = read_disk_json_cache(self.cache_dir, "interface_summary", cache_key)
        if disk_payload is not None:
            with INTERFACE_SUMMARY_CACHE_LOCK:
                INTERFACE_SUMMARY_CACHE[cache_key] = disk_payload
                INTERFACE_SUMMARY_CACHE.move_to_end(cache_key)
                while len(INTERFACE_SUMMARY_CACHE) > INTERFACE_SUMMARY_CACHE_LIMIT:
                    INTERFACE_SUMMARY_CACHE.popitem(last=False)
            log_event("summary", "reuse disk interface summary", file=path.name)
            return disk_payload

        owns_load = False
        with INTERFACE_SUMMARY_CACHE_LOCK:
            future = INTERFACE_SUMMARY_IN_FLIGHT.get(cache_key)
            if future is None:
                future = Future()
                INTERFACE_SUMMARY_IN_FLIGHT[cache_key] = future
                owns_load = True
        if not owns_load:
            with timed_step("summary", "wait for interface summary", file=path.name):
                return future.result()

        try:
            if self.interface_store is not None:
                try:
                    payload = self.interface_store.get_interface_summary_payload(
                        path,
                        interface_filter_settings,
                    )
                except Exception as exc:
                    log_event("store", "interface summary fallback", file=path.name, error=exc)
                    payload = None
            else:
                payload = None
            if payload is None:
                _cache_key, _raw_payload, filtered_payload, _cache_entry = load_cached_interface_view(
                    path,
                    interface_filter_settings,
                )
                raw_rows, alignment_length = collect_interface_alignment_row_metadata(filtered_payload)
                interface_partner_counts = {
                    str(partner_domain): len(rows_by_partner)
                    for partner_domain, rows_by_partner in sorted(filtered_payload.items())
                    if isinstance(rows_by_partner, dict)
                }
                payload = {
                    "file": path.name,
                    "pfam_id": interface_file_pfam_id(path),
                    "filter_settings": interface_filter_settings,
                    "alignment_length": alignment_length,
                    "row_count": len(raw_rows),
                    "interface_partner_domains": list(interface_partner_counts),
                    "interface_partner_counts": interface_partner_counts,
                    "interface_summary": interface_summary_from_payload(filtered_payload),
                }
            with INTERFACE_SUMMARY_CACHE_LOCK:
                INTERFACE_SUMMARY_CACHE[cache_key] = payload
                INTERFACE_SUMMARY_CACHE.move_to_end(cache_key)
                while len(INTERFACE_SUMMARY_CACHE) > INTERFACE_SUMMARY_CACHE_LIMIT:
                    INTERFACE_SUMMARY_CACHE.popitem(last=False)
                INTERFACE_SUMMARY_IN_FLIGHT.pop(cache_key, None)
                future.set_result(payload)
            write_disk_json_cache(self.cache_dir, "interface_summary", cache_key, payload)
            return payload
        except BaseException as exc:
            with INTERFACE_SUMMARY_CACHE_LOCK:
                INTERFACE_SUMMARY_IN_FLIGHT.pop(cache_key, None)
                future.set_exception(exc)
            raise

    def _handle_interface_summary(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
        try:
            payload = self._load_interface_summary_payload(path, interface_filter_settings)
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json(
                {"error": f"Unexpected interface summary error: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self._send_json(payload)

    def _histogram_targets_from_payload(
        self,
        interface_payload: dict[str, dict[str, dict]],
        histogram_type: str,
        bin_start: int,
        bin_end: int,
        partner_domain: str = "",
    ) -> list[dict[str, object]]:
        normalized_partner = str(partner_domain or "").strip()
        targets: list[dict[str, object]] = []
        for current_partner, rows in sorted(interface_payload.items()):
            if normalized_partner and normalized_partner != "__all__" and current_partner != normalized_partner:
                continue
            if not isinstance(rows, dict):
                continue
            for row_key, row_payload in sorted(rows.items()):
                if not isinstance(row_payload, dict):
                    continue
                if histogram_type == "interface_size":
                    value = interface_residue_count(row_payload, "a")
                elif histogram_type == "domain_length":
                    value = domain_length_from_row_payload(row_key, row_payload)
                elif histogram_type == "pfam_row_coverage":
                    value = pfam_row_coverage_percent_from_payload(row_payload, str(row_key).split("_", 2)[1] if "_" in str(row_key) else "")
                else:
                    raise ValueError(
                        "histogram type must be 'interface_size', 'domain_length', or 'pfam_row_coverage'"
                    )
                if value < bin_start or value > bin_end:
                    continue
                targets.append(
                    {
                        "row_key": str(row_key),
                        "partner_domain": str(current_partner),
                        "value": int(value),
                    }
                )
        return targets

    def _handle_histogram_targets(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
        histogram_type = query.get("type", ["interface_size"])[0].strip().lower()
        partner_domain = query.get("partner_domain", [""])[0].strip()
        try:
            bin_start = query_non_negative_int(query, "start", 0)
            bin_end = query_non_negative_int(query, "end", bin_start)
            if bin_end < bin_start:
                raise ValueError("histogram end cannot be smaller than start")
            if histogram_type not in {"interface_size", "domain_length", "pfam_row_coverage"}:
                raise ValueError(
                    "histogram type must be 'interface_size', 'domain_length', or 'pfam_row_coverage'"
                )
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        targets: list[dict[str, object]]
        if self.interface_store is not None:
            try:
                targets = self.interface_store.get_histogram_targets(
                    path,
                    interface_filter_settings,
                    histogram_type=histogram_type,
                    bin_start=bin_start,
                    bin_end=bin_end,
                    partner_domain=partner_domain,
                )
                self._send_json(
                    {
                        "file": path.name,
                        "pfam_id": interface_file_pfam_id(path),
                        "filter_settings": interface_filter_settings,
                        "type": histogram_type,
                        "start": bin_start,
                        "end": bin_end,
                        "partner_domain": partner_domain,
                        "target_count": len(targets),
                        "targets": targets,
                    }
                )
                return
            except Exception as exc:
                log_event("store", "histogram targets fallback", file=path.name, error=exc)
        try:
            _cache_key, _raw_payload, interface_payload, _cache_entry = load_cached_interface_view(
                path,
                interface_filter_settings,
            )
            targets = self._histogram_targets_from_payload(
                interface_payload,
                histogram_type,
                bin_start,
                bin_end,
                partner_domain,
            )
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json(
                {"error": f"Unexpected histogram target error: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self._send_json(
            {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                "type": histogram_type,
                "start": bin_start,
                "end": bin_end,
                "partner_domain": partner_domain,
                "target_count": len(targets),
                "targets": targets,
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
        display_name = str(pfam_info.get("display_name", "")).strip()
        response_pfam_id = str(pfam_info.get("pfam_id", pfam_id)).strip()
        if display_name and response_pfam_id:
            with self.pfam_option_stats_lock:
                stats = self.pfam_option_stats.get(response_pfam_id)
                if isinstance(stats, dict):
                    stats["display_name"] = display_name
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
        interface_payload = self._load_interface_columns_payload(
            path,
            interface_filter_settings,
            fallback_context="embedding columns payload fallback",
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
        cache_key = interface_view_cache_key(path, interface_filter_settings)
        with COLUMNS_PAYLOAD_CACHE_LOCK:
            cached_payload = COLUMNS_PAYLOAD_CACHE.get(cache_key)
            if cached_payload is not None:
                COLUMNS_PAYLOAD_CACHE.move_to_end(cache_key)
                log_event(
                    "store",
                    "reuse interface columns payload",
                    file=path.name,
                    rows=sum(len(rows) for rows in cached_payload.values()),
                )
                return cached_payload
            future = COLUMNS_PAYLOAD_IN_FLIGHT.get(cache_key)
            owns_load = future is None
            if owns_load:
                future = Future()
                COLUMNS_PAYLOAD_IN_FLIGHT[cache_key] = future
        if not owns_load:
            with timed_step("store", "wait for interface columns payload", file=path.name):
                return future.result()

        try:
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
            with COLUMNS_PAYLOAD_CACHE_LOCK:
                COLUMNS_PAYLOAD_CACHE[cache_key] = interface_payload
                COLUMNS_PAYLOAD_CACHE.move_to_end(cache_key)
                while len(COLUMNS_PAYLOAD_CACHE) > COLUMNS_PAYLOAD_CACHE_LIMIT:
                    COLUMNS_PAYLOAD_CACHE.popitem(last=False)
                COLUMNS_PAYLOAD_IN_FLIGHT.pop(cache_key, None)
                future.set_result(interface_payload)
            return interface_payload
        except BaseException as exc:
            with COLUMNS_PAYLOAD_CACHE_LOCK:
                COLUMNS_PAYLOAD_IN_FLIGHT.pop(cache_key, None)
                future.set_exception(exc)
            raise

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
        interface_payload: dict[str, dict[str, dict]] | None = None,
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
        if interface_payload is None:
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
        include_columns_chart = query_flag(query, "include_columns_chart", False)
        try:
            response_payload = self._load_clustering_payload(
                path,
                interface_filter_settings,
                clustering_settings,
            )
            if include_columns_chart:
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

    def _handle_columns_chart(self, query: dict[str, list[str]]) -> None:
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
            clustering_payload = self._load_clustering_payload(
                path,
                interface_filter_settings,
                clustering_settings,
            )
            response_payload = self._attach_columns_chart_payload(
                clustering_payload,
                path,
                interface_filter_settings,
            )
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected columns chart error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json(
            {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                "clustering": response_payload.get("clustering"),
                "distance": response_payload.get("distance"),
                "cluster_count": response_payload.get("cluster_count"),
                "sample_count": response_payload.get("sample_count"),
                "columns_chart": response_payload.get("columns_chart"),
            }
        )

    def _handle_dendrogram(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved = self._resolve_interface_file_and_filter(filename, query)
        if resolved is None:
            return
        path, interface_filter_settings = resolved
        try:
            clustering_settings = parse_clustering_settings(query)
            merge_depth = query_positive_int_or_none(query, "merge_depth") or 5
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            interface_payload = self._load_interface_columns_payload(
                path,
                interface_filter_settings,
                fallback_context="dendrogram columns payload fallback",
            )
            response_payload = load_or_compute_dendrogram_payload(
                self.cache_dir,
                path,
                interface_payload,
                clustering_settings,
                interface_filter_settings,
                cache_workers=self.cache_workers,
                hierarchy_dir=self.hierarchy_dir,
                merge_depth=merge_depth,
            )
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected dendrogram error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json(
            {
                "filter_settings": interface_filter_settings,
                **response_payload,
            }
        )

    def _handle_hierarchy_status(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
        interface_payload = self._load_interface_columns_payload(
            path,
            interface_filter_settings,
            fallback_context="hierarchy-status columns payload fallback",
        )
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
            representative_domain_size_filter = representative_domain_size_filter_from_query(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        interface_payload = self._load_interface_columns_payload(
            path,
            interface_filter_settings,
            fallback_context="cluster-compare columns payload fallback",
        )
        try:
            clustering_payload = self._load_clustering_payload(
                path,
                interface_filter_settings,
                clustering_settings,
                interface_payload=interface_payload,
            )
            response_payload = {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                **compute_cluster_compare_payload_from_clustering(
                    clustering_payload,
                    cluster_label,
                    interface_payload=interface_payload,
                    representative_domain_size_filter=representative_domain_size_filter,
                ),
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
        candidates_cache_key = interface_view_cache_key(path, interface_filter_settings)
        with REPRESENTATIVE_CANDIDATES_CACHE_LOCK:
            cached_candidates = REPRESENTATIVE_CANDIDATES_CACHE.get(candidates_cache_key)
            if cached_candidates is not None:
                REPRESENTATIVE_CANDIDATES_CACHE.move_to_end(candidates_cache_key)
                candidates, alignment_length = cached_candidates
                log_event(
                    "representative",
                    "reuse representative candidates",
                    file=path.name,
                    rows=len(candidates),
                    alignment_length=alignment_length,
                )
                return cached_candidates
        if self.interface_store is not None:
            try:
                candidates_result = self.interface_store.get_representative_candidates(
                    path,
                    interface_filter_settings,
                )
                with REPRESENTATIVE_CANDIDATES_CACHE_LOCK:
                    REPRESENTATIVE_CANDIDATES_CACHE[candidates_cache_key] = candidates_result
                    REPRESENTATIVE_CANDIDATES_CACHE.move_to_end(candidates_cache_key)
                    while len(REPRESENTATIVE_CANDIDATES_CACHE) > REPRESENTATIVE_CANDIDATES_CACHE_LIMIT:
                        REPRESENTATIVE_CANDIDATES_CACHE.popitem(last=False)
                return candidates_result
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
                        "interface_residues_a": (
                            row_payload.get("interface_residues_a", [])
                            if isinstance(row_payload, dict)
                            else []
                        ),
                        "surface_residue_ids_a": (
                            row_payload.get("surface_residue_ids_a", [])
                            if isinstance(row_payload, dict)
                            else []
                        ),
                        "interface_msa_columns_a": (
                            row_payload.get("interface_msa_columns_a", [])
                            if isinstance(row_payload, dict)
                            else []
                        ),
                        "surface_msa_columns_a": (
                            row_payload.get("surface_msa_columns_a", [])
                            if isinstance(row_payload, dict)
                            else []
                        ),
                    }
                )
            timer.set(alignment_length=alignment_length)
        candidates_result = (candidates, alignment_length)
        with REPRESENTATIVE_CANDIDATES_CACHE_LOCK:
            REPRESENTATIVE_CANDIDATES_CACHE[candidates_cache_key] = candidates_result
            REPRESENTATIVE_CANDIDATES_CACHE.move_to_end(candidates_cache_key)
            while len(REPRESENTATIVE_CANDIDATES_CACHE) > REPRESENTATIVE_CANDIDATES_CACHE_LIMIT:
                REPRESENTATIVE_CANDIDATES_CACHE.popitem(last=False)
        return candidates_result

    def _representative_candidate_key_tuple(
        self,
        candidate: dict[str, object],
    ) -> tuple[str, str]:
        return (
            str(candidate.get("interface_row_key") or candidate.get("row_key") or ""),
            str(candidate.get("partner_domain") or ""),
        )

    def _load_representative_candidate_keys(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
        partner_filter: str = "__all__",
    ) -> tuple[list[dict[str, object]], int]:
        cache_key = representative_candidate_keys_cache_key(
            path,
            interface_filter_settings,
            partner_filter,
        )
        disk_payload = read_disk_json_cache(self.cache_dir, "representative_candidate_keys", cache_key)
        if disk_payload is not None:
            candidates = disk_payload.get("candidate_keys")
            alignment_length = int(disk_payload.get("alignment_length") or 0)
            if isinstance(candidates, list):
                log_event(
                    "representative",
                    "reuse disk representative candidate keys",
                    file=path.name,
                    rows=len(candidates),
                    partner=partner_filter,
                )
                return candidates, alignment_length
        if self.interface_store is not None:
            try:
                candidates, alignment_length = self.interface_store.get_representative_candidate_keys(
                    path,
                    interface_filter_settings,
                    partner_filter,
                )
                write_disk_json_cache(
                    self.cache_dir,
                    "representative_candidate_keys",
                    cache_key,
                    {
                        "file": path.name,
                        "pfam_id": interface_file_pfam_id(path),
                        "filter_settings": interface_filter_settings,
                        "partner_filter": partner_filter,
                        "alignment_length": alignment_length,
                        "candidate_keys": candidates,
                    },
                )
                return candidates, alignment_length
            except Exception as exc:
                log_event("store", "representative candidate keys fallback", file=path.name, error=exc)
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
        candidate_keys = [
            {
                "interface_row_key": str(candidate.get("interface_row_key") or ""),
                "partner_domain": str(candidate.get("partner_domain") or ""),
            }
            for candidate in candidates
        ]
        write_disk_json_cache(
            self.cache_dir,
            "representative_candidate_keys",
            cache_key,
            {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                "partner_filter": partner_filter,
                "alignment_length": alignment_length,
                "candidate_keys": candidate_keys,
            },
        )
        return candidate_keys, alignment_length

    def _hydrate_representative_candidates(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
        candidate_keys: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        key_pairs = [
            self._representative_candidate_key_tuple(candidate)
            for candidate in candidate_keys
        ]
        key_pairs = [key for key in key_pairs if key[0] and key[1]]
        if not key_pairs:
            return []
        if self.interface_store is not None:
            try:
                return self.interface_store.get_representative_candidates_by_keys(
                    path,
                    interface_filter_settings,
                    key_pairs,
                )
            except Exception as exc:
                log_event("store", "sampled representative candidates fallback", file=path.name, error=exc)
        key_set = set(key_pairs)
        candidates, _alignment_length = self._load_representative_candidates(
            path,
            interface_filter_settings,
        )
        return [
            candidate
            for candidate in candidates
            if self._representative_candidate_key_tuple(candidate) in key_set
        ]

    def _representative_alignment_length(
        self,
        path: Path,
        interface_filter_settings: dict[str, object],
    ) -> int:
        if self.interface_store is not None:
            try:
                return self.interface_store.get_filtered_alignment_length(path, interface_filter_settings)
            except Exception as exc:
                log_event("store", "representative alignment length fallback", file=path.name, error=exc)
        _candidate_keys, alignment_length = self._load_representative_candidate_keys(
            path,
            interface_filter_settings,
        )
        return alignment_length

    def _cluster_overview_member_candidates(
        self,
        clustering_payload: dict[str, object],
        cluster_labels: list[int],
        partner_filter: str,
        representative_domain_size_filter: tuple[int | None, int | None] | None = None,
    ) -> dict[int, list[dict[str, object]]]:
        requested_labels = set(cluster_labels)
        members_by_cluster: dict[int, list[dict[str, object]]] = {
            cluster_label: []
            for cluster_label in cluster_labels
        }
        seen_by_cluster: dict[int, set[tuple[str, str]]] = {
            cluster_label: set()
            for cluster_label in cluster_labels
        }
        points = clustering_payload.get("points")
        if not isinstance(points, list):
            return members_by_cluster
        for point in points:
            if not isinstance(point, dict):
                continue
            try:
                cluster_label = int(point.get("cluster_label", -1))
            except (TypeError, ValueError):
                continue
            if cluster_label not in requested_labels:
                continue
            row_key = str(point.get("row_key") or "")
            partner_domain = str(point.get("partner_domain") or "")
            if not row_key or not partner_domain:
                continue
            if partner_filter != "__all__" and partner_domain != partner_filter:
                continue
            if not candidate_matches_domain_size_filter(
                {
                    "interface_row_key": row_key,
                    "partner_domain": partner_domain,
                },
                representative_domain_size_filter,
            ):
                continue
            key = (row_key, partner_domain)
            seen = seen_by_cluster.setdefault(cluster_label, set())
            if key in seen:
                continue
            seen.add(key)
            members_by_cluster.setdefault(cluster_label, []).append(
                {
                    "interface_row_key": row_key,
                    "partner_domain": partner_domain,
                }
            )
        return members_by_cluster

    def _query_cluster_labels(self, query: dict[str, list[str]]) -> list[int]:
        raw_values: list[str] = []
        raw_values.extend(query.get("cluster_label", []))
        raw_values.extend(query.get("cluster_labels", []))
        labels: list[int] = []
        seen: set[int] = set()
        for raw_value in raw_values:
            for part in str(raw_value or "").split(","):
                part = part.strip()
                if not part:
                    continue
                label = int(part)
                if label in seen:
                    continue
                seen.add(label)
                labels.append(label)
        return labels

    def _handle_cluster_overview(self, query: dict[str, list[str]]) -> None:
        filename = query.get("file", [""])[0]
        resolved_file = self._resolve_interface_file_and_filter(filename, query)
        if resolved_file is None:
            return
        path, interface_filter_settings = resolved_file
        representative_method = query.get(
            "representative_method",
            [""],
        )[0].strip().lower() or REPRESENTATIVE_METHOD_BALANCED
        partner_filter = query.get("partner", ["__all__"])[0].strip() or "__all__"
        if representative_method not in REPRESENTATIVE_METHODS:
            self._send_json(
                {"error": "representative_method must be either 'balanced' or 'residue'"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            cluster_labels = self._query_cluster_labels(query)
            clustering_settings = parse_clustering_settings(query)
            representative_domain_size_filter = representative_domain_size_filter_from_query(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if not cluster_labels:
            self._send_json({"error": "at least one cluster_label is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        cache_key = cluster_overview_cache_key(
            path,
            interface_filter_settings,
            partner_filter,
            representative_method,
            cluster_labels,
            clustering_settings,
            representative_domain_size_filter,
        )
        with CLUSTER_OVERVIEW_CACHE_LOCK:
            cached_response = CLUSTER_OVERVIEW_CACHE.get(cache_key)
            if cached_response is not None:
                CLUSTER_OVERVIEW_CACHE.move_to_end(cache_key)
                log_event(
                    "representative",
                    "reuse cached cluster overview",
                    file=path.name,
                    clusters=len(cluster_labels),
                    representative_method=representative_method,
                    partner=partner_filter,
                )
                self._send_json(cached_response)
                return
        disk_response = read_disk_json_cache(self.cache_dir, "cluster_overview", cache_key)
        if disk_response is not None:
            with CLUSTER_OVERVIEW_CACHE_LOCK:
                CLUSTER_OVERVIEW_CACHE[cache_key] = disk_response
                CLUSTER_OVERVIEW_CACHE.move_to_end(cache_key)
                while len(CLUSTER_OVERVIEW_CACHE) > CLUSTER_OVERVIEW_CACHE_LIMIT:
                    CLUSTER_OVERVIEW_CACHE.popitem(last=False)
            log_event(
                "representative",
                "reuse disk cluster overview",
                file=path.name,
                clusters=len(cluster_labels),
                representative_method=representative_method,
                partner=partner_filter,
            )
            self._send_json(disk_response)
            return
        try:
            clustering_payload = self._load_clustering_payload(
                path,
                interface_filter_settings,
                clustering_settings,
            )
            alignment_length = self._representative_alignment_length(path, interface_filter_settings)
            members_by_cluster = self._cluster_overview_member_candidates(
                clustering_payload,
                cluster_labels,
                partner_filter,
                representative_domain_size_filter,
            )
            selected_representatives: list[dict[str, object]] = []
            with timed_step(
                "representative",
                "build cluster overview representatives",
                file=path.name,
                clusters=len(cluster_labels),
                representative_method=representative_method,
                partner=partner_filter,
            ) as timer:
                sampled_by_cluster: dict[int, list[dict[str, object]]] = {}
                all_sampled_candidates: list[dict[str, object]] = []
                for cluster_label in cluster_labels:
                    member_candidates = members_by_cluster.get(cluster_label, [])
                    sampled_candidates = sample_representative_candidates(
                        member_candidates,
                        scope="cluster",
                        cluster_label=cluster_label,
                        method=representative_method,
                    )
                    sampled_by_cluster[cluster_label] = sampled_candidates
                    all_sampled_candidates.extend(sampled_candidates)
                hydrated_candidates = self._hydrate_representative_candidates(
                    path,
                    interface_filter_settings,
                    all_sampled_candidates,
                )
                hydrated_by_key = {
                    self._representative_candidate_key_tuple(candidate): candidate
                    for candidate in hydrated_candidates
                }
                for cluster_label in cluster_labels:
                    member_candidates = members_by_cluster.get(cluster_label, [])
                    cluster_hydrated_candidates = [
                        hydrated_by_key[key]
                        for key in (
                            self._representative_candidate_key_tuple(candidate)
                            for candidate in sampled_by_cluster.get(cluster_label, [])
                        )
                        if key in hydrated_by_key
                    ]
                    representative_payload = compute_representative_payload(
                        cluster_hydrated_candidates,
                        alignment_length,
                        scope="cluster",
                        cluster_label=cluster_label,
                        method=representative_method,
                        source_candidate_count=len(member_candidates),
                    )
                    selected_representatives.append(representative_payload)
                timer.set(hydrated_rows=len(hydrated_candidates))
            response_payload = {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                "partner_filter": partner_filter,
                "representative_method": representative_method,
                "representative_domain_size_filter": domain_size_filter_key(
                    representative_domain_size_filter,
                ),
                "alignment_length": alignment_length,
                "cluster_labels": cluster_labels,
                "selected_representatives": selected_representatives,
            }
            with CLUSTER_OVERVIEW_CACHE_LOCK:
                CLUSTER_OVERVIEW_CACHE[cache_key] = response_payload
                CLUSTER_OVERVIEW_CACHE.move_to_end(cache_key)
                while len(CLUSTER_OVERVIEW_CACHE) > CLUSTER_OVERVIEW_CACHE_LIMIT:
                    CLUSTER_OVERVIEW_CACHE.popitem(last=False)
            write_disk_json_cache(self.cache_dir, "cluster_overview", cache_key, response_payload)
            self._send_json(response_payload)
        except (RuntimeError, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json({"error": f"Unexpected cluster overview error: {exc}"}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

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
        include_cluster_summaries = query_flag(query, "include_cluster_summaries", False)
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
        try:
            representative_domain_size_filter = representative_domain_size_filter_from_query(query)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        cluster_label: int | None = None
        clustering_settings: dict[str, object] | None = None
        if scope == "cluster":
            cluster_label_raw = query.get("cluster_label", [""])[0].strip()
            if cluster_label_raw == "":
                self._send_json({"error": "cluster_label is required"}, status=HTTPStatus.BAD_REQUEST)
                return
            try:
                cluster_label = int(cluster_label_raw)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
        if scope == "cluster" or include_cluster_summaries:
            try:
                clustering_settings = parse_clustering_settings(query)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
        cache_key = representative_cache_key(
            path,
            interface_filter_settings,
            partner_filter,
            scope,
            representative_method,
            cluster_label,
            clustering_settings,
            representative_domain_size_filter,
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
        disk_response = read_disk_json_cache(self.cache_dir, "representative", cache_key)
        if disk_response is not None:
            with REPRESENTATIVE_CACHE_LOCK:
                REPRESENTATIVE_CACHE[cache_key] = disk_response
                REPRESENTATIVE_CACHE.move_to_end(cache_key)
                while len(REPRESENTATIVE_CACHE) > REPRESENTATIVE_CACHE_LIMIT:
                    REPRESENTATIVE_CACHE.popitem(last=False)
            log_event(
                "representative",
                "reuse disk representative",
                file=path.name,
                representative_scope=scope,
                representative_method=representative_method,
                partner=partner_filter,
                cluster_label=cluster_label if cluster_label is not None else "",
                row_key=disk_response.get("representative_row_key"),
            )
            self._send_json(disk_response)
            return
        try:
            source_candidate_count: int | None = None
            summary_candidates: list[dict[str, object]] | None = None
            if scope == "overall" and not domain_size_filter_is_active(representative_domain_size_filter):
                candidate_keys, alignment_length = self._load_representative_candidate_keys(
                    path,
                    interface_filter_settings,
                    partner_filter,
                )
                sampled_candidate_keys = sample_representative_candidates(
                    candidate_keys,
                    scope=scope,
                    cluster_label=None,
                    method=representative_method,
                )
                candidates = self._hydrate_representative_candidates(
                    path,
                    interface_filter_settings,
                    sampled_candidate_keys,
                )
                source_candidate_count = len(candidate_keys)
                if include_cluster_summaries:
                    summary_candidates, _summary_alignment_length = self._load_representative_candidates(
                        path,
                        interface_filter_settings,
                    )
                    if partner_filter != "__all__":
                        summary_candidates = [
                            candidate
                            for candidate in summary_candidates
                            if str(candidate.get("partner_domain") or "") == partner_filter
                        ]
            else:
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
                if include_cluster_summaries or scope == "cluster":
                    summary_candidates = candidates
                if scope == "overall" and domain_size_filter_is_active(representative_domain_size_filter):
                    candidates = [
                        candidate
                        for candidate in candidates
                        if candidate_matches_domain_size_filter(candidate, representative_domain_size_filter)
                    ]
                    source_candidate_count = len(candidates)
            cluster_summaries: list[dict[str, object]] | None = None
            if (include_cluster_summaries or scope == "cluster") and clustering_settings is not None:
                if summary_candidates is None:
                    summary_candidates = candidates
                clustering_payload = self._load_clustering_payload(
                    path,
                    interface_filter_settings,
                    clustering_settings,
                )
                cluster_summaries = compute_cluster_summary_payload(
                    summary_candidates,
                    clustering_payload,
                )
                if scope == "cluster" and cluster_label is not None:
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
                    if domain_size_filter_is_active(representative_domain_size_filter):
                        candidates = [
                            candidate
                            for candidate in candidates
                            if candidate_matches_domain_size_filter(candidate, representative_domain_size_filter)
                        ]
            response_payload = {
                "file": path.name,
                "pfam_id": interface_file_pfam_id(path),
                "filter_settings": interface_filter_settings,
                "partner_filter": partner_filter,
                "representative_domain_size_filter": domain_size_filter_key(
                    representative_domain_size_filter,
                ),
                **compute_representative_payload(
                    candidates,
                    alignment_length,
                    scope=scope,
                    cluster_label=cluster_label,
                    method=representative_method,
                    source_candidate_count=source_candidate_count,
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
        write_disk_json_cache(self.cache_dir, "representative", cache_key, response_payload)
        self._send_json(response_payload)

    def _handle_hmm_bit_scores(self, query: dict[str, list[str]]) -> None:
        interface_filename = query.get("interface_file", query.get("file", [""]))[0]
        row_key = query.get("row_key", [""])[0]
        partner = query.get("partner", ["__all__"])[0].strip() or "__all__"
        if not interface_filename or not row_key:
            self._send_json(
                {"error": "interface_file and row_key are required"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if partner == "__all__":
            self._send_json(
                {"error": "choose a single partner before calculating HMM bit scores"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        interface_path = safe_file_path(self.interface_dir, interface_filename)
        if interface_path is None:
            self._send_json(
                {"error": f"missing interface file {interface_filename}"},
                status=HTTPStatus.NOT_FOUND,
            )
            return

        try:
            uniprot_id, fragment_key_name, partner_fragment_key = parse_interface_row_key(row_key)
            if not uniprot_id or not fragment_key_name:
                raise ValueError(f"invalid row_key {row_key}")
            if not partner_fragment_key:
                raise ValueError("could not determine the interacting domain fragment range")
            main_pfam_id = interface_file_pfam_id(interface_path)
            partner_pfam_id = partner
            payload = compute_domain_hmm_bit_scores(
                cache_dir=self.cache_dir,
                accession=uniprot_id,
                main_fragment_key=fragment_key_name,
                partner_fragment_key=partner_fragment_key,
                main_pfam_id=main_pfam_id,
                partner_pfam_id=partner_pfam_id,
                sequence_dir=self.sequence_dir,
                pfam_hmm_path=self.pfam_hmm_path,
                hmmer_bin_dir=DEFAULT_HMMER_BIN_DIR,
            )
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except (FileNotFoundError, KeyError) as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        except RuntimeError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_GATEWAY)
            return
        except Exception as exc:  # pragma: no cover
            self._send_json(
                {"error": f"Unexpected HMM bit-score error: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return

        self._send_json(
            {
                **payload,
                "row_key": row_key,
                "uniprot_id": uniprot_id,
                "main_pfam_id": main_pfam_id,
                "partner_pfam_id": partner_pfam_id,
                "main_fragment_key": fragment_key_name,
                "partner_fragment_key": partner_fragment_key,
            }
        )

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
        row_alignment = alignment_payload_for_structure_row(
            interface_data,
            row_key,
            partner,
            fragment_key_name,
        )
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
                "aligned_sequence": row_alignment["aligned_sequence"],
                "residue_ids": row_alignment["residue_ids"],
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
    default_runtime: DatasetRuntime,
    dataset_runtimes: OrderedDict[str, DatasetRuntime],
    cache_dir: Path,
    pfam_hmm_path: Path,
    sequence_dir: Path,
    cache_workers: int,
    pfam_option_stats_lock: threading.Lock,
):
    class ConfiguredHandler(ViewerRequestHandler):
        pass

    ConfiguredHandler.interface_dir = default_runtime.interface_dir
    ConfiguredHandler.cache_dir = cache_dir
    ConfiguredHandler.hierarchy_dir = default_runtime.hierarchy_dir
    ConfiguredHandler.pfam_hmm_path = pfam_hmm_path
    ConfiguredHandler.sequence_dir = sequence_dir
    ConfiguredHandler.interface_store = default_runtime.interface_store
    ConfiguredHandler.cache_workers = max(1, int(cache_workers))
    ConfiguredHandler.dataset_runtimes = dataset_runtimes
    ConfiguredHandler.default_dataset_key = default_runtime.key
    ConfiguredHandler.pfam_option_stats = default_runtime.pfam_option_stats
    ConfiguredHandler.pfam_option_stats_lock = pfam_option_stats_lock
    ConfiguredHandler.pfam_option_stats_status = default_runtime.pfam_option_stats_status
    ConfiguredHandler.dataset_start_lock = threading.Lock()
    return ConfiguredHandler


def start_background_pfam_option_stats_refresh(
    cache_dir: Path,
    interface_dir: Path,
    cache_workers: int,
    pfam_option_stats: dict[str, dict[str, object]],
    pfam_option_stats_lock: threading.Lock,
    pfam_option_stats_status: dict[str, object],
    signature: str,
) -> threading.Thread:
    def refresh() -> None:
        try:
            refreshed_stats = compute_and_cache_pfam_option_stats(
                cache_dir,
                interface_dir,
                max(1, int(cache_workers)),
                signature=signature,
            )
        except Exception as exc:
            with pfam_option_stats_lock:
                pfam_option_stats_status.clear()
                pfam_option_stats_status.update(
                    {
                        "state": "error",
                        "cached": bool(pfam_option_stats),
                        "refreshing": False,
                        "message": str(exc),
                    }
                )
            print(
                f"WARNING: failed to refresh PFAM selector stats cache: {exc}",
                file=sys.stderr,
                flush=True,
            )
            return

        metadata = load_cached_pfam_metadata(cache_dir, sorted(refreshed_stats))
        for pfam_id, stats in refreshed_stats.items():
            if isinstance(stats, dict):
                stats["display_name"] = str(metadata.get(pfam_id, {}).get("display_name", "")).strip()

        with pfam_option_stats_lock:
            pfam_option_stats.clear()
            pfam_option_stats.update(refreshed_stats)
            pfam_option_stats_status.clear()
            pfam_option_stats_status.update(
                {
                    "state": "ready",
                    "cached": True,
                    "refreshing": False,
                    "message": "",
                }
            )
        start_background_pfam_metadata_refresh(cache_dir, pfam_option_stats, pfam_option_stats_lock)

    thread = threading.Thread(
        target=refresh,
        daemon=True,
        name="pfam-selector-stats-refresh",
    )
    thread.start()
    return thread


def main() -> None:
    args = parse_args()
    cache_workers = max(1, int(args.cache_workers))
    interface_root = args.interface_dir.resolve()
    cache_dir = args.cache_dir.resolve()
    hierarchy_root = args.hierarchy_dir.resolve() if args.hierarchy_dir is not None else None
    pfam_hmm_path = args.pfam_hmm_path.resolve()
    sequence_dir = args.sequence_dir.resolve()
    pymol_status = validate_pymol_api()
    if not pymol_status.available:
        print(
            "WARNING: "
            f"{pymol_status.reason}. Alignment-based structure outputs will fall back to raw models.",
            file=sys.stderr,
            flush=True,
        )
    dataset_configs = discover_dataset_configs(interface_root, hierarchy_root)
    legacy_store_path = (
        len(dataset_configs) == 1
        and dataset_configs[0].interface_dir == interface_root
        and directory_has_interface_json(interface_root)
    )
    dataset_runtimes: OrderedDict[str, DatasetRuntime] = OrderedDict()
    for dataset_config in dataset_configs:
        interface_store = InterfaceStore(
            interface_store_db_path(
                cache_dir,
                dataset_config,
                use_legacy_path=legacy_store_path,
            ),
            dataset_config.interface_dir,
        )
        dataset_runtimes[dataset_config.key] = DatasetRuntime(
            key=dataset_config.key,
            label=dataset_config.label,
            interface_dir=dataset_config.interface_dir,
            hierarchy_dir=dataset_config.hierarchy_dir,
            interface_store=interface_store,
        )
    default_dataset_key = default_dataset_key_from_configs(
        dataset_configs,
        args.default_dataset,
    )
    default_runtime = dataset_runtimes[default_dataset_key]
    pfam_option_stats_lock = threading.Lock()
    pfam_option_stats, pfam_option_stats_current, pfam_option_stats_signature = (
        load_available_pfam_option_stats(
            cache_dir,
            default_runtime.interface_dir,
        )
    )
    default_runtime.pfam_option_stats.update(pfam_option_stats)
    default_runtime.pfam_option_stats_status.update(
        {
            "state": "ready" if pfam_option_stats_current else "refreshing",
            "cached": bool(pfam_option_stats),
            "refreshing": not pfam_option_stats_current,
            "message": "" if pfam_option_stats_current else "Refreshing PFAM selector stats cache",
        }
    )
    default_runtime.pfam_option_stats_loaded = True
    default_runtime.pfam_option_stats_current = pfam_option_stats_current
    default_runtime.pfam_option_stats_signature = pfam_option_stats_signature
    if not default_runtime.pfam_option_stats_current:
        cached_label = "stale cached stats" if pfam_option_stats else "no cached stats"
        print(
            f"PFAM selector stats cache for dataset {default_runtime.key} is stale or missing ({cached_label}); "
            "serving available files while refreshing in the background.",
            flush=True,
        )
    handler = build_handler(
        default_runtime,
        dataset_runtimes,
        cache_dir,
        pfam_hmm_path,
        sequence_dir,
        cache_workers,
        pfam_option_stats_lock,
    )
    server = ThreadingHTTPServer((args.host, args.port), handler)
    dataset_summary = ", ".join(
        f"{runtime.key}={runtime.interface_dir}"
        for runtime in dataset_runtimes.values()
    )
    print(
        f"Serving Domain Interface Explorer at http://{args.host}:{args.port} "
        f"(interface-dir={args.interface_dir}, cache-dir={args.cache_dir}, "
        f"datasets=[{dataset_summary}], "
        f"pfam-hmm={pfam_hmm_path}, "
        f"sequence-dir={sequence_dir}, "
        f"default-dataset={default_runtime.key}, "
        f"hierarchy-root={hierarchy_root or 'none'}, "
        f"workers={cache_workers}, "
        f"pymol-api={'available' if pymol_status.available else 'unavailable'})"
    )

    def start_background_refreshes() -> None:
        handler._start_dataset_background_tasks(handler, default_runtime)

    background_refresh_timer = threading.Timer(0.5, start_background_refreshes)
    background_refresh_timer.daemon = True
    background_refresh_timer.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
