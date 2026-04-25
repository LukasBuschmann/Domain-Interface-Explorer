from __future__ import annotations

import hashlib
import re
import json
import math
import os
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from html import unescape
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from .config import (
    INTERPRO_PFAM_ENTRY_API,
    INTERPRO_PFAM_ENTRY_PAGE,
    INTERPRO_PFAM_LIST_API,
    PFAM_INFO_CACHE_VERSION,
    PFAM_INFO_REFRESH_MAX_AGE_SECONDS,
    PFAM_METADATA_CACHE_VERSION,
    PFAM_METADATA_REFRESH_MAX_AGE_SECONDS,
    SELECTOR_STATS_CACHE_VERSION,
)
from .interface_embedding import build_interface_alignment_rows
from .interface_files import (
    directory_interface_json_paths,
    interface_file_pfam_id,
    load_interface_json,
)

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

CLEAN_COLUMN_IDENTITY_CACHE_LIMIT = 32
CLEAN_COLUMN_IDENTITY_CACHE: dict[str, list[int]] = {}
PFAM_ACCESSION_PATTERN = re.compile(r"^PF\d+$", re.IGNORECASE)


def clean_column_identity_cache_key(interface_path: Path) -> str:
    stat = interface_path.stat()
    return "|".join(
        (
            str(interface_path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
        )
    )


def calc_identity_from_values(values: list[str]) -> int:
    if not values:
        return 0
    counts: dict[str, int] = {}
    for value in values:
        normalized = str(value).upper()
        counts[normalized] = counts.get(normalized, 0) + 1
    total = sum(counts.values())
    for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        if value.isalpha():
            return (count * 100) // total
    return 0


def calc_column_identity(aligned_sequences: list[str]) -> list[int]:
    if not aligned_sequences:
        return []
    return [
        calc_identity_from_values(list(column_values))
        for column_values in zip(*aligned_sequences)
    ]


def load_or_compute_clean_column_identity(
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
) -> list[int]:
    cache_key = clean_column_identity_cache_key(interface_path)
    cached = CLEAN_COLUMN_IDENTITY_CACHE.get(cache_key)
    if cached is not None:
        return cached
    rows, _alignment_length = build_interface_alignment_rows(interface_payload)
    unique_sequences_by_row_key: dict[str, str] = {}
    for row in rows:
        row_key = str(row.get("interface_row_key") or "")
        unique_sequences_by_row_key.setdefault(row_key, str(row.get("aligned_sequence") or ""))
    column_identity = calc_column_identity(list(unique_sequences_by_row_key.values()))
    CLEAN_COLUMN_IDENTITY_CACHE[cache_key] = column_identity
    while len(CLEAN_COLUMN_IDENTITY_CACHE) > CLEAN_COLUMN_IDENTITY_CACHE_LIMIT:
        CLEAN_COLUMN_IDENTITY_CACHE.pop(next(iter(CLEAN_COLUMN_IDENTITY_CACHE)))
    return column_identity


def selector_stats_cache_path(cache_dir: Path, interface_dir: Path) -> Path:
    key = hashlib.sha1(f"{interface_dir.resolve()}".encode("utf-8")).hexdigest()
    return cache_dir / "selector_stats" / f"{key}.json"


def pfam_metadata_cache_path(cache_dir: Path) -> Path:
    return cache_dir / "pfam_metadata" / "interpro_pfam.json"


def pfam_info_cache_path(cache_dir: Path, pfam_id: str) -> Path:
    return cache_dir / "pfam_info" / f"{pfam_id}.json"


def source_signature(paths: list[Path]) -> str:
    hasher = hashlib.sha1()
    for path in sorted(paths, key=lambda item: str(item)):
        if not path.exists():
            hasher.update(f"{path}:missing\n".encode("utf-8"))
            continue
        stat = path.stat()
        hasher.update(f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}\n".encode("utf-8"))
    return hasher.hexdigest()


def directory_json_paths(directory: Path) -> list[Path]:
    return directory_interface_json_paths(directory)


def selector_stats_signature(interface_dir: Path) -> str:
    return hashlib.sha1(
        (
            SELECTOR_STATS_CACHE_VERSION
            + "|"
            + source_signature(directory_json_paths(interface_dir))
        ).encode("utf-8")
    ).hexdigest()


def classify_z_score(z_score: float) -> str:
    if z_score <= -3.0:
        return "very_small"
    if z_score <= -1.0:
        return "small"
    if z_score < 1.0:
        return "normal"
    if z_score < 3.0:
        return "big"
    return "very_big"


def add_metric_categories(pfam_option_stats: dict[str, dict[str, object]], metric_key: str) -> None:
    raw_values = [float(stats[metric_key]) for stats in pfam_option_stats.values()]
    values = [math.log1p(value) for value in raw_values]
    if len(values) < 2:
        for stats in pfam_option_stats.values():
            stats[f"{metric_key}_category"] = "normal"
        return
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    std_dev = math.sqrt(variance)
    for stats in pfam_option_stats.values():
        if std_dev == 0:
            category = "normal"
        else:
            z_score = (math.log1p(float(stats[metric_key])) - mean) / std_dev
            category = classify_z_score(z_score)
        stats[f"{metric_key}_category"] = category


def interface_size_from_payload(row_payload: object) -> int:
    if not isinstance(row_payload, dict):
        return 0
    source_values = row_payload.get("interface_msa_columns_a")
    if source_values is None:
        source_values = row_payload.get("interface_residues_a", [])
    interface_columns: set[int] = set()
    for value in source_values or []:
        try:
            interface_columns.add(int(value))
        except (TypeError, ValueError):
            continue
    return len(interface_columns)


def compute_pfam_option_stat(task: tuple[str, list[str]]) -> tuple[str, dict[str, object]]:
    pfam_id, path_strings = task
    interface_columns_by_row: dict[str, set[int]] = {}
    dataset_domains: set[tuple[str, str]] = set()
    interaction_partners: set[str] = set()
    alignment_length = 0
    dataset_interfaces = 0
    for path_string in path_strings:
        path = Path(path_string)
        payload = load_interface_json(path)
        _rows, file_alignment_length = build_interface_alignment_rows(payload)
        alignment_length = max(alignment_length, file_alignment_length)
        interaction_partners.update(partner for partner in payload.keys() if isinstance(partner, str))
        for rows in payload.values():
            if not isinstance(rows, dict):
                continue
            for row_key, row_payload in rows.items():
                if not isinstance(row_payload, dict):
                    continue
                row_key_parts = str(row_key or "").split("_", 2)
                protein_id = row_key_parts[0] if len(row_key_parts) > 0 else ""
                fragment_key = row_key_parts[1] if len(row_key_parts) > 1 else ""
                dataset_domains.add((protein_id, fragment_key))
                row_columns = interface_columns_by_row.setdefault(row_key, set())
                source_values = row_payload.get("interface_msa_columns_a")
                if source_values is None:
                    source_values = row_payload.get("interface_residues_a", [])
                interface_size = interface_size_from_payload(row_payload)
                for value in source_values or []:
                    try:
                        row_columns.add(int(value))
                    except (TypeError, ValueError):
                        continue
                if interface_size > 0:
                    dataset_interfaces += 1
    interface_rows = len(interface_columns_by_row)
    avg_interface_residues_per_row = 0.0
    if interface_rows:
        avg_interface_residues_per_row = sum(len(columns) for columns in interface_columns_by_row.values()) / interface_rows
    return pfam_id, {
        "alignment_length": alignment_length,
        "dataset_domains": len(dataset_domains),
        "interface_rows": interface_rows,
        "dataset_interfaces": dataset_interfaces,
        "interaction_partners": len(interaction_partners),
        "avg_interface_residues_per_row": round(avg_interface_residues_per_row, 2),
    }


def normalize_text(value: object) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def normalize_pfam_accession(value: object) -> str:
    accession = normalize_text(value).upper()
    if PFAM_ACCESSION_PATTERN.fullmatch(accession):
        return accession
    return ""


def strip_html_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    text = re.sub(r"\[\[cite:[^\]]+\]\]", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+([.,;:!?])", r"\1", text)
    return normalize_text(text)


def normalize_description_text(value: object) -> str:
    if isinstance(value, list):
        parts = [
            strip_html_text((entry or {}).get("text", ""))
            for entry in value
            if isinstance(entry, dict)
        ]
        return "\n\n".join(part for part in parts if part)
    return strip_html_text(value)


def normalize_named_reference(value: object) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    accession = normalize_text(value.get("accession", ""))
    name = normalize_text(value.get("name", ""))
    if not accession and not name:
        return None
    return {"accession": accession, "name": name}


def normalize_count(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def normalize_name_payload(value: object) -> tuple[str, str]:
    if isinstance(value, dict):
        return (
            normalize_text(value.get("name", "")),
            normalize_text(value.get("short", "")),
        )
    if isinstance(value, str):
        return normalize_text(value), ""
    return "", ""


def parse_pfam_info_payload(payload: object, pfam_id: str) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("unexpected InterPro API response: expected object payload")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("unexpected InterPro API response: missing metadata object")
    accession = normalize_pfam_accession(metadata.get("accession", "")) or pfam_id
    display_name, short_name = normalize_name_payload(metadata.get("name"))
    counters = metadata.get("counters") if isinstance(metadata.get("counters"), dict) else {}
    structural_models = (
        counters.get("structural_models")
        if isinstance(counters.get("structural_models"), dict)
        else {}
    )
    return {
        "pfam_id": accession,
        "display_name": display_name,
        "short_name": short_name,
        "description": normalize_description_text(metadata.get("description")),
        "type": normalize_text(metadata.get("type", "")),
        "integrated_interpro": normalize_text(metadata.get("integrated", "")),
        "set_info": normalize_named_reference(metadata.get("set_info")),
        "representative_structure": normalize_named_reference(
            metadata.get("representative_structure")
        ),
        "stats": {
            "proteins": normalize_count(counters.get("proteins")),
            "matches": normalize_count(counters.get("matches")),
            "proteomes": normalize_count(counters.get("proteomes")),
            "taxa": normalize_count(counters.get("taxa")),
            "structures": normalize_count(counters.get("structures")),
            "domain_architectures": normalize_count(counters.get("domain_architectures")),
            "alphafold_models": normalize_count(structural_models.get("alphafold")),
        },
        "link_url": INTERPRO_PFAM_ENTRY_PAGE.format(accession=accession),
    }


def read_cached_pfam_info(cache_dir: Path, pfam_id: str) -> dict[str, object] | None:
    cache_path = pfam_info_cache_path(cache_dir, pfam_id)
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("version") != PFAM_INFO_CACHE_VERSION:
        return None
    cached_data = payload.get("data")
    if not isinstance(cached_data, dict):
        return None
    if normalize_pfam_accession(cached_data.get("pfam_id", "")) != pfam_id:
        return None
    return cached_data


def write_pfam_info_cache(
    cache_dir: Path,
    pfam_id: str,
    pfam_info: dict[str, object],
) -> None:
    cache_path = pfam_info_cache_path(cache_dir, pfam_id)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=cache_path.parent,
        prefix=f".{cache_path.name}.",
        delete=False,
        mode="w",
        encoding="utf-8",
    ) as handle:
        temp_path = Path(handle.name)
        json.dump({"version": PFAM_INFO_CACHE_VERSION, "data": pfam_info}, handle)
    temp_path.replace(cache_path)


def pfam_info_cache_is_stale(cache_dir: Path, pfam_id: str) -> bool:
    cache_path = pfam_info_cache_path(cache_dir, pfam_id)
    if not cache_path.exists():
        return True
    if read_cached_pfam_info(cache_dir, pfam_id) is None:
        return True
    age_seconds = time.time() - cache_path.stat().st_mtime
    return age_seconds >= PFAM_INFO_REFRESH_MAX_AGE_SECONDS


def fetch_pfam_info(pfam_id: str, timeout: float = 10.0) -> dict[str, object]:
    request = Request(
        INTERPRO_PFAM_ENTRY_API.format(accession=pfam_id),
        headers={"Accept": "application/json"},
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    return parse_pfam_info_payload(payload, pfam_id)


def load_or_fetch_pfam_info(cache_dir: Path, pfam_id: str) -> dict[str, object]:
    normalized_pfam_id = normalize_pfam_accession(pfam_id)
    if not normalized_pfam_id:
        raise ValueError("invalid PFAM accession")
    cached_info = read_cached_pfam_info(cache_dir, normalized_pfam_id)
    if cached_info is not None and not pfam_info_cache_is_stale(cache_dir, normalized_pfam_id):
        return cached_info
    try:
        pfam_info = fetch_pfam_info(normalized_pfam_id)
    except Exception:
        if cached_info is not None:
            return cached_info
        raise
    write_pfam_info_cache(cache_dir, normalized_pfam_id, pfam_info)
    return pfam_info


def read_pfam_metadata_cache(cache_dir: Path) -> dict[str, dict[str, str]]:
    cache_path = pfam_metadata_cache_path(cache_dir)
    if not cache_path.exists():
        return {}
    try:
        with cache_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    if payload.get("version") != PFAM_METADATA_CACHE_VERSION:
        return {}
    raw_metadata = payload.get("metadata", {})
    if not isinstance(raw_metadata, dict):
        return {}
    return {
        str(pfam_id): {
            "display_name": normalize_text((entry or {}).get("display_name", "")),
        }
        for pfam_id, entry in raw_metadata.items()
        if isinstance(entry, dict)
    }


def load_cached_pfam_metadata(cache_dir: Path, pfam_ids: list[str]) -> dict[str, dict[str, str]]:
    cached_metadata = read_pfam_metadata_cache(cache_dir)
    return {pfam_id: cached_metadata.get(pfam_id, {"display_name": ""}) for pfam_id in pfam_ids}


def write_pfam_metadata_cache(cache_dir: Path, metadata: dict[str, dict[str, str]]) -> None:
    cache_path = pfam_metadata_cache_path(cache_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=cache_path.parent,
        prefix=f".{cache_path.name}.",
        delete=False,
        mode="w",
        encoding="utf-8",
    ) as handle:
        temp_path = Path(handle.name)
        json.dump({"version": PFAM_METADATA_CACHE_VERSION, "metadata": metadata}, handle)
    temp_path.replace(cache_path)


def pfam_metadata_cache_is_stale(cache_dir: Path) -> bool:
    cache_path = pfam_metadata_cache_path(cache_dir)
    if not cache_path.exists():
        return True
    if not read_pfam_metadata_cache(cache_dir):
        return True
    age_seconds = time.time() - cache_path.stat().st_mtime
    return age_seconds >= PFAM_METADATA_REFRESH_MAX_AGE_SECONDS


def fetch_all_pfam_metadata(
    timeout: float = 10.0,
    progress_callback: Callable[[int, int | None, int], None] | None = None,
) -> dict[str, dict[str, str]]:
    next_url: str | None = INTERPRO_PFAM_LIST_API
    metadata_by_accession: dict[str, dict[str, str]] = {}
    page_index = 0
    while next_url:
        page_index += 1
        request = Request(next_url, headers={"Accept": "application/json"})
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
        if not isinstance(payload, dict):
            raise ValueError("unexpected InterPro API response: expected object payload")
        results = payload.get("results")
        if not isinstance(results, list):
            raise ValueError("unexpected InterPro API response: missing results list")
        for item in results:
            if not isinstance(item, dict):
                continue
            metadata = item.get("metadata")
            if not isinstance(metadata, dict):
                continue
            accession = normalize_text(metadata.get("accession", ""))
            if not accession:
                continue
            metadata_by_accession[accession] = {
                "display_name": normalize_text(metadata.get("name", "")),
            }
        if progress_callback is not None:
            total_count = payload.get("count")
            progress_callback(
                received_count=len(metadata_by_accession),
                total_count=total_count if isinstance(total_count, int) else None,
                page_index=page_index,
            )
        next_value = payload.get("next")
        next_url = normalize_text(next_value) or None
        if next_url:
            time.sleep(0.1)
    return metadata_by_accession


def refresh_pfam_metadata_cache(
    cache_dir: Path,
    pfam_option_stats: dict[str, dict[str, object]],
) -> None:
    def log_progress(received_count: int, total_count: int | None, page_index: int) -> None:
        total_label = str(total_count) if total_count is not None else "?"
        print(
            f"PFAM metadata refresh: received {received_count}/{total_label} families "
            f"(page {page_index})",
            file=sys.stderr,
            flush=True,
        )

    print(
        "PFAM metadata refresh: starting bulk InterPro family fetch...",
        file=sys.stderr,
        flush=True,
    )
    try:
        metadata = fetch_all_pfam_metadata(progress_callback=log_progress)
    except URLError as exc:
        print(
            "WARNING: "
            f"could not refresh PFAM metadata from InterPro ({exc}). "
            "Continuing with cached PFAM names.",
            file=sys.stderr,
            flush=True,
        )
        return
    except Exception as exc:
        print(
            f"WARNING: failed to refresh PFAM metadata from InterPro: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return

    write_pfam_metadata_cache(cache_dir, metadata)
    for pfam_id, stats in pfam_option_stats.items():
        stats["display_name"] = normalize_text(metadata.get(pfam_id, {}).get("display_name", ""))
    print(
        f"PFAM metadata refresh: cached {len(metadata)} families.",
        file=sys.stderr,
        flush=True,
    )


def start_background_pfam_metadata_refresh(
    cache_dir: Path,
    pfam_option_stats: dict[str, dict[str, object]],
) -> threading.Thread | None:
    if not pfam_option_stats or not pfam_metadata_cache_is_stale(cache_dir):
        return None
    thread = threading.Thread(
        target=refresh_pfam_metadata_cache,
        args=(cache_dir, pfam_option_stats),
        daemon=True,
        name="pfam-metadata-refresh",
    )
    thread.start()
    return thread


def merge_pfam_metadata(
    pfam_option_stats: dict[str, dict[str, object]],
    pfam_metadata: dict[str, dict[str, str]],
) -> dict[str, dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}
    for pfam_id, stats in pfam_option_stats.items():
        metadata = pfam_metadata.get(pfam_id, {})
        merged[pfam_id] = {
            **stats,
            "display_name": normalize_text(metadata.get("display_name", "")),
        }
    return merged


def build_pfam_option_stats(interface_dir: Path) -> dict[str, dict[str, object]]:
    grouped_interface_files: dict[str, list[Path]] = {}
    for path in directory_json_paths(interface_dir):
        pfam_id = interface_file_pfam_id(path)
        grouped_interface_files.setdefault(pfam_id, []).append(path)
    pfam_items = sorted(grouped_interface_files.items())
    tasks = [
        (
            pfam_id,
            [str(path) for path in paths],
        )
        for pfam_id, paths in pfam_items
    ]
    pfam_option_stats: dict[str, dict[str, object]] = {}
    worker_count = min(len(tasks), max(1, min(16, os.cpu_count() or 1)))
    if worker_count <= 1:
        results_iter = map(compute_pfam_option_stat, tasks)
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(compute_pfam_option_stat, task): task[0]
                for task in tasks
            }
            completed_futures = as_completed(futures)
            if tqdm is not None:
                completed_futures = tqdm(
                    completed_futures,
                    total=len(tasks),
                    desc=f"Building PFAM selector stats ({worker_count} workers)",
                    unit="pfam",
                    miniters=1,
                    mininterval=0,
                )
            for future in completed_futures:
                pfam_id, stats = future.result()
                pfam_option_stats[pfam_id] = stats
        add_metric_categories(pfam_option_stats, "alignment_length")
        add_metric_categories(pfam_option_stats, "interface_rows")
        add_metric_categories(pfam_option_stats, "interaction_partners")
        add_metric_categories(pfam_option_stats, "avg_interface_residues_per_row")
        return pfam_option_stats
    if tqdm is not None:
        results_iter = tqdm(results_iter, total=len(tasks), desc="Building PFAM selector stats", unit="pfam")
    for pfam_id, stats in results_iter:
        pfam_option_stats[pfam_id] = stats
    add_metric_categories(pfam_option_stats, "alignment_length")
    add_metric_categories(pfam_option_stats, "interface_rows")
    add_metric_categories(pfam_option_stats, "interaction_partners")
    add_metric_categories(pfam_option_stats, "avg_interface_residues_per_row")
    return pfam_option_stats


def load_cached_pfam_option_stats(
    cache_dir: Path,
    interface_dir: Path,
) -> dict[str, dict[str, object]]:
    cache_path = selector_stats_cache_path(cache_dir, interface_dir)
    signature = selector_stats_signature(interface_dir)
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as handle:
            cached_payload = json.load(handle)
        if cached_payload.get("signature") == signature:
            pfam_option_stats = cached_payload.get("pfam_option_stats", {})
            if not isinstance(pfam_option_stats, dict):
                pfam_option_stats = {}
            pfam_metadata = load_cached_pfam_metadata(cache_dir, sorted(pfam_option_stats))
            return merge_pfam_metadata(pfam_option_stats, pfam_metadata)
    print("Building PFAM selector stats cache...")
    pfam_option_stats = build_pfam_option_stats(interface_dir)
    pfam_metadata = load_cached_pfam_metadata(cache_dir, sorted(pfam_option_stats))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as handle:
        json.dump({"signature": signature, "pfam_option_stats": pfam_option_stats}, handle)
    return merge_pfam_metadata(pfam_option_stats, pfam_metadata)
