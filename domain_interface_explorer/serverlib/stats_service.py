from __future__ import annotations

import hashlib
import json
import math
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from .config import SELECTOR_STATS_CACHE_VERSION
from .interface_embedding import build_interface_alignment_rows

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

CLEAN_COLUMN_IDENTITY_CACHE_LIMIT = 32
CLEAN_COLUMN_IDENTITY_CACHE: dict[str, list[int]] = {}


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
    if not directory.exists():
        return []
    return sorted(path for path in directory.iterdir() if path.is_file() and path.suffix == ".json")


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


def compute_pfam_option_stat(task: tuple[str, list[str]]) -> tuple[str, dict[str, object]]:
    pfam_id, path_strings = task
    interface_columns_by_row: dict[str, set[int]] = {}
    interaction_partners: set[str] = set()
    alignment_length = 0
    for path_string in path_strings:
        path = Path(path_string)
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        _rows, file_alignment_length = build_interface_alignment_rows(payload)
        alignment_length = max(alignment_length, file_alignment_length)
        interaction_partners.update(partner for partner in payload.keys() if isinstance(partner, str))
        for rows in payload.values():
            if not isinstance(rows, dict):
                continue
            for row_key, row_payload in rows.items():
                if not isinstance(row_payload, dict):
                    continue
                row_columns = interface_columns_by_row.setdefault(row_key, set())
                source_values = row_payload.get("interface_msa_columns_a")
                if source_values is None:
                    source_values = row_payload.get("interface_residues_a", [])
                for value in source_values or []:
                    try:
                        row_columns.add(int(value))
                    except (TypeError, ValueError):
                        continue
    interface_rows = len(interface_columns_by_row)
    avg_interface_residues_per_row = 0.0
    if interface_rows:
        avg_interface_residues_per_row = sum(len(columns) for columns in interface_columns_by_row.values()) / interface_rows
    return pfam_id, {
        "alignment_length": alignment_length,
        "interface_rows": interface_rows,
        "interaction_partners": len(interaction_partners),
        "avg_interface_residues_per_row": round(avg_interface_residues_per_row, 2),
    }


def build_pfam_option_stats(interface_dir: Path) -> dict[str, dict[str, object]]:
    grouped_interface_files: dict[str, list[Path]] = {}
    for path in directory_json_paths(interface_dir):
        pfam_id = path.stem.split("_", maxsplit=1)[0]
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
            return cached_payload.get("pfam_option_stats", {})
    print("Building PFAM selector stats cache...")
    pfam_option_stats = build_pfam_option_stats(interface_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as handle:
        json.dump({"signature": signature, "pfam_option_stats": pfam_option_stats}, handle)
    return pfam_option_stats
