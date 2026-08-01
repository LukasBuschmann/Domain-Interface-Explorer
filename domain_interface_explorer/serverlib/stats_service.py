from __future__ import annotations

import gzip
import hashlib
import re
import json
import math
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from html import unescape
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from .config import (
    DEFAULT_CACHE_WORKERS,
    INTERPRO_PFAM_ENTRY_API,
    INTERPRO_PFAM_ENTRY_PAGE,
    INTERPRO_PFAM_LIST_API,
    PFAM_INFO_CACHE_VERSION,
    PFAM_INFO_REFRESH_MAX_AGE_SECONDS,
    PFAM_METADATA_CACHE_VERSION,
    PFAM_METADATA_REFRESH_MAX_AGE_SECONDS,
    SELECTOR_STATS_CACHE_VERSION,
)
from .interface_embedding import fragment_ranges
from .interface_files import (
    directory_interface_json_paths,
    interface_file_pfam_id,
)
from .timing import log_event, timed_step

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

CLEAN_COLUMN_IDENTITY_CACHE_LIMIT = 32
CLEAN_COLUMN_IDENTITY_CACHE: dict[str, list[int]] = {}
CLEAN_COLUMN_IDENTITY_CACHE_LOCK = threading.Lock()
CLEAN_COLUMN_IDENTITY_IN_FLIGHT: dict[str, Future[list[int]]] = {}
CLEAN_COLUMN_IDENTITY_CACHE_VERSION = "3"
CLEAN_COLUMN_IDENTITY_BATCH_SIZE = 2048
PFAM_ACCESSION_PATTERN = re.compile(r"^PF\d+$", re.IGNORECASE)
PFAM_METADATA_REFRESH_IN_FLIGHT: set[str] = set()
PFAM_METADATA_REFRESH_IN_FLIGHT_LOCK = threading.Lock()


def open_selector_stats_interface_json(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def load_selector_stats_interface_json(path: Path) -> dict[str, object]:
    stat = path.stat()
    with timed_step(
        "stats",
        "load selector stats interface json",
        file=path.name,
        bytes=stat.st_size,
    ) as timer:
        with open_selector_stats_interface_json(path) as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError(f"expected top-level object in {path}")
        timer.set(top_level_keys=len(payload))
        return payload


def clean_column_identity_cache_key(interface_path: Path) -> str:
    stat = interface_path.stat()
    return hashlib.sha1(
        (
            CLEAN_COLUMN_IDENTITY_CACHE_VERSION
            + "|"
            + str(interface_path.resolve())
            + "|"
            + str(stat.st_size)
            + "|"
            + str(stat.st_mtime_ns)
        ).encode("utf-8")
    ).hexdigest()


def clean_column_identity_cache_path(cache_dir: Path, interface_path: Path) -> Path:
    return cache_dir / "clean_column_identity" / f"{clean_column_identity_cache_key(interface_path)}.json"


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


def raw_interface_alignment_length(interface_payload: dict[str, dict[str, dict]]) -> int:
    alignment_length = 0
    for rows in interface_payload.values():
        if not isinstance(rows, dict):
            continue
        for payload in rows.values():
            if not isinstance(payload, dict):
                continue
            aligned_sequence = payload.get("aligned_seq")
            if isinstance(aligned_sequence, str):
                alignment_length = max(alignment_length, len(aligned_sequence))
    return alignment_length


def interface_fragment_key_from_row_key(row_key: str) -> str:
    parts = str(row_key or "").split("_", 2)
    return parts[1] if len(parts) > 1 else ""


def collect_unique_identity_rows(
    interface_payload: dict[str, dict[str, dict]],
) -> tuple[list[tuple[str, tuple[tuple[int, int], ...]]], int]:
    rows_for_identity: list[tuple[str, tuple[tuple[int, int], ...]]] = []
    seen_row_keys: set[str] = set()
    alignment_length = 0
    for partner_domain in sorted(interface_payload):
        rows = interface_payload.get(partner_domain)
        if not isinstance(rows, dict):
            continue
        for row_key in sorted(rows):
            if row_key in seen_row_keys:
                continue
            payload = rows.get(row_key)
            if not isinstance(payload, dict):
                continue
            seen_row_keys.add(row_key)
            aligned_sequence = payload.get("aligned_seq")
            aligned_sequence = aligned_sequence if isinstance(aligned_sequence, str) else ""
            alignment_length = max(alignment_length, len(aligned_sequence))
            fragment_key = interface_fragment_key_from_row_key(str(row_key))
            rows_for_identity.append((aligned_sequence, tuple(fragment_ranges(fragment_key))))
    return rows_for_identity, alignment_length


def ascii_sequence_array(aligned_sequence: str, alignment_length: int) -> object:
    import numpy as np

    encoded = aligned_sequence.encode("ascii", "replace")[:alignment_length]
    return np.frombuffer(encoded, dtype=np.uint8)


def count_clean_identity_batch(
    batch_rows: list[tuple[str, tuple[tuple[int, int], ...]]],
    alignment_length: int,
) -> object:
    import numpy as np

    batch_size = len(batch_rows)
    sequence_matrix = np.full((batch_size, alignment_length), ord("-"), dtype=np.uint8)
    for row_index, (aligned_sequence, _ranges) in enumerate(batch_rows):
        sequence_values = ascii_sequence_array(aligned_sequence, alignment_length)
        if sequence_values.size:
            sequence_matrix[row_index, : sequence_values.size] = sequence_values

    lower_mask = (sequence_matrix >= ord("a")) & (sequence_matrix <= ord("z"))
    uppercase_matrix = sequence_matrix.copy()
    uppercase_matrix[lower_mask] -= 32
    alpha_mask = (uppercase_matrix >= ord("A")) & (uppercase_matrix <= ord("Z"))

    starts = np.array(
        [ranges[0][0] if ranges else 1 for _sequence, ranges in batch_rows],
        dtype=np.int32,
    )
    residue_ids = np.cumsum(alpha_mask, axis=1, dtype=np.int32) + starts[:, None] - 1
    valid_mask = np.zeros_like(alpha_mask, dtype=bool)

    range_counts = np.array([len(ranges) for _sequence, ranges in batch_rows], dtype=np.int16)
    single_range_rows = np.flatnonzero(range_counts == 1)
    if single_range_rows.size:
        ends = np.array(
            [batch_rows[int(row_index)][1][0][1] for row_index in single_range_rows],
            dtype=np.int32,
        )
        valid_mask[single_range_rows] = (
            alpha_mask[single_range_rows]
            & (residue_ids[single_range_rows] <= ends[:, None])
        )

    for row_index in np.flatnonzero(range_counts > 1).tolist():
        row_allowed = np.zeros((alignment_length,), dtype=bool)
        for start, end in batch_rows[row_index][1]:
            row_allowed |= (residue_ids[row_index] >= start) & (residue_ids[row_index] <= end)
        valid_mask[row_index] = alpha_mask[row_index] & row_allowed

    valid_rows, valid_columns = np.nonzero(valid_mask)
    if valid_columns.size == 0:
        return np.zeros((alignment_length, 26), dtype=np.int64)
    letter_indexes = uppercase_matrix[valid_rows, valid_columns].astype(np.int16) - ord("A")
    flat_indexes = (valid_columns.astype(np.int64) * 26) + letter_indexes.astype(np.int64)
    return np.bincount(
        flat_indexes,
        minlength=alignment_length * 26,
    ).reshape(alignment_length, 26)


def compute_clean_column_identity_direct(interface_payload: dict[str, dict[str, dict]]) -> tuple[list[int], int]:
    import numpy as np

    with timed_step("json", "collect clean identity rows") as timer:
        rows_for_identity, alignment_length = collect_unique_identity_rows(interface_payload)
        timer.set(unique_rows=len(rows_for_identity), alignment_length=alignment_length)
    unique_row_count = len(rows_for_identity)
    if unique_row_count <= 0 or alignment_length <= 0:
        return [0] * alignment_length, 0

    column_letter_counts = np.zeros((alignment_length, 26), dtype=np.int64)
    with timed_step(
        "json",
        "count clean identity residues",
        unique_rows=unique_row_count,
        alignment_length=alignment_length,
        batch_size=CLEAN_COLUMN_IDENTITY_BATCH_SIZE,
    ) as timer:
        batch_count = 0
        for batch_start in range(0, unique_row_count, CLEAN_COLUMN_IDENTITY_BATCH_SIZE):
            batch_count += 1
            batch_rows = rows_for_identity[
                batch_start : batch_start + CLEAN_COLUMN_IDENTITY_BATCH_SIZE
            ]
            column_letter_counts += count_clean_identity_batch(batch_rows, alignment_length)
        timer.set(batches=batch_count)
    identity = (column_letter_counts.max(axis=1) * 100) // unique_row_count
    return identity.astype(int).tolist(), unique_row_count


def remember_clean_column_identity(cache_key: str, column_identity: list[int]) -> None:
    CLEAN_COLUMN_IDENTITY_CACHE[cache_key] = column_identity
    while len(CLEAN_COLUMN_IDENTITY_CACHE) > CLEAN_COLUMN_IDENTITY_CACHE_LIMIT:
        CLEAN_COLUMN_IDENTITY_CACHE.pop(next(iter(CLEAN_COLUMN_IDENTITY_CACHE)))


def load_or_compute_clean_column_identity(
    cache_dir: Path,
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
) -> list[int]:
    cache_key = clean_column_identity_cache_key(interface_path)
    owner = False
    with CLEAN_COLUMN_IDENTITY_CACHE_LOCK:
        cached = CLEAN_COLUMN_IDENTITY_CACHE.get(cache_key)
        if cached is not None:
            log_event(
                "json",
                "reuse clean column identity",
                file=interface_path.name,
                columns=len(cached),
            )
            return cached
        future = CLEAN_COLUMN_IDENTITY_IN_FLIGHT.get(cache_key)
        if future is None:
            future = Future()
            CLEAN_COLUMN_IDENTITY_IN_FLIGHT[cache_key] = future
            owner = True
    if not owner:
        with timed_step("json", "wait for clean column identity", file=interface_path.name):
            return future.result()

    disk_cache_path = clean_column_identity_cache_path(cache_dir, interface_path)
    try:
        if disk_cache_path.exists():
            try:
                with timed_step(
                    "json",
                    "load clean column identity cache",
                    file=disk_cache_path.name,
                ) as timer:
                    with disk_cache_path.open("r", encoding="utf-8") as handle:
                        column_identity = json.load(handle)
                    if not isinstance(column_identity, list):
                        raise ValueError("clean column identity cache is not a list")
                    column_identity = [int(value) for value in column_identity]
                    timer.set(columns=len(column_identity))
                with CLEAN_COLUMN_IDENTITY_CACHE_LOCK:
                    remember_clean_column_identity(cache_key, column_identity)
                    CLEAN_COLUMN_IDENTITY_IN_FLIGHT.pop(cache_key, None)
                    future.set_result(column_identity)
                return column_identity
            except (OSError, ValueError, json.JSONDecodeError):
                log_event("json", "clean column identity cache unreadable", file=disk_cache_path.name)
        with timed_step("json", "compute clean column identity", file=interface_path.name) as timer:
            with timed_step(
                "json",
                "calculate clean column identity",
                file=interface_path.name,
            ) as identity_timer:
                column_identity, unique_rows = compute_clean_column_identity_direct(interface_payload)
                identity_timer.set(columns=len(column_identity), unique_rows=unique_rows)
            try:
                with timed_step(
                    "json",
                    "write clean column identity cache",
                    file=disk_cache_path.name,
                    columns=len(column_identity),
                ):
                    disk_cache_path.parent.mkdir(parents=True, exist_ok=True)
                    with disk_cache_path.open("w", encoding="utf-8") as handle:
                        json.dump(column_identity, handle)
            except OSError:
                log_event("json", "failed to write clean column identity cache", file=disk_cache_path.name)
            timer.set(unique_rows=unique_rows, columns=len(column_identity))
        with CLEAN_COLUMN_IDENTITY_CACHE_LOCK:
            remember_clean_column_identity(cache_key, column_identity)
            CLEAN_COLUMN_IDENTITY_IN_FLIGHT.pop(cache_key, None)
            future.set_result(column_identity)
        return column_identity
    except Exception as exc:
        with CLEAN_COLUMN_IDENTITY_CACHE_LOCK:
            CLEAN_COLUMN_IDENTITY_IN_FLIGHT.pop(cache_key, None)
            future.set_exception(exc)
        raise


def selector_stats_cache_path(cache_dir: Path, interface_dir: Path) -> Path:
    key = hashlib.sha1(f"{interface_dir.resolve()}".encode("utf-8")).hexdigest()
    return cache_dir / "selector_stats" / f"{key}.json"


def read_selector_stats_cache(
    cache_dir: Path,
    interface_dir: Path,
) -> tuple[str, dict[str, dict[str, object]]] | None:
    cache_path = selector_stats_cache_path(cache_dir, interface_dir)
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("r", encoding="utf-8") as handle:
            cached_payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    signature = cached_payload.get("signature")
    pfam_option_stats = cached_payload.get("pfam_option_stats", {})
    if not isinstance(signature, str) or not isinstance(pfam_option_stats, dict):
        return None
    return signature, {
        str(pfam_id): stats
        for pfam_id, stats in pfam_option_stats.items()
        if isinstance(stats, dict)
    }


def write_selector_stats_cache(
    cache_dir: Path,
    interface_dir: Path,
    signature: str,
    pfam_option_stats: dict[str, dict[str, object]],
) -> None:
    cache_path = selector_stats_cache_path(cache_dir, interface_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=cache_path.parent,
        prefix=f".{cache_path.name}.",
        delete=False,
        mode="w",
        encoding="utf-8",
    ) as handle:
        temp_path = Path(handle.name)
        json.dump({"signature": signature, "pfam_option_stats": pfam_option_stats}, handle)
    temp_path.replace(cache_path)


def selector_stats_partial_cache_path(cache_dir: Path, interface_dir: Path) -> Path:
    key = hashlib.sha1(f"{interface_dir.resolve()}".encode("utf-8")).hexdigest()
    return cache_dir / "selector_stats" / f"{key}.partial.json"


def selector_stats_base_stats(stats: dict[str, object]) -> dict[str, object]:
    return {
        str(key): value
        for key, value in stats.items()
        if not str(key).endswith("_category") and key != "display_name"
    }


def read_selector_stats_partial_cache(
    cache_dir: Path,
    interface_dir: Path,
) -> dict[str, tuple[str, dict[str, object]]]:
    cache_path = selector_stats_partial_cache_path(cache_dir, interface_dir)
    if not cache_path.exists():
        return {}
    try:
        with cache_path.open("r", encoding="utf-8") as handle:
            cached_payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    if cached_payload.get("cache_version") != SELECTOR_STATS_CACHE_VERSION:
        return {}
    raw_entries = cached_payload.get("pfam_entries", {})
    if not isinstance(raw_entries, dict):
        return {}
    entries: dict[str, tuple[str, dict[str, object]]] = {}
    for pfam_id, raw_entry in raw_entries.items():
        if not isinstance(raw_entry, dict):
            continue
        signature = raw_entry.get("signature")
        stats = raw_entry.get("stats")
        if isinstance(signature, str) and isinstance(stats, dict):
            entries[str(pfam_id)] = (signature, selector_stats_base_stats(stats))
    return entries


def write_selector_stats_partial_cache(
    cache_dir: Path,
    interface_dir: Path,
    pfam_entries: dict[str, tuple[str, dict[str, object]]],
) -> None:
    cache_path = selector_stats_partial_cache_path(cache_dir, interface_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cache_version": SELECTOR_STATS_CACHE_VERSION,
        "interface_dir": str(interface_dir.resolve()),
        "pfam_entries": {
            str(pfam_id): {
                "signature": signature,
                "stats": selector_stats_base_stats(stats),
            }
            for pfam_id, (signature, stats) in sorted(pfam_entries.items())
        },
    }
    with tempfile.NamedTemporaryFile(
        dir=cache_path.parent,
        prefix=f".{cache_path.name}.",
        delete=False,
        mode="w",
        encoding="utf-8",
    ) as handle:
        temp_path = Path(handle.name)
        json.dump(payload, handle)
    temp_path.replace(cache_path)


def add_selector_stats_categories(pfam_option_stats: dict[str, dict[str, object]]) -> None:
    add_metric_categories(pfam_option_stats, "alignment_length")
    add_metric_categories(pfam_option_stats, "interface_rows")
    add_metric_categories(pfam_option_stats, "interaction_partners")
    add_metric_categories(pfam_option_stats, "avg_interface_residues_per_row")


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


def interface_columns_from_payload(row_payload: object) -> list[int]:
    if not isinstance(row_payload, dict):
        return []
    source_values = row_payload.get("interface_msa_columns_a")
    if source_values is None:
        source_values = row_payload.get("interface_residues_a", [])
    interface_columns: set[int] = set()
    for value in source_values or []:
        try:
            interface_columns.add(int(value))
        except (TypeError, ValueError):
            continue
    return sorted(interface_columns)


def interface_size_from_payload(row_payload: object) -> int:
    return len(interface_columns_from_payload(row_payload))


def domain_length_from_fragment_key(fragment_key: object) -> int:
    try:
        ranges = fragment_ranges(str(fragment_key or ""))
    except (TypeError, ValueError):
        return 0
    total = 0
    for start, end in ranges:
        if end >= start:
            total += end - start + 1
    return total


def domain_a_length_from_payload(row_payload: object, fragment_key: object) -> int:
    if isinstance(row_payload, dict):
        try:
            residue_count = int(row_payload.get("n_residues_a", 0))
        except (TypeError, ValueError):
            residue_count = 0
        if residue_count > 0:
            return residue_count
    return domain_length_from_fragment_key(fragment_key)


def aligned_sequence_residue_count(aligned_sequence: object) -> int:
    if not isinstance(aligned_sequence, str):
        return 0
    return sum(1 for char in aligned_sequence if char.isalpha())


def pfam_row_coverage_percent(domain_residue_count: int, aligned_sequence: object) -> int:
    row_residue_count = aligned_sequence_residue_count(aligned_sequence)
    if domain_residue_count <= 0 or row_residue_count <= 0:
        return 0
    percent = int(round((float(domain_residue_count) / float(row_residue_count)) * 100.0))
    return max(0, min(100, percent))


def pfam_row_coverage_percent_from_payload(row_payload: object, fragment_key: object) -> int:
    if not isinstance(row_payload, dict):
        return 0
    domain_residue_count = domain_a_length_from_payload(row_payload, fragment_key)
    aligned_sequence = row_payload.get("aligned_seq")
    if not isinstance(aligned_sequence, str):
        aligned_sequence = row_payload.get("aligned_sequence")
    return pfam_row_coverage_percent(domain_residue_count, aligned_sequence)


def histogram_entries_from_counts(counts: dict[int, int]) -> list[dict[str, int]]:
    return [
        {"size": size, "count": count}
        for size, count in sorted(counts.items())
    ]


def interface_summary_from_payload(interface_payload: object) -> dict[str, object]:
    dataset_domains: set[tuple[str, str]] = set()
    domain_lengths_by_domain: dict[tuple[str, str], int] = {}
    unique_interfaces: set[tuple[str, tuple[int, ...]]] = set()
    interface_size_histogram: dict[int, int] = {}
    pfam_row_coverage_histogram: dict[int, int] = {}
    plip_interaction_count_histogram: dict[int, int] = {}
    plip_type_counts = {bit: 0 for bit in (1, 2, 4, 8, 16, 32, 64, 128)}
    dataset_interfaces = 0
    if not isinstance(interface_payload, dict):
        return {
            "dataset_domains": 0,
            "dataset_interfaces": 0,
            "unique_interfaces": 0,
            "interface_size_histogram": [],
            "domain_length_histogram": [],
            "pfam_row_coverage_histogram": [],
        }
    for partner_domain, rows in interface_payload.items():
        if not isinstance(rows, dict):
            continue
        for row_key, row_payload in rows.items():
            if not isinstance(row_payload, dict):
                continue
            dataset_interfaces += 1
            row_key_parts = str(row_key or "").split("_", 2)
            protein_id = row_key_parts[0] if len(row_key_parts) > 0 else ""
            fragment_key = row_key_parts[1] if len(row_key_parts) > 1 else ""
            domain_key = (protein_id, fragment_key)
            dataset_domains.add(domain_key)
            if domain_key not in domain_lengths_by_domain:
                domain_length = domain_a_length_from_payload(row_payload, fragment_key)
                if domain_length > 0:
                    domain_lengths_by_domain[domain_key] = domain_length
            coverage_percent = pfam_row_coverage_percent_from_payload(row_payload, fragment_key)
            if coverage_percent > 0:
                pfam_row_coverage_histogram[coverage_percent] = (
                    pfam_row_coverage_histogram.get(coverage_percent, 0) + 1
                )
            columns = interface_columns_from_payload(row_payload)
            interface_size = len(columns)
            if interface_size <= 0:
                continue
            unique_interfaces.add((str(partner_domain), tuple(columns)))
            interface_size_histogram[interface_size] = (
                interface_size_histogram.get(interface_size, 0) + 1
            )
            plip_interactions = row_payload.get("plip_interactions")
            plip_interactions = plip_interactions if isinstance(plip_interactions, list) else []
            plip_count = len(plip_interactions)
            plip_interaction_count_histogram[plip_count] = (
                plip_interaction_count_histogram.get(plip_count, 0) + 1
            )
            for interaction in plip_interactions:
                if not isinstance(interaction, (list, tuple)) or len(interaction) < 3:
                    continue
                mask = int(interaction[2])
                for bit in plip_type_counts:
                    if mask & bit:
                        plip_type_counts[bit] += 1
    domain_length_histogram: dict[int, int] = {}
    for domain_length in domain_lengths_by_domain.values():
        domain_length_histogram[domain_length] = domain_length_histogram.get(domain_length, 0) + 1
    return {
        "dataset_domains": len(dataset_domains),
        "dataset_interfaces": dataset_interfaces,
        "unique_interfaces": len(unique_interfaces),
        "interface_size_histogram": histogram_entries_from_counts(interface_size_histogram),
        "domain_length_histogram": histogram_entries_from_counts(domain_length_histogram),
        "pfam_row_coverage_histogram": histogram_entries_from_counts(pfam_row_coverage_histogram),
        "plip_interaction_count_histogram": histogram_entries_from_counts(
            plip_interaction_count_histogram
        ),
        "plip_type_counts": {
            str(bit): count for bit, count in plip_type_counts.items() if count > 0
        },
    }


def compute_pfam_option_stat(task: tuple[str, list[str]]) -> tuple[str, dict[str, object]]:
    pfam_id, path_strings = task
    interface_columns_by_row: dict[str, set[int]] = {}
    dataset_domains: set[tuple[str, str]] = set()
    domain_lengths_by_domain: dict[tuple[str, str], int] = {}
    unique_interfaces: set[tuple[str, str, tuple[int, ...]]] = set()
    interface_size_histogram: dict[int, int] = {}
    pfam_row_coverage_histogram: dict[int, int] = {}
    interaction_partners: set[str] = set()
    alignment_length = 0
    dataset_interfaces = 0
    for path_string in path_strings:
        path = Path(path_string)
        payload = load_selector_stats_interface_json(path)
        alignment_length = max(alignment_length, raw_interface_alignment_length(payload))
        interaction_partners.update(partner for partner in payload.keys() if isinstance(partner, str))
        for partner_domain, rows in payload.items():
            if not isinstance(rows, dict):
                continue
            for row_key, row_payload in rows.items():
                if not isinstance(row_payload, dict):
                    continue
                row_key_parts = str(row_key or "").split("_", 2)
                protein_id = row_key_parts[0] if len(row_key_parts) > 0 else ""
                fragment_key = row_key_parts[1] if len(row_key_parts) > 1 else ""
                domain_key = (protein_id, fragment_key)
                dataset_domains.add(domain_key)
                if domain_key not in domain_lengths_by_domain:
                    domain_length = domain_a_length_from_payload(row_payload, fragment_key)
                    if domain_length > 0:
                        domain_lengths_by_domain[domain_key] = domain_length
                coverage_percent = pfam_row_coverage_percent_from_payload(row_payload, fragment_key)
                if coverage_percent > 0:
                    pfam_row_coverage_histogram[coverage_percent] = (
                        pfam_row_coverage_histogram.get(coverage_percent, 0) + 1
                    )
                row_columns = interface_columns_by_row.setdefault(row_key, set())
                interface_columns = interface_columns_from_payload(row_payload)
                row_columns.update(interface_columns)
                interface_size = len(interface_columns)
                if interface_size > 0:
                    dataset_interfaces += 1
                    unique_interfaces.add((str(path.name), str(partner_domain), tuple(interface_columns)))
                    interface_size_histogram[interface_size] = (
                        interface_size_histogram.get(interface_size, 0) + 1
                    )
    interface_rows = len(interface_columns_by_row)
    avg_interface_residues_per_row = 0.0
    if interface_rows:
        avg_interface_residues_per_row = (
            sum(len(columns) for columns in interface_columns_by_row.values()) / interface_rows
        )
    domain_length_histogram: dict[int, int] = {}
    for domain_length in domain_lengths_by_domain.values():
        domain_length_histogram[domain_length] = domain_length_histogram.get(domain_length, 0) + 1
    return pfam_id, {
        "alignment_length": alignment_length,
        "dataset_domains": len(dataset_domains),
        "interface_rows": interface_rows,
        "dataset_interfaces": dataset_interfaces,
        "unique_interfaces": len(unique_interfaces),
        "interface_size_histogram": histogram_entries_from_counts(interface_size_histogram),
        "domain_length_histogram": histogram_entries_from_counts(domain_length_histogram),
        "pfam_row_coverage_histogram": histogram_entries_from_counts(pfam_row_coverage_histogram),
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


def cached_pfam_info_display_name(cache_dir: Path, pfam_id: str) -> str:
    cached_info = read_cached_pfam_info(cache_dir, pfam_id)
    if not cached_info:
        return ""
    return normalize_text(cached_info.get("display_name", ""))


def load_cached_pfam_metadata(cache_dir: Path, pfam_ids: list[str]) -> dict[str, dict[str, str]]:
    cached_metadata = read_pfam_metadata_cache(cache_dir)
    metadata_by_pfam: dict[str, dict[str, str]] = {}
    for pfam_id in pfam_ids:
        display_name = normalize_text(cached_metadata.get(pfam_id, {}).get("display_name", ""))
        if not display_name:
            display_name = cached_pfam_info_display_name(cache_dir, pfam_id)
        metadata_by_pfam[pfam_id] = {"display_name": display_name}
    return metadata_by_pfam


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
    stats_lock: threading.Lock | None = None,
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
    if stats_lock is None:
        for pfam_id, stats in pfam_option_stats.items():
            stats["display_name"] = normalize_text(metadata.get(pfam_id, {}).get("display_name", ""))
    else:
        with stats_lock:
            for pfam_id, stats in pfam_option_stats.items():
                stats["display_name"] = normalize_text(
                    metadata.get(pfam_id, {}).get("display_name", "")
                )
    print(
        f"PFAM metadata refresh: cached {len(metadata)} families.",
        file=sys.stderr,
        flush=True,
    )


def start_background_pfam_metadata_refresh(
    cache_dir: Path,
    pfam_option_stats: dict[str, dict[str, object]],
    stats_lock: threading.Lock | None = None,
) -> threading.Thread | None:
    if not pfam_metadata_cache_is_stale(cache_dir):
        return None
    cache_key = str(cache_dir.resolve())
    with PFAM_METADATA_REFRESH_IN_FLIGHT_LOCK:
        if cache_key in PFAM_METADATA_REFRESH_IN_FLIGHT:
            return None
        PFAM_METADATA_REFRESH_IN_FLIGHT.add(cache_key)

    def refresh_and_clear() -> None:
        try:
            refresh_pfam_metadata_cache(cache_dir, pfam_option_stats, stats_lock)
        finally:
            with PFAM_METADATA_REFRESH_IN_FLIGHT_LOCK:
                PFAM_METADATA_REFRESH_IN_FLIGHT.discard(cache_key)

    thread = threading.Thread(
        target=refresh_and_clear,
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


def build_pfam_option_stats(
    interface_dir: Path,
    cache_workers: int = DEFAULT_CACHE_WORKERS,
    cache_dir: Path | None = None,
) -> dict[str, dict[str, object]]:
    grouped_interface_files: dict[str, list[Path]] = {}
    for path in directory_json_paths(interface_dir):
        pfam_id = interface_file_pfam_id(path)
        grouped_interface_files.setdefault(pfam_id, []).append(path)

    pfam_records = [
        (pfam_id, sorted(paths, key=lambda item: str(item)), source_signature(paths))
        for pfam_id, paths in sorted(grouped_interface_files.items())
    ]
    partial_entries = (
        read_selector_stats_partial_cache(cache_dir, interface_dir)
        if cache_dir is not None
        else {}
    )
    current_partial_entries: dict[str, tuple[str, dict[str, object]]] = {}
    pfam_option_stats: dict[str, dict[str, object]] = {}
    tasks: list[tuple[str, list[str]]] = []
    task_signatures: dict[str, str] = {}

    for pfam_id, paths, pfam_signature in pfam_records:
        cached_entry = partial_entries.get(pfam_id)
        if cached_entry is not None and cached_entry[0] == pfam_signature:
            stats = dict(cached_entry[1])
            pfam_option_stats[pfam_id] = stats
            current_partial_entries[pfam_id] = (pfam_signature, selector_stats_base_stats(stats))
            continue
        tasks.append((pfam_id, [str(path) for path in paths]))
        task_signatures[pfam_id] = pfam_signature

    cached_count = len(pfam_option_stats)
    if cache_dir is not None and cached_count > 0:
        log_event(
            "stats",
            "reuse partial PFAM selector stats",
            cached=cached_count,
            missing=len(tasks),
            total=len(pfam_records),
        )
    if not tasks:
        add_selector_stats_categories(pfam_option_stats)
        return pfam_option_stats

    worker_count = min(len(tasks), max(1, int(cache_workers)))
    dirty_count = 0
    last_write = time.monotonic()

    def remember_result(pfam_id: str, stats: dict[str, object]) -> None:
        nonlocal dirty_count, last_write
        pfam_option_stats[pfam_id] = stats
        if cache_dir is None:
            return
        signature = task_signatures.get(pfam_id)
        if signature is None:
            return
        current_partial_entries[pfam_id] = (signature, selector_stats_base_stats(stats))
        dirty_count += 1
        now = time.monotonic()
        if dirty_count >= 25 or now - last_write >= 5.0:
            write_selector_stats_partial_cache(cache_dir, interface_dir, current_partial_entries)
            dirty_count = 0
            last_write = now

    if worker_count <= 1:
        results_iter = map(compute_pfam_option_stat, tasks)
        if tqdm is not None:
            results_iter = tqdm(
                results_iter,
                total=len(tasks),
                desc=f"Building PFAM selector stats ({cached_count} cached)",
                unit="pfam",
            )
        for pfam_id, stats in results_iter:
            remember_result(pfam_id, stats)
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
                    desc=f"Building PFAM selector stats ({worker_count} workers, {cached_count} cached)",
                    unit="pfam",
                    miniters=1,
                    mininterval=0,
                )
            for future in completed_futures:
                pfam_id, stats = future.result()
                remember_result(pfam_id, stats)

    if cache_dir is not None and dirty_count > 0:
        write_selector_stats_partial_cache(cache_dir, interface_dir, current_partial_entries)
    add_selector_stats_categories(pfam_option_stats)
    return pfam_option_stats


def merge_cached_pfam_option_stats_with_metadata(
    cache_dir: Path,
    pfam_option_stats: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    pfam_metadata = load_cached_pfam_metadata(cache_dir, sorted(pfam_option_stats))
    return merge_pfam_metadata(pfam_option_stats, pfam_metadata)


def compute_and_cache_pfam_option_stats(
    cache_dir: Path,
    interface_dir: Path,
    cache_workers: int = DEFAULT_CACHE_WORKERS,
    signature: str | None = None,
) -> dict[str, dict[str, object]]:
    print(f"Building PFAM selector stats cache ({max(1, int(cache_workers))} workers max)...")
    if signature is None:
        signature = selector_stats_signature(interface_dir)
    pfam_option_stats = build_pfam_option_stats(interface_dir, cache_workers, cache_dir)
    write_selector_stats_cache(cache_dir, interface_dir, signature, pfam_option_stats)
    return merge_cached_pfam_option_stats_with_metadata(cache_dir, pfam_option_stats)


def load_partial_pfam_option_stats(
    cache_dir: Path,
    interface_dir: Path,
) -> dict[str, dict[str, object]]:
    partial_entries = read_selector_stats_partial_cache(cache_dir, interface_dir)
    if not partial_entries:
        return {}
    grouped_interface_files: dict[str, list[Path]] = {}
    for path in directory_json_paths(interface_dir):
        pfam_id = interface_file_pfam_id(path)
        grouped_interface_files.setdefault(pfam_id, []).append(path)
    pfam_option_stats: dict[str, dict[str, object]] = {}
    for pfam_id, paths in grouped_interface_files.items():
        cached_entry = partial_entries.get(pfam_id)
        if cached_entry is None:
            continue
        pfam_signature = source_signature(paths)
        if cached_entry[0] == pfam_signature:
            pfam_option_stats[pfam_id] = dict(cached_entry[1])
    if pfam_option_stats:
        add_selector_stats_categories(pfam_option_stats)
    return pfam_option_stats


def load_available_pfam_option_stats(
    cache_dir: Path,
    interface_dir: Path,
) -> tuple[dict[str, dict[str, object]], bool, str]:
    signature = selector_stats_signature(interface_dir)
    cached = read_selector_stats_cache(cache_dir, interface_dir)
    if cached is None:
        partial_stats = load_partial_pfam_option_stats(cache_dir, interface_dir)
        return merge_cached_pfam_option_stats_with_metadata(cache_dir, partial_stats), False, signature
    cached_signature, pfam_option_stats = cached
    if cached_signature == signature:
        return (
            merge_cached_pfam_option_stats_with_metadata(cache_dir, pfam_option_stats),
            True,
            signature,
        )
    partial_stats = load_partial_pfam_option_stats(cache_dir, interface_dir)
    if partial_stats:
        return merge_cached_pfam_option_stats_with_metadata(cache_dir, partial_stats), False, signature
    return merge_cached_pfam_option_stats_with_metadata(cache_dir, pfam_option_stats), False, signature


def load_cached_pfam_option_stats(
    cache_dir: Path,
    interface_dir: Path,
    cache_workers: int = DEFAULT_CACHE_WORKERS,
) -> dict[str, dict[str, object]]:
    pfam_option_stats, cache_is_current, signature = load_available_pfam_option_stats(
        cache_dir,
        interface_dir,
    )
    if cache_is_current:
        return pfam_option_stats
    return compute_and_cache_pfam_option_stats(
        cache_dir,
        interface_dir,
        cache_workers,
        signature=signature,
    )
