from __future__ import annotations

import hashlib
import csv
import heapq
import json
import math
import random
import threading
from collections import OrderedDict
from concurrent.futures import Future
from pathlib import Path

import numpy as np

from .config import (
    CLUSTERING_CACHE_VERSION,
    DEFAULT_CACHE_WORKERS,
    DEFAULT_CLUSTER_COMPARE_LIMIT,
    DEFAULT_CLUSTER_MIN_SIZE,
    DEFAULT_CLUSTER_SELECTION_EPSILON,
    DEFAULT_CLUSTERING_METHOD,
    DEFAULT_DISTANCE_METRIC,
    DEFAULT_HIERARCHICAL_DISTANCE_THRESHOLD,
    DEFAULT_HIERARCHICAL_LINKAGE,
    DEFAULT_HIERARCHICAL_MIN_CLUSTER_SIZE,
    DEFAULT_HIERARCHICAL_N_CLUSTERS,
    DEFAULT_HIERARCHICAL_PERSISTENCE_MIN_LIFETIME,
    DEFAULT_HIERARCHICAL_TARGET,
    DEFAULT_EMBEDDING_DISTANCE,
    DEFAULT_EMBEDDING_METHOD,
    DEFAULT_MIN_INTERFACE_SIZE,
    DEFAULT_TSNE_EARLY_EXAGGERATION,
    DEFAULT_TSNE_EARLY_EXAGGERATION_ITER,
    DEFAULT_TSNE_LEARNING_RATE,
    DEFAULT_TSNE_MAX_ITER,
    DEFAULT_TSNE_NEIGHBORS,
    DEFAULT_TSNE_RANDOM_STATE,
    DEFAULT_TSNE_THETA,
    DISTANCE_DATA_CACHE_LIMIT,
    EMBEDDING_CACHE_VERSION,
    HIERARCHY_CACHE_VERSION,
)
from .interface_files import interface_file_pfam_id
from .timing import log_event, timed_step

DISTANCE_DATA_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
DISTANCE_DATA_CACHE_LOCK = threading.Lock()
DISTANCE_DATA_IN_FLIGHT: dict[str, Future] = {}
COMPRESSED_INTERFACE_CACHE_LIMIT = 4
COMPRESSED_INTERFACE_CACHE: OrderedDict[str, tuple[list[dict[str, object]], dict[str, object]]] = OrderedDict()
COMPRESSED_INTERFACE_CACHE_LOCK = threading.Lock()
COMPRESSED_INTERFACE_IN_FLIGHT: dict[str, Future] = {}
PRECOMPUTED_HIERARCHY_CACHE_LIMIT = 4
PRECOMPUTED_HIERARCHY_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
PRECOMPUTED_HIERARCHY_CACHE_LOCK = threading.Lock()
PRECOMPUTED_HIERARCHY_IN_FLIGHT: dict[str, Future] = {}
INTERFACE_COMPRESSION_MODE = "partner_domain+interface_columns"
DISTANCE_CACHE_FORMAT_VERSION = 1
DISTANCE_CACHE_SCALE = 65535
METRIC_CODES = {
    "jaccard": 0,
    "overlap": 1,
    "dice": 2,
}
VALID_HIERARCHICAL_LINKAGES = {
    "single",
    "complete",
    "average",
    "average_deduplicated",
    "weighted",
}
VALID_HIERARCHICAL_TARGETS = {
    "distance_threshold",
    "n_clusters",
    "persistence",
}

try:
    from numba import njit, prange, set_num_threads
except ImportError:  # pragma: no cover
    njit = None
    prange = range
    set_num_threads = None

if njit is not None:

    @njit(cache=True, parallel=True)
    def _fill_condensed_distances_numba(
        offsets: object,
        values: object,
        metric_code: int,
        out: object,
    ) -> None:
        n = offsets.shape[0] - 1
        for i in prange(n - 1):
            start_i = offsets[i]
            end_i = offsets[i + 1]
            size_i = end_i - start_i
            base = n * i - (i * (i + 1)) // 2
            for j in range(i + 1, n):
                start_j = offsets[j]
                end_j = offsets[j + 1]
                size_j = end_j - start_j

                a = start_i
                b = start_j
                intersection = 0
                while a < end_i and b < end_j:
                    left_value = values[a]
                    right_value = values[b]
                    if left_value == right_value:
                        intersection += 1
                        a += 1
                        b += 1
                    elif left_value < right_value:
                        a += 1
                    else:
                        b += 1

                if metric_code == 0:
                    denominator = size_i + size_j - intersection
                    similarity = 1.0 if denominator == 0 else intersection / denominator
                elif metric_code == 1:
                    minimum_size = size_i if size_i < size_j else size_j
                    similarity = 1.0 if minimum_size == 0 else intersection / minimum_size
                else:
                    denominator = size_i + size_j
                    similarity = 1.0 if denominator == 0 else (2.0 * intersection) / denominator

                distance = 1.0 - similarity
                if distance < 0.0:
                    distance = 0.0
                elif distance > 1.0:
                    distance = 1.0
                out[base + (j - i - 1)] = distance

    @njit(cache=True)
    def _hierarchy_condensed_index(n: int, left: int, right: int) -> int:
        if left > right:
            left, right = right, left
        return n * left - (left * (left + 1)) // 2 + (right - left - 1)

    @njit(cache=True)
    def _hierarchy_condensed_distance(
        distances: object,
        n: int,
        left: int,
        right: int,
    ) -> float:
        return distances[_hierarchy_condensed_index(n, left, right)]

    @njit(cache=True)
    def _set_hierarchy_condensed_distance(
        distances: object,
        n: int,
        left: int,
        right: int,
        value: float,
    ) -> None:
        distances[_hierarchy_condensed_index(n, left, right)] = value

    @njit(cache=True)
    def _nearest_active_hierarchy_neighbor(
        distances: object,
        n: int,
        active: object,
        cluster_ids: object,
        slot: int,
    ) -> tuple[int, float]:
        best_slot = -1
        best_distance = math.inf
        best_cluster_id = np.iinfo(np.int64).max
        for other in range(n):
            if other == slot or not active[other]:
                continue
            distance = _hierarchy_condensed_distance(distances, n, slot, other)
            other_cluster_id = cluster_ids[other]
            if (
                distance < best_distance
                or (distance == best_distance and other_cluster_id < best_cluster_id)
            ):
                best_slot = other
                best_distance = distance
                best_cluster_id = other_cluster_id
        return best_slot, best_distance

    @njit(cache=True)
    def _weighted_average_linkage_numba(
        distances: object,
        leaf_interface_counts: object,
    ) -> tuple[object, object, object]:
        n = leaf_interface_counts.shape[0]
        children = np.empty((n - 1, 2), dtype=np.int64)
        merge_distances = np.empty(n - 1, dtype=np.float64)
        counts = np.empty(n - 1, dtype=np.int64)

        active = np.ones(n, dtype=np.bool_)
        cluster_ids = np.empty(n, dtype=np.int64)
        weights = np.empty(n, dtype=np.float64)
        slot_counts = np.empty(n, dtype=np.int64)
        chain = np.empty(n, dtype=np.int64)

        for index in range(n):
            count = np.int64(leaf_interface_counts[index])
            if count <= 0:
                raise ValueError("leaf interface counts must be positive")
            cluster_ids[index] = index
            weights[index] = float(count)
            slot_counts[index] = count

        for merge_index in range(n - 1):
            first_slot = -1
            first_cluster_id = np.iinfo(np.int64).max
            for slot in range(n):
                if active[slot] and cluster_ids[slot] < first_cluster_id:
                    first_slot = slot
                    first_cluster_id = cluster_ids[slot]

            if first_slot < 0:
                raise ValueError("could not find an active cluster pair to merge")

            chain[0] = first_slot
            chain_length = 1
            best_distance = math.inf
            left_slot = -1
            right_slot = -1

            while True:
                current_slot = chain[chain_length - 1]
                nearest_slot, nearest_distance = _nearest_active_hierarchy_neighbor(
                    distances,
                    n,
                    active,
                    cluster_ids,
                    current_slot,
                )
                if nearest_slot < 0:
                    raise ValueError("could not find a nearest active cluster")
                if chain_length > 1 and nearest_slot == chain[chain_length - 2]:
                    left_slot = current_slot
                    right_slot = nearest_slot
                    best_distance = nearest_distance
                    break
                if chain_length >= n:
                    raise ValueError("nearest-neighbor chain did not converge")
                chain[chain_length] = nearest_slot
                chain_length += 1

            left_cluster_id = cluster_ids[left_slot]
            right_cluster_id = cluster_ids[right_slot]
            if left_cluster_id <= right_cluster_id:
                children[merge_index, 0] = left_cluster_id
                children[merge_index, 1] = right_cluster_id
            else:
                children[merge_index, 0] = right_cluster_id
                children[merge_index, 1] = left_cluster_id
            merge_distances[merge_index] = best_distance

            left_weight = weights[left_slot]
            right_weight = weights[right_slot]
            new_weight = left_weight + right_weight
            new_count = slot_counts[left_slot] + slot_counts[right_slot]
            counts[merge_index] = new_count

            for other in range(n):
                if not active[other] or other == left_slot or other == right_slot:
                    continue
                left_distance = _hierarchy_condensed_distance(distances, n, left_slot, other)
                right_distance = _hierarchy_condensed_distance(distances, n, right_slot, other)
                updated_distance = (
                    (left_weight * left_distance) + (right_weight * right_distance)
                ) / new_weight
                _set_hierarchy_condensed_distance(distances, n, left_slot, other, updated_distance)

            active[right_slot] = False
            cluster_ids[left_slot] = n + merge_index
            weights[left_slot] = new_weight
            slot_counts[left_slot] = new_count

        return children, merge_distances, counts
else:
    _fill_condensed_distances_numba = None
    _weighted_average_linkage_numba = None


def rank_non_negative_cluster_labels_by_size(labels: list[int]) -> list[int]:
    label_counts: dict[int, int] = {}
    normalized_labels: list[int] = []
    for label in labels:
        numeric_label = int(label)
        normalized_labels.append(numeric_label)
        if numeric_label >= 0:
            label_counts[numeric_label] = label_counts.get(numeric_label, 0) + 1
    ordered_labels = sorted(label_counts, key=lambda label: (-label_counts[label], label))
    label_mapping = {
        cluster_label: ranked_label
        for ranked_label, cluster_label in enumerate(ordered_labels)
    }
    return [label_mapping.get(label, -1) if label >= 0 else -1 for label in normalized_labels]


def normalize_hierarchical_linkage(linkage: str) -> str:
    return linkage.strip().lower().replace("-", "_").replace(" ", "_")


def normalize_hierarchical_target(target: str) -> str:
    normalized = target.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"cutoff", "threshold", "distance_cutoff"}:
        return "distance_threshold"
    if normalized in {"cluster_count", "clusters", "n_cluster"}:
        return "n_clusters"
    if normalized in {"lifetime", "cluster_lifetime", "persistent_clusters"}:
        return "persistence"
    return normalized


def scipy_hierarchical_linkage_method(linkage: str) -> str:
    if linkage == "average_deduplicated":
        return "average"
    return linkage


def compressed_entry_member_counts(compressed_entries: object) -> np.ndarray:
    if not isinstance(compressed_entries, list):
        raise ValueError("compressed interface groups are unavailable")
    counts = np.empty(len(compressed_entries), dtype=np.int64)
    for index, entry in enumerate(compressed_entries):
        member_count = 1
        if isinstance(entry, dict):
            try:
                member_count = int(entry.get("member_count", 1))
            except (TypeError, ValueError):
                member_count = 1
        if member_count <= 0:
            raise ValueError("compressed interface groups must have positive member counts")
        counts[index] = member_count
    return counts


def cluster_interface_counts_from_children(
    children: object,
    leaf_interface_counts: np.ndarray,
) -> np.ndarray:
    leaf_count = len(leaf_interface_counts)
    if leaf_count <= 1:
        return np.zeros((0,), dtype=np.int64)
    children_array = np.asarray(children, dtype=np.int64)
    node_counts = np.empty((2 * leaf_count) - 1, dtype=np.int64)
    node_counts[:leaf_count] = leaf_interface_counts
    counts = np.empty((len(children_array),), dtype=np.int64)
    for merge_index, (left_child, right_child) in enumerate(children_array):
        max_valid_child = leaf_count + merge_index
        left = int(left_child)
        right = int(right_child)
        if left >= max_valid_child or right >= max_valid_child:
            raise ValueError("hierarchy children are not ordered like a scipy linkage matrix")
        merged_count = node_counts[left] + node_counts[right]
        node_counts[max_valid_child] = merged_count
        counts[merge_index] = merged_count
    return counts


def reorder_linkage_by_distance(
    children: np.ndarray,
    distances: np.ndarray,
    counts: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    merge_count = len(children)
    if merge_count <= 1:
        return children, distances, counts

    leaf_count = merge_count + 1
    remaining_dependencies = np.zeros(merge_count, dtype=np.int32)
    dependents: list[list[int]] = [[] for _index in range(merge_count)]
    for merge_index, row in enumerate(children):
        for child in row:
            child_index = int(child) - leaf_count
            if child_index >= 0:
                remaining_dependencies[merge_index] += 1
                dependents[child_index].append(merge_index)

    heap: list[tuple[float, int]] = []
    for merge_index, dependency_count in enumerate(remaining_dependencies):
        if dependency_count == 0:
            heapq.heappush(heap, (float(distances[merge_index]), merge_index))

    reordered_children = np.empty_like(children)
    reordered_distances = np.empty_like(distances)
    reordered_counts = np.empty_like(counts)
    old_merge_to_new_node = np.full(merge_count, -1, dtype=np.int64)

    next_merge_index = 0
    while heap:
        _distance, old_merge_index = heapq.heappop(heap)
        mapped_children: list[int] = []
        for child in children[old_merge_index]:
            raw_child = int(child)
            if raw_child < leaf_count:
                mapped_children.append(raw_child)
            else:
                mapped_child = int(old_merge_to_new_node[raw_child - leaf_count])
                if mapped_child < 0:
                    raise ValueError("hierarchy dependency was not processed before parent")
                mapped_children.append(mapped_child)
        mapped_children.sort()
        reordered_children[next_merge_index, 0] = mapped_children[0]
        reordered_children[next_merge_index, 1] = mapped_children[1]
        reordered_distances[next_merge_index] = distances[old_merge_index]
        reordered_counts[next_merge_index] = counts[old_merge_index]
        old_merge_to_new_node[old_merge_index] = leaf_count + next_merge_index
        next_merge_index += 1

        for dependent in dependents[old_merge_index]:
            remaining_dependencies[dependent] -= 1
            if remaining_dependencies[dependent] == 0:
                heapq.heappush(heap, (float(distances[dependent]), dependent))

    if next_merge_index != merge_count:
        raise ValueError("hierarchy rows contain cyclic dependencies")
    return reordered_children, reordered_distances, reordered_counts


def normalize_clustering_payload_cluster_labels(payload: dict[str, object]) -> dict[str, object]:
    def payload_int(value: object, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    points = payload.get("points")
    if not isinstance(points, list):
        return payload
    labels: list[int] = []
    for point in points:
        if not isinstance(point, dict):
            labels.append(-1)
            continue
        try:
            labels.append(int(point.get("cluster_label", -1)))
        except (TypeError, ValueError):
            labels.append(-1)
    ranked_labels = rank_non_negative_cluster_labels_by_size(labels)
    cluster_count = len({label for label in ranked_labels if label >= 0})
    noise_count = sum(1 for label in ranked_labels if label < 0)
    normalized_points: list[object] = []
    changed = False
    for point, ranked_label, original_label in zip(points, ranked_labels, labels, strict=True):
        if isinstance(point, dict):
            if ranked_label != original_label:
                changed = True
                normalized_points.append({**point, "cluster_label": ranked_label})
            else:
                normalized_points.append(point)
        else:
            normalized_points.append(point)
    if payload_int(payload.get("cluster_count", cluster_count), cluster_count) != cluster_count:
        changed = True
    if payload_int(payload.get("noise_count", noise_count), noise_count) != noise_count:
        changed = True
    if payload.get("cluster_label_order") != "member_count_desc":
        changed = True
    if not changed:
        return payload
    return {
        **payload,
        "cluster_count": cluster_count,
        "noise_count": noise_count,
        "cluster_label_order": "member_count_desc",
        "points": normalized_points,
    }


def parse_interface_row_key(row_key: str) -> dict[str, object]:
    parts = str(row_key or "").split("_", 2)
    protein_id = parts[0] if len(parts) > 0 else ""
    fragment_key = parts[1] if len(parts) > 1 else ""
    partner_fragment_key = parts[2] if len(parts) > 2 else ""
    return {
        "interface_row_key": str(row_key or ""),
        "protein_id": protein_id,
        "fragment_key": fragment_key,
        "partner_fragment_key": partner_fragment_key,
    }


def fragment_ranges(fragment_key: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for part in str(fragment_key or "").split(","):
        fragment_part = part.strip()
        if not fragment_part:
            continue
        start_text, end_text = fragment_part.split("-", maxsplit=1)
        ranges.append((int(start_text), int(end_text)))
    return ranges


def fragment_start(fragment_key: str) -> int:
    ranges = fragment_ranges(fragment_key)
    return ranges[0][0] if ranges else 1


def alignment_fragment_key(fragment_key: str) -> str:
    ranges = fragment_ranges(fragment_key)
    if not ranges:
        return ""
    return f"{ranges[0][0]}-{ranges[-1][1]}"


def mask_alignment_to_fragment_ranges(
    aligned_sequence: str,
    alignment_fragment: str,
    exact_fragment: str,
) -> tuple[str, list[int | None]]:
    if not aligned_sequence:
        return "", []
    allowed_residue_ids: set[int] = set()
    for start, end in fragment_ranges(exact_fragment):
        allowed_residue_ids.update(range(start, end + 1))
    next_residue = fragment_start(alignment_fragment)
    masked_chars: list[str] = []
    residue_ids: list[int | None] = []
    for char in aligned_sequence:
        if not char.isalpha():
            masked_chars.append(char)
            residue_ids.append(None)
            continue
        residue_id = next_residue
        next_residue += 1
        if residue_id in allowed_residue_ids:
            masked_chars.append(char)
            residue_ids.append(residue_id)
            continue
        masked_chars.append("-")
        residue_ids.append(None)
    return "".join(masked_chars), residue_ids


def collect_interface_alignment_row_metadata(
    interface_payload: dict[str, dict[str, dict]],
) -> tuple[list[dict[str, object]], int]:
    with timed_step(
        "json",
        "collect alignment rows",
        partner_domains=len(interface_payload),
    ) as timer:
        raw_rows: list[dict[str, object]] = []
        alignment_length = 0
        for partner_domain in sorted(interface_payload):
            rows = interface_payload.get(partner_domain)
            if not isinstance(rows, dict):
                continue
            for interface_row_key in sorted(rows):
                payload = rows.get(interface_row_key)
                if not isinstance(payload, dict):
                    continue
                parsed = parse_interface_row_key(interface_row_key)
                aligned_sequence = payload.get("aligned_seq")
                aligned_sequence = aligned_sequence if isinstance(aligned_sequence, str) else ""
                alignment_length = max(alignment_length, len(aligned_sequence))
                raw_rows.append(
                    {
                        **parsed,
                        "partner_domain": str(partner_domain),
                        "aligned_sequence": aligned_sequence,
                    }
                )
        total_rows = len(raw_rows)
        timer.set(raw_rows=total_rows, alignment_length=alignment_length)
    return raw_rows, alignment_length


def build_interface_alignment_rows_from_metadata(
    raw_rows: list[dict[str, object]],
    alignment_length: int,
    row_offset: int = 0,
    row_limit: int | None = None,
    include_total: bool = False,
) -> tuple[list[dict], int] | tuple[list[dict], int, int]:
    total_rows = len(raw_rows)
    normalized_offset = max(0, int(row_offset or 0))
    normalized_limit = None if row_limit is None else max(0, int(row_limit))
    selected_raw_rows = (
        raw_rows[normalized_offset:]
        if normalized_limit is None
        else raw_rows[normalized_offset:normalized_offset + normalized_limit]
    )

    with timed_step(
        "json",
        "build alignment row payload",
        raw_rows=len(raw_rows),
        selected_rows=len(selected_raw_rows),
        alignment_length=alignment_length,
        row_offset=normalized_offset,
        row_limit=normalized_limit if normalized_limit is not None else "all",
    ) as timer:
        rows: list[dict] = []
        for raw_row in selected_raw_rows:
            interface_row_key = str(raw_row["interface_row_key"])
            protein_id = str(raw_row["protein_id"])
            partner_domain = str(raw_row["partner_domain"])
            fragment_key = str(raw_row["fragment_key"])
            alignment_key = alignment_fragment_key(fragment_key)
            aligned_sequence, residue_ids = mask_alignment_to_fragment_ranges(
                str(raw_row["aligned_sequence"] or ""),
                alignment_key,
                fragment_key,
            )
            if len(aligned_sequence) < alignment_length:
                padding = "-" * (alignment_length - len(aligned_sequence))
                aligned_sequence = aligned_sequence + padding
                residue_ids = residue_ids + ([None] * len(padding))
            rows.append(
                {
                    "protein_id": protein_id,
                    "fragment_key": fragment_key,
                    "alignment_fragment_key": alignment_key,
                    "partner_fragment_key": str(raw_row["partner_fragment_key"]),
                    "interface_row_key": interface_row_key,
                    "partner_domain": partner_domain,
                    "row_key": f"{interface_row_key}@@{partner_domain}" if partner_domain else interface_row_key,
                    "display_row_key": f"{protein_id} | {partner_domain}" if partner_domain else protein_id,
                    "aligned_sequence": aligned_sequence,
                    "residue_ids": residue_ids,
                    "has_alignment": bool(str(raw_row["aligned_sequence"] or "")),
                }
            )
        timer.set(rows=len(rows), total_rows=total_rows)
    if include_total:
        return rows, alignment_length, total_rows
    return rows, alignment_length


def build_interface_alignment_rows(
    interface_payload: dict[str, dict[str, dict]],
    row_offset: int = 0,
    row_limit: int | None = None,
    include_total: bool = False,
) -> tuple[list[dict], int] | tuple[list[dict], int, int]:
    raw_rows, alignment_length = collect_interface_alignment_row_metadata(interface_payload)
    return build_interface_alignment_rows_from_metadata(
        raw_rows,
        alignment_length,
        row_offset=row_offset,
        row_limit=row_limit,
        include_total=include_total,
    )


def load_interface_entries(interface_payload: dict[str, dict[str, dict]]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for partner_domain in sorted(interface_payload):
        rows = interface_payload[partner_domain]
        for row_key in sorted(rows):
            raw_columns = rows[row_key].get("interface_msa_columns_a", [])
            columns = tuple(sorted({int(column) for column in raw_columns}))
            if not columns:
                continue
            entries.append(
                {
                    "partner_domain": partner_domain,
                    "row_key": row_key,
                    "columns": columns,
                }
            )
    return entries


def build_indicator_matrix(entries: list[dict[str, object]]) -> tuple[object, list[int]]:
    import numpy as np

    msa_columns = sorted({int(column) for entry in entries for column in entry["columns"]})
    column_index = {column: index for index, column in enumerate(msa_columns)}
    indicator_matrix = np.zeros((len(entries), len(msa_columns)), dtype=bool)
    for row_index, entry in enumerate(entries):
        for column in entry["columns"]:
            indicator_matrix[row_index, column_index[int(column)]] = True
    return indicator_matrix, msa_columns


def build_column_offsets(entries: list[dict[str, object]]) -> tuple[object, object]:
    import numpy as np

    offsets = np.zeros((len(entries) + 1,), dtype=np.int64)
    total_values = 0
    normalized_columns: list[tuple[int, ...]] = []
    for entry in entries:
        columns = tuple(int(column) for column in entry["columns"])
        normalized_columns.append(columns)
        total_values += len(columns)
    values = np.empty((total_values,), dtype=np.int64)
    cursor = 0
    for index, columns in enumerate(normalized_columns):
        offsets[index] = cursor
        next_cursor = cursor + len(columns)
        values[cursor:next_cursor] = columns
        cursor = next_cursor
    offsets[len(entries)] = cursor
    return offsets, values


def distance_entries_signature(entries: list[dict[str, object]]) -> str:
    hasher = hashlib.sha1()
    for entry in entries:
        hasher.update(str(entry.get("partner_domain", "")).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(str(entry.get("row_key", "")).encode("utf-8"))
        hasher.update(b"\0")
        for column in entry.get("columns", ()):
            hasher.update(str(int(column)).encode("ascii"))
            hasher.update(b",")
        hasher.update(b"\0")
        members = entry.get("members")
        if isinstance(members, list):
            for member in members:
                if not isinstance(member, dict):
                    continue
                hasher.update(str(member.get("partner_domain", "")).encode("utf-8"))
                hasher.update(b"\0")
                hasher.update(str(member.get("row_key", "")).encode("utf-8"))
                hasher.update(b"\0")
        hasher.update(b"\n")
    return hasher.hexdigest()


def condensed_distance_count(entry_count: int) -> int:
    return max(0, entry_count * (entry_count - 1) // 2)


def condensed_to_square_distance_matrix(condensed: object, entry_count: int, dtype: object | None = None) -> object:
    import numpy as np

    output_dtype = dtype or np.float32
    matrix = np.zeros((entry_count, entry_count), dtype=output_dtype)
    cursor = 0
    for row_index in range(entry_count - 1):
        width = entry_count - row_index - 1
        row_values = np.asarray(condensed[cursor:cursor + width], dtype=output_dtype)
        matrix[row_index, row_index + 1:] = row_values
        matrix[row_index + 1:, row_index] = row_values
        cursor += width
    return matrix


def square_distance_matrix_to_condensed(distance_matrix: object) -> object:
    import numpy as np

    matrix = np.asarray(distance_matrix)
    entry_count = matrix.shape[0]
    condensed = np.empty((condensed_distance_count(entry_count),), dtype=np.float64)
    cursor = 0
    for row_index in range(entry_count - 1):
        width = entry_count - row_index - 1
        condensed[cursor:cursor + width] = matrix[row_index, row_index + 1:]
        cursor += width
    return condensed


def quantize_condensed_distances(condensed: object) -> object:
    import numpy as np

    values = np.asarray(condensed, dtype=np.float64)
    return np.rint(np.clip(values, 0.0, 1.0) * DISTANCE_CACHE_SCALE).astype(np.uint16)


def dequantize_condensed_distances(condensed_u16: object, distance_scale: float = DISTANCE_CACHE_SCALE) -> object:
    import numpy as np

    scale = float(distance_scale or DISTANCE_CACHE_SCALE)
    return np.asarray(condensed_u16, dtype=np.float32) / scale


def compute_condensed_distances_numba(
    entries: list[dict[str, object]],
    distance_metric: str,
    worker_count: int = DEFAULT_CACHE_WORKERS,
) -> object | None:
    if _fill_condensed_distances_numba is None:
        return None
    if distance_metric not in METRIC_CODES:
        return None
    import numpy as np

    offsets, values = build_column_offsets(entries)
    condensed = np.empty((condensed_distance_count(len(entries)),), dtype=np.float64)
    if set_num_threads is not None:
        try:
            set_num_threads(max(1, int(worker_count)))
        except ValueError:
            log_event(
                "distance",
                "numba thread request rejected",
                requested=max(1, int(worker_count)),
            )
    _fill_condensed_distances_numba(offsets, values, METRIC_CODES[distance_metric], condensed)
    return condensed


def compress_interface_entries(entries: list[dict[str, object]]) -> dict[str, object]:
    groups_by_key: dict[tuple[str, tuple[int, ...]], list[int]] = {}
    for index, entry in enumerate(entries):
        partner_domain = str(entry["partner_domain"])
        columns = tuple(int(column) for column in entry["columns"])
        groups_by_key.setdefault((partner_domain, columns), []).append(index)

    sorted_items = sorted(
        groups_by_key.items(),
        key=lambda item: (
            item[0][0],
            item[0][1],
            min(str(entries[index]["row_key"]) for index in item[1]),
        ),
    )
    compressed_entries: list[dict[str, object]] = []
    group_index_by_entry = [-1] * len(entries)
    for group_index, ((partner_domain, columns), member_indices) in enumerate(sorted_items):
        sorted_member_indices = tuple(
            sorted(
                member_indices,
                key=lambda index: (
                    str(entries[index]["partner_domain"]),
                    str(entries[index]["row_key"]),
                    index,
                ),
            )
        )
        representative_index = sorted_member_indices[0]
        representative = entries[representative_index]
        members = [
            {
                "row_key": str(entries[index]["row_key"]),
                "partner_domain": str(entries[index]["partner_domain"]),
            }
            for index in sorted_member_indices
        ]
        for member_index in sorted_member_indices:
            group_index_by_entry[member_index] = group_index
        compressed_entries.append(
            {
                "group_id": f"g{group_index}",
                "partner_domain": partner_domain,
                "row_key": str(representative["row_key"]),
                "columns": columns,
                "member_indices": sorted_member_indices,
                "member_count": len(sorted_member_indices),
                "members": members,
                "representative_index": representative_index,
            }
        )
    return {
        "compression_mode": INTERFACE_COMPRESSION_MODE,
        "entries": compressed_entries,
        "group_index_by_entry": group_index_by_entry,
    }


def build_compressed_interface_data_uncached(
    interface_payload: dict[str, dict[str, dict]],
    log_category: str = "interface",
    parse_message: str = "parse interfaces",
    compress_message: str = "compress identical interfaces",
) -> tuple[list[dict[str, object]], dict[str, object]]:
    with timed_step(log_category, parse_message) as timer:
        entries = load_interface_entries(interface_payload)
        timer.set(interface_count=len(entries))
    if len(entries) < 2:
        raise ValueError("need at least two interfaces with non-empty interface sets")
    with timed_step(log_category, compress_message, interface_count=len(entries)) as timer:
        compression = compress_interface_entries(entries)
        timer.set(compressed_count=len(compression["entries"]))
    compression["cache_signature_hash"] = compressed_entries_signature_hash(compression["entries"])
    return entries, compression


def load_or_build_compressed_interface_data(
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
    interface_filter_settings: dict[str, object] | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    cache_key = compressed_interface_cache_key(interface_path, interface_filter_settings)
    owner = False
    with COMPRESSED_INTERFACE_CACHE_LOCK:
        cached = COMPRESSED_INTERFACE_CACHE.get(cache_key)
        if cached is not None:
            COMPRESSED_INTERFACE_CACHE.move_to_end(cache_key)
            entries, compression = cached
            log_event(
                "interface",
                "reuse compressed interface data",
                file=interface_path.name,
                interface_count=len(entries),
                compressed_count=len(compression.get("entries", [])),
            )
            return cached
        future = COMPRESSED_INTERFACE_IN_FLIGHT.get(cache_key)
        if future is None:
            future = Future()
            COMPRESSED_INTERFACE_IN_FLIGHT[cache_key] = future
            owner = True
    if not owner:
        with timed_step("interface", "wait for in-flight compressed interface data", file=interface_path.name):
            return future.result()
    try:
        compressed_data = build_compressed_interface_data_uncached(
            interface_payload,
            log_category="interface",
            parse_message="parse interfaces for shared cache",
            compress_message="compress identical interfaces for shared cache",
        )
    except Exception as exc:
        with COMPRESSED_INTERFACE_CACHE_LOCK:
            COMPRESSED_INTERFACE_IN_FLIGHT.pop(cache_key, None)
            future.set_exception(exc)
        raise
    with COMPRESSED_INTERFACE_CACHE_LOCK:
        COMPRESSED_INTERFACE_CACHE[cache_key] = compressed_data
        COMPRESSED_INTERFACE_CACHE.move_to_end(cache_key)
        while len(COMPRESSED_INTERFACE_CACHE) > COMPRESSED_INTERFACE_CACHE_LIMIT:
            COMPRESSED_INTERFACE_CACHE.popitem(last=False)
        COMPRESSED_INTERFACE_IN_FLIGHT.pop(cache_key, None)
        future.set_result(compressed_data)
    return compressed_data


def compute_distance_matrix_from_entries(
    entries: list[dict[str, object]],
    distance_metric: str,
    worker_count: int = DEFAULT_CACHE_WORKERS,
) -> tuple[object, list[int], object, object]:
    import numpy as np

    indicator_matrix, msa_columns = build_indicator_matrix(entries)
    if len(entries) <= 1:
        condensed = np.zeros((0,), dtype=np.float64)
        return indicator_matrix, msa_columns, np.zeros((len(entries), len(entries)), dtype=np.float32), condensed
    condensed = compute_condensed_distances_numba(entries, distance_metric, worker_count=worker_count)
    if condensed is not None:
        distance_matrix = condensed_to_square_distance_matrix(condensed, len(entries))
        return indicator_matrix, msa_columns, distance_matrix, condensed
    if distance_metric == "overlap":
        distance_matrix = np.zeros((len(entries), len(entries)), dtype=np.float32)
        condensed = np.empty((condensed_distance_count(len(entries)),), dtype=np.float64)
        column_sets = [set(int(column) for column in entry["columns"]) for entry in entries]
        cursor = 0
        for left_index in range(len(entries) - 1):
            left = column_sets[left_index]
            for right_index in range(left_index + 1, len(entries)):
                distance = overlap_distance_for_sets(left, column_sets[right_index])
                distance_matrix[left_index, right_index] = distance
                distance_matrix[right_index, left_index] = distance
                condensed[cursor] = distance
                cursor += 1
        return indicator_matrix, msa_columns, distance_matrix, condensed
    try:
        from sklearn.metrics import pairwise_distances
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Jaccard and dice interface distances require scikit-learn in the Python environment running the server."
        ) from exc
    distance_matrix = pairwise_distances(indicator_matrix, metric=distance_metric).astype(
        np.float32, copy=False
    )
    condensed = square_distance_matrix_to_condensed(distance_matrix)
    return indicator_matrix, msa_columns, distance_matrix, condensed


def overlap_distance_for_sets(left: set[int], right: set[int]) -> float:
    if not left and not right:
        return 0.0
    if not left or not right:
        return 1.0
    minimum_size = min(len(left), len(right))
    return 1.0 - (len(left & right) / minimum_size)


def parse_embedding_distance(query: dict[str, list[str]], default_metric: str = DEFAULT_EMBEDDING_DISTANCE) -> str:
    distance_raw = query.get("distance", [default_metric])[0].strip().lower()
    if distance_raw not in {"binary", "jaccard", "dice", "overlap"}:
        raise ValueError("point distance must be one of 'binary', 'jaccard', 'dice', or 'overlap'")
    return distance_raw


def parse_embedding_settings(query: dict[str, list[str]]) -> dict[str, object]:
    method_raw = query.get("embedding_method", [DEFAULT_EMBEDDING_METHOD])[0].strip().lower()
    distance_raw = parse_embedding_distance(query)
    perplexity_raw = query.get("perplexity", ["auto"])[0].strip().lower()
    learning_rate_raw = query.get("learning_rate", [DEFAULT_TSNE_LEARNING_RATE])[0].strip().lower()
    max_iter_raw = query.get("max_iter", [str(DEFAULT_TSNE_MAX_ITER)])[0].strip()
    early_exaggeration_iter_raw = query.get(
        "early_exaggeration_iter",
        [str(DEFAULT_TSNE_EARLY_EXAGGERATION_ITER)],
    )[0].strip()
    early_exaggeration_raw = query.get(
        "early_exaggeration", [str(DEFAULT_TSNE_EARLY_EXAGGERATION)]
    )[0].strip()
    neighbors_raw = query.get("neighbors", [DEFAULT_TSNE_NEIGHBORS])[0].strip().lower()
    theta_raw = query.get("theta", [str(DEFAULT_TSNE_THETA)])[0].strip()
    if method_raw not in {"tsne", "pca"}:
        raise ValueError("point method must be either 'tsne' or 'pca'")
    if perplexity_raw in {"", "auto"}:
        perplexity = "auto"
    else:
        perplexity = float(perplexity_raw)
        if perplexity <= 0:
            raise ValueError("perplexity must be positive")
    if learning_rate_raw in {"", "auto"}:
        learning_rate: str | float = "auto"
    else:
        learning_rate = float(learning_rate_raw)
        if learning_rate <= 0:
            raise ValueError("learning rate must be positive")
    max_iter = int(max_iter_raw)
    if max_iter <= 0:
        raise ValueError("iterations must be positive")
    early_exaggeration_iter = int(early_exaggeration_iter_raw)
    if early_exaggeration_iter <= 0:
        raise ValueError("early exaggeration iterations must be positive")
    early_exaggeration = float(early_exaggeration_raw)
    if early_exaggeration <= 0:
        raise ValueError("early exaggeration must be positive")
    if neighbors_raw not in {"approx", "auto", "exact"}:
        raise ValueError("nearest neighbors must be one of 'approx', 'auto', or 'exact'")
    theta = float(theta_raw)
    if theta < 0 or theta > 1:
        raise ValueError("theta must be between 0 and 1")
    return {
        "method": method_raw,
        "distance": distance_raw,
        "perplexity": perplexity,
        "learning_rate": learning_rate,
        "max_iter": max_iter,
        "early_exaggeration_iter": early_exaggeration_iter,
        "early_exaggeration": early_exaggeration,
        "neighbors": neighbors_raw,
        "theta": theta,
        "random_state": DEFAULT_TSNE_RANDOM_STATE,
    }


def parse_clustering_settings(query: dict[str, list[str]]) -> dict[str, object]:
    method_raw = query.get("method", [DEFAULT_CLUSTERING_METHOD])[0].strip().lower()
    distance_raw = query.get("distance", [DEFAULT_DISTANCE_METRIC])[0].strip().lower()
    min_cluster_size_raw = query.get("min_cluster_size", [str(DEFAULT_CLUSTER_MIN_SIZE)])[0].strip()
    min_samples_raw = query.get("min_samples", [""])[0].strip()
    cluster_selection_epsilon_raw = query.get(
        "cluster_selection_epsilon", [str(DEFAULT_CLUSTER_SELECTION_EPSILON)]
    )[0].strip()
    linkage_raw = normalize_hierarchical_linkage(
        query.get("linkage", [DEFAULT_HIERARCHICAL_LINKAGE])[0]
    )
    hierarchical_target_query = query.get("hierarchical_target")
    hierarchical_target_raw = (
        normalize_hierarchical_target(hierarchical_target_query[0])
        if hierarchical_target_query
        else ""
    )
    default_n_clusters_raw = (
        str(DEFAULT_HIERARCHICAL_N_CLUSTERS)
        if DEFAULT_HIERARCHICAL_TARGET == "n_clusters"
        else ""
    )
    default_distance_threshold_raw = (
        str(DEFAULT_HIERARCHICAL_DISTANCE_THRESHOLD)
        if DEFAULT_HIERARCHICAL_TARGET == "distance_threshold"
        else ""
    )
    n_clusters_default_raw = default_n_clusters_raw
    distance_threshold_default_raw = default_distance_threshold_raw
    n_clusters_requested = "n_clusters" in query
    distance_threshold_requested = "distance_threshold" in query
    if hierarchical_target_raw == "n_clusters":
        distance_threshold_default_raw = ""
    elif hierarchical_target_raw == "distance_threshold":
        n_clusters_default_raw = ""
    elif hierarchical_target_raw == "persistence":
        n_clusters_default_raw = ""
        distance_threshold_default_raw = ""
    elif n_clusters_requested and not distance_threshold_requested:
        distance_threshold_default_raw = ""
    elif distance_threshold_requested and not n_clusters_requested:
        n_clusters_default_raw = ""
    default_persistence_min_lifetime_raw = str(DEFAULT_HIERARCHICAL_PERSISTENCE_MIN_LIFETIME)
    n_clusters_raw = query.get("n_clusters", [n_clusters_default_raw])[0].strip()
    distance_threshold_raw = query.get("distance_threshold", [distance_threshold_default_raw])[0].strip()
    persistence_min_lifetime_raw = query.get(
        "persistence_min_lifetime",
        [default_persistence_min_lifetime_raw],
    )[0].strip()
    hierarchical_min_cluster_size_raw = query.get(
        "hierarchical_min_cluster_size",
        [str(DEFAULT_HIERARCHICAL_MIN_CLUSTER_SIZE)],
    )[0].strip()
    if method_raw not in {"hdbscan", "hierarchical"}:
        raise ValueError("clustering method must be either 'hdbscan' or 'hierarchical'")
    if distance_raw not in {"jaccard", "dice", "overlap"}:
        raise ValueError("distance must be one of 'jaccard', 'dice', or 'overlap'")
    if linkage_raw not in VALID_HIERARCHICAL_LINKAGES:
        raise ValueError(
            "linkage must be one of 'single', 'complete', 'average', "
            "'average_deduplicated', or 'weighted'"
        )
    if hierarchical_target_raw and hierarchical_target_raw not in VALID_HIERARCHICAL_TARGETS:
        raise ValueError(
            "hierarchical target must be one of 'distance_threshold', 'n_clusters', or 'persistence'"
        )
    min_cluster_size = int(min_cluster_size_raw)
    if min_cluster_size <= 0:
        raise ValueError("min cluster size must be positive")
    min_samples: int | None
    if min_samples_raw == "":
        min_samples = None
    else:
        min_samples = int(min_samples_raw)
        if min_samples <= 0:
            raise ValueError("min samples must be positive")
    cluster_selection_epsilon = float(cluster_selection_epsilon_raw)
    if cluster_selection_epsilon < 0:
        raise ValueError("cluster selection epsilon must be non-negative")
    n_clusters: int | None
    if n_clusters_raw == "":
        n_clusters = None
    else:
        n_clusters = int(n_clusters_raw)
        if n_clusters <= 0:
            raise ValueError("n clusters must be positive")
    distance_threshold: float | None
    if distance_threshold_raw == "":
        distance_threshold = None
    else:
        distance_threshold = float(distance_threshold_raw)
        if distance_threshold < 0:
            raise ValueError("distance threshold must be non-negative")
    persistence_min_lifetime = (
        DEFAULT_HIERARCHICAL_PERSISTENCE_MIN_LIFETIME
        if persistence_min_lifetime_raw == ""
        else float(persistence_min_lifetime_raw)
    )
    if persistence_min_lifetime < 0:
        raise ValueError("cluster lifetime must be non-negative")
    hierarchical_min_cluster_size = int(hierarchical_min_cluster_size_raw)
    if hierarchical_min_cluster_size <= 0:
        raise ValueError("hierarchical minimal cluster size must be positive")
    hierarchical_target = hierarchical_target_raw
    if not hierarchical_target:
        if n_clusters is not None and distance_threshold is not None:
            raise ValueError("set either n clusters or cutoff distance for hierarchical clustering, not both")
        if n_clusters is not None:
            hierarchical_target = "n_clusters"
        elif distance_threshold is not None:
            hierarchical_target = "distance_threshold"
        else:
            hierarchical_target = DEFAULT_HIERARCHICAL_TARGET
    if method_raw == "hierarchical":
        if hierarchical_target == "n_clusters":
            if n_clusters is None:
                raise ValueError("n-cluster hierarchical cutthrough requires n clusters")
            distance_threshold = None
        elif hierarchical_target == "distance_threshold":
            if distance_threshold is None:
                raise ValueError("distance-threshold hierarchical cutthrough requires cutoff distance")
            n_clusters = None
        elif hierarchical_target == "persistence":
            n_clusters = None
            distance_threshold = None
        else:
            raise ValueError(
                "hierarchical target must be one of 'distance_threshold', 'n_clusters', or 'persistence'"
            )
    return {
        "method": method_raw,
        "distance": distance_raw,
        "min_cluster_size": min_cluster_size,
        "min_samples": min_samples,
        "cluster_selection_epsilon": cluster_selection_epsilon,
        "linkage": linkage_raw,
        "hierarchical_target": hierarchical_target,
        "n_clusters": n_clusters,
        "distance_threshold": distance_threshold,
        "persistence_min_lifetime": persistence_min_lifetime,
        "hierarchical_min_cluster_size": hierarchical_min_cluster_size,
    }


def parse_interface_filter_settings(query: dict[str, list[str]]) -> dict[str, object]:
    min_interface_size_raw = query.get("min_interface_size", [str(DEFAULT_MIN_INTERFACE_SIZE)])[0].strip()
    if min_interface_size_raw == "":
        min_interface_size = DEFAULT_MIN_INTERFACE_SIZE
    else:
        min_interface_size = int(min_interface_size_raw)
    if min_interface_size < 0:
        raise ValueError("minimal interface size must be non-negative")
    return {
        "min_interface_size": min_interface_size,
    }


def interface_filter_settings_key(settings: dict[str, object] | None = None) -> str:
    normalized = {
        "min_interface_size": int((settings or {}).get("min_interface_size", DEFAULT_MIN_INTERFACE_SIZE)),
    }
    return json.dumps(normalized, sort_keys=True)


def compressed_interface_cache_key(
    interface_path: Path,
    interface_filter_settings: dict[str, object] | None = None,
) -> str:
    stat = interface_path.stat()
    return "|".join(
        (
            str(interface_path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            interface_filter_settings_key(interface_filter_settings),
        )
    )


def interface_residue_count(payload: dict[str, object], side: str = "a") -> int:
    normalized_side = str(side or "a").lower()
    if normalized_side not in {"a", "b"}:
        raise ValueError("interface side must be 'a' or 'b'")
    raw_values = payload.get(f"interface_residues_{normalized_side}")
    if raw_values is None:
        raw_values = payload.get(f"interface_msa_columns_{normalized_side}", [])
    residue_ids: set[int] = set()
    for value in raw_values or []:
        try:
            residue_ids.add(int(value))
        except (TypeError, ValueError):
            continue
    return len(residue_ids)


def filter_interface_payload(
    interface_payload: dict[str, dict[str, dict]],
    filter_settings: dict[str, object] | None = None,
) -> dict[str, dict[str, dict]]:
    min_interface_size = int((filter_settings or {}).get("min_interface_size", DEFAULT_MIN_INTERFACE_SIZE))
    raw_row_count = sum(
        len(rows)
        for rows in interface_payload.values()
        if isinstance(rows, dict)
    )
    with timed_step(
        "json",
        "filter interface payload",
        min_interface_size=min_interface_size,
        raw_rows=raw_row_count,
    ) as timer:
        if min_interface_size <= 0:
            timer.set(filtered_rows=raw_row_count, partner_domains=len(interface_payload))
            return interface_payload
        filtered_payload: dict[str, dict[str, dict]] = {}
        for partner_domain in sorted(interface_payload):
            rows = interface_payload.get(partner_domain)
            if not isinstance(rows, dict):
                continue
            kept_rows = {
                row_key: row_payload
                for row_key, row_payload in rows.items()
                if (
                    isinstance(row_payload, dict)
                    and interface_residue_count(row_payload, "a") >= min_interface_size
                    and interface_residue_count(row_payload, "b") >= min_interface_size
                )
            }
            if kept_rows:
                filtered_payload[str(partner_domain)] = kept_rows
        filtered_row_count = sum(len(rows) for rows in filtered_payload.values())
        timer.set(filtered_rows=filtered_row_count, partner_domains=len(filtered_payload))
        return filtered_payload


def distance_data_cache_key(
    interface_path: Path,
    distance_metric: str,
    interface_filter_settings: dict[str, object] | None = None,
    distance_scope: str = "expanded",
) -> str:
    stat = interface_path.stat()
    return "|".join(
        (
            str(interface_path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            distance_metric,
            distance_scope,
            interface_filter_settings_key(interface_filter_settings),
        )
    )


def distance_matrix_cache_path(
    cache_dir: Path,
    interface_path: Path,
    distance_metric: str,
    interface_filter_settings: dict[str, object] | None = None,
    distance_scope: str = "expanded",
) -> Path:
    _ = interface_filter_settings
    pfam_id = interface_file_pfam_id(interface_path)
    return cache_dir / distance_metric / distance_scope / "distance" / f"{pfam_id}.distance.npz"


def load_cached_distance_matrix(
    cache_path: Path,
    entry_count: int,
    distance_metric: str,
    distance_scope: str,
    expected_entry_signature: str | None = None,
    interface_path: Path | None = None,
    interface_filter_settings: dict[str, object] | None = None,
) -> tuple[object, object] | None:
    if not cache_path.exists():
        log_event("distance", "distance matrix disk cache missing", file=str(cache_path))
        return None
    try:
        import numpy as np

        with timed_step(
            "distance",
            "load cached distance matrix",
            file=str(cache_path),
            metric=distance_metric,
            distance_scope=distance_scope,
            entries=entry_count,
        ):
            with np.load(cache_path, allow_pickle=False) as data:
                format_version = int(np.asarray(data["format_version"]).reshape(-1)[0])
                cached_entry_count = int(np.asarray(data["entry_count"]).reshape(-1)[0])
                cached_metric = str(np.asarray(data["metric"]).reshape(-1)[0])
                cached_scope = str(np.asarray(data["distance_scope"]).reshape(-1)[0])
                distance_scale = float(np.asarray(data["distance_scale"]).reshape(-1)[0])
                condensed_u16 = data["condensed_distance"].astype(np.uint16, copy=False)
                cached_entry_signature = (
                    str(np.asarray(data["entry_signature"]).reshape(-1)[0])
                    if "entry_signature" in data.files
                    else ""
                )
                cached_source_file = (
                    str(np.asarray(data["source_file"]).reshape(-1)[0])
                    if "source_file" in data.files
                    else ""
                )
                cached_source_size = (
                    int(np.asarray(data["source_size_bytes"]).reshape(-1)[0])
                    if "source_size_bytes" in data.files
                    else None
                )
                cached_filter_settings = (
                    str(np.asarray(data["interface_filter_settings"]).reshape(-1)[0])
                    if "interface_filter_settings" in data.files
                    else ""
                )
            expected_count = condensed_distance_count(entry_count)
            expected_filter_settings = interface_filter_settings_key(interface_filter_settings)
            source_stat = interface_path.stat() if interface_path is not None else None
            if (
                format_version != DISTANCE_CACHE_FORMAT_VERSION
                or cached_entry_count != entry_count
                or cached_metric != distance_metric
                or cached_scope != distance_scope
                or len(condensed_u16) != expected_count
                or (
                    expected_entry_signature is not None
                    and cached_entry_signature != expected_entry_signature
                )
                or (
                    interface_path is not None
                    and cached_source_file
                    and cached_source_file != interface_path.name
                )
                or (
                    source_stat is not None
                    and cached_source_size is not None
                    and cached_source_size != source_stat.st_size
                )
                or (
                    cached_filter_settings
                    and cached_filter_settings != expected_filter_settings
                )
            ):
                log_event(
                    "distance",
                    "cached distance matrix metadata mismatch",
                    file=str(cache_path),
                    metric=distance_metric,
                    distance_scope=distance_scope,
                    entries=entry_count,
                )
                return None
            condensed = dequantize_condensed_distances(condensed_u16, distance_scale)
            return condensed_to_square_distance_matrix(condensed, entry_count), condensed
    except (OSError, ValueError, KeyError):
        log_event(
            "distance",
            "cached distance matrix unreadable",
            file=str(cache_path),
            metric=distance_metric,
            distance_scope=distance_scope,
            entries=entry_count,
        )
        return None


def write_distance_matrix_cache(
    cache_path: Path,
    condensed: object,
    entry_count: int,
    distance_metric: str,
    distance_scope: str,
    entry_signature: str | None = None,
    interface_path: Path | None = None,
    interface_filter_settings: dict[str, object] | None = None,
) -> None:
    import numpy as np

    with timed_step(
        "distance",
        "write distance matrix cache",
        file=str(cache_path),
        metric=distance_metric,
        distance_scope=distance_scope,
        entries=entry_count,
        condensed_values=condensed_distance_count(entry_count),
    ):
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "format_version": np.array([DISTANCE_CACHE_FORMAT_VERSION], dtype=np.uint16),
            "distance_scale": np.array([DISTANCE_CACHE_SCALE], dtype=np.uint32),
            "metric": np.array(distance_metric),
            "distance_scope": np.array(distance_scope),
            "entry_count": np.array([entry_count], dtype=np.uint32),
            "entry_signature": np.array(entry_signature or ""),
            "interface_filter_settings": np.array(interface_filter_settings_key(interface_filter_settings)),
            "condensed_distance": quantize_condensed_distances(condensed),
        }
        if interface_path is not None:
            payload.update(
                {
                    "pfam_id": np.array(interface_file_pfam_id(interface_path)),
                    "source_file": np.array(interface_path.name),
                    "source_size_bytes": np.array([interface_path.stat().st_size], dtype=np.uint64),
                }
            )
        np.savez_compressed(cache_path, **payload)


def compute_interface_feature_data(
    interface_payload: dict[str, dict[str, dict]],
    interface_path: Path | None = None,
    interface_filter_settings: dict[str, object] | None = None,
) -> dict[str, object]:
    if interface_path is not None:
        entries, compression = load_or_build_compressed_interface_data(
            interface_path,
            interface_payload,
            interface_filter_settings,
        )
    else:
        entries, compression = build_compressed_interface_data_uncached(
            interface_payload,
            log_category="points",
            parse_message="parse interfaces",
            compress_message="compress identical interfaces",
        )
    compressed_entries = compression["entries"]
    with timed_step(
        "points",
        "parse interfaces to binary vectors",
        compressed_count=len(compressed_entries),
    ) as timer:
        compressed_indicator_matrix, compressed_msa_columns = build_indicator_matrix(compressed_entries)
        timer.set(columns=len(compressed_msa_columns))
    return {
        "entries": entries,
        "distance": "binary",
        "compression_mode": compression["compression_mode"],
        "group_index_by_entry": compression["group_index_by_entry"],
        "compressed_entries": compressed_entries,
        "compressed_indicator_matrix": compressed_indicator_matrix,
        "compressed_msa_columns": compressed_msa_columns,
        "original_sample_count": len(entries),
        "compressed_sample_count": len(compressed_entries),
    }


def compute_interface_distance_data(interface_payload: dict[str, object]) -> dict[str, object]:
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Embedding view requires numpy in the Python environment running the server."
        ) from exc
    distance_metric = str(interface_payload["distance_metric"])
    interface_path = interface_payload.get("interface_path")
    cache_dir = interface_payload.get("cache_dir")
    raw_interface_payload = interface_payload["payload"]
    interface_filter_settings = interface_payload.get("interface_filter_settings")
    cache_workers = int(interface_payload.get("cache_workers") or DEFAULT_CACHE_WORKERS)
    distance_scope = str(interface_payload.get("distance_scope") or "expanded")
    if isinstance(interface_path, Path):
        entries, compression = load_or_build_compressed_interface_data(
            interface_path,
            raw_interface_payload,
            interface_filter_settings,
        )
    else:
        entries, compression = build_compressed_interface_data_uncached(
            raw_interface_payload,
            log_category="distance",
            parse_message="parse interfaces",
            compress_message="compress identical interfaces",
        )
    compressed_entries = compression["entries"]
    entry_signature = distance_entries_signature(entries)
    compressed_entry_signature = distance_entries_signature(compressed_entries)
    compressed_cache_path = (
        distance_matrix_cache_path(
            cache_dir,
            interface_path,
            distance_metric,
            interface_filter_settings,
            distance_scope="compressed",
        )
        if isinstance(cache_dir, Path) and isinstance(interface_path, Path)
        else None
    )
    cached_compressed_matrix = (
        load_cached_distance_matrix(
            compressed_cache_path,
            len(compressed_entries),
            distance_metric,
            "compressed",
            expected_entry_signature=compressed_entry_signature,
            interface_path=interface_path,
            interface_filter_settings=interface_filter_settings,
        )
        if compressed_cache_path is not None
        else None
    )
    if cached_compressed_matrix is not None:
        compressed_distance_matrix, compressed_distance_condensed = cached_compressed_matrix
        with timed_step(
            "distance",
            "parse compressed interfaces to binary vectors",
            metric=distance_metric,
            compressed_count=len(compressed_entries),
        ) as timer:
            compressed_indicator_matrix, compressed_msa_columns = build_indicator_matrix(compressed_entries)
            timer.set(columns=len(compressed_msa_columns))
    else:
        with timed_step(
            "distance",
            "calculate compressed distance matrix",
            metric=distance_metric,
            compressed_count=len(compressed_entries),
            workers=cache_workers,
        ) as timer:
            (
                compressed_indicator_matrix,
                compressed_msa_columns,
                compressed_distance_matrix,
                compressed_distance_condensed,
            ) = compute_distance_matrix_from_entries(
                compressed_entries,
                distance_metric,
                worker_count=cache_workers,
            )
            timer.set(columns=len(compressed_msa_columns))
        if compressed_cache_path is not None:
            write_distance_matrix_cache(
                compressed_cache_path,
                compressed_distance_condensed,
                len(compressed_entries),
                distance_metric,
                "compressed",
                entry_signature=compressed_entry_signature,
                interface_path=interface_path,
                interface_filter_settings=interface_filter_settings,
            )
    response: dict[str, object] = {
        "entries": entries,
        "distance": distance_metric,
        "compression_mode": compression["compression_mode"],
        "group_index_by_entry": compression["group_index_by_entry"],
        "compressed_entries": compressed_entries,
        "compressed_indicator_matrix": compressed_indicator_matrix,
        "compressed_msa_columns": compressed_msa_columns,
        "compressed_distance_matrix": compressed_distance_matrix,
        "compressed_distance_condensed": compressed_distance_condensed,
        "original_sample_count": len(entries),
        "compressed_sample_count": len(compressed_entries),
    }
    if distance_scope == "compressed":
        return response

    expanded_cache_path = (
        distance_matrix_cache_path(
            cache_dir,
            interface_path,
            distance_metric,
            interface_filter_settings,
            distance_scope="expanded",
        )
        if isinstance(cache_dir, Path) and isinstance(interface_path, Path)
        else None
    )
    cached_expanded_matrix = (
        load_cached_distance_matrix(
            expanded_cache_path,
            len(entries),
            distance_metric,
            "expanded",
            expected_entry_signature=entry_signature,
            interface_path=interface_path,
            interface_filter_settings=interface_filter_settings,
        )
        if expanded_cache_path is not None
        else None
    )
    if cached_expanded_matrix is not None:
        distance_matrix, distance_condensed = cached_expanded_matrix
        with timed_step(
            "distance",
            "parse expanded interfaces to binary vectors",
            metric=distance_metric,
            interface_count=len(entries),
        ) as timer:
            indicator_matrix, msa_columns = build_indicator_matrix(entries)
            timer.set(columns=len(msa_columns))
    else:
        with timed_step(
            "distance",
            "calculate expanded distance matrix",
            metric=distance_metric,
            interface_count=len(entries),
            workers=cache_workers,
        ) as timer:
            indicator_matrix, msa_columns, distance_matrix, distance_condensed = compute_distance_matrix_from_entries(
                entries,
                distance_metric,
                worker_count=cache_workers,
            )
            timer.set(columns=len(msa_columns))
        if expanded_cache_path is not None:
            write_distance_matrix_cache(
                expanded_cache_path,
                distance_condensed,
                len(entries),
                distance_metric,
                "expanded",
                entry_signature=entry_signature,
                interface_path=interface_path,
                interface_filter_settings=interface_filter_settings,
            )
    response.update(
        {
            "indicator_matrix": indicator_matrix,
            "msa_columns": msa_columns,
            "distance_matrix": distance_matrix,
            "distance_condensed": distance_condensed,
        }
    )
    return response


def load_interface_distance_data(
    cache_dir: Path,
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
    distance_metric: str,
    interface_filter_settings: dict[str, object] | None = None,
    distance_scope: str = "expanded",
    cache_workers: int = DEFAULT_CACHE_WORKERS,
) -> dict[str, object]:
    cache_key = distance_data_cache_key(
        interface_path,
        distance_metric,
        interface_filter_settings,
        distance_scope,
    )
    owner = False
    with DISTANCE_DATA_CACHE_LOCK:
        cached = DISTANCE_DATA_CACHE.get(cache_key)
        if cached is not None:
            DISTANCE_DATA_CACHE.move_to_end(cache_key)
            log_event(
                "distance",
                "reuse in-memory distance data",
                file=interface_path.name,
                metric=distance_metric,
                distance_scope=distance_scope,
            )
            return cached
        future = DISTANCE_DATA_IN_FLIGHT.get(cache_key)
        if future is None:
            future = Future()
            DISTANCE_DATA_IN_FLIGHT[cache_key] = future
            owner = True
    if not owner:
        with timed_step(
            "distance",
            "wait for in-flight distance data",
            file=interface_path.name,
            metric=distance_metric,
            distance_scope=distance_scope,
        ):
            return future.result()
    try:
        with timed_step(
            "distance",
            "load distance data",
            file=interface_path.name,
            metric=distance_metric,
            distance_scope=distance_scope,
        ) as timer:
            distance_data = compute_interface_distance_data(
                {
                    "payload": interface_payload,
                    "distance_metric": distance_metric,
                    "interface_path": interface_path,
                    "cache_dir": cache_dir,
                    "interface_filter_settings": interface_filter_settings,
                    "distance_scope": distance_scope,
                    "cache_workers": cache_workers,
                }
            )
            timer.set(
                interface_count=distance_data.get("original_sample_count"),
                compressed_count=distance_data.get("compressed_sample_count"),
            )
    except Exception as exc:
        with DISTANCE_DATA_CACHE_LOCK:
            DISTANCE_DATA_IN_FLIGHT.pop(cache_key, None)
            future.set_exception(exc)
        raise
    with DISTANCE_DATA_CACHE_LOCK:
        DISTANCE_DATA_CACHE[cache_key] = distance_data
        DISTANCE_DATA_CACHE.move_to_end(cache_key)
        while len(DISTANCE_DATA_CACHE) > DISTANCE_DATA_CACHE_LIMIT:
            DISTANCE_DATA_CACHE.popitem(last=False)
        DISTANCE_DATA_IN_FLIGHT.pop(cache_key, None)
        future.set_result(distance_data)
    return distance_data


def load_interface_point_data(
    cache_dir: Path,
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
    distance_metric: str,
    interface_filter_settings: dict[str, object] | None = None,
    cache_workers: int = DEFAULT_CACHE_WORKERS,
) -> dict[str, object]:
    with timed_step(
        "points",
        "load point data",
        file=interface_path.name,
        distance=distance_metric,
    ) as timer:
        if distance_metric == "binary":
            point_data = compute_interface_feature_data(
                interface_payload,
                interface_path=interface_path,
                interface_filter_settings=interface_filter_settings,
            )
        else:
            point_data = load_interface_distance_data(
                cache_dir,
                interface_path,
                interface_payload,
                distance_metric,
                interface_filter_settings,
                distance_scope="compressed",
                cache_workers=cache_workers,
            )
        timer.set(
            interface_count=point_data.get("original_sample_count"),
            compressed_count=point_data.get("compressed_sample_count"),
        )
        return point_data


def normalize_coordinates(coordinates: object) -> object:
    import numpy as np

    coordinates = np.asarray(coordinates, dtype=np.float64)
    if coordinates.ndim != 2:
        raise ValueError("embedding coordinates must be a 2D array")
    if coordinates.shape[1] < 3:
        padding = np.zeros((coordinates.shape[0], 3 - coordinates.shape[1]), dtype=np.float64)
        coordinates = np.hstack((coordinates, padding))
    elif coordinates.shape[1] > 3:
        coordinates = coordinates[:, :3]
    coordinates -= coordinates.mean(axis=0, keepdims=True)
    max_radius = float(np.linalg.norm(coordinates, axis=1).max()) if coordinates.size else 0.0
    if max_radius > 0.0:
        coordinates /= max_radius
    return coordinates


def embedding_points_payload(
    point_data: dict[str, object],
    coordinates: object,
    *,
    embedding_name: str,
    settings: dict[str, object],
    extra_payload: dict[str, object] | None = None,
) -> dict[str, object]:
    entries = point_data["compressed_entries"]
    msa_columns = point_data["compressed_msa_columns"]
    with timed_step(
        "points",
        "serialize point payload",
        embedding=embedding_name,
        compressed_count=len(entries),
    ) as timer:
        coordinates = normalize_coordinates(coordinates)
        points = []
        for entry, coords in zip(entries, coordinates.tolist(), strict=True):
            points.append(
                {
                    "group_id": str(entry["group_id"]),
                    "row_key": str(entry["row_key"]),
                    "partner_domain": str(entry["partner_domain"]),
                    "interface_size": len(entry["columns"]),
                    "member_count": int(entry["member_count"]),
                    "members": entry["members"],
                    "x": float(coords[0]),
                    "y": float(coords[1]),
                    "z": float(coords[2]),
                }
            )
        timer.set(points=len(points), columns=len(msa_columns))
    payload = {
        "embedding": embedding_name,
        "method": str(settings["method"]),
        "distance": str(point_data["distance"]),
        "dimensions": 3,
        "compression_mode": point_data["compression_mode"],
        "sample_count": len(points),
        "original_sample_count": int(point_data["original_sample_count"]),
        "compressed_sample_count": int(point_data["compressed_sample_count"]),
        "column_count": len(msa_columns),
        "settings": {
            "method": settings["method"],
            "distance": settings["distance"],
            "random_state": settings["random_state"],
        },
        "points": points,
    }
    if extra_payload:
        base_settings = payload["settings"]
        extra_settings = extra_payload.get("settings", {})
        payload.update({key: value for key, value in extra_payload.items() if key != "settings"})
        payload["settings"] = {
            **base_settings,
            **extra_settings,
        }
    return payload


def compute_tsne_embedding_payload(
    point_data: dict[str, object],
    settings: dict[str, object],
    worker_count: int = DEFAULT_CACHE_WORKERS,
) -> dict[str, object]:
    try:
        with timed_step("points", "import openTSNE"):
            import numpy as np
            from openTSNE import TSNE
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "t-SNE point layout requires openTSNE in the Python environment running the server."
        ) from exc

    entries = point_data["compressed_entries"]
    distance_metric = str(point_data["distance"])
    sample_count = len(entries)
    perplexity = settings["perplexity"]
    if sample_count == 1:
        log_event("points", "skip openTSNE fit for single point", samples=sample_count)
        coordinates = np.zeros((1, 3), dtype=np.float64)
        resolved_perplexity = 0.0
        resolved_neighbors = "none"
    else:
        if perplexity == "auto":
            perplexity = min(30.0, max(1.0, (sample_count - 1) / 3.0))
        if float(perplexity) >= sample_count:
            raise ValueError(f"perplexity must be smaller than the number of samples ({sample_count})")
        resolved_perplexity = float(perplexity)
        with timed_step(
            "points",
            "prepare openTSNE input",
            distance=distance_metric,
            samples=sample_count,
        ) as timer:
            if distance_metric == "binary":
                tsne_input = np.asarray(point_data["compressed_indicator_matrix"], dtype=np.float64)
                metric = "euclidean"
                resolved_neighbors = str(settings["neighbors"])
                initialization = "pca"
            else:
                tsne_input = np.asarray(point_data["compressed_distance_matrix"], dtype=np.float64)
                metric = "precomputed"
                resolved_neighbors = "exact"
                initialization = "random"
            timer.set(columns=tsne_input.shape[1], metric=metric, neighbors=resolved_neighbors)
        with timed_step(
            "points",
            "run openTSNE",
            samples=sample_count,
            dimensions=3,
            metric=metric,
            neighbors=resolved_neighbors,
            theta=settings["theta"],
            workers=max(1, int(worker_count)),
        ):
            coordinates = TSNE(
                n_components=3,
                metric=metric,
                initialization=initialization,
                random_state=int(settings["random_state"]),
                perplexity=resolved_perplexity,
                learning_rate=settings["learning_rate"],
                early_exaggeration_iter=int(settings["early_exaggeration_iter"]),
                n_iter=int(settings["max_iter"]),
                early_exaggeration=float(settings["early_exaggeration"]),
                negative_gradient_method="bh",
                theta=float(settings["theta"]),
                neighbors=resolved_neighbors,
                n_jobs=max(1, int(worker_count)),
            ).fit(tsne_input)
    return embedding_points_payload(
        point_data,
        coordinates,
        embedding_name="opentsne",
        settings=settings,
        extra_payload={
            "input_metric": "binary_euclidean" if distance_metric == "binary" else "precomputed",
            "perplexity": resolved_perplexity,
            "neighbors": resolved_neighbors,
            "settings": {
                "perplexity": settings["perplexity"],
                "learning_rate": settings["learning_rate"],
                "max_iter": settings["max_iter"],
                "early_exaggeration_iter": settings["early_exaggeration_iter"],
                "early_exaggeration": settings["early_exaggeration"],
                "neighbors": settings["neighbors"],
                "resolved_neighbors": resolved_neighbors,
                "theta": settings["theta"],
            },
        },
    )


def compute_pca_embedding_payload(point_data: dict[str, object], settings: dict[str, object]) -> dict[str, object]:
    import numpy as np

    distance_metric = str(point_data["distance"])
    with timed_step("points", "prepare PCA input", distance=distance_metric) as timer:
        if distance_metric == "binary":
            matrix = np.asarray(point_data["compressed_indicator_matrix"], dtype=np.float64)
            input_metric = "binary_columns"
        else:
            matrix = np.asarray(point_data["compressed_distance_matrix"], dtype=np.float64)
            input_metric = "distance_profile"
        timer.set(samples=matrix.shape[0], columns=matrix.shape[1], input_metric=input_metric)
    sample_count = matrix.shape[0]
    if sample_count == 1:
        log_event("points", "skip PCA SVD for single point", samples=sample_count)
        coordinates = np.zeros((1, 3), dtype=np.float64)
        explained_variance = [0.0, 0.0, 0.0]
    else:
        with timed_step(
            "points",
            "run PCA SVD",
            samples=sample_count,
            columns=matrix.shape[1],
            input_metric=input_metric,
        ):
            centered = matrix - matrix.mean(axis=0, keepdims=True)
            _, singular_values, components = np.linalg.svd(centered, full_matrices=False)
            component_count = min(3, components.shape[0])
            coordinates = centered @ components[:component_count].T
            if component_count < 3:
                padding = np.zeros((sample_count, 3 - component_count), dtype=np.float64)
                coordinates = np.hstack((coordinates, padding))
            variance = singular_values ** 2
            total_variance = float(variance.sum())
            explained_variance = [
                (float(value) / total_variance) if total_variance > 0 else 0.0
                for value in variance[:3].tolist()
            ]
            while len(explained_variance) < 3:
                explained_variance.append(0.0)
    return embedding_points_payload(
        point_data,
        coordinates,
        embedding_name="pca",
        settings=settings,
        extra_payload={
            "input_metric": input_metric,
            "explained_variance_ratio": explained_variance,
            "settings": {
                "explained_variance_ratio": explained_variance,
            },
        },
    )


def compute_embedding_payload(
    point_data: dict[str, object],
    settings: dict[str, object],
    worker_count: int = DEFAULT_CACHE_WORKERS,
) -> dict[str, object]:
    with timed_step(
        "points",
        "compute point layout",
        method=settings["method"],
        distance=settings["distance"],
        workers=worker_count,
    ) as timer:
        if settings["method"] == "pca":
            payload = compute_pca_embedding_payload(point_data, settings)
        else:
            payload = compute_tsne_embedding_payload(point_data, settings, worker_count=worker_count)
        timer.set(points=len(payload.get("points", [])))
        return payload


def compute_hdbscan_clustering_payload(distance_data: dict[str, object], settings: dict[str, object]) -> dict[str, object]:
    try:
        with timed_step("clustering", "import HDBSCAN"):
            from hdbscan import HDBSCAN
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Embedding clustering requires the standalone hdbscan package in the Python environment running the server."
        ) from exc
    entries = distance_data["entries"]
    distance_matrix = distance_data["distance_matrix"]
    distance_metric = str(distance_data["distance"])
    min_cluster_size = int(settings["min_cluster_size"])
    min_samples = settings["min_samples"]
    cluster_selection_epsilon = float(settings["cluster_selection_epsilon"])
    clusterer = HDBSCAN(
        metric="precomputed",
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        cluster_selection_epsilon=cluster_selection_epsilon,
        allow_single_cluster=True,
        core_dist_n_jobs=1,
    )
    try:
        with timed_step(
            "clustering",
            "run HDBSCAN",
            samples=len(entries),
            metric=distance_metric,
            min_cluster_size=min_cluster_size,
        ):
            labels = clusterer.fit_predict(distance_matrix)
    except Exception as exc:
        raise RuntimeError(f"HDBSCAN failed unexpectedly: {exc}") from exc
    labels_list = rank_non_negative_cluster_labels_by_size([int(label) for label in labels.tolist()])
    unique_cluster_labels = sorted({label for label in labels_list if label >= 0})
    cluster_count = len(unique_cluster_labels)
    noise_count = int(sum(1 for label in labels_list if label < 0))
    with timed_step(
        "clustering",
        "serialize HDBSCAN labels",
        samples=len(entries),
        clusters=cluster_count,
        noise=noise_count,
    ):
        points = [
            {
                "row_key": str(entry["row_key"]),
                "partner_domain": str(entry["partner_domain"]),
                "cluster_label": label,
            }
            for entry, label in zip(entries, labels_list, strict=True)
        ]
    return {
        "clustering": "hdbscan",
        "distance": distance_metric,
        "sample_count": len(points),
        "cluster_count": cluster_count,
        "noise_count": noise_count,
        "settings": {
            "distance": distance_metric,
            "min_cluster_size": min_cluster_size,
            "min_samples": min_samples,
            "cluster_selection_epsilon": cluster_selection_epsilon,
        },
        "points": points,
    }


def compute_hierarchical_clustering_payload(distance_data: dict[str, object], settings: dict[str, object]) -> dict[str, object]:
    distance_metric = str(distance_data["distance"])
    linkage = str(settings["linkage"])
    with timed_step(
        "hierarchy",
        "compute hierarchy for clustering",
        metric=distance_metric,
        linkage=linkage,
    ):
        hierarchy = compute_local_hierarchy(distance_data, linkage)
    return compute_hierarchical_clustering_payload_from_hierarchy(
        distance_data["entries"],
        distance_data["compressed_entries"],
        distance_data["group_index_by_entry"],
        distance_metric,
        settings,
        hierarchy,
    )


def embedding_cache_path(
    cache_dir: Path,
    interface_path: Path,
    settings: dict[str, object],
    interface_filter_settings: dict[str, object] | None = None,
) -> Path:
    stat = interface_path.stat()
    key = hashlib.sha1(
        (
            EMBEDDING_CACHE_VERSION
            + "|"
            + str(interface_path.resolve())
            + "|"
            + str(stat.st_size)
            + "|"
            + str(stat.st_mtime_ns)
            + "|"
            + interface_filter_settings_key(interface_filter_settings)
            + "|"
            + json.dumps(settings, sort_keys=True)
        ).encode("utf-8")
    ).hexdigest()
    return cache_dir / "embeddings" / f"{key}.json"


def clustering_cache_path(
    cache_dir: Path,
    interface_path: Path,
    settings: dict[str, object],
    interface_filter_settings: dict[str, object] | None = None,
) -> Path:
    stat = interface_path.stat()
    key = hashlib.sha1(
        (
            CLUSTERING_CACHE_VERSION
            + "|"
            + str(interface_path.resolve())
            + "|"
            + str(stat.st_size)
            + "|"
            + str(stat.st_mtime_ns)
            + "|"
            + interface_filter_settings_key(interface_filter_settings)
            + "|"
            + json.dumps(settings, sort_keys=True)
        ).encode("utf-8")
    ).hexdigest()
    return cache_dir / "clusterings" / f"{key}.json"


def hierarchy_cache_path(
    cache_dir: Path,
    interface_path: Path,
    distance_metric: str,
    linkage_method: str,
    interface_filter_settings: dict[str, object] | None = None,
) -> Path:
    key = hashlib.sha1(
        (
            HIERARCHY_CACHE_VERSION
            + "|"
            + distance_data_cache_key(
                interface_path,
                distance_metric,
                interface_filter_settings,
                distance_scope="compressed",
            )
            + "|"
            + str(linkage_method)
        ).encode("utf-8")
    ).hexdigest()
    return cache_dir / "hierarchies" / f"{key}.npz"


def compressed_entry_signature(entry: dict[str, object]) -> tuple[tuple[int, ...], tuple[tuple[str, str], ...]]:
    members = entry.get("members", [])
    normalized_members: list[tuple[str, str]] = []
    if isinstance(members, list):
        for member in members:
            if not isinstance(member, dict):
                continue
            normalized_members.append(
                (
                    str(member.get("partner_domain", "")),
                    str(member.get("row_key", "")),
                )
            )
    return (
        tuple(int(column) for column in entry.get("columns", [])),
        tuple(sorted(normalized_members)),
    )


def compressed_entry_columns_signature(entry: dict[str, object]) -> tuple[int, ...]:
    return tuple(int(column) for column in entry.get("columns", []))


def leaf_signature(leaf: dict[str, object]) -> tuple[tuple[int, ...], tuple[tuple[str, str], ...]]:
    raw_interfaces = leaf.get("interfaces", [])
    normalized_interfaces: list[tuple[str, str]] = []
    if isinstance(raw_interfaces, list):
        for interface in raw_interfaces:
            if not isinstance(interface, dict):
                continue
            normalized_interfaces.append(
                (
                    str(interface.get("partner_domain", "")),
                    str(interface.get("row_key", "")),
                )
            )
    return (
        tuple(int(column) for column in leaf.get("interface_msa_columns_a", [])),
        tuple(sorted(normalized_interfaces)),
    )


def leaf_columns_signature(leaf: dict[str, object]) -> tuple[int, ...]:
    return tuple(int(column) for column in leaf.get("interface_msa_columns_a", []))


def compressed_entry_member_set(entry: dict[str, object]) -> set[tuple[str, str]]:
    _columns, members = compressed_entry_signature(entry)
    return set(members)


def leaf_interface_set(leaf: dict[str, object]) -> set[tuple[str, str]]:
    _columns, interfaces = leaf_signature(leaf)
    return set(interfaces)


def compressed_entries_signature_hash(compressed_entries: list[dict[str, object]]) -> str:
    normalized = [
        {
            "columns": list(columns),
            "interfaces": [
                {"partner_domain": partner_domain, "row_key": row_key}
                for partner_domain, row_key in members
            ],
        }
        for columns, members in (compressed_entry_signature(entry) for entry in compressed_entries)
    ]
    return hashlib.sha1(
        json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def np_scalar(value: object, default: object = None) -> object:
    try:
        item = value.item()  # type: ignore[attr-defined]
    except AttributeError:
        return default if value is None else value
    except ValueError:
        return default
    return item


def load_hierarchy_linkage_file(
    linkage_path: Path,
    expected_distance_metric: str,
    expected_linkage_method: str,
) -> dict[str, object]:
    import numpy as np

    with timed_step(
        "hierarchy",
        "load linkage file",
        file=linkage_path.name,
        metric=expected_distance_metric,
        linkage=expected_linkage_method,
    ) as timer:
        with np.load(linkage_path, allow_pickle=False) as data:
            metric_value = np_scalar(data["metric"]) if "metric" in data.files else expected_distance_metric
            linkage_value = (
                np_scalar(data["linkage"])
                if "linkage" in data.files and getattr(data["linkage"], "ndim", 1) == 0
                else expected_linkage_method
            )
            metric = str(metric_value)
            linkage = str(linkage_value)
            if metric and metric != expected_distance_metric:
                raise ValueError(
                    f"precalculated hierarchy metric is {metric}, expected {expected_distance_metric}"
                )
            if linkage and linkage != expected_linkage_method:
                raise ValueError(
                    f"precalculated hierarchy linkage is {linkage}, expected {expected_linkage_method}"
                )
            leaf_weighting = (
                str(np_scalar(data["linkage_leaf_weighting"]))
                if "linkage_leaf_weighting" in data.files
                else ""
            )
            if expected_linkage_method == "average" and leaf_weighting != "interface_count":
                raise ValueError(
                    "precalculated average hierarchy is not interface-weighted; "
                    "regenerate it with the current precompute script, or use "
                    "average_deduplicated for old equal-leaf average linkage"
                )
            if expected_linkage_method == "average_deduplicated" and leaf_weighting not in {
                "",
                "unit_leaf",
            }:
                raise ValueError(
                    "precalculated average_deduplicated hierarchy has incompatible "
                    f"leaf weighting: {leaf_weighting}"
                )
            if "children" in data.files and "distance" in data.files:
                children = data["children"].astype(np.int64, copy=False)
                raw_distances = data["distance"]
                distance_scale = float(np_scalar(data["distance_scale"], 0.0) or 0.0)
                if np.issubdtype(raw_distances.dtype, np.integer) and distance_scale > 0.0:
                    distances = raw_distances.astype(np.float64) / distance_scale
                else:
                    distances = raw_distances.astype(np.float64, copy=False)
                counts = (
                    data["count"].astype(np.int64, copy=False)
                    if "count" in data.files
                    else np.zeros((len(distances),), dtype=np.int64)
                )
                leaf_interface_counts = (
                    data["leaf_interface_count"].astype(np.int64, copy=False)
                    if "leaf_interface_count" in data.files
                    else None
                )
                if "leaf_count" in data.files:
                    leaf_count = int(np.asarray(data["leaf_count"]).reshape(-1)[0])
                else:
                    leaf_count = int(len(children) + 1)
            else:
                matrix_key = next(
                    (
                        key
                        for key in ("linkage_matrix", "Z", "linkage")
                        if key in data.files
                        and getattr(data[key], "ndim", 0) == 2
                        and data[key].shape[1] >= 4
                    ),
                    None,
                )
                if matrix_key is None:
                    raise ValueError(f"{linkage_path} is not a supported hierarchy npz file")
                matrix = data[matrix_key].astype(np.float64, copy=False)
                children = matrix[:, :2].astype(np.int64)
                distances = matrix[:, 2].astype(np.float64)
                counts = matrix[:, 3].astype(np.int64)
                leaf_interface_counts = None
                leaf_count = int(len(matrix) + 1)

        if children.ndim != 2 or children.shape[1] != 2:
            raise ValueError(f"{linkage_path} has invalid children shape")
        if len(children) != max(0, leaf_count - 1) or len(distances) != len(children):
            raise ValueError(f"{linkage_path} has inconsistent linkage dimensions")
        if leaf_interface_counts is not None and len(leaf_interface_counts) != leaf_count:
            raise ValueError(f"{linkage_path} has inconsistent leaf_interface_count length")
        timer.set(leaf_count=leaf_count, merges=len(children))
        hierarchy = {
            "children": children,
            "distances": distances,
            "counts": counts,
            "leaf_count": leaf_count,
            "distance": expected_distance_metric,
            "linkage": expected_linkage_method,
        }
        if leaf_interface_counts is not None:
            hierarchy["leaf_interface_counts"] = leaf_interface_counts
        return hierarchy


def precomputed_hierarchy_paths(
    hierarchy_dir: Path | None,
    pfam_id: str,
    distance_metric: str,
    linkage_method: str,
) -> tuple[Path, Path] | None:
    if hierarchy_dir is None:
        return None
    option_dir = hierarchy_dir / distance_metric / linkage_method
    linkage_path = option_dir / "linkage" / f"{pfam_id}.linkage.npz"
    resolver_path = option_dir / "resolver" / f"{pfam_id}.leaves.json"
    if not linkage_path.exists() or not resolver_path.exists():
        return None
    return linkage_path, resolver_path


def precomputed_hierarchy_cache_key(
    linkage_path: Path,
    resolver_path: Path,
    distance_metric: str,
    linkage_method: str,
    compressed_signature_hash: str,
) -> str:
    linkage_stat = linkage_path.stat()
    resolver_stat = resolver_path.stat()
    return "|".join(
        (
            str(linkage_path.resolve()),
            str(linkage_stat.st_size),
            str(linkage_stat.st_mtime_ns),
            str(resolver_path.resolve()),
            str(resolver_stat.st_size),
            str(resolver_stat.st_mtime_ns),
            distance_metric,
            linkage_method,
            compressed_signature_hash,
        )
    )


def map_resolver_leaves_to_compressed_entries(
    resolver_path: Path,
    compressed_entries: list[dict[str, object]],
) -> list[list[int]]:
    with timed_step("hierarchy", "read resolver", file=resolver_path.name) as timer:
        with resolver_path.open("r", encoding="utf-8") as handle:
            resolver = json.load(handle)
        timer.set(leaves=len(resolver.get("leaves", [])) if isinstance(resolver, dict) else None)
    if not isinstance(resolver, dict) or not isinstance(resolver.get("leaves"), list):
        raise ValueError(f"{resolver_path} is not a supported hierarchy resolver")
    leaves = resolver["leaves"]
    with timed_step(
        "hierarchy",
        "apply resolver",
        file=resolver_path.name,
        leaves=len(leaves),
        compressed_count=len(compressed_entries),
    ) as timer:
        columns_to_group_indices: dict[tuple[int, ...], list[int]] = {}
        member_sets_by_group_index: dict[int, set[tuple[str, str]]] = {}
        for group_index, entry in enumerate(compressed_entries):
            columns = compressed_entry_columns_signature(entry)
            columns_to_group_indices.setdefault(columns, []).append(group_index)
            member_sets_by_group_index[group_index] = compressed_entry_member_set(entry)

        leaf_to_group_indices: list[list[int]] = [[] for _ in leaves]
        covered_group_indices: set[int] = set()
        for fallback_leaf_id, leaf in enumerate(leaves):
            if not isinstance(leaf, dict):
                raise ValueError(f"{resolver_path} contains an invalid leaf entry")
            leaf_id = int(leaf.get("leaf_id", fallback_leaf_id))
            if leaf_id < 0 or leaf_id >= len(leaves):
                raise ValueError(f"{resolver_path} contains invalid leaf id {leaf_id}")
            leaf_columns = leaf_columns_signature(leaf)
            candidate_group_indices = columns_to_group_indices.get(leaf_columns, [])
            if not candidate_group_indices:
                continue
            resolver_interfaces = leaf_interface_set(leaf)
            if resolver_interfaces:
                group_indices = [
                    group_index
                    for group_index in candidate_group_indices
                    if member_sets_by_group_index[group_index].issubset(resolver_interfaces)
                ]
            else:
                group_indices = [
                    group_index
                    for group_index in candidate_group_indices
                    if group_index not in covered_group_indices
                ]
            if not group_indices:
                continue
            duplicate_group_indices = covered_group_indices.intersection(group_indices)
            if duplicate_group_indices:
                raise ValueError("precalculated hierarchy resolver maps an interface group more than once")
            leaf_to_group_indices[leaf_id] = list(group_indices)
            covered_group_indices.update(group_indices)

        if covered_group_indices != set(range(len(compressed_entries))):
            raise ValueError("precalculated hierarchy resolver does not match current compressed interfaces")
        timer.set(
            mapped_leaves=sum(1 for group_indices in leaf_to_group_indices if group_indices),
            mapped_groups=len(covered_group_indices),
            skipped_leaves=sum(1 for group_indices in leaf_to_group_indices if not group_indices),
        )
        return leaf_to_group_indices


def load_precomputed_hierarchy(
    hierarchy_dir: Path | None,
    interface_path: Path,
    distance_metric: str,
    linkage_method: str,
    compressed_entries: list[dict[str, object]],
    compressed_signature_hash: str | None = None,
) -> dict[str, object] | None:
    pfam_id = interface_file_pfam_id(interface_path)
    with timed_step(
        "hierarchy",
        "lookup precalculated hierarchy",
        pfam_id=pfam_id,
        metric=distance_metric,
        linkage=linkage_method,
        hierarchy_dir=hierarchy_dir,
    ) as timer:
        paths = precomputed_hierarchy_paths(
            hierarchy_dir,
            pfam_id,
            distance_metric,
            linkage_method,
        )
        timer.set(found=paths is not None)
    if paths is None:
        return None
    linkage_path, resolver_path = paths
    signature_hash = compressed_signature_hash or compressed_entries_signature_hash(compressed_entries)
    cache_key = precomputed_hierarchy_cache_key(
        linkage_path,
        resolver_path,
        distance_metric,
        linkage_method,
        signature_hash,
    )
    owner = False
    with PRECOMPUTED_HIERARCHY_CACHE_LOCK:
        cached = PRECOMPUTED_HIERARCHY_CACHE.get(cache_key)
        if cached is not None:
            PRECOMPUTED_HIERARCHY_CACHE.move_to_end(cache_key)
            log_event(
                "hierarchy",
                "reuse precalculated hierarchy",
                linkage_file=linkage_path.name,
                resolver_file=resolver_path.name,
                leaf_count=cached.get("leaf_count"),
            )
            return cached
        future = PRECOMPUTED_HIERARCHY_IN_FLIGHT.get(cache_key)
        if future is None:
            future = Future()
            PRECOMPUTED_HIERARCHY_IN_FLIGHT[cache_key] = future
            owner = True
    if not owner:
        with timed_step(
            "hierarchy",
            "wait for in-flight precalculated hierarchy",
            linkage_file=linkage_path.name,
            resolver_file=resolver_path.name,
        ):
            return future.result()
    try:
        leaf_to_group_indices = map_resolver_leaves_to_compressed_entries(resolver_path, compressed_entries)
        hierarchy = load_hierarchy_linkage_file(linkage_path, distance_metric, linkage_method)
        with timed_step(
            "hierarchy",
            "validate precalculated hierarchy",
            linkage_file=linkage_path.name,
            resolver_file=resolver_path.name,
            leaf_count=hierarchy["leaf_count"],
        ):
            if int(hierarchy["leaf_count"]) != len(leaf_to_group_indices):
                raise ValueError("precalculated hierarchy linkage and resolver leaf counts differ")
            hierarchy.update(
                {
                    "source": "precalculated",
                    "leaf_to_group_indices": leaf_to_group_indices,
                    "linkage_file": str(linkage_path),
                    "resolver_file": str(resolver_path),
                }
            )
    except Exception as exc:
        with PRECOMPUTED_HIERARCHY_CACHE_LOCK:
            PRECOMPUTED_HIERARCHY_IN_FLIGHT.pop(cache_key, None)
            future.set_exception(exc)
        raise
    with PRECOMPUTED_HIERARCHY_CACHE_LOCK:
        PRECOMPUTED_HIERARCHY_CACHE[cache_key] = hierarchy
        PRECOMPUTED_HIERARCHY_CACHE.move_to_end(cache_key)
        while len(PRECOMPUTED_HIERARCHY_CACHE) > PRECOMPUTED_HIERARCHY_CACHE_LIMIT:
            PRECOMPUTED_HIERARCHY_CACHE.popitem(last=False)
        PRECOMPUTED_HIERARCHY_IN_FLIGHT.pop(cache_key, None)
        future.set_result(hierarchy)
    return hierarchy


def local_hierarchy_cache_is_valid(cache_path: Path, leaf_signature_hash: str) -> bool:
    if not cache_path.exists():
        log_event("hierarchy", "local hierarchy cache missing", file=cache_path.name)
        return False
    try:
        import numpy as np

        with timed_step("hierarchy", "validate local hierarchy cache", file=cache_path.name) as timer:
            with np.load(cache_path, allow_pickle=False) as data:
                cached_hash = str(np_scalar(data["leaf_signature_hash"])) if "leaf_signature_hash" in data.files else ""
            matches = cached_hash == leaf_signature_hash
            timer.set(matches=matches)
            return matches
    except (OSError, ValueError, KeyError):
        log_event("hierarchy", "local hierarchy cache unreadable", file=cache_path.name)
        return False


def load_cached_local_hierarchy(cache_path: Path, leaf_signature_hash: str) -> dict[str, object] | None:
    if not cache_path.exists():
        return None
    try:
        import numpy as np

        with timed_step("hierarchy", "load cached local hierarchy", file=cache_path.name) as timer:
            with np.load(cache_path, allow_pickle=False) as data:
                cached_hash = str(np_scalar(data["leaf_signature_hash"])) if "leaf_signature_hash" in data.files else ""
                if cached_hash != leaf_signature_hash:
                    timer.set(matches=False)
                    return None
                children = data["children"].astype(np.int64, copy=False)
                distances = data["distance"].astype(np.float64, copy=False)
                counts = data["count"].astype(np.int64, copy=False)
                leaf_count = int(np.asarray(data["leaf_count"]).reshape(-1)[0])
            timer.set(matches=True, leaf_count=leaf_count, merges=len(children))
    except (OSError, ValueError, KeyError):
        log_event("hierarchy", "cached local hierarchy unreadable", file=cache_path.name)
        return None
    return {
        "source": "local_cache",
        "children": children,
        "distances": distances,
        "counts": counts,
        "leaf_count": leaf_count,
        "leaf_to_group_index": list(range(leaf_count)),
    }


def compute_local_hierarchy(distance_data: dict[str, object], linkage_method: str) -> dict[str, object]:
    try:
        with timed_step("hierarchy", "import scipy hierarchy"):
            import numpy as np
            from scipy.cluster.hierarchy import linkage as scipy_linkage
            from scipy.spatial.distance import squareform
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Local hierarchical clustering requires scipy in the Python environment running the server."
        ) from exc

    leaf_count = len(distance_data["compressed_entries"])
    if leaf_count <= 1:
        log_event("hierarchy", "skip local hierarchy for single leaf", leaf_count=leaf_count)
        return {
            "source": "local_computed",
            "children": np.zeros((0, 2), dtype=np.int64),
            "distances": np.zeros((0,), dtype=np.float64),
            "counts": np.zeros((0,), dtype=np.int64),
            "leaf_count": leaf_count,
            "leaf_to_group_index": list(range(leaf_count)),
        }
    with timed_step(
        "hierarchy",
        "prepare condensed distances",
        leaf_count=leaf_count,
        pairs=leaf_count * (leaf_count - 1) // 2,
    ):
        if "compressed_distance_condensed" in distance_data:
            condensed_distances = distance_data["compressed_distance_condensed"]
        else:
            condensed_distances = squareform(distance_data["compressed_distance_matrix"], checks=False)
        leaf_interface_counts = compressed_entry_member_counts(distance_data["compressed_entries"])
    with timed_step(
        "hierarchy",
        "create local hierarchy",
        leaf_count=leaf_count,
        linkage=linkage_method,
    ):
        if linkage_method == "average":
            if _weighted_average_linkage_numba is None:
                raise RuntimeError(
                    "Interface-weighted average linkage requires numba when computing "
                    "local hierarchies."
                )
            children, distances, counts = _weighted_average_linkage_numba(
                np.asarray(condensed_distances, dtype=np.float64).copy(),
                leaf_interface_counts,
            )
            children, distances, counts = reorder_linkage_by_distance(
                children,
                distances,
                counts,
            )
        else:
            linkage_matrix = scipy_linkage(
                condensed_distances,
                method=scipy_hierarchical_linkage_method(linkage_method),
            )
            children = linkage_matrix[:, :2].astype(np.int64)
            distances = linkage_matrix[:, 2].astype(np.float64)
            counts = cluster_interface_counts_from_children(children, leaf_interface_counts)
    return {
        "source": "local_computed",
        "children": children.astype(np.int64, copy=False),
        "distances": distances.astype(np.float64, copy=False),
        "counts": counts.astype(np.int64, copy=False),
        "leaf_count": leaf_count,
        "leaf_to_group_index": list(range(leaf_count)),
    }


def load_or_compute_local_hierarchy(
    cache_dir: Path,
    interface_path: Path,
    distance_data: dict[str, object],
    distance_metric: str,
    linkage_method: str,
    interface_filter_settings: dict[str, object] | None = None,
) -> dict[str, object]:
    compressed_entries = distance_data["compressed_entries"]
    leaf_signature_hash = str(compression.get("cache_signature_hash") or "") or compressed_entries_signature_hash(
        compressed_entries
    )
    cache_path = hierarchy_cache_path(
        cache_dir,
        interface_path,
        distance_metric,
        linkage_method,
        interface_filter_settings,
    )
    cached_hierarchy = load_cached_local_hierarchy(cache_path, leaf_signature_hash)
    if cached_hierarchy is not None:
        log_event(
            "hierarchy",
            "reuse cached local hierarchy",
            file=cache_path.name,
            metric=distance_metric,
            linkage=linkage_method,
        )
        return cached_hierarchy
    hierarchy = compute_local_hierarchy(distance_data, linkage_method)
    try:
        import numpy as np

        with timed_step(
            "hierarchy",
            "write local hierarchy cache",
            file=cache_path.name,
            leaf_count=hierarchy["leaf_count"],
            metric=distance_metric,
            linkage=linkage_method,
        ):
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            np.savez_compressed(
                cache_path,
                format_version=np.array([1], dtype=np.uint16),
                metric=np.array(distance_metric),
                linkage=np.array(linkage_method),
                pfam_id=np.array(interface_file_pfam_id(interface_path)),
                leaf_count=np.array([int(hierarchy["leaf_count"])], dtype=np.uint32),
                leaf_signature_hash=np.array(leaf_signature_hash),
                children=hierarchy["children"],
                distance=hierarchy["distances"],
                count=hierarchy["counts"],
            )
    except OSError:
        log_event("hierarchy", "failed to write local hierarchy cache", file=cache_path.name)
        pass
    return hierarchy


def cut_hierarchy_leaf_labels(
    leaf_count: int,
    children: object,
    distances: object,
    n_clusters: int | None = None,
    distance_threshold: float | None = None,
) -> list[int]:
    if leaf_count <= 1:
        return [0] * leaf_count
    if n_clusters is None and distance_threshold is None:
        raise ValueError("hierarchical clustering requires n clusters or cutoff distance")
    parent = list(range(leaf_count))
    rank = [0] * leaf_count
    node_representative = list(range((2 * leaf_count) - 1))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> int:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return left_root
        if rank[left_root] < rank[right_root]:
            left_root, right_root = right_root, left_root
        parent[right_root] = left_root
        if rank[left_root] == rank[right_root]:
            rank[left_root] += 1
        return left_root

    if n_clusters is not None:
        target_cluster_count = max(1, min(int(n_clusters), leaf_count))
        merge_count = leaf_count - target_cluster_count
    else:
        threshold = float(distance_threshold)
        merge_count = 0
        for distance in distances:
            if float(distance) <= threshold:
                merge_count += 1
                continue
            break

    for merge_index in range(merge_count):
        left_child = int(children[merge_index][0])
        right_child = int(children[merge_index][1])
        max_valid_child = leaf_count + merge_index
        if left_child >= max_valid_child or right_child >= max_valid_child:
            raise ValueError("hierarchy children are not ordered like a scipy linkage matrix")
        left_representative = node_representative[left_child]
        right_representative = node_representative[right_child]
        merged_representative = union(left_representative, right_representative)
        node_representative[leaf_count + merge_index] = merged_representative

    labels_by_root: dict[int, int] = {}
    labels: list[int] = []
    for leaf_index in range(leaf_count):
        root = find(leaf_index)
        if root not in labels_by_root:
            labels_by_root[root] = len(labels_by_root)
        labels.append(labels_by_root[root])
    return labels


def hierarchy_leaf_member_counts(
    compressed_entries: list[dict[str, object]],
    leaf_to_group_indices: object,
    stored_leaf_member_counts: object | None = None,
) -> np.ndarray:
    if stored_leaf_member_counts is not None:
        leaf_member_counts = np.asarray(stored_leaf_member_counts, dtype=np.int64)
        if leaf_member_counts.shape[0] != len(leaf_to_group_indices):
            raise ValueError("stored hierarchy leaf counts are inconsistent")
        if np.any(leaf_member_counts < 0):
            raise ValueError("stored hierarchy leaf counts must be non-negative")
        return leaf_member_counts

    group_member_counts = compressed_entry_member_counts(compressed_entries)
    leaf_member_counts = np.empty(len(leaf_to_group_indices), dtype=np.int64)
    for leaf_index, raw_group_indices in enumerate(leaf_to_group_indices):
        if isinstance(raw_group_indices, (str, bytes)):
            raise ValueError("hierarchy leaf mapping is inconsistent")
        total = 0
        for raw_group_index in raw_group_indices:
            group_index = int(raw_group_index)
            if group_index < 0 or group_index >= group_member_counts.shape[0]:
                raise ValueError("hierarchy leaf mapping points outside compressed entries")
            total += int(group_member_counts[group_index])
        leaf_member_counts[leaf_index] = total
    return leaf_member_counts


def cut_hierarchy_leaf_labels_by_persistence(
    leaf_count: int,
    children: object,
    distances: object,
    leaf_member_counts: object,
    min_lifetime: float,
    min_cluster_size: int,
) -> list[int]:
    if leaf_count <= 1:
        return [0] * leaf_count
    leaf_member_counts = np.asarray(leaf_member_counts, dtype=np.int64)
    if leaf_member_counts.shape[0] != leaf_count:
        raise ValueError("hierarchy leaf counts are inconsistent")
    if np.any(leaf_member_counts < 0):
        raise ValueError("hierarchy leaf counts must be non-negative")

    total_node_count = (2 * leaf_count) - 1
    node_sizes = np.zeros(total_node_count, dtype=np.int64)
    node_sizes[:leaf_count] = leaf_member_counts
    parent_distance = np.full(total_node_count, np.nan, dtype=np.float64)

    for merge_index in range(leaf_count - 1):
        left_child = int(children[merge_index][0])
        right_child = int(children[merge_index][1])
        node_id = leaf_count + merge_index
        if left_child >= node_id or right_child >= node_id:
            raise ValueError("hierarchy children are not ordered like a scipy linkage matrix")
        merge_distance = float(distances[merge_index])
        node_sizes[node_id] = node_sizes[left_child] + node_sizes[right_child]
        parent_distance[left_child] = merge_distance
        parent_distance[right_child] = merge_distance

    root_node_id = total_node_count - 1
    candidates: list[tuple[float, int, int]] = []
    for node_id in range(root_node_id):
        birth_distance = 0.0 if node_id < leaf_count else float(distances[node_id - leaf_count])
        death_distance = float(parent_distance[node_id])
        if not math.isfinite(death_distance):
            continue
        lifetime = death_distance - birth_distance
        if lifetime < min_lifetime:
            continue
        cluster_size = int(node_sizes[node_id])
        if cluster_size < min_cluster_size:
            continue
        candidates.append((lifetime, cluster_size, node_id))
    candidates.sort(key=lambda candidate: (-candidate[0], -candidate[1], candidate[2]))

    def collect_leaf_indices(node_id: int) -> list[int]:
        leaves: list[int] = []
        stack = [node_id]
        while stack:
            node = stack.pop()
            if node < leaf_count:
                leaves.append(node)
                continue
            child_index = node - leaf_count
            if child_index < 0 or child_index >= leaf_count - 1:
                raise ValueError("hierarchy children are inconsistent")
            left_child = int(children[child_index][0])
            right_child = int(children[child_index][1])
            stack.append(right_child)
            stack.append(left_child)
        return leaves

    labels = [-1] * leaf_count
    next_label = 0
    for _, _, node_id in candidates:
        leaves = collect_leaf_indices(node_id)
        if any(labels[leaf_index] >= 0 for leaf_index in leaves):
            continue
        for leaf_index in leaves:
            labels[leaf_index] = next_label
        next_label += 1

    if next_label == 0:
        return [0] * leaf_count
    return labels


def compute_hierarchical_clustering_payload_from_hierarchy(
    entries: list[dict[str, object]],
    compressed_entries: list[dict[str, object]],
    group_index_by_entry: list[int],
    distance_metric: str,
    settings: dict[str, object],
    hierarchy: dict[str, object],
) -> dict[str, object]:
    linkage = str(settings["linkage"])
    hierarchical_target = str(settings.get("hierarchical_target", DEFAULT_HIERARCHICAL_TARGET))
    n_clusters = settings["n_clusters"]
    distance_threshold = settings["distance_threshold"]
    persistence_min_lifetime = float(
        settings.get(
            "persistence_min_lifetime",
            DEFAULT_HIERARCHICAL_PERSISTENCE_MIN_LIFETIME,
        )
    )
    hierarchical_min_cluster_size = int(
        settings.get("hierarchical_min_cluster_size", DEFAULT_HIERARCHICAL_MIN_CLUSTER_SIZE)
    )
    compressed_count = len(compressed_entries)
    leaf_count = int(hierarchy["leaf_count"])
    if compressed_count == 0:
        raise ValueError("need at least one compressed interface group")
    leaf_to_group_index = hierarchy.get("leaf_to_group_index")
    leaf_to_group_indices = hierarchy.get("leaf_to_group_indices")
    if leaf_to_group_indices is None:
        if leaf_to_group_index is None:
            raise ValueError("hierarchy leaf mapping is inconsistent")
        leaf_to_group_indices = [
            [int(group_index)]
            for group_index in leaf_to_group_index
        ]
    if leaf_count != len(leaf_to_group_indices):
        raise ValueError("hierarchy leaf mapping is inconsistent")
    if compressed_count == 1:
        compressed_labels = [0]
    else:
        with timed_step(
            "hierarchy",
            "apply hierarchy cutoff",
            source=hierarchy.get("source", "local_computed"),
            leaf_count=leaf_count,
            target=hierarchical_target,
            n_clusters=n_clusters,
            distance_threshold=distance_threshold,
            persistence_min_lifetime=persistence_min_lifetime,
        ) as timer:
            if hierarchical_target == "persistence":
                leaf_member_counts = hierarchy_leaf_member_counts(
                    compressed_entries,
                    leaf_to_group_indices,
                )
                leaf_labels = cut_hierarchy_leaf_labels_by_persistence(
                    leaf_count,
                    hierarchy["children"],
                    hierarchy["distances"],
                    leaf_member_counts,
                    min_lifetime=persistence_min_lifetime,
                    min_cluster_size=hierarchical_min_cluster_size,
                )
            else:
                leaf_labels = cut_hierarchy_leaf_labels(
                    leaf_count,
                    hierarchy["children"],
                    hierarchy["distances"],
                    n_clusters=int(n_clusters) if n_clusters is not None else None,
                    distance_threshold=float(distance_threshold) if distance_threshold is not None else None,
                )
            timer.set(
                cluster_labels=len({label for label in leaf_labels if label >= 0}),
                noise_leaves=sum(1 for label in leaf_labels if label < 0),
            )
        with timed_step(
            "hierarchy",
            "map hierarchy leaves to compressed entries",
            leaf_count=leaf_count,
            compressed_count=compressed_count,
        ):
            unmapped_label = -10**12
            compressed_labels = [unmapped_label] * compressed_count
            for leaf_index, label in enumerate(leaf_labels):
                raw_group_indices = leaf_to_group_indices[leaf_index]
                if isinstance(raw_group_indices, (str, bytes)):
                    raise ValueError("hierarchy leaf mapping is inconsistent")
                for raw_group_index in raw_group_indices:
                    group_index = int(raw_group_index)
                    if group_index < 0 or group_index >= compressed_count:
                        raise ValueError("hierarchy leaf mapping points outside compressed entries")
                    if (
                        compressed_labels[group_index] != unmapped_label
                        and compressed_labels[group_index] != int(label)
                    ):
                        raise ValueError("hierarchy leaf mapping assigns conflicting labels")
                    compressed_labels[group_index] = int(label)
            if any(label == unmapped_label for label in compressed_labels):
                raise ValueError("hierarchy leaf mapping did not cover all compressed entries")
    if hierarchical_target in {"distance_threshold", "n_clusters"}:
        with timed_step(
            "hierarchy",
            "apply hierarchical min cluster size",
            target=hierarchical_target,
            min_cluster_size=hierarchical_min_cluster_size,
            compressed_count=compressed_count,
        ) as timer:
            label_counts: dict[int, int] = {}
            for label, compressed_entry in zip(compressed_labels, compressed_entries, strict=True):
                if label < 0:
                    continue
                label_counts[label] = label_counts.get(label, 0) + int(compressed_entry["member_count"])
            too_small_labels = {
                label for label, count in label_counts.items() if count < hierarchical_min_cluster_size
            }
            if too_small_labels:
                compressed_labels = [
                    -1 if label in too_small_labels else label
                    for label in compressed_labels
                ]
            timer.set(removed_clusters=len(too_small_labels))
    labels_list = [compressed_labels[int(group_index)] for group_index in group_index_by_entry]
    labels_list = rank_non_negative_cluster_labels_by_size(labels_list)
    unique_cluster_labels = sorted({label for label in labels_list if label >= 0})
    cluster_count = len(unique_cluster_labels)
    noise_count = int(sum(1 for label in labels_list if label < 0))
    with timed_step(
        "clustering",
        "serialize hierarchical labels",
        samples=len(entries),
        clusters=cluster_count,
        noise=noise_count,
    ):
        points = [
            {
                "row_key": str(entry["row_key"]),
                "partner_domain": str(entry["partner_domain"]),
                "cluster_label": label,
            }
            for entry, label in zip(entries, labels_list, strict=True)
        ]
    hierarchy_source = str(hierarchy.get("source", "local_computed"))
    return {
        "clustering": "hierarchical",
        "distance": distance_metric,
        "compression_mode": INTERFACE_COMPRESSION_MODE,
        "sample_count": len(points),
        "compressed_sample_count": compressed_count,
        "cluster_count": cluster_count,
        "noise_count": noise_count,
        "hierarchy_source": hierarchy_source,
        "hierarchy_precalculated": hierarchy_source == "precalculated",
        "hierarchy_leaf_count": leaf_count,
        "settings": {
            "method": "hierarchical",
            "distance": distance_metric,
            "linkage": linkage,
            "hierarchical_target": hierarchical_target,
            "n_clusters": n_clusters,
            "distance_threshold": distance_threshold,
            "persistence_min_lifetime": persistence_min_lifetime,
            "hierarchical_min_cluster_size": hierarchical_min_cluster_size,
        },
        "points": points,
    }


def interface_payload_alignment_length_fallback(
    interface_payload: dict[str, dict[str, dict]],
) -> int:
    max_column = -1
    for rows in interface_payload.values():
        if not isinstance(rows, dict):
            continue
        for row_payload in rows.values():
            if not isinstance(row_payload, dict):
                continue
            aligned_sequence = row_payload.get("aligned_seq")
            if isinstance(aligned_sequence, str) and aligned_sequence:
                max_column = max(max_column, len(aligned_sequence) - 1)
            for raw_column in row_payload.get("interface_msa_columns_a", []) or []:
                try:
                    max_column = max(max_column, int(raw_column))
                except (TypeError, ValueError):
                    continue
    return max(0, max_column + 1)


def numeric_cluster_sort_key(cluster_label: str) -> tuple[float, str]:
    try:
        numeric_label = float(cluster_label)
    except ValueError:
        numeric_label = math.inf
    return numeric_label, cluster_label


def compute_columns_chart_payload(
    interface_payload: dict[str, dict[str, dict]],
    clustering_payload: dict[str, object],
    alignment_length: int | None = None,
) -> dict[str, object]:
    points = clustering_payload.get("points")
    if not isinstance(points, list):
        return {
            "alignmentLength": int(alignment_length or 0),
            "clusters": [],
            "clusterSizes": {},
            "relativeByCluster": {},
            "maxStackValue": 0.0,
        }
    normalized_alignment_length = int(alignment_length or 0)
    if normalized_alignment_length <= 0:
        normalized_alignment_length = interface_payload_alignment_length_fallback(interface_payload)

    with timed_step(
        "columns",
        "compute cluster column histogram",
        samples=len(points),
        alignment_length=normalized_alignment_length,
    ) as timer:
        cluster_sizes: dict[str, int] = {}
        normalized_points: list[tuple[str, str, str]] = []
        for point in points:
            if not isinstance(point, dict):
                continue
            row_key = str(point.get("row_key") or "")
            partner_domain = str(point.get("partner_domain") or "")
            cluster_label = str(point.get("cluster_label"))
            if not row_key or not partner_domain:
                continue
            normalized_points.append((partner_domain, row_key, cluster_label))
            cluster_sizes[cluster_label] = cluster_sizes.get(cluster_label, 0) + 1

        cluster_keys = sorted(cluster_sizes, key=numeric_cluster_sort_key)
        counts_by_cluster = {
            cluster_label: [0] * normalized_alignment_length
            for cluster_label in cluster_keys
        }

        for partner_domain, row_key, cluster_label in normalized_points:
            rows = interface_payload.get(partner_domain)
            row_payload = rows.get(row_key) if isinstance(rows, dict) else None
            if not isinstance(row_payload, dict):
                continue
            counts = counts_by_cluster.get(cluster_label)
            if counts is None:
                continue
            for raw_column in row_payload.get("interface_msa_columns_a", []) or []:
                try:
                    column_index = int(raw_column)
                except (TypeError, ValueError):
                    continue
                if 0 <= column_index < normalized_alignment_length:
                    counts[column_index] += 1

        relative_by_cluster: dict[str, list[float]] = {}
        stack_totals = [0.0] * normalized_alignment_length
        for cluster_label in cluster_keys:
            cluster_size = max(1, int(cluster_sizes.get(cluster_label, 0)))
            counts = counts_by_cluster.get(cluster_label) or [0] * normalized_alignment_length
            relative = [float(count) / cluster_size for count in counts]
            relative_by_cluster[cluster_label] = relative
            for column_index, value in enumerate(relative):
                stack_totals[column_index] += value
        max_stack_value = max(stack_totals) if stack_totals else 0.0
        timer.set(clusters=len(cluster_keys), max_stack_value=max_stack_value)

    return {
        "alignmentLength": normalized_alignment_length,
        "clusters": cluster_keys,
        "clusterSizes": cluster_sizes,
        "relativeByCluster": relative_by_cluster,
        "maxStackValue": max_stack_value,
    }


def build_compressed_interface_data(
    interface_payload: dict[str, dict[str, dict]],
    interface_path: Path | None = None,
    interface_filter_settings: dict[str, object] | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    if interface_path is not None:
        return load_or_build_compressed_interface_data(
            interface_path,
            interface_payload,
            interface_filter_settings,
        )
    return build_compressed_interface_data_uncached(
        interface_payload,
        log_category="clustering",
        parse_message="parse interfaces for hierarchy",
        compress_message="compress interfaces for hierarchy",
    )


def summary_float(row: dict[str, str], key: str) -> float | None:
    value = (row.get(key) or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def estimate_local_hierarchy_cost(
    hierarchy_dir: Path | None,
    distance_metric: str,
    linkage_method: str,
    leaf_count: int,
) -> dict[str, object] | None:
    if hierarchy_dir is None or not hierarchy_dir.exists():
        log_event(
            "hierarchy",
            "skip local hierarchy estimate",
            reason="hierarchy directory unavailable",
            metric=distance_metric,
            linkage=linkage_method,
            leaf_count=leaf_count,
        )
        return None
    with timed_step(
        "hierarchy",
        "estimate local hierarchy cost",
        metric=distance_metric,
        linkage=linkage_method,
        leaf_count=leaf_count,
    ) as timer:
        candidates: list[dict[str, object]] = []
        summary_file_count = 0
        for summary_path in hierarchy_dir.glob("*/*/summary.csv"):
            summary_file_count += 1
            summary_metric = summary_path.parent.parent.name
            summary_linkage = summary_path.parent.name
            try:
                with summary_path.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        if (row.get("status") or "").strip() != "ok":
                            continue
                        row_leaf_count = int(float(row.get("leaf_count") or 0))
                        if row_leaf_count <= 1:
                            continue
                        total_seconds = summary_float(row, "total_seconds")
                        peak_rss_delta_bytes = summary_float(row, "peak_rss_delta_bytes")
                        if total_seconds is None and peak_rss_delta_bytes is None:
                            continue
                        row_metric = (row.get("metric") or summary_metric).strip()
                        row_linkage = (row.get("linkage") or summary_linkage).strip()
                        candidates.append(
                            {
                                "summary_file": str(summary_path),
                                "source_file": row.get("source_file") or "",
                                "pfam_id": row.get("pfam_id") or "",
                                "metric": row_metric,
                                "linkage": row_linkage,
                                "leaf_count": row_leaf_count,
                                "total_seconds": total_seconds,
                                "peak_rss_delta_bytes": peak_rss_delta_bytes,
                            }
                        )
            except (OSError, ValueError):
                log_event("hierarchy", "skip unreadable summary file", file=summary_path)
                continue
        timer.set(summary_files=summary_file_count, candidates=len(candidates))
    if not candidates:
        return None
    requested_condensed_count = max(1, leaf_count * (leaf_count - 1) // 2)

    def candidate_score(candidate: dict[str, object]) -> tuple[int, int, float]:
        candidate_leaf_count = int(candidate["leaf_count"])
        return (
            0 if str(candidate["linkage"]) == linkage_method else 1,
            0 if str(candidate["metric"]) == distance_metric else 1,
            abs(math.log1p(candidate_leaf_count) - math.log1p(max(1, leaf_count))),
        )

    closest = min(candidates, key=candidate_score)
    closest_leaf_count = int(closest["leaf_count"])
    closest_condensed_count = max(1, closest_leaf_count * (closest_leaf_count - 1) // 2)
    scale = requested_condensed_count / closest_condensed_count
    total_seconds = closest.get("total_seconds")
    peak_rss_delta_bytes = closest.get("peak_rss_delta_bytes")
    return {
        "basis": {
            "summary_file": closest["summary_file"],
            "source_file": closest["source_file"],
            "pfam_id": closest["pfam_id"],
            "metric": closest["metric"],
            "linkage": closest["linkage"],
            "leaf_count": closest_leaf_count,
        },
        "leaf_count": leaf_count,
        "scale": scale,
        "estimated_total_seconds": (
            float(total_seconds) * scale
            if total_seconds is not None
            else None
        ),
        "estimated_peak_rss_delta_bytes": (
            float(peak_rss_delta_bytes) * scale
            if peak_rss_delta_bytes is not None
            else None
        ),
    }


def hierarchy_status_payload(
    cache_dir: Path,
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
    clustering_settings: dict[str, object],
    interface_filter_settings: dict[str, object] | None = None,
    hierarchy_dir: Path | None = None,
) -> dict[str, object]:
    if clustering_settings["method"] != "hierarchical":
        return {
            "method": str(clustering_settings["method"]),
            "local_calculation_required": False,
        }
    distance_metric = str(clustering_settings["distance"])
    linkage_method = str(clustering_settings["linkage"])
    entries, compression = build_compressed_interface_data(
        interface_payload,
        interface_path=interface_path,
        interface_filter_settings=interface_filter_settings,
    )
    compressed_entries = compression["entries"]
    leaf_count = len(compressed_entries)
    precomputed_state = "unavailable"
    precomputed_reason = "hierarchy directory is not configured"
    try:
        hierarchy = load_precomputed_hierarchy(
            hierarchy_dir,
            interface_path,
            distance_metric,
            linkage_method,
            compressed_entries,
            compressed_signature_hash=str(compression.get("cache_signature_hash") or ""),
        )
        if hierarchy is not None:
            return {
                "method": "hierarchical",
                "distance": distance_metric,
                "linkage": linkage_method,
                "source": "precalculated",
                "local_calculation_required": False,
                "interface_count": len(entries),
                "leaf_count": leaf_count,
                "precalculated": {
                    "linkage_file": hierarchy.get("linkage_file"),
                    "resolver_file": hierarchy.get("resolver_file"),
                },
            }
        if hierarchy_dir is not None:
            precomputed_reason = "no matching precalculated hierarchy files were found"
    except ValueError as exc:
        precomputed_state = "mismatch"
        precomputed_reason = str(exc)

    leaf_signature_hash = compressed_entries_signature_hash(compressed_entries)
    cache_path = hierarchy_cache_path(
        cache_dir,
        interface_path,
        distance_metric,
        linkage_method,
        interface_filter_settings,
    )
    if local_hierarchy_cache_is_valid(cache_path, leaf_signature_hash):
        return {
            "method": "hierarchical",
            "distance": distance_metric,
            "linkage": linkage_method,
            "source": "local_cache",
            "local_calculation_required": False,
            "interface_count": len(entries),
            "leaf_count": leaf_count,
            "local_cache_file": str(cache_path),
            "precomputed": {
                "state": precomputed_state,
                "reason": precomputed_reason,
            },
        }

    clustering_cache_file = clustering_cache_path(
        cache_dir,
        interface_path,
        clustering_settings,
        interface_filter_settings,
    )
    if clustering_cache_file.exists():
        try:
            with timed_step(
                "clustering",
                "check cached hierarchical clustering for warning",
                file=clustering_cache_file.name,
            ):
                with clustering_cache_file.open("r", encoding="utf-8") as handle:
                    cached_clustering = json.load(handle)
            if clustering_payload_matches_entries(cached_clustering, entries):
                return {
                    "method": "hierarchical",
                    "distance": distance_metric,
                    "linkage": linkage_method,
                    "source": "clustering_cache",
                    "local_calculation_required": False,
                    "interface_count": len(entries),
                    "leaf_count": leaf_count,
                    "clustering_cache_file": str(clustering_cache_file),
                    "precomputed": {
                        "state": precomputed_state,
                        "reason": precomputed_reason,
                    },
                }
        except (OSError, json.JSONDecodeError, ValueError):
            log_event(
                "clustering",
                "cached hierarchical clustering warning check failed",
                file=clustering_cache_file.name,
            )

    return {
        "method": "hierarchical",
        "distance": distance_metric,
        "linkage": linkage_method,
        "source": "local",
        "local_calculation_required": True,
        "interface_count": len(entries),
        "leaf_count": leaf_count,
        "local_cache_file": str(cache_path),
        "precomputed": {
            "state": precomputed_state,
            "reason": precomputed_reason,
        },
        "estimate": estimate_local_hierarchy_cost(
            hierarchy_dir,
            distance_metric,
            linkage_method,
            leaf_count,
        ),
    }


def load_or_compute_clustering_payload(
    cache_dir: Path,
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
    clustering_settings: dict[str, object],
    interface_filter_settings: dict[str, object] | None = None,
    cache_workers: int = DEFAULT_CACHE_WORKERS,
    hierarchy_dir: Path | None = None,
) -> dict[str, object]:
    log_event(
        "clustering",
        "load or compute clustering",
        file=interface_path.name,
        method=clustering_settings["method"],
        distance=clustering_settings["distance"],
    )
    cache_path = clustering_cache_path(
        cache_dir,
        interface_path,
        clustering_settings,
        interface_filter_settings,
    )
    if clustering_settings["method"] == "hierarchical":
        entries, compression = build_compressed_interface_data(
            interface_payload,
            interface_path=interface_path,
            interface_filter_settings=interface_filter_settings,
        )
        distance_metric = str(clustering_settings["distance"])
        linkage_method = str(clustering_settings["linkage"])
        compressed_entries = compression["entries"]
        group_index_by_entry = compression["group_index_by_entry"]
        try:
            hierarchy = load_precomputed_hierarchy(
                hierarchy_dir,
                interface_path,
                distance_metric,
                linkage_method,
                compressed_entries,
                compressed_signature_hash=str(compression.get("cache_signature_hash") or ""),
            )
        except ValueError as exc:
            log_event(
                "hierarchy",
                "precalculated hierarchy rejected",
                file=interface_path.name,
                metric=distance_metric,
                linkage=linkage_method,
                error=exc,
            )
            hierarchy = None
        if cache_path.exists():
            with timed_step(
                "clustering",
                "load cached hierarchical clustering",
                file=cache_path.name,
            ):
                with cache_path.open("r", encoding="utf-8") as handle:
                    cached_payload = json.load(handle)
            cached_hierarchy_source = str(cached_payload.get("hierarchy_source", ""))
            if (
                clustering_payload_matches_entries(cached_payload, entries)
                and (hierarchy is None or cached_hierarchy_source == "precalculated")
            ):
                log_event(
                    "clustering",
                    "reuse cached hierarchical clustering",
                    file=cache_path.name,
                    hierarchy_source=cached_hierarchy_source,
                )
                return normalize_clustering_payload_cluster_labels(cached_payload)
            log_event(
                "clustering",
                "cached hierarchical clustering invalid",
                file=cache_path.name,
                hierarchy_source=cached_hierarchy_source,
            )
        if hierarchy is None:
            distance_data = load_interface_distance_data(
                cache_dir,
                interface_path,
                interface_payload,
                distance_metric,
                interface_filter_settings,
                distance_scope="compressed",
                cache_workers=cache_workers,
            )
            entries = distance_data["entries"]
            compressed_entries = distance_data["compressed_entries"]
            group_index_by_entry = distance_data["group_index_by_entry"]
            hierarchy = load_or_compute_local_hierarchy(
                cache_dir,
                interface_path,
                distance_data,
                distance_metric,
                linkage_method,
                interface_filter_settings,
            )
        clustering_payload = compute_hierarchical_clustering_payload_from_hierarchy(
            entries,
            compressed_entries,
            group_index_by_entry,
            distance_metric,
            clustering_settings,
            hierarchy,
        )
    else:
        distance_data = load_interface_distance_data(
            cache_dir,
            interface_path,
            interface_payload,
            str(clustering_settings["distance"]),
            interface_filter_settings,
            distance_scope="expanded",
            cache_workers=cache_workers,
        )
        if cache_path.exists():
            with timed_step("clustering", "load cached clustering", file=cache_path.name):
                with cache_path.open("r", encoding="utf-8") as handle:
                    cached_payload = json.load(handle)
            if clustering_payload_matches_distance_data(cached_payload, distance_data):
                log_event(
                    "clustering",
                    "reuse cached clustering",
                    file=cache_path.name,
                    method=clustering_settings["method"],
                )
                return normalize_clustering_payload_cluster_labels(cached_payload)
            log_event("clustering", "cached clustering invalid", file=cache_path.name)
        clustering_payload = compute_hdbscan_clustering_payload(distance_data, clustering_settings)
    response_payload = {
        "file": interface_path.name,
        "pfam_id": interface_file_pfam_id(interface_path),
        "filter_settings": interface_filter_settings or {"min_interface_size": DEFAULT_MIN_INTERFACE_SIZE},
        **clustering_payload,
    }
    response_payload = normalize_clustering_payload_cluster_labels(response_payload)
    with timed_step(
        "clustering",
        "write clustering cache",
        file=cache_path.name,
        method=clustering_settings["method"],
        points=len(response_payload.get("points", [])),
    ):
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("w", encoding="utf-8") as handle:
            json.dump(response_payload, handle)
    return response_payload


def clustering_payload_matches_distance_data(
    clustering_payload: dict[str, object],
    distance_data: dict[str, object],
) -> bool:
    entries = distance_data.get("entries")
    if not isinstance(entries, list):
        return False
    return clustering_payload_matches_entries(clustering_payload, entries)


def clustering_payload_matches_entries(
    clustering_payload: dict[str, object],
    entries: list[dict[str, object]],
) -> bool:
    points = clustering_payload.get("points")
    if not isinstance(points, list):
        return False
    if len(points) != len(entries):
        return False
    for point, entry in zip(points, entries, strict=True):
        if (
            str(point.get("row_key", "")) != str(entry.get("row_key", ""))
            or str(point.get("partner_domain", "")) != str(entry.get("partner_domain", ""))
        ):
            return False
    return True


def compute_cluster_compare_payload(
    distance_data: dict[str, object],
    clustering_payload: dict[str, object],
    cluster_label: int,
    limit: int = DEFAULT_CLUSTER_COMPARE_LIMIT,
) -> dict[str, object]:
    clustering_points = clustering_payload["points"]
    entries = distance_data["entries"]
    distance_matrix = distance_data.get("distance_matrix")
    if distance_matrix is None:
        return compute_compressed_cluster_compare_payload(
            distance_data,
            clustering_payload,
            cluster_label,
            limit,
        )
    if len(clustering_points) != len(entries) or distance_matrix.shape[0] != len(entries):
        raise ValueError(
            "clustering data is out of sync with the current filtered interface rows; "
            "please recompute clustering"
        )
    cluster_indices = [
        index for index, point in enumerate(clustering_points) if int(point["cluster_label"]) == cluster_label
    ]
    if not cluster_indices:
        raise ValueError(f"cluster {cluster_label} has no entries")
    selection_limit = min(max(1, int(limit)), len(cluster_indices))
    selected_indices = [random.choice(cluster_indices)]
    selected_set = set(selected_indices)

    def compare_entry_order(left_index: int, right_index: int) -> int:
        left_key = (str(entries[left_index]["row_key"]), str(entries[left_index]["partner_domain"]), left_index)
        right_key = (str(entries[right_index]["row_key"]), str(entries[right_index]["partner_domain"]), right_index)
        if left_key < right_key:
            return -1
        if left_key > right_key:
            return 1
        return 0

    while len(selected_indices) < selection_limit:
        best_index: int | None = None
        best_min_distance = -1.0
        best_mean_distance = -1.0
        for candidate_index in cluster_indices:
            if candidate_index in selected_set:
                continue
            min_distance = math.inf
            total_distance = 0.0
            for selected_index in selected_indices:
                distance = float(distance_matrix[candidate_index][selected_index])
                min_distance = min(min_distance, distance)
                total_distance += distance
            mean_distance = total_distance / len(selected_indices)
            if (
                min_distance > best_min_distance
                or (
                    min_distance == best_min_distance
                    and (
                        mean_distance > best_mean_distance
                        or (
                            mean_distance == best_mean_distance
                            and (best_index is None or compare_entry_order(candidate_index, best_index) < 0)
                        )
                    )
                )
            ):
                best_index = candidate_index
                best_min_distance = min_distance
                best_mean_distance = mean_distance
        if best_index is None:
            break
        selected_indices.append(best_index)
        selected_set.add(best_index)

    remaining_indices = [index for index in cluster_indices if index not in selected_set]
    assigned_counts: dict[int, int] = {index: 0 for index in selected_indices}
    selected_rank_by_index = {index: rank for rank, index in enumerate(selected_indices)}
    for remaining_index in remaining_indices:
        best_selected_index: int | None = None
        best_distance = math.inf
        best_rank = math.inf
        for selected_index in selected_indices:
            distance = float(distance_matrix[remaining_index][selected_index])
            selected_rank = selected_rank_by_index[selected_index]
            if (
                distance < best_distance
                or (
                    distance == best_distance
                    and selected_rank < best_rank
                )
            ):
                best_selected_index = selected_index
                best_distance = distance
                best_rank = selected_rank
        if best_selected_index is not None:
            assigned_counts[best_selected_index] = assigned_counts.get(best_selected_index, 0) + 1

    remaining_count = len(remaining_indices)
    selected_entries = [
        {
            "row_key": str(entries[index]["row_key"]),
            "partner_domain": str(entries[index]["partner_domain"]),
            "selection_rank": selection_rank,
            "coverage_count": assigned_counts.get(index, 0),
            "coverage_fraction": (
                (assigned_counts.get(index, 0) / remaining_count)
                if remaining_count > 0
                else 0.0
            ),
            "coverage_percent": (
                (assigned_counts.get(index, 0) * 100.0 / remaining_count)
                if remaining_count > 0
                else 0.0
            ),
        }
        for selection_rank, index in enumerate(selected_indices)
    ]
    return {
        "cluster_label": cluster_label,
        "distance": str(distance_data["distance"]),
        "entry_count": len(cluster_indices),
        "remaining_entry_count": remaining_count,
        "selected_entries": selected_entries,
    }


def compute_compressed_cluster_compare_payload(
    distance_data: dict[str, object],
    clustering_payload: dict[str, object],
    cluster_label: int,
    limit: int = DEFAULT_CLUSTER_COMPARE_LIMIT,
) -> dict[str, object]:
    clustering_points = clustering_payload["points"]
    entries = distance_data["entries"]
    compressed_entries = distance_data["compressed_entries"]
    group_index_by_entry = distance_data["group_index_by_entry"]
    distance_matrix = distance_data["compressed_distance_matrix"]
    if (
        len(clustering_points) != len(entries)
        or len(group_index_by_entry) != len(entries)
        or distance_matrix.shape[0] != len(compressed_entries)
    ):
        raise ValueError(
            "clustering data is out of sync with the current filtered interface rows; "
            "please recompute clustering"
        )

    member_indices_by_group: dict[int, list[int]] = {}
    for entry_index, point in enumerate(clustering_points):
        if int(point["cluster_label"]) != cluster_label:
            continue
        group_index = int(group_index_by_entry[entry_index])
        member_indices_by_group.setdefault(group_index, []).append(entry_index)
    if not member_indices_by_group:
        raise ValueError(f"cluster {cluster_label} has no entries")

    def group_order_key(group_index: int) -> tuple[str, str, int]:
        return min(
            (
                str(entries[entry_index]["row_key"]),
                str(entries[entry_index]["partner_domain"]),
                entry_index,
            )
            for entry_index in member_indices_by_group[group_index]
        )

    def compare_group_order(left_index: int, right_index: int) -> int:
        left_key = group_order_key(left_index)
        right_key = group_order_key(right_index)
        if left_key < right_key:
            return -1
        if left_key > right_key:
            return 1
        return 0

    cluster_group_indices = sorted(member_indices_by_group, key=group_order_key)
    selection_limit = min(max(1, int(limit)), len(cluster_group_indices))
    selected_indices = [random.choice(cluster_group_indices)]
    selected_set = set(selected_indices)

    while len(selected_indices) < selection_limit:
        best_index: int | None = None
        best_min_distance = -1.0
        best_mean_distance = -1.0
        for candidate_index in cluster_group_indices:
            if candidate_index in selected_set:
                continue
            min_distance = math.inf
            total_distance = 0.0
            for selected_index in selected_indices:
                distance = float(distance_matrix[candidate_index][selected_index])
                min_distance = min(min_distance, distance)
                total_distance += distance
            mean_distance = total_distance / len(selected_indices)
            if (
                min_distance > best_min_distance
                or (
                    min_distance == best_min_distance
                    and (
                        mean_distance > best_mean_distance
                        or (
                            mean_distance == best_mean_distance
                            and (best_index is None or compare_group_order(candidate_index, best_index) < 0)
                        )
                    )
                )
            ):
                best_index = candidate_index
                best_min_distance = min_distance
                best_mean_distance = mean_distance
        if best_index is None:
            break
        selected_indices.append(best_index)
        selected_set.add(best_index)

    group_member_counts = {
        group_index: len(member_indices)
        for group_index, member_indices in member_indices_by_group.items()
    }
    remaining_indices = [index for index in cluster_group_indices if index not in selected_set]
    assigned_counts: dict[int, int] = {index: 0 for index in selected_indices}
    selected_rank_by_index = {index: rank for rank, index in enumerate(selected_indices)}
    for remaining_index in remaining_indices:
        best_selected_index: int | None = None
        best_distance = math.inf
        best_rank = math.inf
        for selected_index in selected_indices:
            distance = float(distance_matrix[remaining_index][selected_index])
            selected_rank = selected_rank_by_index[selected_index]
            if (
                distance < best_distance
                or (
                    distance == best_distance
                    and selected_rank < best_rank
                )
            ):
                best_selected_index = selected_index
                best_distance = distance
                best_rank = selected_rank
        if best_selected_index is not None:
            assigned_counts[best_selected_index] = (
                assigned_counts.get(best_selected_index, 0)
                + group_member_counts.get(remaining_index, 0)
            )

    remaining_count = sum(group_member_counts[index] for index in remaining_indices)
    selected_entries = []
    for selection_rank, group_index in enumerate(selected_indices):
        entry_index = random.choice(member_indices_by_group[group_index])
        selected_entries.append(
            {
                "row_key": str(entries[entry_index]["row_key"]),
                "partner_domain": str(entries[entry_index]["partner_domain"]),
                "selection_rank": selection_rank,
                "coverage_count": assigned_counts.get(group_index, 0),
                "coverage_fraction": (
                    (assigned_counts.get(group_index, 0) / remaining_count)
                    if remaining_count > 0
                    else 0.0
                ),
                "coverage_percent": (
                    (assigned_counts.get(group_index, 0) * 100.0 / remaining_count)
                    if remaining_count > 0
                    else 0.0
                ),
            }
        )

    return {
        "cluster_label": cluster_label,
        "distance": str(distance_data["distance"]),
        "entry_count": sum(group_member_counts.values()),
        "remaining_entry_count": remaining_count,
        "selected_entries": selected_entries,
    }
