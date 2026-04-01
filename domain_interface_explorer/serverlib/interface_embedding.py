from __future__ import annotations

import hashlib
import json
import math
import random
from pathlib import Path

from interface_distance import compute_interface_distance_matrix

from .config import (
    CLUSTERING_CACHE_VERSION,
    DEFAULT_CLUSTER_COMPARE_LIMIT,
    DEFAULT_CLUSTER_MIN_SIZE,
    DEFAULT_CLUSTER_SELECTION_EPSILON,
    DEFAULT_CLUSTERING_METHOD,
    DEFAULT_DISTANCE_METRIC,
    DEFAULT_HIERARCHICAL_DISTANCE_THRESHOLD,
    DEFAULT_HIERARCHICAL_LINKAGE,
    DEFAULT_HIERARCHICAL_MIN_CLUSTER_SIZE,
    DEFAULT_HIERARCHICAL_N_CLUSTERS,
    DEFAULT_HIERARCHICAL_TARGET,
    DEFAULT_MIN_INTERFACE_SIZE,
    DEFAULT_TSNE_EARLY_EXAGGERATION,
    DEFAULT_TSNE_LEARNING_RATE,
    DEFAULT_TSNE_MAX_ITER,
    DEFAULT_TSNE_RANDOM_STATE,
    DISTANCE_DATA_CACHE_LIMIT,
    EMBEDDING_CACHE_VERSION,
)

DISTANCE_DATA_CACHE: dict[str, dict[str, object]] = {}
MAX_DISTANCE_MATRIX_ROWS = 900


def remap_non_negative_cluster_labels(labels: list[int]) -> list[int]:
    unique_cluster_labels = sorted({label for label in labels if label >= 0})
    label_mapping = {
        cluster_label: mapped_label
        for mapped_label, cluster_label in enumerate(unique_cluster_labels)
    }
    return [label_mapping.get(label, -1) if label >= 0 else -1 for label in labels]


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


def build_interface_alignment_rows(interface_payload: dict[str, dict[str, dict]]) -> tuple[list[dict], int]:
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

    rows: list[dict] = []
    for raw_row in raw_rows:
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
    return rows, alignment_length


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


def overlap_distance_for_sets(left: set[int], right: set[int]) -> float:
    if not left and not right:
        return 0.0
    if not left or not right:
        return 1.0
    minimum_size = min(len(left), len(right))
    return 1.0 - (len(left & right) / minimum_size)


def parse_embedding_settings(query: dict[str, list[str]]) -> dict[str, object]:
    distance_raw = query.get("distance", [DEFAULT_DISTANCE_METRIC])[0].strip().lower()
    perplexity_raw = query.get("perplexity", ["auto"])[0].strip().lower()
    learning_rate_raw = query.get("learning_rate", [DEFAULT_TSNE_LEARNING_RATE])[0].strip().lower()
    max_iter_raw = query.get("max_iter", [str(DEFAULT_TSNE_MAX_ITER)])[0].strip()
    early_exaggeration_raw = query.get(
        "early_exaggeration", [str(DEFAULT_TSNE_EARLY_EXAGGERATION)]
    )[0].strip()
    if distance_raw not in {"jaccard", "dice", "overlap"}:
        raise ValueError("distance must be one of 'jaccard', 'dice', or 'overlap'")
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
    early_exaggeration = float(early_exaggeration_raw)
    if early_exaggeration <= 0:
        raise ValueError("early exaggeration must be positive")
    return {
        "distance": distance_raw,
        "perplexity": perplexity,
        "learning_rate": learning_rate,
        "max_iter": max_iter,
        "early_exaggeration": early_exaggeration,
        "random_state": DEFAULT_TSNE_RANDOM_STATE,
    }


def parse_distance_metric(query: dict[str, list[str]], default_metric: str = DEFAULT_DISTANCE_METRIC) -> str:
    distance_raw = query.get("distance", [default_metric])[0].strip().lower()
    if distance_raw not in {"jaccard", "dice", "overlap"}:
        raise ValueError("distance must be one of 'jaccard', 'dice', or 'overlap'")
    return distance_raw


def parse_clustering_settings(query: dict[str, list[str]]) -> dict[str, object]:
    method_raw = query.get("method", [DEFAULT_CLUSTERING_METHOD])[0].strip().lower()
    distance_raw = query.get("distance", [DEFAULT_DISTANCE_METRIC])[0].strip().lower()
    min_cluster_size_raw = query.get("min_cluster_size", [str(DEFAULT_CLUSTER_MIN_SIZE)])[0].strip()
    min_samples_raw = query.get("min_samples", [""])[0].strip()
    cluster_selection_epsilon_raw = query.get(
        "cluster_selection_epsilon", [str(DEFAULT_CLUSTER_SELECTION_EPSILON)]
    )[0].strip()
    linkage_raw = query.get("linkage", [DEFAULT_HIERARCHICAL_LINKAGE])[0].strip().lower()
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
    n_clusters_raw = query.get("n_clusters", [default_n_clusters_raw])[0].strip()
    distance_threshold_raw = query.get("distance_threshold", [default_distance_threshold_raw])[0].strip()
    hierarchical_min_cluster_size_raw = query.get(
        "hierarchical_min_cluster_size",
        [str(DEFAULT_HIERARCHICAL_MIN_CLUSTER_SIZE)],
    )[0].strip()
    if method_raw not in {"hdbscan", "hierarchical"}:
        raise ValueError("clustering method must be either 'hdbscan' or 'hierarchical'")
    if distance_raw not in {"jaccard", "dice", "overlap"}:
        raise ValueError("distance must be one of 'jaccard', 'dice', or 'overlap'")
    if linkage_raw not in {"single", "complete", "average"}:
        raise ValueError("linkage must be one of 'single', 'complete', or 'average'")
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
    hierarchical_min_cluster_size = int(hierarchical_min_cluster_size_raw)
    if hierarchical_min_cluster_size <= 0:
        raise ValueError("hierarchical minimal cluster size must be positive")
    if method_raw == "hierarchical":
        if n_clusters is None and distance_threshold is None:
            raise ValueError("hierarchical clustering requires n clusters or cutoff distance")
        if n_clusters is not None and distance_threshold is not None:
            raise ValueError("set either n clusters or cutoff distance for hierarchical clustering, not both")
    return {
        "method": method_raw,
        "distance": distance_raw,
        "min_cluster_size": min_cluster_size,
        "min_samples": min_samples,
        "cluster_selection_epsilon": cluster_selection_epsilon,
        "linkage": linkage_raw,
        "n_clusters": n_clusters,
        "distance_threshold": distance_threshold,
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


def interface_residue_count(payload: dict[str, object]) -> int:
    raw_values = payload.get("interface_residues_a")
    if raw_values is None:
        raw_values = payload.get("interface_msa_columns_a", [])
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
    if min_interface_size <= 0:
        return interface_payload
    filtered_payload: dict[str, dict[str, dict]] = {}
    for partner_domain in sorted(interface_payload):
        rows = interface_payload.get(partner_domain)
        if not isinstance(rows, dict):
            continue
        kept_rows = {
            row_key: row_payload
            for row_key, row_payload in rows.items()
            if isinstance(row_payload, dict) and interface_residue_count(row_payload) >= min_interface_size
        }
        if kept_rows:
            filtered_payload[str(partner_domain)] = kept_rows
    return filtered_payload


def distance_data_cache_key(
    interface_path: Path,
    distance_metric: str,
    interface_filter_settings: dict[str, object] | None = None,
) -> str:
    stat = interface_path.stat()
    return "|".join(
        (
            str(interface_path.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            distance_metric,
            interface_filter_settings_key(interface_filter_settings),
        )
    )


def compute_interface_distance_data(interface_payload: dict[str, object]) -> dict[str, object]:
    try:
        import numpy as np
        from sklearn.metrics import pairwise_distances
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Embedding view requires scikit-learn in the Python environment running the server."
        ) from exc
    distance_metric = str(interface_payload["distance_metric"])
    interface_path = interface_payload.get("interface_path")
    raw_interface_payload = interface_payload["payload"]
    interface_filter_settings = interface_payload.get("interface_filter_settings")
    entries = load_interface_entries(raw_interface_payload)
    if len(entries) < 2:
        raise ValueError("need at least two interfaces with non-empty interface sets")
    indicator_matrix, msa_columns = build_indicator_matrix(entries)
    if distance_metric == "overlap":
        if (
            int((interface_filter_settings or {}).get("min_interface_size", DEFAULT_MIN_INTERFACE_SIZE)) <= 0
            and isinstance(interface_path, Path)
        ):
            distance_matrix = compute_interface_distance_matrix(input_file=interface_path)
        else:
            row_sets = [set(int(column) for column in entry["columns"]) for entry in entries]
            distance_matrix = np.zeros((len(row_sets), len(row_sets)), dtype=np.float64)
            for left_index in range(len(row_sets)):
                left = row_sets[left_index]
                for right_index in range(left_index + 1, len(row_sets)):
                    right = row_sets[right_index]
                    distance = overlap_distance_for_sets(left, right)
                    distance_matrix[left_index, right_index] = distance
                    distance_matrix[right_index, left_index] = distance
    else:
        distance_matrix = pairwise_distances(indicator_matrix, metric=distance_metric).astype(
            np.float64, copy=False
        )
    return {
        "entries": entries,
        "indicator_matrix": indicator_matrix,
        "msa_columns": msa_columns,
        "distance_matrix": distance_matrix,
        "distance": distance_metric,
    }


def load_interface_distance_data(
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
    distance_metric: str,
    interface_filter_settings: dict[str, object] | None = None,
) -> dict[str, object]:
    cache_key = distance_data_cache_key(interface_path, distance_metric, interface_filter_settings)
    cached = DISTANCE_DATA_CACHE.get(cache_key)
    if cached is not None:
        return cached
    distance_data = compute_interface_distance_data(
        {
            "payload": interface_payload,
            "distance_metric": distance_metric,
            "interface_path": interface_path,
            "interface_filter_settings": interface_filter_settings,
        }
    )
    DISTANCE_DATA_CACHE[cache_key] = distance_data
    while len(DISTANCE_DATA_CACHE) > DISTANCE_DATA_CACHE_LIMIT:
        DISTANCE_DATA_CACHE.pop(next(iter(DISTANCE_DATA_CACHE)))
    return distance_data


def compute_tsne_embedding_payload(distance_data: dict[str, object], settings: dict[str, object]) -> dict[str, object]:
    try:
        import numpy as np
        from sklearn.manifold import TSNE
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Embedding view requires scikit-learn in the Python environment running the server."
        ) from exc
    entries = distance_data["entries"]
    indicator_matrix = distance_data["indicator_matrix"]
    msa_columns = distance_data["msa_columns"]
    distance_matrix = distance_data["distance_matrix"]
    distance_metric = str(distance_data["distance"])
    sample_count = indicator_matrix.shape[0]
    perplexity = settings["perplexity"]
    if perplexity == "auto":
        perplexity = min(30.0, max(1.0, (sample_count - 1) / 3.0))
    if float(perplexity) >= sample_count:
        raise ValueError(f"perplexity must be smaller than the number of samples ({sample_count})")
    coordinates = TSNE(
        n_components=3,
        metric="precomputed",
        init="random",
        random_state=int(settings["random_state"]),
        perplexity=float(perplexity),
        learning_rate=settings["learning_rate"],
        max_iter=int(settings["max_iter"]),
        early_exaggeration=float(settings["early_exaggeration"]),
    ).fit_transform(distance_matrix)
    coordinates -= coordinates.mean(axis=0, keepdims=True)
    max_radius = float(np.linalg.norm(coordinates, axis=1).max())
    if max_radius > 0.0:
        coordinates /= max_radius
    points = []
    for entry, coords in zip(entries, coordinates.tolist(), strict=True):
        points.append(
            {
                "row_key": str(entry["row_key"]),
                "partner_domain": str(entry["partner_domain"]),
                "interface_size": len(entry["columns"]),
                "x": float(coords[0]),
                "y": float(coords[1]),
                "z": float(coords[2]),
            }
        )
    return {
        "embedding": "tsne",
        "distance": distance_metric,
        "dimensions": 3,
        "sample_count": len(points),
        "column_count": len(msa_columns),
        "perplexity": perplexity,
        "settings": {
            "distance": settings["distance"],
            "perplexity": settings["perplexity"],
            "learning_rate": settings["learning_rate"],
            "max_iter": settings["max_iter"],
            "early_exaggeration": settings["early_exaggeration"],
            "random_state": settings["random_state"],
        },
        "points": points,
    }


def compute_hdbscan_clustering_payload(distance_data: dict[str, object], settings: dict[str, object]) -> dict[str, object]:
    try:
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
        labels = clusterer.fit_predict(distance_matrix)
    except Exception as exc:
        raise RuntimeError(f"HDBSCAN failed unexpectedly: {exc}") from exc
    unique_cluster_labels = sorted({int(label) for label in labels if int(label) >= 0})
    cluster_count = len(unique_cluster_labels)
    noise_count = int(sum(1 for label in labels if int(label) < 0))
    points = [
        {
            "row_key": str(entry["row_key"]),
            "partner_domain": str(entry["partner_domain"]),
            "cluster_label": int(label),
        }
        for entry, label in zip(entries, labels.tolist(), strict=True)
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
    try:
        from sklearn.cluster import AgglomerativeClustering
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Hierarchical clustering requires scikit-learn in the Python environment running the server."
        ) from exc
    entries = distance_data["entries"]
    distance_matrix = distance_data["distance_matrix"]
    distance_metric = str(distance_data["distance"])
    linkage = str(settings["linkage"])
    n_clusters = settings["n_clusters"]
    distance_threshold = settings["distance_threshold"]
    hierarchical_min_cluster_size = int(
        settings.get("hierarchical_min_cluster_size", DEFAULT_HIERARCHICAL_MIN_CLUSTER_SIZE)
    )
    clusterer = AgglomerativeClustering(
        metric="precomputed",
        linkage=linkage,
        n_clusters=n_clusters,
        distance_threshold=distance_threshold,
        compute_distances=False,
    )
    try:
        labels = clusterer.fit_predict(distance_matrix)
    except Exception as exc:
        raise RuntimeError(f"Hierarchical clustering failed unexpectedly: {exc}") from exc
    labels_list = [int(label) for label in labels.tolist()]
    if distance_threshold is not None:
        label_counts: dict[int, int] = {}
        for label in labels_list:
            if label < 0:
                continue
            label_counts[label] = label_counts.get(label, 0) + 1
        too_small_labels = {
            label for label, count in label_counts.items() if count < hierarchical_min_cluster_size
        }
        if too_small_labels:
            labels_list = [
                -1 if label in too_small_labels else label
                for label in labels_list
            ]
    labels_list = remap_non_negative_cluster_labels(labels_list)
    unique_cluster_labels = sorted({label for label in labels_list if label >= 0})
    cluster_count = len(unique_cluster_labels)
    noise_count = int(sum(1 for label in labels_list if label < 0))
    points = [
        {
            "row_key": str(entry["row_key"]),
            "partner_domain": str(entry["partner_domain"]),
            "cluster_label": label,
        }
        for entry, label in zip(entries, labels_list, strict=True)
    ]
    return {
        "clustering": "hierarchical",
        "distance": distance_metric,
        "sample_count": len(points),
        "cluster_count": cluster_count,
        "noise_count": noise_count,
        "settings": {
            "method": "hierarchical",
            "distance": distance_metric,
            "linkage": linkage,
            "n_clusters": n_clusters,
            "distance_threshold": distance_threshold,
            "hierarchical_min_cluster_size": hierarchical_min_cluster_size,
        },
        "points": points,
    }


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


def load_or_compute_clustering_payload(
    cache_dir: Path,
    interface_path: Path,
    interface_payload: dict[str, dict[str, dict]],
    clustering_settings: dict[str, object],
    interface_filter_settings: dict[str, object] | None = None,
) -> dict[str, object]:
    cache_path = clustering_cache_path(
        cache_dir,
        interface_path,
        clustering_settings,
        interface_filter_settings,
    )
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    distance_data = load_interface_distance_data(
        interface_path,
        interface_payload,
        str(clustering_settings["distance"]),
        interface_filter_settings,
    )
    if clustering_settings["method"] == "hierarchical":
        clustering_payload = compute_hierarchical_clustering_payload(distance_data, clustering_settings)
    else:
        clustering_payload = compute_hdbscan_clustering_payload(distance_data, clustering_settings)
    response_payload = {
        "file": interface_path.name,
        "pfam_id": interface_path.name.split("_", maxsplit=1)[0],
        "filter_settings": interface_filter_settings or {"min_interface_size": DEFAULT_MIN_INTERFACE_SIZE},
        **clustering_payload,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as handle:
        json.dump(response_payload, handle)
    return response_payload


def compute_cluster_compare_payload(
    distance_data: dict[str, object],
    clustering_payload: dict[str, object],
    cluster_label: int,
    limit: int = DEFAULT_CLUSTER_COMPARE_LIMIT,
) -> dict[str, object]:
    clustering_points = clustering_payload["points"]
    entries = distance_data["entries"]
    distance_matrix = distance_data["distance_matrix"]
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


def compute_row_distance_matrix_payload(
    interface_payload: dict[str, dict[str, dict]],
    distance_metric: str,
    max_rows: int = MAX_DISTANCE_MATRIX_ROWS,
) -> dict[str, object]:
    try:
        import numpy as np
        from sklearn.metrics import pairwise_distances
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Distance matrix view requires scikit-learn in the Python environment running the server."
        ) from exc

    columns_by_row: dict[str, set[int]] = {}
    partner_counts_by_row: dict[str, dict[str, int]] = {}
    for partner_domain in sorted(interface_payload):
        rows = interface_payload[partner_domain]
        for row_key in sorted(rows):
            raw_columns = rows[row_key].get("interface_msa_columns_a", [])
            if not raw_columns:
                continue
            bucket = columns_by_row.setdefault(row_key, set())
            for column in raw_columns:
                bucket.add(int(column))
            partner_counts = partner_counts_by_row.setdefault(row_key, {})
            partner_counts[partner_domain] = partner_counts.get(partner_domain, 0) + len(raw_columns)

    row_keys = sorted(row_key for row_key, columns in columns_by_row.items() if columns)
    if len(row_keys) < 2:
        raise ValueError("need at least two rows with non-empty interface sets")

    dominant_partner_by_row: dict[str, str] = {}
    for row_key in row_keys:
        partner_counts = partner_counts_by_row.get(row_key, {})
        if not partner_counts:
            continue
        dominant_partner = sorted(
            partner_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
        dominant_partner_by_row[row_key] = dominant_partner

    row_keys = sorted(
        row_keys,
        key=lambda row_key: (
            dominant_partner_by_row.get(row_key, ""),
            row_key,
        ),
    )

    original_count = len(row_keys)
    truncated = False
    if len(row_keys) > max_rows:
        truncated = True
        sample_positions = np.linspace(0, len(row_keys) - 1, num=max_rows, dtype=int)
        row_keys = [row_keys[int(position)] for position in sample_positions.tolist()]

    msa_columns = sorted({column for row_key in row_keys for column in columns_by_row[row_key]})
    if not msa_columns:
        raise ValueError("distance matrix cannot be computed: no interface columns found")
    column_index = {column: index for index, column in enumerate(msa_columns)}
    indicator_matrix = np.zeros((len(row_keys), len(msa_columns)), dtype=bool)
    for row_index, row_key in enumerate(row_keys):
        for column in columns_by_row[row_key]:
            indicator_matrix[row_index, column_index[column]] = True

    if distance_metric == "overlap":
        distance_matrix = np.zeros((len(row_keys), len(row_keys)), dtype=np.float64)
        row_sets = [columns_by_row[row_key] for row_key in row_keys]
        for left_index in range(len(row_sets)):
            left = row_sets[left_index]
            for right_index in range(left_index + 1, len(row_sets)):
                right = row_sets[right_index]
                distance = overlap_distance_for_sets(left, right)
                distance_matrix[left_index, right_index] = distance
                distance_matrix[right_index, left_index] = distance
    else:
        distance_matrix = pairwise_distances(indicator_matrix, metric=distance_metric).astype(
            np.float64, copy=False
        )

    return {
        "distance": distance_metric,
        "sort": "dominant_partner_domain",
        "row_count": len(row_keys),
        "original_row_count": original_count,
        "truncated": truncated,
        "row_keys": row_keys,
        "row_partners": [dominant_partner_by_row.get(row_key, "") for row_key in row_keys],
        "matrix": distance_matrix.tolist(),
    }
