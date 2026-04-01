from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def active_conda_prefix() -> Path | None:
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        return Path(conda_prefix)
    prefix = Path(sys.prefix)
    if (prefix / "conda-meta").exists():
        return prefix
    return None


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_INTERFACE_DIR = PROJECT_ROOT / "data"
DEFAULT_CACHE_DIR = PROJECT_ROOT / "cache"
DEFAULT_PYMOL_BIN = (active_conda_prefix() or (PROJECT_ROOT / ".conda_env")) / "bin" / "pymol"
STATIC_DIR = PROJECT_ROOT / "domain_interface_explorer" / "static"
ALPHAFOLD_API = "https://alphafold.ebi.ac.uk/api/prediction/{accession}"
SELECTOR_STATS_CACHE_VERSION = "4"
EMBEDDING_CACHE_VERSION = "2"
CLUSTERING_CACHE_VERSION = "2"
DEFAULT_TSNE_LEARNING_RATE = "auto"
DEFAULT_TSNE_MAX_ITER = 1000
DEFAULT_TSNE_EARLY_EXAGGERATION = 12.0
DEFAULT_TSNE_RANDOM_STATE = 42
DEFAULT_DISTANCE_METRIC = "overlap"
DEFAULT_CLUSTER_MIN_SIZE = 25
DEFAULT_CLUSTER_SELECTION_EPSILON = 0.25
DEFAULT_CLUSTERING_METHOD = "hierarchical"
DEFAULT_HIERARCHICAL_LINKAGE = "average"
DEFAULT_HIERARCHICAL_TARGET = "distance_threshold"
DEFAULT_HIERARCHICAL_N_CLUSTERS = 8
DEFAULT_HIERARCHICAL_DISTANCE_THRESHOLD = 0.5
DEFAULT_HIERARCHICAL_MIN_CLUSTER_SIZE = 10
DEFAULT_CLUSTER_COMPARE_LIMIT = 9
DEFAULT_MIN_INTERFACE_SIZE = 5
DISTANCE_DATA_CACHE_LIMIT = 8
