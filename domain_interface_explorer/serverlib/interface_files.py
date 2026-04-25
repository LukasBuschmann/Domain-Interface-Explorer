from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import IO


INTERFACE_JSON_SUFFIXES = (".json", ".json.gz")


def interface_file_stem(path_or_name: Path | str) -> str:
    name = Path(path_or_name).name
    lower_name = name.lower()
    for suffix in INTERFACE_JSON_SUFFIXES:
        if lower_name.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem


def interface_file_pfam_id(path_or_name: Path | str) -> str:
    return interface_file_stem(path_or_name).split("_", maxsplit=1)[0]


def is_interface_json_path(path: Path) -> bool:
    return path.name.lower().endswith(INTERFACE_JSON_SUFFIXES)


def directory_interface_json_paths(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and is_interface_json_path(path)
    )


def open_interface_json(path: Path) -> IO[str]:
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def load_interface_json(path: Path) -> dict[str, object]:
    with open_interface_json(path) as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected top-level object in {path}")
    return payload
