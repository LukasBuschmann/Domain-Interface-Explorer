from __future__ import annotations

import gzip
import json
import threading
from collections import OrderedDict
from concurrent.futures import Future
from pathlib import Path
from typing import IO

from .timing import log_event, timed_step


INTERFACE_JSON_SUFFIXES = (".json", ".json.gz")
INTERFACE_JSON_CACHE_LIMIT = 2
INTERFACE_JSON_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
INTERFACE_JSON_IN_FLIGHT: dict[str, Future[dict[str, object]]] = {}
INTERFACE_JSON_CACHE_LOCK = threading.Lock()


def interface_json_cache_key(path: Path, size: int, mtime_ns: int) -> str:
    return "|".join((str(path.resolve()), str(size), str(mtime_ns)))


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
    stat = path.stat()
    cache_key = interface_json_cache_key(path, stat.st_size, stat.st_mtime_ns)
    owns_load = False
    with INTERFACE_JSON_CACHE_LOCK:
        cached_payload = INTERFACE_JSON_CACHE.get(cache_key)
        if cached_payload is not None:
            INTERFACE_JSON_CACHE.move_to_end(cache_key)
            log_event(
                "json",
                "reuse cached interface json",
                file=path.name,
                bytes=stat.st_size,
                top_level_keys=len(cached_payload),
            )
            return cached_payload
        load_future = INTERFACE_JSON_IN_FLIGHT.get(cache_key)
        if load_future is None:
            load_future = Future()
            INTERFACE_JSON_IN_FLIGHT[cache_key] = load_future
            owns_load = True
    if not owns_load:
        with timed_step(
            "json",
            "wait for in-flight interface json",
            file=path.name,
            bytes=stat.st_size,
        ):
            return load_future.result()
    try:
        with timed_step(
            "json",
            "load interface json",
            file=path.name,
            bytes=stat.st_size,
        ) as timer:
            with open_interface_json(path) as handle:
                payload = json.load(handle)
            if not isinstance(payload, dict):
                raise ValueError(f"expected top-level object in {path}")
            timer.set(top_level_keys=len(payload))
    except BaseException as exc:
        with INTERFACE_JSON_CACHE_LOCK:
            INTERFACE_JSON_IN_FLIGHT.pop(cache_key, None)
            load_future.set_exception(exc)
        raise
    with INTERFACE_JSON_CACHE_LOCK:
        INTERFACE_JSON_CACHE[cache_key] = payload
        INTERFACE_JSON_CACHE.move_to_end(cache_key)
        while len(INTERFACE_JSON_CACHE) > INTERFACE_JSON_CACHE_LIMIT:
            evicted_key, evicted_payload = INTERFACE_JSON_CACHE.popitem(last=False)
            log_event(
                "json",
                "evict cached interface json",
                cache_key=evicted_key,
                top_level_keys=len(evicted_payload),
            )
        INTERFACE_JSON_IN_FLIGHT.pop(cache_key, None)
        load_future.set_result(payload)
        return payload
