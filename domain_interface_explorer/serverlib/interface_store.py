from __future__ import annotations

import json
import sqlite3
import threading
import time
from array import array
from concurrent.futures import Future
from pathlib import Path

from .config import DEFAULT_MIN_INTERFACE_SIZE
from .interface_embedding import (
    build_interface_alignment_rows_from_metadata,
    interface_residue_count,
    parse_interface_row_key,
)
from .interface_files import directory_interface_json_paths, interface_file_pfam_id, load_interface_json
from .stats_service import (
    CLEAN_COLUMN_IDENTITY_BATCH_SIZE,
    count_clean_identity_batch,
    fragment_ranges,
)
from .timing import log_event, timed_step


INTERFACE_STORE_SCHEMA_VERSION = 1


def pack_uints(values: object) -> bytes:
    if not isinstance(values, list):
        return b""
    packed = array("I")
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed >= 0:
            packed.append(parsed)
    return packed.tobytes()


def unpack_uints(blob: object) -> list[int]:
    if not blob:
        return []
    packed = array("I")
    packed.frombytes(bytes(blob))
    return packed.tolist()


def pack_uint_pairs(values: object) -> bytes:
    if not isinstance(values, list):
        return b""
    packed = array("I")
    for item in values:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            left = int(item[0])
            right = int(item[1])
        except (TypeError, ValueError):
            continue
        if left >= 0 and right >= 0:
            packed.append(left)
            packed.append(right)
    return packed.tobytes()


def unpack_uint_pairs(blob: object) -> list[list[int]]:
    values = unpack_uints(blob)
    return [[values[index], values[index + 1]] for index in range(0, len(values) - 1, 2)]


def pack_uint16(values: list[int]) -> bytes:
    packed = array("H")
    for value in values:
        parsed = max(0, min(65535, int(value)))
        packed.append(parsed)
    return packed.tobytes()


def unpack_uint16(blob: object) -> list[int]:
    if not blob:
        return []
    packed = array("H")
    packed.frombytes(bytes(blob))
    return packed.tolist()


def filter_min_interface_size(filter_settings: dict[str, object] | None) -> int:
    return int((filter_settings or {}).get("min_interface_size", DEFAULT_MIN_INTERFACE_SIZE))


class InterfaceStore:
    def __init__(self, db_path: Path, interface_dir: Path):
        self.db_path = db_path
        self.interface_dir = interface_dir
        self._import_lock = threading.Lock()
        self._in_flight: dict[str, Future[int]] = {}
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=60)
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=60000")
        return connection

    def ensure_schema(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sources (
                    source_id INTEGER PRIMARY KEY,
                    path TEXT NOT NULL UNIQUE,
                    filename TEXT NOT NULL,
                    pfam_id TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    import_status TEXT NOT NULL,
                    imported_at REAL,
                    raw_row_count INTEGER NOT NULL DEFAULT 0,
                    alignment_length INTEGER NOT NULL DEFAULT 0,
                    schema_version INTEGER NOT NULL,
                    error TEXT
                );

                CREATE TABLE IF NOT EXISTS interface_rows (
                    row_id INTEGER PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
                    row_order INTEGER NOT NULL,
                    partner_domain TEXT NOT NULL,
                    interface_row_key TEXT NOT NULL,
                    protein_id TEXT NOT NULL,
                    fragment_key TEXT NOT NULL,
                    partner_fragment_key TEXT NOT NULL,
                    aligned_seq TEXT NOT NULL,
                    interface_size_a INTEGER NOT NULL,
                    interface_size_b INTEGER NOT NULL,
                    interface_residues_a BLOB NOT NULL,
                    interface_residues_b BLOB NOT NULL,
                    surface_residue_ids_a BLOB NOT NULL,
                    surface_residue_ids_b BLOB NOT NULL,
                    interface_msa_columns_a BLOB NOT NULL,
                    surface_msa_columns_a BLOB NOT NULL,
                    residue_contacts BLOB NOT NULL,
                    fragments_b BLOB NOT NULL,
                    UNIQUE(source_id, partner_domain, interface_row_key)
                );

                CREATE INDEX IF NOT EXISTS interface_rows_source_order_idx
                    ON interface_rows(source_id, row_order);
                CREATE INDEX IF NOT EXISTS interface_rows_source_filter_order_idx
                    ON interface_rows(source_id, interface_size_a, interface_size_b, row_order);
                CREATE INDEX IF NOT EXISTS interface_rows_source_key_idx
                    ON interface_rows(source_id, interface_row_key);
                CREATE INDEX IF NOT EXISTS interface_rows_source_partner_key_idx
                    ON interface_rows(source_id, partner_domain, interface_row_key);

                CREATE TABLE IF NOT EXISTS clean_column_identity (
                    source_id INTEGER PRIMARY KEY REFERENCES sources(source_id) ON DELETE CASCADE,
                    identity BLOB NOT NULL,
                    unique_rows INTEGER NOT NULL,
                    alignment_length INTEGER NOT NULL,
                    computed_at REAL NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?)",
                (str(INTERFACE_STORE_SCHEMA_VERSION),),
            )

    def start_background_sync(self) -> threading.Thread:
        thread = threading.Thread(
            target=self.sync_interface_dir,
            daemon=True,
            name="interface-store-sync",
        )
        thread.start()
        return thread

    def sync_interface_dir(self) -> None:
        paths = directory_interface_json_paths(self.interface_dir)
        missing = 0
        with timed_step("store", "sync interface store", files=len(paths)) as timer:
            for path in paths:
                if self.source_is_ready(path):
                    continue
                missing += 1
                try:
                    self.ensure_source_ready(path)
                except Exception as exc:  # pragma: no cover
                    log_event("store", "failed to import interface source", file=path.name, error=exc)
            timer.set(imported=missing)

    def source_signature(self, path: Path) -> tuple[str, int, int]:
        stat = path.stat()
        return str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns)

    def source_is_ready(self, path: Path) -> bool:
        resolved, size_bytes, mtime_ns = self.source_signature(path)
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT import_status, schema_version
                FROM sources
                WHERE path = ? AND size_bytes = ? AND mtime_ns = ?
                """,
                (resolved, size_bytes, mtime_ns),
            ).fetchone()
        return bool(
            row
            and row[0] == "ready"
            and int(row[1]) == INTERFACE_STORE_SCHEMA_VERSION
        )

    def source_id_if_ready(self, path: Path) -> int | None:
        resolved, size_bytes, mtime_ns = self.source_signature(path)
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT source_id
                FROM sources
                WHERE path = ? AND size_bytes = ? AND mtime_ns = ?
                  AND import_status = 'ready' AND schema_version = ?
                """,
                (resolved, size_bytes, mtime_ns, INTERFACE_STORE_SCHEMA_VERSION),
            ).fetchone()
        return int(row[0]) if row else None

    def ensure_source_ready(self, path: Path) -> int:
        ready_source_id = self.source_id_if_ready(path)
        if ready_source_id is not None:
            return ready_source_id
        resolved, _size_bytes, _mtime_ns = self.source_signature(path)
        owns_import = False
        with self._import_lock:
            future = self._in_flight.get(resolved)
            if future is None:
                future = Future()
                self._in_flight[resolved] = future
                owns_import = True
        if not owns_import:
            with timed_step("store", "wait for interface source import", file=path.name):
                return int(future.result())
        try:
            source_id = self.import_source(path)
        except BaseException as exc:
            self.mark_import_error(path, exc)
            with self._import_lock:
                self._in_flight.pop(resolved, None)
                future.set_exception(exc)
            raise
        with self._import_lock:
            self._in_flight.pop(resolved, None)
            future.set_result(source_id)
        return source_id

    def import_source(self, path: Path) -> int:
        resolved, size_bytes, mtime_ns = self.source_signature(path)
        pfam_id = interface_file_pfam_id(path)
        with timed_step("store", "import interface source", file=path.name, bytes=size_bytes) as timer:
            payload = load_interface_json(path)
            if not isinstance(payload, dict):
                raise ValueError(f"expected top-level object in {path}")
            row_values: list[tuple[object, ...]] = []
            raw_row_count = 0
            alignment_length = 0
            row_order = 0
            for partner_domain in sorted(payload):
                rows = payload.get(partner_domain)
                if not isinstance(rows, dict):
                    continue
                for interface_row_key in sorted(rows):
                    row_payload = rows.get(interface_row_key)
                    if not isinstance(row_payload, dict):
                        continue
                    parsed = parse_interface_row_key(str(interface_row_key))
                    aligned_seq = row_payload.get("aligned_seq")
                    aligned_seq = aligned_seq if isinstance(aligned_seq, str) else ""
                    alignment_length = max(alignment_length, len(aligned_seq))
                    interface_size_a = interface_residue_count(row_payload, "a")
                    interface_size_b = interface_residue_count(row_payload, "b")
                    row_values.append(
                        (
                            row_order,
                            str(partner_domain),
                            str(interface_row_key),
                            str(parsed["protein_id"]),
                            str(parsed["fragment_key"]),
                            str(parsed["partner_fragment_key"]),
                            aligned_seq,
                            interface_size_a,
                            interface_size_b,
                            sqlite3.Binary(pack_uints(row_payload.get("interface_residues_a"))),
                            sqlite3.Binary(pack_uints(row_payload.get("interface_residues_b"))),
                            sqlite3.Binary(pack_uints(row_payload.get("surface_residue_ids_a"))),
                            sqlite3.Binary(pack_uints(row_payload.get("surface_residue_ids_b"))),
                            sqlite3.Binary(pack_uints(row_payload.get("interface_msa_columns_a"))),
                            sqlite3.Binary(pack_uints(row_payload.get("surface_msa_columns_a"))),
                            sqlite3.Binary(pack_uint_pairs(row_payload.get("residue_contacts"))),
                            sqlite3.Binary(pack_uint_pairs(row_payload.get("fragments_b"))),
                        )
                    )
                    row_order += 1
                    raw_row_count += 1
            with self.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT source_id FROM sources WHERE path = ?",
                    (resolved,),
                ).fetchone()
                if existing:
                    source_id = int(existing[0])
                    connection.execute("DELETE FROM clean_column_identity WHERE source_id = ?", (source_id,))
                    connection.execute("DELETE FROM interface_rows WHERE source_id = ?", (source_id,))
                    connection.execute(
                        """
                        UPDATE sources
                        SET filename = ?, pfam_id = ?, size_bytes = ?, mtime_ns = ?,
                            import_status = 'importing', imported_at = NULL,
                            raw_row_count = 0, alignment_length = 0,
                            schema_version = ?, error = NULL
                        WHERE source_id = ?
                        """,
                        (
                            path.name,
                            pfam_id,
                            size_bytes,
                            mtime_ns,
                            INTERFACE_STORE_SCHEMA_VERSION,
                            source_id,
                        ),
                    )
                else:
                    cursor = connection.execute(
                        """
                        INSERT INTO sources (
                            path, filename, pfam_id, size_bytes, mtime_ns, import_status,
                            schema_version
                        ) VALUES (?, ?, ?, ?, ?, 'importing', ?)
                        """,
                        (resolved, path.name, pfam_id, size_bytes, mtime_ns, INTERFACE_STORE_SCHEMA_VERSION),
                    )
                    source_id = int(cursor.lastrowid)
                connection.executemany(
                    """
                    INSERT INTO interface_rows (
                        source_id, row_order, partner_domain, interface_row_key,
                        protein_id, fragment_key, partner_fragment_key, aligned_seq,
                        interface_size_a, interface_size_b, interface_residues_a,
                        interface_residues_b, surface_residue_ids_a, surface_residue_ids_b,
                        interface_msa_columns_a, surface_msa_columns_a,
                        residue_contacts, fragments_b
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ((source_id, *row) for row in row_values),
                )
                connection.execute(
                    """
                    UPDATE sources
                    SET import_status = 'ready', imported_at = ?, raw_row_count = ?,
                        alignment_length = ?, error = NULL
                    WHERE source_id = ?
                    """,
                    (time.time(), raw_row_count, alignment_length, source_id),
                )
                connection.commit()
            timer.set(rows=raw_row_count, alignment_length=alignment_length)
            return source_id

    def mark_import_error(self, path: Path, error: Exception) -> None:
        resolved, size_bytes, mtime_ns = self.source_signature(path)
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO sources (
                    path, filename, pfam_id, size_bytes, mtime_ns, import_status,
                    schema_version, error
                ) VALUES (?, ?, ?, ?, ?, 'error', ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    filename = excluded.filename,
                    pfam_id = excluded.pfam_id,
                    size_bytes = excluded.size_bytes,
                    mtime_ns = excluded.mtime_ns,
                    import_status = excluded.import_status,
                    schema_version = excluded.schema_version,
                    error = excluded.error
                """,
                (
                    resolved,
                    path.name,
                    interface_file_pfam_id(path),
                    size_bytes,
                    mtime_ns,
                    INTERFACE_STORE_SCHEMA_VERSION,
                    str(error),
                ),
            )

    def filtered_where(self, min_interface_size: int) -> tuple[str, tuple[int, int]]:
        return "interface_size_a >= ? AND interface_size_b >= ?", (
            min_interface_size,
            min_interface_size,
        )

    def source_summary(self, connection: sqlite3.Connection, source_id: int) -> tuple[str, str, int]:
        row = connection.execute(
            "SELECT filename, pfam_id, alignment_length FROM sources WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"missing source_id {source_id}")
        return str(row[0]), str(row[1]), int(row[2])

    def get_interface_page(
        self,
        path: Path,
        filter_settings: dict[str, object],
        *,
        row_offset: int,
        row_limit: int | None,
        include_rows: bool,
        include_data: bool,
        data_offset: int,
        data_limit: int | None,
        include_clean_column_identity: bool,
    ) -> dict[str, object]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        with timed_step(
            "store",
            "build interface endpoint payload",
            file=path.name,
            row_offset=row_offset,
            row_limit=row_limit if row_limit is not None else "all",
        ) as timer:
            with self.connect() as connection:
                filename, pfam_id, alignment_length = self.source_summary(connection, source_id)
                total_rows = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM interface_rows WHERE source_id = ? AND {where_sql}",
                        (source_id, *where_args),
                    ).fetchone()[0]
                )
                filtered_alignment_length = int(
                    connection.execute(
                        f"""
                        SELECT COALESCE(MAX(LENGTH(aligned_seq)), 0)
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}
                        """,
                        (source_id, *where_args),
                    ).fetchone()[0]
                )
                partner_counts = {
                    str(partner): int(count)
                    for partner, count in connection.execute(
                        f"""
                        SELECT partner_domain, COUNT(*)
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}
                        GROUP BY partner_domain
                        ORDER BY partner_domain
                        """,
                        (source_id, *where_args),
                    )
                }
                raw_rows = (
                    self.query_alignment_rows(
                        connection,
                        source_id,
                        where_sql,
                        where_args,
                        row_offset,
                        row_limit,
                    )
                    if include_rows
                    else []
                )
                rows, _alignment_length, _total = build_interface_alignment_rows_from_metadata(
                    raw_rows,
                    filtered_alignment_length,
                    row_offset=0,
                    row_limit=None,
                    include_total=True,
                )
                overlay_payload = (
                    self.query_overlay_payload(
                        connection,
                        source_id,
                        where_sql,
                        where_args,
                        data_offset,
                        data_limit,
                    )
                    if include_data
                    else None
                )
            rows_loaded = len(rows)
            data_loaded = (
                sum(len(rows_by_partner) for rows_by_partner in overlay_payload.values())
                if overlay_payload is not None
                else 0
            )
            response: dict[str, object] = {
                "file": filename,
                "pfam_id": pfam_id,
                "filter_settings": filter_settings,
                "alignment_length": filtered_alignment_length,
                "row_count": total_rows,
                "interface_partner_domains": list(partner_counts),
                "interface_partner_counts": partner_counts,
                "row_offset": row_offset,
                "row_limit": row_limit,
                "rows_loaded": rows_loaded,
                "rows_complete": row_offset + rows_loaded >= total_rows,
                "rows": rows,
            }
            if include_clean_column_identity:
                response["clean_column_identity"] = self.get_clean_column_identity(path)
            if overlay_payload is not None:
                response.update(
                    {
                        "data": overlay_payload,
                        "data_row_count": total_rows,
                        "data_offset": data_offset,
                        "data_limit": data_limit,
                        "data_loaded": data_loaded,
                        "data_complete": data_offset + data_loaded >= total_rows,
                    }
                )
            timer.set(
                rows=rows_loaded,
                total_rows=total_rows,
                overlay_rows=data_loaded,
                partner_domains=len(partner_counts),
                clean_columns=len(response.get("clean_column_identity", [])),
            )
            return response

    def query_alignment_rows(
        self,
        connection: sqlite3.Connection,
        source_id: int,
        where_sql: str,
        where_args: tuple[int, int],
        row_offset: int,
        row_limit: int | None,
    ) -> list[dict[str, object]]:
        limit_sql = "" if row_limit is None else "LIMIT ?"
        args: tuple[object, ...] = (source_id, *where_args)
        if row_limit is not None:
            args = (*args, int(row_limit), int(row_offset))
        else:
            args = (*args, int(row_offset))
        return [
            {
                "interface_row_key": str(row[0]),
                "protein_id": str(row[1]),
                "fragment_key": str(row[2]),
                "partner_fragment_key": str(row[3]),
                "partner_domain": str(row[4]),
                "aligned_sequence": str(row[5] or ""),
            }
            for row in connection.execute(
                f"""
                SELECT interface_row_key, protein_id, fragment_key,
                       partner_fragment_key, partner_domain, aligned_seq
                FROM interface_rows
                WHERE source_id = ? AND {where_sql}
                ORDER BY row_order
                {limit_sql} OFFSET ?
                """,
                args,
            )
        ]

    def query_overlay_payload(
        self,
        connection: sqlite3.Connection,
        source_id: int,
        where_sql: str,
        where_args: tuple[int, int],
        row_offset: int,
        row_limit: int | None,
    ) -> dict[str, dict[str, dict[str, object]]]:
        limit_sql = "" if row_limit is None else "LIMIT ?"
        args: tuple[object, ...] = (source_id, *where_args)
        if row_limit is not None:
            args = (*args, int(row_limit), int(row_offset))
        else:
            args = (*args, int(row_offset))
        payload: dict[str, dict[str, dict[str, object]]] = {}
        for row in connection.execute(
            f"""
            SELECT partner_domain, interface_row_key,
                   interface_msa_columns_a, surface_msa_columns_a
            FROM interface_rows
            WHERE source_id = ? AND {where_sql}
            ORDER BY row_order
            {limit_sql} OFFSET ?
            """,
            args,
        ):
            partner_domain = str(row[0])
            row_key = str(row[1])
            payload.setdefault(partner_domain, {})[row_key] = {
                "interface_msa_columns_a": unpack_uints(row[2]),
                "surface_msa_columns_a": unpack_uints(row[3]),
            }
        return payload

    def get_columns_payload(
        self,
        path: Path,
        filter_settings: dict[str, object],
    ) -> dict[str, dict[str, dict[str, object]]]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        with timed_step("store", "load interface columns payload", file=path.name) as timer:
            payload: dict[str, dict[str, dict[str, object]]] = {}
            row_count = 0
            with self.connect() as connection:
                for row in connection.execute(
                    f"""
                    SELECT partner_domain, interface_row_key, interface_msa_columns_a
                    FROM interface_rows
                    WHERE source_id = ? AND {where_sql}
                    ORDER BY row_order
                    """,
                    (source_id, *where_args),
                ):
                    partner_domain = str(row[0])
                    row_key = str(row[1])
                    payload.setdefault(partner_domain, {})[row_key] = {
                        "interface_msa_columns_a": unpack_uints(row[2]),
                    }
                    row_count += 1
            timer.set(rows=row_count, partner_domains=len(payload))
            return payload

    def get_structure_interface_payload(
        self,
        path: Path,
        filter_settings: dict[str, object],
        row_key: str,
        partner_filter: str,
    ) -> dict[str, dict[str, dict[str, object]]]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        partner_sql = "" if partner_filter == "__all__" else "AND partner_domain = ?"
        args: tuple[object, ...] = (source_id, *where_args, row_key)
        if partner_filter != "__all__":
            args = (*args, partner_filter)
        with timed_step("store", "load structure row payload", file=path.name, row_key=row_key) as timer:
            payload: dict[str, dict[str, dict[str, object]]] = {}
            with self.connect() as connection:
                for row in connection.execute(
                    f"""
                    SELECT partner_domain, interface_row_key,
                           interface_residues_a, surface_residue_ids_a,
                           interface_residues_b, surface_residue_ids_b,
                           residue_contacts, fragments_b
                    FROM interface_rows
                    WHERE source_id = ? AND {where_sql}
                      AND interface_row_key = ?
                      {partner_sql}
                    ORDER BY partner_domain
                    """,
                    args,
                ):
                    partner_domain = str(row[0])
                    current_row_key = str(row[1])
                    payload.setdefault(partner_domain, {})[current_row_key] = {
                        "interface_residues_a": unpack_uints(row[2]),
                        "surface_residue_ids_a": unpack_uints(row[3]),
                        "interface_residues_b": unpack_uints(row[4]),
                        "surface_residue_ids_b": unpack_uints(row[5]),
                        "residue_contacts": unpack_uint_pairs(row[6]),
                        "fragments_b": unpack_uint_pairs(row[7]),
                    }
            timer.set(rows=sum(len(rows) for rows in payload.values()))
            return payload

    def get_clean_column_identity(self, path: Path) -> list[int]:
        source_id = self.ensure_source_ready(path)
        with self.connect() as connection:
            cached = connection.execute(
                "SELECT identity FROM clean_column_identity WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            if cached is not None:
                return unpack_uint16(cached[0])
        with timed_step("store", "compute clean column identity", file=path.name) as timer:
            rows_for_identity: list[tuple[str, tuple[tuple[int, int], ...]]] = []
            seen_row_keys: set[str] = set()
            alignment_length = 0
            with self.connect() as connection:
                for row_key, fragment_key, aligned_seq in connection.execute(
                    """
                    SELECT interface_row_key, fragment_key, aligned_seq
                    FROM interface_rows
                    WHERE source_id = ?
                    ORDER BY partner_domain, interface_row_key
                    """,
                    (source_id,),
                ):
                    row_key = str(row_key)
                    if row_key in seen_row_keys:
                        continue
                    seen_row_keys.add(row_key)
                    sequence = str(aligned_seq or "")
                    alignment_length = max(alignment_length, len(sequence))
                    rows_for_identity.append((sequence, tuple(fragment_ranges(str(fragment_key)))))
            unique_rows = len(rows_for_identity)
            if unique_rows <= 0 or alignment_length <= 0:
                identity = [0] * alignment_length
            else:
                import numpy as np

                column_letter_counts = np.zeros((alignment_length, 26), dtype=np.int64)
                for batch_start in range(0, unique_rows, CLEAN_COLUMN_IDENTITY_BATCH_SIZE):
                    batch_rows = rows_for_identity[
                        batch_start: batch_start + CLEAN_COLUMN_IDENTITY_BATCH_SIZE
                    ]
                    column_letter_counts += count_clean_identity_batch(batch_rows, alignment_length)
                identity = ((column_letter_counts.max(axis=1) * 100) // unique_rows).astype(int).tolist()
            with self.connect() as connection:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO clean_column_identity (
                        source_id, identity, unique_rows, alignment_length, computed_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        source_id,
                        sqlite3.Binary(pack_uint16(identity)),
                        unique_rows,
                        alignment_length,
                        time.time(),
                    ),
                )
            timer.set(columns=len(identity), unique_rows=unique_rows)
            return identity
