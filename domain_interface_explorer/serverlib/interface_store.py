from __future__ import annotations

import json
import sqlite3
import threading
import time
import zlib
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
    domain_length_from_fragment_key,
    fragment_ranges,
    histogram_entries_from_counts,
    pfam_row_coverage_percent,
)
from .timing import log_event, timed_step


INTERFACE_STORE_SCHEMA_VERSION = 2
COLUMN_STATS_CACHE_VERSION = 2


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


def pack_uint_triples(values: object) -> bytes:
    if not isinstance(values, list):
        return b""
    packed = array("I")
    for item in values:
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            continue
        try:
            left = int(item[0])
            right = int(item[1])
            mask = int(item[2])
        except (TypeError, ValueError):
            continue
        if left >= 0 and right >= 0 and 1 <= mask <= 255:
            packed.extend((left, right, mask))
    return packed.tobytes()


def unpack_uint_triples(blob: object) -> list[list[int]]:
    values = unpack_uints(blob)
    return [
        [values[index], values[index + 1], values[index + 2]]
        for index in range(0, len(values) - 2, 3)
    ]


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
                    plip_interactions BLOB NOT NULL,
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

                CREATE TABLE IF NOT EXISTS column_stats_cache (
                    source_id INTEGER NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
                    min_interface_size INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    alignment_length INTEGER NOT NULL DEFAULT 0,
                    unique_rows INTEGER NOT NULL DEFAULT 0,
                    conservation BLOB,
                    claimed_at REAL NOT NULL,
                    computed_at REAL,
                    cache_version INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(source_id, min_interface_size)
                );

                CREATE TABLE IF NOT EXISTS column_stats_scope (
                    source_id INTEGER NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
                    min_interface_size INTEGER NOT NULL,
                    partner_domain TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    residue_counts BLOB NOT NULL,
                    interface_counts BLOB NOT NULL,
                    surface_counts BLOB NOT NULL,
                    plip_counts BLOB NOT NULL,
                    PRIMARY KEY(source_id, min_interface_size, partner_domain)
                );
                """
            )
            interface_row_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(interface_rows)")
            }
            if "plip_interactions" not in interface_row_columns:
                connection.execute(
                    "ALTER TABLE interface_rows "
                    "ADD COLUMN plip_interactions BLOB NOT NULL DEFAULT X''"
                )
            column_stats_cache_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(column_stats_cache)")
            }
            if "cache_version" not in column_stats_cache_columns:
                connection.execute(
                    "ALTER TABLE column_stats_cache "
                    "ADD COLUMN cache_version INTEGER NOT NULL DEFAULT 1"
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
        ready = 0
        pending = 0
        failed = 0
        with timed_step("store", "sync interface store", files=len(paths)) as timer:
            for path in paths:
                try:
                    status = self.register_source_lazy(path)
                except Exception as exc:  # pragma: no cover
                    failed += 1
                    log_event("store", "failed to register interface source", file=path.name, error=exc)
                    continue
                if status == "ready":
                    ready += 1
                else:
                    pending += 1
            timer.set(ready=ready, pending=pending, imported=0, failed=failed)

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

    def register_source_lazy(self, path: Path) -> str:
        resolved, size_bytes, mtime_ns = self.source_signature(path)
        pfam_id = interface_file_pfam_id(path)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT source_id, size_bytes, mtime_ns, import_status, schema_version
                FROM sources
                WHERE path = ?
                """,
                (resolved,),
            ).fetchone()
            if existing is not None:
                source_id = int(existing[0])
                current_ready = (
                    int(existing[1]) == size_bytes
                    and int(existing[2]) == mtime_ns
                    and str(existing[3]) == "ready"
                    and int(existing[4]) == INTERFACE_STORE_SCHEMA_VERSION
                )
                if current_ready:
                    connection.commit()
                    return "ready"
                connection.execute("DELETE FROM clean_column_identity WHERE source_id = ?", (source_id,))
                connection.execute("DELETE FROM column_stats_scope WHERE source_id = ?", (source_id,))
                connection.execute("DELETE FROM column_stats_cache WHERE source_id = ?", (source_id,))
                connection.execute("DELETE FROM interface_rows WHERE source_id = ?", (source_id,))
                connection.execute(
                    """
                    UPDATE sources
                    SET filename = ?, pfam_id = ?, size_bytes = ?, mtime_ns = ?,
                        import_status = 'pending', imported_at = NULL,
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
                connection.commit()
                return "pending"
            connection.execute(
                """
                INSERT INTO sources (
                    path, filename, pfam_id, size_bytes, mtime_ns, import_status,
                    schema_version
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (resolved, path.name, pfam_id, size_bytes, mtime_ns, INTERFACE_STORE_SCHEMA_VERSION),
            )
            connection.commit()
            return "pending"

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
                            sqlite3.Binary(pack_uint_triples(row_payload.get("plip_interactions"))),
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
                    connection.execute("DELETE FROM column_stats_scope WHERE source_id = ?", (source_id,))
                    connection.execute("DELETE FROM column_stats_cache WHERE source_id = ?", (source_id,))
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
                        residue_contacts, plip_interactions, fragments_b
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def get_interface_summary(
        self,
        connection: sqlite3.Connection,
        source_id: int,
        where_sql: str,
        where_args: tuple[int, int],
        total_rows: int,
    ) -> dict[str, object]:
        dataset_domains = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT 1
                    FROM interface_rows
                    WHERE source_id = ? AND {where_sql}
                    GROUP BY protein_id, fragment_key
                )
                """,
                (source_id, *where_args),
            ).fetchone()[0]
        )
        domain_length_histogram: dict[int, int] = {}
        for row in connection.execute(
            f"""
            SELECT fragment_key
            FROM interface_rows
            WHERE source_id = ? AND {where_sql}
            GROUP BY protein_id, fragment_key
            """,
            (source_id, *where_args),
        ):
            domain_length = domain_length_from_fragment_key(row[0])
            if domain_length > 0:
                domain_length_histogram[domain_length] = (
                    domain_length_histogram.get(domain_length, 0) + 1
                )
        unique_interfaces: set[tuple[str, tuple[int, ...]]] = set()
        interface_size_histogram: dict[int, int] = {}
        pfam_row_coverage_histogram: dict[int, int] = {}
        plip_interaction_count_histogram: dict[int, int] = {}
        plip_type_counts = {bit: 0 for bit in (1, 2, 4, 8, 16, 32, 64, 128)}
        for row in connection.execute(
            f"""
            SELECT partner_domain, interface_msa_columns_a, interface_residues_a,
                   fragment_key, aligned_seq, plip_interactions
            FROM interface_rows
            WHERE source_id = ? AND {where_sql}
            """,
            (source_id, *where_args),
        ):
            domain_length = domain_length_from_fragment_key(row[3])
            coverage_percent = pfam_row_coverage_percent(domain_length, row[4])
            if coverage_percent > 0:
                pfam_row_coverage_histogram[coverage_percent] = (
                    pfam_row_coverage_histogram.get(coverage_percent, 0) + 1
                )
            columns = sorted(set(unpack_uints(row[1]) or unpack_uints(row[2])))
            interface_size = len(columns)
            if interface_size <= 0:
                continue
            unique_interfaces.add((str(row[0]), tuple(columns)))
            interface_size_histogram[interface_size] = (
                interface_size_histogram.get(interface_size, 0) + 1
            )
            plip_interactions = unpack_uint_triples(row[5])
            plip_count = len(plip_interactions)
            plip_interaction_count_histogram[plip_count] = (
                plip_interaction_count_histogram.get(plip_count, 0) + 1
            )
            for _main_residue, _partner_residue, mask in plip_interactions:
                for bit in plip_type_counts:
                    if int(mask) & bit:
                        plip_type_counts[bit] += 1
        return {
            "dataset_domains": dataset_domains,
            "dataset_interfaces": total_rows,
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

    def get_interface_summary_payload(
        self,
        path: Path,
        filter_settings: dict[str, object],
    ) -> dict[str, object]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        with timed_step("store", "load interface summary payload", file=path.name) as timer:
            with self.connect() as connection:
                filename, pfam_id, _alignment_length = self.source_summary(connection, source_id)
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
                interface_summary = self.get_interface_summary(
                    connection,
                    source_id,
                    where_sql,
                    where_args,
                    total_rows,
                )
            timer.set(
                total_rows=total_rows,
                partner_domains=len(partner_counts),
                alignment_length=filtered_alignment_length,
            )
            return {
                "file": filename,
                "pfam_id": pfam_id,
                "filter_settings": filter_settings,
                "alignment_length": filtered_alignment_length,
                "row_count": total_rows,
                "interface_partner_domains": list(partner_counts),
                "interface_partner_counts": partner_counts,
                "interface_summary": interface_summary,
            }

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
        include_summary: bool = True,
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
                interface_summary = (
                    self.get_interface_summary(
                        connection,
                        source_id,
                        where_sql,
                        where_args,
                        total_rows,
                    )
                    if include_summary
                    else None
                )
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
            if interface_summary is not None:
                response["interface_summary"] = interface_summary
            if include_clean_column_identity:
                column_statistics = self.get_column_statistics(path, filter_settings)
                response["clean_column_identity"] = column_statistics["conservation"]
                response["column_statistics"] = {
                    "cached": True,
                    "unique_rows": column_statistics["unique_rows"],
                    "alignment_length": column_statistics["alignment_length"],
                }
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
        limit_sql = "LIMIT -1" if row_limit is None else "LIMIT ?"
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
                "interface_residues_a": unpack_uints(row[6]),
                "surface_residue_ids_a": unpack_uints(row[7]),
                "interface_msa_columns_a": unpack_uints(row[8]),
                "surface_msa_columns_a": unpack_uints(row[9]),
            }
            for row in connection.execute(
                f"""
                SELECT interface_row_key, protein_id, fragment_key,
                       partner_fragment_key, partner_domain, aligned_seq,
                       interface_residues_a, surface_residue_ids_a,
                       interface_msa_columns_a, surface_msa_columns_a
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
        limit_sql = "LIMIT -1" if row_limit is None else "LIMIT ?"
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

    def get_histogram_targets(
        self,
        path: Path,
        filter_settings: dict[str, object],
        *,
        histogram_type: str,
        bin_start: int,
        bin_end: int,
        partner_domain: str = "",
    ) -> list[dict[str, object]]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        normalized_partner = str(partner_domain or "").strip()
        partner_sql = ""
        args: tuple[object, ...] = (source_id, *where_args)
        if normalized_partner and normalized_partner != "__all__":
            partner_sql = " AND partner_domain = ?"
            args = (*args, normalized_partner)
        targets: list[dict[str, object]] = []
        with timed_step(
            "store",
            "load histogram targets",
            file=path.name,
            histogram_type=histogram_type,
            bin_start=bin_start,
            bin_end=bin_end,
            partner=normalized_partner or "all",
        ) as timer:
            with self.connect() as connection:
                if histogram_type == "domain_length":
                    rows = connection.execute(
                        f"""
                        SELECT partner_domain, interface_row_key, fragment_key
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}{partner_sql}
                        ORDER BY row_order
                        """,
                        args,
                    )
                    for row in rows:
                        value = domain_length_from_fragment_key(row[2])
                        if value < bin_start or value > bin_end:
                            continue
                        targets.append(
                            {
                                "row_key": str(row[1]),
                                "partner_domain": str(row[0]),
                                "value": value,
                            }
                        )
                elif histogram_type == "interface_size":
                    rows = connection.execute(
                        f"""
                        SELECT partner_domain, interface_row_key, interface_msa_columns_a
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}{partner_sql}
                        ORDER BY row_order
                        """,
                        args,
                    )
                    for row in rows:
                        value = len(set(unpack_uints(row[2])))
                        if value < bin_start or value > bin_end:
                            continue
                        targets.append(
                            {
                                "row_key": str(row[1]),
                                "partner_domain": str(row[0]),
                                "value": value,
                            }
                        )
                elif histogram_type == "pfam_row_coverage":
                    rows = connection.execute(
                        f"""
                        SELECT partner_domain, interface_row_key, fragment_key, aligned_seq
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}{partner_sql}
                        ORDER BY row_order
                        """,
                        args,
                    )
                    for row in rows:
                        domain_length = domain_length_from_fragment_key(row[2])
                        value = pfam_row_coverage_percent(domain_length, row[3])
                        if value < bin_start or value > bin_end:
                            continue
                        targets.append(
                            {
                                "row_key": str(row[1]),
                                "partner_domain": str(row[0]),
                                "value": value,
                            }
                        )
                elif histogram_type == "plip_interaction_count":
                    rows = connection.execute(
                        f"""
                        SELECT partner_domain, interface_row_key, plip_interactions
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}{partner_sql}
                        ORDER BY row_order
                        """,
                        args,
                    )
                    for row in rows:
                        value = len(unpack_uint_triples(row[2]))
                        if value < bin_start or value > bin_end:
                            continue
                        targets.append(
                            {
                                "row_key": str(row[1]),
                                "partner_domain": str(row[0]),
                                "value": value,
                            }
                        )
                else:
                    raise ValueError(
                        "histogram type must be 'interface_size', 'domain_length', "
                        "'pfam_row_coverage', or 'plip_interaction_count'"
                    )
            timer.set(targets=len(targets))
        return targets

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
                    SELECT partner_domain, interface_row_key, interface_msa_columns_a, aligned_seq
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
                        "aligned_seq": str(row[3] or ""),
                    }
                    row_count += 1
            timer.set(rows=row_count, partner_domains=len(payload))
            return payload

    def get_filtered_alignment_length(
        self,
        path: Path,
        filter_settings: dict[str, object],
    ) -> int:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        with timed_step("store", "load filtered alignment length", file=path.name) as timer:
            with self.connect() as connection:
                alignment_length = int(
                    connection.execute(
                        f"""
                        SELECT COALESCE(MAX(LENGTH(aligned_seq)), 0)
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}
                        """,
                        (source_id, *where_args),
                    ).fetchone()[0]
                )
            timer.set(alignment_length=alignment_length)
            return alignment_length

    def get_representative_candidates(
        self,
        path: Path,
        filter_settings: dict[str, object],
    ) -> tuple[list[dict[str, object]], int]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        with timed_step("store", "load representative candidates", file=path.name) as timer:
            candidates: list[dict[str, object]] = []
            with self.connect() as connection:
                alignment_length = int(
                    connection.execute(
                        f"""
                        SELECT COALESCE(MAX(LENGTH(aligned_seq)), 0)
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}
                        """,
                        (source_id, *where_args),
                    ).fetchone()[0]
                )
                for row in connection.execute(
                    f"""
                    SELECT interface_row_key, protein_id, fragment_key,
                           partner_fragment_key, partner_domain, aligned_seq,
                           interface_residues_a, surface_residue_ids_a,
                           interface_msa_columns_a, surface_msa_columns_a
                    FROM interface_rows
                    WHERE source_id = ? AND {where_sql}
                    ORDER BY row_order
                    """,
                    (source_id, *where_args),
                ):
                    candidates.append(
                        {
                            "interface_row_key": str(row[0]),
                            "protein_id": str(row[1]),
                            "fragment_key": str(row[2]),
                            "partner_fragment_key": str(row[3]),
                            "partner_domain": str(row[4]),
                            "aligned_sequence": str(row[5] or ""),
                            "interface_residues_a": unpack_uints(row[6]),
                            "surface_residue_ids_a": unpack_uints(row[7]),
                            "interface_msa_columns_a": unpack_uints(row[8]),
                            "surface_msa_columns_a": unpack_uints(row[9]),
                        }
                    )
            timer.set(rows=len(candidates), alignment_length=alignment_length)
            return candidates, alignment_length

    def get_representative_candidate_keys(
        self,
        path: Path,
        filter_settings: dict[str, object],
        partner_filter: str = "__all__",
    ) -> tuple[list[dict[str, object]], int]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        partner_sql = "" if partner_filter == "__all__" else "AND partner_domain = ?"
        args: tuple[object, ...] = (source_id, *where_args)
        if partner_filter != "__all__":
            args = (*args, partner_filter)
        with timed_step("store", "load representative candidate keys", file=path.name) as timer:
            with self.connect() as connection:
                alignment_length = int(
                    connection.execute(
                        f"""
                        SELECT COALESCE(MAX(LENGTH(aligned_seq)), 0)
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}
                          {partner_sql}
                        """,
                        args,
                    ).fetchone()[0]
                )
                candidates = [
                    {
                        "interface_row_key": str(row[0]),
                        "partner_domain": str(row[1]),
                    }
                    for row in connection.execute(
                        f"""
                        SELECT interface_row_key, partner_domain
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}
                          {partner_sql}
                        ORDER BY row_order
                        """,
                        args,
                    )
                ]
            timer.set(rows=len(candidates), alignment_length=alignment_length, partner=partner_filter)
            return candidates, alignment_length

    def get_representative_candidates_by_keys(
        self,
        path: Path,
        filter_settings: dict[str, object],
        keys: list[tuple[str, str]],
    ) -> list[dict[str, object]]:
        if not keys:
            return []
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        where_sql, where_args = self.filtered_where(min_size)
        unique_keys: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for row_key, partner_domain in keys:
            key = (str(row_key), str(partner_domain))
            if not key[0] or not key[1] or key in seen:
                continue
            seen.add(key)
            unique_keys.append(key)
        if not unique_keys:
            return []
        with timed_step("store", "load sampled representative candidates", file=path.name, keys=len(unique_keys)) as timer:
            candidates_by_key: dict[tuple[str, str], dict[str, object]] = {}
            with self.connect() as connection:
                for offset in range(0, len(unique_keys), 400):
                    chunk = unique_keys[offset:offset + 400]
                    pair_sql = " OR ".join(
                        "(partner_domain = ? AND interface_row_key = ?)"
                        for _row_key, _partner_domain in chunk
                    )
                    pair_args: list[object] = []
                    for row_key, partner_domain in chunk:
                        pair_args.extend([partner_domain, row_key])
                    for row in connection.execute(
                        f"""
                        SELECT interface_row_key, protein_id, fragment_key,
                               partner_fragment_key, partner_domain, aligned_seq,
                               interface_residues_a, surface_residue_ids_a,
                               interface_msa_columns_a, surface_msa_columns_a
                        FROM interface_rows
                        WHERE source_id = ? AND {where_sql}
                          AND ({pair_sql})
                        """,
                        (source_id, *where_args, *pair_args),
                    ):
                        key = (str(row[0]), str(row[4]))
                        candidates_by_key[key] = {
                            "interface_row_key": str(row[0]),
                            "protein_id": str(row[1]),
                            "fragment_key": str(row[2]),
                            "partner_fragment_key": str(row[3]),
                            "partner_domain": str(row[4]),
                            "aligned_sequence": str(row[5] or ""),
                            "interface_residues_a": unpack_uints(row[6]),
                            "surface_residue_ids_a": unpack_uints(row[7]),
                            "interface_msa_columns_a": unpack_uints(row[8]),
                            "surface_msa_columns_a": unpack_uints(row[9]),
                        }
            candidates = [
                candidates_by_key[key]
                for key in unique_keys
                if key in candidates_by_key
            ]
            timer.set(rows=len(candidates))
            return candidates

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
                           residue_contacts, plip_interactions, fragments_b, aligned_seq,
                           interface_msa_columns_a, surface_msa_columns_a
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
                        "plip_interactions": unpack_uint_triples(row[7]),
                        "fragments_b": unpack_uint_pairs(row[8]),
                        "aligned_sequence": str(row[9] or ""),
                        "interface_msa_columns_a": unpack_uints(row[10]),
                        "surface_msa_columns_a": unpack_uints(row[11]),
                    }
            timer.set(rows=sum(len(rows) for rows in payload.values()))
            return payload

    def get_plip_column_distribution(
        self,
        path: Path,
        filter_settings: dict[str, object],
        column_index: int,
        partner_filter: str = "__all__",
    ) -> dict[str, object]:
        min_size = filter_min_interface_size(filter_settings)
        cache = self.get_column_statistics(path, filter_settings)
        source_id = int(cache["source_id"])
        alignment_length = int(cache["alignment_length"])
        if column_index >= alignment_length:
            raise ValueError(f"column {column_index} is outside alignment length {alignment_length}")
        with self.connect() as connection:
            scope = connection.execute(
                """
                SELECT row_count, residue_counts, interface_counts, surface_counts, plip_counts
                FROM column_stats_scope
                WHERE source_id = ? AND min_interface_size = ? AND partner_domain = ?
                """,
                (source_id, min_size, partner_filter),
            ).fetchone()
        if scope is None:
            total_rows = 0
            residue_counts = [0] * 26
            interface_count = surface_count = 0
            plip = {}
        else:
            total_rows = int(scope[0])
            residue_counts = self._unpack_count_matrix_column(scope[1], alignment_length, column_index)
            interface_count = self._unpack_count_vector_value(scope[2], column_index)
            surface_count = self._unpack_count_vector_value(scope[3], column_index)
            plip_payload = json.loads(zlib.decompress(bytes(scope[4])).decode("utf-8"))
            plip = plip_payload.get(str(column_index), {})
        letter_count = sum(residue_counts)
        gap_count = max(0, total_rows - letter_count)
        core_count = max(0, letter_count - interface_count - surface_count)
        type_counts = plip.get("types", {})
        row_mask_counts = plip.get("row_masks", {})
        interaction_count = int(plip.get("interactions", 0))
        interaction_row_count = int(plip.get("rows", 0))
        return {
            "file": path.name,
            "filter_settings": filter_settings,
            "column_index": int(column_index),
            "partner": partner_filter,
            "row_count": total_rows,
            "interaction_row_count": interaction_row_count,
            "interaction_count": interaction_count,
            "type_counts": type_counts,
            "row_mask_counts": row_mask_counts,
            "residue_counts": {
                chr(ord("A") + index): int(count)
                for index, count in enumerate(residue_counts)
                if count > 0
            },
            "state_counts": {
                "interface": interface_count,
                "surface": surface_count,
                "core": core_count,
                "gap": gap_count,
            },
        }

    @staticmethod
    def _pack_count_array(values: object) -> bytes:
        import numpy as np

        return zlib.compress(np.asarray(values, dtype=np.uint32).tobytes(), level=6)

    @staticmethod
    def _unpack_count_matrix_column(blob: object, alignment_length: int, column_index: int) -> list[int]:
        import numpy as np

        values = np.frombuffer(zlib.decompress(bytes(blob)), dtype=np.uint32)
        matrix = values.reshape((alignment_length, 26))
        return matrix[column_index].astype(int).tolist()

    @staticmethod
    def _unpack_count_vector_value(blob: object, column_index: int) -> int:
        import numpy as np

        values = np.frombuffer(zlib.decompress(bytes(blob)), dtype=np.uint32)
        return int(values[column_index])

    def get_column_statistics(
        self,
        path: Path,
        filter_settings: dict[str, object],
    ) -> dict[str, object]:
        source_id = self.ensure_source_ready(path)
        min_size = filter_min_interface_size(filter_settings)
        while True:
            with self.connect() as connection:
                cached = connection.execute(
                    """
                    SELECT status, alignment_length, unique_rows, conservation, claimed_at,
                           cache_version
                    FROM column_stats_cache
                    WHERE source_id = ? AND min_interface_size = ?
                    """,
                    (source_id, min_size),
                ).fetchone()
                if (
                    cached is not None
                    and str(cached[0]) == "ready"
                    and int(cached[5]) == COLUMN_STATS_CACHE_VERSION
                ):
                    return {
                        "source_id": source_id,
                        "alignment_length": int(cached[1]),
                        "unique_rows": int(cached[2]),
                        "conservation": unpack_uint16(cached[3]),
                    }
                now = time.time()
                if cached is None:
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO column_stats_cache (
                            source_id, min_interface_size, status, claimed_at, cache_version
                        ) VALUES (?, ?, 'building', ?, ?)
                        """,
                        (source_id, min_size, now, COLUMN_STATS_CACHE_VERSION),
                    )
                    owns_build = cursor.rowcount == 1
                elif int(cached[5]) != COLUMN_STATS_CACHE_VERSION:
                    cursor = connection.execute(
                        """
                        UPDATE column_stats_cache
                        SET status = 'building', claimed_at = ?, cache_version = ?
                        WHERE source_id = ? AND min_interface_size = ?
                          AND cache_version = ?
                        """,
                        (
                            now, COLUMN_STATS_CACHE_VERSION, source_id, min_size,
                            int(cached[5]),
                        ),
                    )
                    owns_build = cursor.rowcount == 1
                    if owns_build:
                        connection.execute(
                            "DELETE FROM column_stats_scope "
                            "WHERE source_id = ? AND min_interface_size = ?",
                            (source_id, min_size),
                        )
                elif now - float(cached[4]) > 900:
                    cursor = connection.execute(
                        """
                        UPDATE column_stats_cache SET claimed_at = ?
                        WHERE source_id = ? AND min_interface_size = ?
                          AND status = 'building' AND claimed_at = ?
                        """,
                        (now, source_id, min_size, float(cached[4])),
                    )
                    owns_build = cursor.rowcount == 1
                else:
                    owns_build = False
            if owns_build:
                break
            time.sleep(0.1)
        try:
            return self._build_column_statistics(path, source_id, min_size)
        except BaseException:
            with self.connect() as connection:
                connection.execute(
                    "DELETE FROM column_stats_cache WHERE source_id = ? AND min_interface_size = ?",
                    (source_id, min_size),
                )
                connection.execute(
                    "DELETE FROM column_stats_scope WHERE source_id = ? AND min_interface_size = ?",
                    (source_id, min_size),
                )
            raise

    def _build_column_statistics(self, path: Path, source_id: int, min_size: int) -> dict[str, object]:
        import numpy as np

        where_sql, where_args = self.filtered_where(min_size)
        with self.connect() as connection:
            alignment_length = int(connection.execute(
                f"SELECT COALESCE(MAX(LENGTH(aligned_seq)), 0) FROM interface_rows "
                f"WHERE source_id = ? AND {where_sql}",
                (source_id, *where_args),
            ).fetchone()[0])
        conservation_counts = np.zeros((alignment_length, 26), dtype=np.int64)
        global_residue_counts = np.zeros((alignment_length, 26), dtype=np.uint64)
        global_interface = np.zeros(alignment_length, dtype=np.uint64)
        global_surface = np.zeros(alignment_length, dtype=np.uint64)
        global_plip: dict[str, dict[str, object]] = {}
        global_rows = 0
        unique_rows = 0
        seen_row_keys: set[str] = set()
        unique_batch: list[tuple[str, tuple[tuple[int, int], ...]]] = []

        def flush_unique() -> None:
            nonlocal unique_batch
            if unique_batch:
                conservation_counts[:] += count_clean_identity_batch(unique_batch, alignment_length)
                unique_batch = []

        with timed_step("store", "build persisted column statistics", file=path.name, min_size=min_size) as timer:
            with self.connect() as connection:
                rows = connection.execute(
                    f"""
                    SELECT partner_domain, interface_row_key, fragment_key, aligned_seq,
                           interface_msa_columns_a, surface_msa_columns_a,
                           interface_residues_a, surface_residue_ids_a, plip_interactions
                    FROM interface_rows
                    WHERE source_id = ? AND {where_sql}
                    ORDER BY partner_domain, row_order
                    """,
                    (source_id, *where_args),
                )
                current_partner: str | None = None
                partner_batch: list[tuple[str, tuple[tuple[int, int], ...]]] = []
                partner_interface = np.zeros(alignment_length, dtype=np.uint64)
                partner_surface = np.zeros(alignment_length, dtype=np.uint64)
                partner_plip: dict[str, dict[str, object]] = {}
                partner_rows = 0

                def add_plip(target: dict[str, dict[str, object]], column: int, mask: int) -> None:
                    entry = target.setdefault(
                        str(column),
                        {"rows": 0, "interactions": 0, "types": {}, "row_masks": {}},
                    )
                    entry["interactions"] = int(entry["interactions"]) + 1
                    types = entry["types"]
                    for bit in (1, 2, 4, 8, 16, 32, 64, 128):
                        if mask & bit:
                            types[str(bit)] = int(types.get(str(bit), 0)) + 1

                def add_plip_row_masks(
                    target: dict[str, dict[str, object]],
                    row_masks: dict[int, int],
                ) -> None:
                    for column, mask in row_masks.items():
                        entry = target[str(column)]
                        entry["rows"] = int(entry["rows"]) + 1
                        masks = entry["row_masks"]
                        masks[str(mask)] = int(masks.get(str(mask), 0)) + 1

                def flush_partner() -> None:
                    nonlocal partner_batch, partner_rows
                    if current_partner is None:
                        return
                    counts = np.zeros((alignment_length, 26), dtype=np.int64)
                    for start in range(0, len(partner_batch), CLEAN_COLUMN_IDENTITY_BATCH_SIZE):
                        counts += count_clean_identity_batch(
                            partner_batch[start:start + CLEAN_COLUMN_IDENTITY_BATCH_SIZE], alignment_length
                        )
                    global_residue_counts[:] += counts.astype(np.uint64)
                    global_interface[:] += partner_interface
                    global_surface[:] += partner_surface
                    with self.connect() as write_connection:
                        write_connection.execute(
                            """
                            INSERT OR REPLACE INTO column_stats_scope (
                                source_id, min_interface_size, partner_domain, row_count,
                                residue_counts, interface_counts, surface_counts, plip_counts
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                source_id, min_size, current_partner, partner_rows,
                                sqlite3.Binary(self._pack_count_array(counts)),
                                sqlite3.Binary(self._pack_count_array(partner_interface)),
                                sqlite3.Binary(self._pack_count_array(partner_surface)),
                                sqlite3.Binary(zlib.compress(json.dumps(partner_plip, separators=(",", ":")).encode("utf-8"))),
                            ),
                        )
                    partner_batch = []
                    partner_rows = 0

                for row in rows:
                    partner = str(row[0])
                    if current_partner is not None and partner != current_partner:
                        flush_partner()
                        partner_interface.fill(0)
                        partner_surface.fill(0)
                        partner_plip.clear()
                    current_partner = partner
                    row_key = str(row[1])
                    ranges = tuple(fragment_ranges(str(row[2])))
                    sequence = str(row[3] or "")
                    partner_batch.append((sequence, ranges))
                    partner_rows += 1
                    global_rows += 1
                    if row_key not in seen_row_keys:
                        seen_row_keys.add(row_key)
                        unique_rows += 1
                        unique_batch.append((sequence, ranges))
                        if len(unique_batch) >= CLEAN_COLUMN_IDENTITY_BATCH_SIZE:
                            flush_unique()
                    interface_columns = set(unpack_uints(row[4]))
                    surface_columns = set(unpack_uints(row[5])) - interface_columns
                    for column in interface_columns:
                        if column < alignment_length:
                            partner_interface[column] += 1
                    for column in surface_columns:
                        if column < alignment_length:
                            partner_surface[column] += 1
                    residue_to_column = dict(zip(unpack_uints(row[6]), unpack_uints(row[4])))
                    residue_to_column.update(dict(zip(unpack_uints(row[7]), unpack_uints(row[5]))))
                    row_masks: dict[int, int] = {}
                    for main_residue, _partner_residue, mask in unpack_uint_triples(row[8]):
                        column = residue_to_column.get(main_residue)
                        if column is None or column >= alignment_length:
                            continue
                        add_plip(partner_plip, column, mask)
                        add_plip(global_plip, column, mask)
                        row_masks[column] = row_masks.get(column, 0) | mask
                    add_plip_row_masks(partner_plip, row_masks)
                    add_plip_row_masks(global_plip, row_masks)
                flush_partner()
            flush_unique()
            if unique_rows:
                conservation = ((conservation_counts.max(axis=1) * 100) // unique_rows).astype(int).tolist()
            else:
                conservation = [0] * alignment_length
            with self.connect() as connection:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO column_stats_scope (
                        source_id, min_interface_size, partner_domain, row_count,
                        residue_counts, interface_counts, surface_counts, plip_counts
                    ) VALUES (?, ?, '__all__', ?, ?, ?, ?, ?)
                    """,
                    (
                        source_id, min_size, global_rows,
                        sqlite3.Binary(self._pack_count_array(global_residue_counts)),
                        sqlite3.Binary(self._pack_count_array(global_interface)),
                        sqlite3.Binary(self._pack_count_array(global_surface)),
                        sqlite3.Binary(zlib.compress(json.dumps(global_plip, separators=(",", ":")).encode("utf-8"))),
                    ),
                )
                connection.execute(
                    """
                    UPDATE column_stats_cache
                    SET status = 'ready', alignment_length = ?, unique_rows = ?,
                        conservation = ?, computed_at = ?, cache_version = ?
                    WHERE source_id = ? AND min_interface_size = ?
                    """,
                    (
                        alignment_length, unique_rows, sqlite3.Binary(pack_uint16(conservation)),
                        time.time(), COLUMN_STATS_CACHE_VERSION, source_id, min_size,
                    ),
                )
            timer.set(columns=alignment_length, rows=global_rows, unique_rows=unique_rows)
        return {
            "source_id": source_id,
            "alignment_length": alignment_length,
            "unique_rows": unique_rows,
            "conservation": conservation,
        }

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
