from __future__ import annotations

import argparse
import csv
import json
import math
import platform
import subprocess
import sys
import tempfile
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np


TOOL_DIR = Path(__file__).resolve().parent
DEFAULT_BINARY_NAME = "interface_distance"
RUNTIME_BINARY_DIR = TOOL_DIR / "bin"
UINT16_SCALE = 65535.0


@dataclass(frozen=True)
class RuntimeBinaryStatus:
    available: bool
    binary_path: Path | None
    platform_key: str | None
    reason: str = ""


_RUNTIME_BINARY_STATUS: RuntimeBinaryStatus | None = None


@dataclass(frozen=True)
class MetadataRow:
    index: int
    partner_domain: str
    row_key: str
    column_count: int

    @property
    def label(self) -> str:
        return f"{self.partner_domain}:{self.row_key}"


@dataclass(frozen=True)
class InterfaceEntry:
    partner_domain: str
    row_key: str
    columns: tuple[int, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute and decode interface distance matrices."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    compute_parser = subparsers.add_parser(
        "compute",
        help="Compute an overlap-coefficient distance vector from an interface JSON file.",
    )
    compute_parser.add_argument("--input-file", type=Path, required=True)
    compute_parser.add_argument("--output-file", type=Path, required=True)
    compute_parser.add_argument("--metadata-out", type=Path, default=None)

    decode_parser = subparsers.add_parser(
        "decode",
        help="Decode a raw uint16 condensed vector into a full distance matrix.",
    )
    decode_parser.add_argument("--input-file", type=Path, required=True)
    decode_parser.add_argument("--output-file", type=Path, required=True)
    decode_parser.add_argument("--metadata-file", type=Path, default=None)
    decode_parser.add_argument(
        "--delimiter",
        choices=[",", "\t"],
        default=",",
    )
    decode_parser.add_argument(
        "--include-labels",
        action=argparse.BooleanOptionalAction,
        default=True,
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.command == "compute":
        metadata_out = args.metadata_out or default_metadata_path(args.output_file)
        compute_interface_distance_matrix(
            input_file=args.input_file,
            output_file=args.output_file,
            metadata_out=metadata_out,
        )
        print(f"wrote {args.output_file}")
        print(f"wrote {metadata_out}")
        return 0

    metadata_file = args.metadata_file
    if metadata_file is None:
        candidate = default_metadata_path(args.input_file)
        if candidate.exists():
            metadata_file = candidate

    decode_binary_file(
        input_file=args.input_file,
        output_file=args.output_file,
        metadata_file=metadata_file,
        delimiter=args.delimiter,
        include_labels=args.include_labels,
    )
    print(f"wrote {args.output_file}")
    if metadata_file is not None:
        print(f"used metadata {metadata_file}")
    return 0


def compute_interface_distance_matrix(
    *,
    input_file: Path,
    output_file: Path | None = None,
    metadata_out: Path | None = None,
) -> np.ndarray:
    runtime_binary = validate_runtime_binary()
    if output_file is not None:
        resolved_metadata_out = metadata_out or default_metadata_path(output_file)
        if runtime_binary.available and runtime_binary.binary_path is not None:
            compute_to_file(
                binary_path=runtime_binary.binary_path,
                input_file=input_file,
                output_file=output_file,
                metadata_out=resolved_metadata_out,
            )
        else:
            compute_to_file_python(
                input_file=input_file,
                output_file=output_file,
                metadata_out=resolved_metadata_out,
            )
        return load_distance_matrix(output_file)

    with tempfile.TemporaryDirectory(prefix="interface_distance_compute_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        temp_output_file = tmp_path / f"{input_file.stem}.bin"
        resolved_metadata_out = metadata_out or default_metadata_path(temp_output_file)
        if runtime_binary.available and runtime_binary.binary_path is not None:
            compute_to_file(
                binary_path=runtime_binary.binary_path,
                input_file=input_file,
                output_file=temp_output_file,
                metadata_out=resolved_metadata_out,
            )
        else:
            compute_to_file_python(
                input_file=input_file,
                output_file=temp_output_file,
                metadata_out=resolved_metadata_out,
            )
        return load_distance_matrix(temp_output_file)


def validate_runtime_binary(*, refresh: bool = False) -> RuntimeBinaryStatus:
    global _RUNTIME_BINARY_STATUS
    if refresh or _RUNTIME_BINARY_STATUS is None:
        _RUNTIME_BINARY_STATUS = probe_runtime_binary()
    return _RUNTIME_BINARY_STATUS


def probe_runtime_binary() -> RuntimeBinaryStatus:
    platform_key = current_platform_key()
    if platform_key is None:
        return RuntimeBinaryStatus(
            available=False,
            binary_path=None,
            platform_key=None,
            reason=(
                f"no bundled interface_distance binary is available for "
                f"{sys.platform}/{platform.machine()}"
            ),
        )

    binary_path = bundled_binary_path(platform_key)
    if not binary_path.exists():
        return RuntimeBinaryStatus(
            available=False,
            binary_path=None,
            platform_key=platform_key,
            reason=f"missing bundled interface_distance binary at {binary_path}",
        )

    try:
        result = subprocess.run(
            [str(binary_path), "--help"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        return RuntimeBinaryStatus(
            available=False,
            binary_path=binary_path,
            platform_key=platform_key,
            reason=f"bundled interface_distance binary is not runnable: {exc}",
        )

    if result.returncode != 0:
        return RuntimeBinaryStatus(
            available=False,
            binary_path=binary_path,
            platform_key=platform_key,
            reason=(
                f"bundled interface_distance binary exited with status "
                f"{result.returncode} during startup validation"
            ),
        )

    return RuntimeBinaryStatus(
        available=True,
        binary_path=binary_path,
        platform_key=platform_key,
    )


def current_platform_key() -> str | None:
    machine = platform.machine().lower()
    if sys.platform.startswith("linux") and machine in {"x86_64", "amd64"}:
        return "linux-x86_64"
    if sys.platform == "darwin" and machine in {"x86_64", "amd64"}:
        return "macos-x86_64"
    if sys.platform == "darwin" and machine in {"arm64", "aarch64"}:
        return "macos-arm64"
    if sys.platform == "win32" and machine in {"x86_64", "amd64"}:
        return "windows-x86_64"
    return None


def bundled_binary_path(platform_key: str) -> Path:
    suffix = ".exe" if platform_key.startswith("windows-") else ""
    return RUNTIME_BINARY_DIR / platform_key / f"{DEFAULT_BINARY_NAME}{suffix}"


def compute_to_file(
    *,
    binary_path: Path,
    input_file: Path,
    output_file: Path,
    metadata_out: Path,
) -> None:
    run_command = [
        str(binary_path),
        "--input-file",
        str(input_file),
        "--output-file",
        str(output_file),
        "--metadata-out",
        str(metadata_out),
    ]
    subprocess.run(run_command, check=True)


def compute_to_file_python(
    *,
    input_file: Path,
    output_file: Path,
    metadata_out: Path,
) -> None:
    entries = load_interface_entries(input_file)
    if len(entries) < 2:
        raise ValueError(
            f"need at least two non-empty interface_msa_columns_a entries, found {len(entries)}"
        )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("wb") as handle:
        for left_index in range(len(entries) - 1):
            left = entries[left_index].columns
            for right_index in range(left_index + 1, len(entries)):
                right = entries[right_index].columns
                handle.write(quantize_unit_interval(overlap_distance(left, right)).to_bytes(2, "little"))
    write_metadata(entries, metadata_out)


def load_interface_entries(input_file: Path) -> list[InterfaceEntry]:
    with input_file.open("r", encoding="utf-8") as handle:
        parsed = json.load(handle)
    if not isinstance(parsed, dict):
        raise ValueError(f"expected top-level object in {input_file}")

    entries: list[InterfaceEntry] = []
    for partner_domain in sorted(parsed):
        rows = parsed.get(partner_domain)
        if not isinstance(rows, dict):
            continue
        for row_key in sorted(rows):
            payload = rows.get(row_key)
            if not isinstance(payload, dict):
                continue
            raw_columns = payload.get("interface_msa_columns_a", [])
            columns = tuple(sorted({int(column) for column in raw_columns}))
            if not columns:
                continue
            entries.append(
                InterfaceEntry(
                    partner_domain=str(partner_domain),
                    row_key=str(row_key),
                    columns=columns,
                )
            )
    return entries


def write_metadata(entries: list[InterfaceEntry], metadata_out: Path) -> None:
    metadata_out.parent.mkdir(parents=True, exist_ok=True)
    with metadata_out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["index", "partner_domain", "row_key", "column_count"])
        for index, entry in enumerate(entries):
            writer.writerow([index, entry.partner_domain, entry.row_key, len(entry.columns)])


def overlap_distance(left: tuple[int, ...], right: tuple[int, ...]) -> float:
    if not left and not right:
        return 0.0
    if not left or not right:
        return 1.0

    left_index = 0
    right_index = 0
    intersection = 0
    while left_index < len(left) and right_index < len(right):
        left_value = left[left_index]
        right_value = right[right_index]
        if left_value < right_value:
            left_index += 1
        elif left_value > right_value:
            right_index += 1
        else:
            intersection += 1
            left_index += 1
            right_index += 1
    return 1.0 - (intersection / min(len(left), len(right)))


def quantize_unit_interval(value: float) -> int:
    clamped = min(max(value, 0.0), 1.0)
    return round(clamped * 65535.0)


def decode_binary_file(
    *,
    input_file: Path,
    output_file: Path,
    metadata_file: Path | None,
    delimiter: str,
    include_labels: bool,
) -> None:
    values = load_u16_vector(input_file)
    entry_count = infer_entry_count(len(values))
    metadata_rows = None

    if metadata_file is not None:
        metadata_rows = load_metadata(metadata_file)
        if len(metadata_rows) != entry_count:
            raise ValueError(
                f"metadata row count {len(metadata_rows)} does not match inferred "
                f"entry count {entry_count}"
            )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=delimiter)
        if include_labels and metadata_rows is not None:
            writer.writerow([""] + [row.label for row in metadata_rows])

        for left_index in range(entry_count):
            row_values = list(
                iter_decoded_row(values=values, entry_count=entry_count, row_index=left_index)
            )
            formatted = [f"{value:.6f}" for value in row_values]
            if include_labels and metadata_rows is not None:
                writer.writerow([metadata_rows[left_index].label] + formatted)
            else:
                writer.writerow(formatted)


def load_distance_matrix(input_file: Path) -> np.ndarray:
    values = load_u16_vector(input_file)
    entry_count = infer_entry_count(len(values))
    matrix = np.zeros((entry_count, entry_count), dtype=np.float64)

    for row_index in range(entry_count):
        matrix[row_index, :] = list(
            iter_decoded_row(values=values, entry_count=entry_count, row_index=row_index)
        )

    return matrix


def load_u16_vector(input_file: Path) -> array:
    data = input_file.read_bytes()
    if len(data) % 2 != 0:
        raise ValueError(f"invalid uint16 binary length: {len(data)}")

    values = array("H")
    values.frombytes(data)
    if sys.byteorder != "little":
        values.byteswap()
    return values


def infer_entry_count(vector_length: int) -> int:
    discriminant = 1 + (8 * vector_length)
    root = math.isqrt(discriminant)
    if root * root != discriminant:
        raise ValueError(f"invalid condensed vector length: {vector_length}")
    entry_count = (1 + root) // 2
    if condensed_size(entry_count) != vector_length:
        raise ValueError(f"invalid condensed vector length: {vector_length}")
    return entry_count


def condensed_size(entry_count: int) -> int:
    return (entry_count * (entry_count - 1)) // 2


def condensed_index(entry_count: int, left_index: int, right_index: int) -> int:
    return (
        entry_count * left_index
        - (left_index * (left_index + 1)) // 2
        + (right_index - left_index - 1)
    )


def iter_decoded_row(
    *,
    values: array,
    entry_count: int,
    row_index: int,
) -> Iterable[float]:
    for column_index in range(entry_count):
        if row_index == column_index:
            yield 0.0
            continue
        if row_index < column_index:
            idx = condensed_index(entry_count, row_index, column_index)
        else:
            idx = condensed_index(entry_count, column_index, row_index)
        yield values[idx] / UINT16_SCALE


def load_metadata(metadata_file: Path) -> list[MetadataRow]:
    with metadata_file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = [
            MetadataRow(
                index=int(row["index"]),
                partner_domain=row["partner_domain"],
                row_key=row["row_key"],
                column_count=int(row["column_count"]),
            )
            for row in reader
        ]

    for expected_index, row in enumerate(rows):
        if row.index != expected_index:
            raise ValueError(
                f"metadata index mismatch at row {expected_index}: found {row.index}"
            )

    return rows


def default_metadata_path(output_file: Path) -> Path:
    return output_file.parent / f"{output_file.stem}.rows.tsv"
