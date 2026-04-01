from __future__ import annotations

import argparse
import csv
import math
import subprocess
import sys
import tempfile
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np


TOOL_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = TOOL_DIR / "Cargo.toml"
DEFAULT_BINARY_NAME = "interface_distance"
UINT16_SCALE = 65535.0


@dataclass(frozen=True)
class MetadataRow:
    index: int
    partner_domain: str
    row_key: str
    column_count: int

    @property
    def label(self) -> str:
        return f"{self.partner_domain}:{self.row_key}"


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
    binary_path = ensure_rust_binary()
    if output_file is not None:
        resolved_metadata_out = metadata_out or default_metadata_path(output_file)
        compute_to_file(
            binary_path=binary_path,
            input_file=input_file,
            output_file=output_file,
            metadata_out=resolved_metadata_out,
        )
        return load_distance_matrix(output_file)

    with tempfile.TemporaryDirectory(prefix="interface_distance_compute_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        temp_output_file = tmp_path / f"{input_file.stem}.bin"
        resolved_metadata_out = metadata_out or default_metadata_path(temp_output_file)
        compute_to_file(
            binary_path=binary_path,
            input_file=input_file,
            output_file=temp_output_file,
            metadata_out=resolved_metadata_out,
        )
        return load_distance_matrix(temp_output_file)


def rust_binary_path() -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    return TOOL_DIR / "target" / "release" / f"{DEFAULT_BINARY_NAME}{suffix}"


def ensure_rust_binary() -> Path:
    binary_path = rust_binary_path()
    if binary_is_current(binary_path):
        return binary_path

    build_command = ["cargo", "build", "--manifest-path", str(DEFAULT_MANIFEST_PATH), "--release"]
    subprocess.run(build_command, check=True)
    return binary_path


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


def binary_is_current(binary_path: Path) -> bool:
    if not binary_path.exists():
        return False

    binary_mtime = binary_path.stat().st_mtime
    source_files = [
        DEFAULT_MANIFEST_PATH,
        TOOL_DIR / "Cargo.lock",
        TOOL_DIR / "src" / "main.rs",
    ]
    return all(path.exists() and path.stat().st_mtime <= binary_mtime for path in source_files)


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
