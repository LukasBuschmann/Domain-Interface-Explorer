from __future__ import annotations

import hashlib
import json
import mmap
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from .config import (
    DEFAULT_HMMER_BIN_DIR,
    DEFAULT_PFAM_HMM_PATH,
    DEFAULT_SEQUENCE_BY_DOMAIN_DIR,
    PROJECT_HMMER_BIN_DIR,
)
from .structure_service import fragment_ranges


PFAM_ID_RE = re.compile(r"^PF\d{5}$", re.IGNORECASE)

def normalize_pfam_id(pfam_id: object) -> str:
    normalized = str(pfam_id or "").strip().split(".", 1)[0].upper()
    if not PFAM_ID_RE.fullmatch(normalized):
        raise ValueError(f"invalid PFAM accession {pfam_id!r}")
    return normalized


def threshold_values_from_line(line: str) -> dict[str, float | None] | None:
    values: list[float] = []
    for part in line[2:].replace(";", " ").split():
        try:
            values.append(float(part))
        except ValueError:
            continue
    if not values:
        return None
    return {
        "sequence": values[0],
        "domain": values[1] if len(values) > 1 else None,
    }


def parse_hmm_metadata(lines: list[str]) -> dict[str, object]:
    metadata: dict[str, object] = {
        "pfam_id": "",
        "accession": "",
        "name": "",
        "description": "",
        "length": None,
        "thresholds": {},
    }
    thresholds: dict[str, object] = {}
    for line in lines:
        tag = line[:5].strip()
        value = line[5:].strip()
        if tag == "ACC":
            accession = value.split(None, 1)[0] if value else ""
            metadata["accession"] = accession
            try:
                metadata["pfam_id"] = normalize_pfam_id(accession)
            except ValueError:
                pass
        elif tag == "NAME":
            metadata["name"] = value.split(None, 1)[0] if value else ""
        elif tag == "DESC":
            metadata["description"] = value
        elif tag == "LENG":
            try:
                metadata["length"] = int(value.split(None, 1)[0])
            except (IndexError, ValueError):
                metadata["length"] = None
        elif tag in {"GA", "TC", "NC"}:
            parsed = threshold_values_from_line(line)
            if parsed is not None:
                thresholds[tag.lower()] = parsed
    metadata["thresholds"] = thresholds
    return metadata


def pfam_hmm_cache_path(cache_dir: Path, pfam_id: str) -> Path:
    return cache_dir / "hmmer" / "hmms" / f"{pfam_id}.hmm"


def cached_hmm_metadata(cache_dir: Path, pfam_id: str) -> dict[str, object] | None:
    path = pfam_hmm_cache_path(cache_dir, pfam_id)
    if not path.exists() or not path.is_file() or path.stat().st_size <= 0:
        return None
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return parse_hmm_metadata(handle.readlines())
    except OSError:
        return None


def write_hmm_cache(cache_dir: Path, pfam_id: str, lines: list[str]) -> Path:
    path = pfam_hmm_cache_path(cache_dir, pfam_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    with temporary_path.open("w", encoding="utf-8") as handle:
        handle.writelines(lines)
    temporary_path.replace(path)
    return path


def extract_pfam_hmms(
    pfam_ids: list[str],
    cache_dir: Path,
    pfam_hmm_path: Path = DEFAULT_PFAM_HMM_PATH,
) -> dict[str, dict[str, object]]:
    normalized_ids = []
    for pfam_id in pfam_ids:
        normalized = normalize_pfam_id(pfam_id)
        if normalized not in normalized_ids:
            normalized_ids.append(normalized)

    records: dict[str, dict[str, object]] = {}
    missing: set[str] = set()
    for pfam_id in normalized_ids:
        metadata = cached_hmm_metadata(cache_dir, pfam_id)
        if metadata is None:
            missing.add(pfam_id)
            continue
        records[pfam_id] = {
            "path": str(pfam_hmm_cache_path(cache_dir, pfam_id)),
            "metadata": metadata,
        }
    if not missing:
        return records
    if not pfam_hmm_path.exists() or not pfam_hmm_path.is_file():
        raise FileNotFoundError(f"Pfam HMM file not found: {pfam_hmm_path}")

    with pfam_hmm_path.open("r", encoding="utf-8", errors="replace") as handle:
        record_lines: list[str] = []
        for line in handle:
            record_lines.append(line)
            if not line.startswith("//"):
                continue
            metadata = parse_hmm_metadata(record_lines)
            pfam_id = str(metadata.get("pfam_id") or "")
            if pfam_id in missing:
                hmm_path = write_hmm_cache(cache_dir, pfam_id, record_lines)
                records[pfam_id] = {
                    "path": str(hmm_path),
                    "metadata": metadata,
                }
                missing.remove(pfam_id)
                if not missing:
                    break
            record_lines = []

    if missing:
        raise FileNotFoundError(
            "PFAM HMM record not found: " + ", ".join(sorted(missing))
        )
    return records


def _candidate_path_key(path: Path) -> str:
    return str(path.expanduser().resolve(strict=False))


def _executable_in_dir(directory: Path, command: str) -> str | None:
    candidate = directory.expanduser() / command
    if candidate.exists() and os.access(candidate, os.X_OK):
        return str(candidate)
    return None


def hmmer_command(command: str, hmmer_bin_dir: Path = DEFAULT_HMMER_BIN_DIR) -> str:
    candidates: list[tuple[Path, str]] = []
    seen: set[str] = set()

    def add_candidate(directory: Path | str | None, label: str) -> None:
        if directory is None:
            return
        path = Path(directory)
        key = _candidate_path_key(path)
        if key in seen:
            return
        seen.add(key)
        candidates.append((path, label))

    explicit_hmmer_bin_dir = os.environ.get("DIE_HMMER_BIN_DIR")
    add_candidate(explicit_hmmer_bin_dir, "DIE_HMMER_BIN_DIR")

    configured_bin_dir = Path(hmmer_bin_dir)
    configured_key = _candidate_path_key(configured_bin_dir)
    project_fallback_key = _candidate_path_key(PROJECT_HMMER_BIN_DIR)
    if configured_key != project_fallback_key:
        add_candidate(configured_bin_dir, "configured HMMER bin dir")

    add_candidate(Path(sys.executable).resolve().parent, "current Python environment")

    for directory, _label in candidates:
        executable = _executable_in_dir(directory, command)
        if executable is not None:
            return executable

    discovered = shutil.which(command)
    if discovered:
        return discovered

    fallback = _executable_in_dir(PROJECT_HMMER_BIN_DIR, command)
    if fallback is not None:
        return fallback

    searched = [f"{label}: {directory}" for directory, label in candidates]
    searched.append(f"PATH: {os.environ.get('PATH', '')}")
    searched.append(f"project fallback: {PROJECT_HMMER_BIN_DIR}")
    raise RuntimeError(
        f"HMMER command {command!r} was not found. Install the 'hmmer' conda package "
        "in the Python environment running this server, or set DIE_HMMER_BIN_DIR. "
        "Searched " + "; ".join(searched)
    )


def _json_string_end(buffer: mmap.mmap, start_index: int) -> int:
    escaped = False
    index = start_index
    while index < len(buffer):
        value = buffer[index]
        if escaped:
            escaped = False
        elif value == 92:
            escaped = True
        elif value == 34:
            return index
        index += 1
    raise ValueError("unterminated JSON string in sequence file")


def full_sequence_for_domain_accession(
    sequence_dir: Path,
    pfam_id: str,
    accession: str,
) -> str:
    pfam_id = normalize_pfam_id(pfam_id)
    accession = str(accession or "").strip()
    if not accession:
        raise ValueError("missing protein accession for domain sequence lookup")
    sequence_path = sequence_dir / f"{pfam_id}.json"
    if not sequence_path.exists() or not sequence_path.is_file():
        raise FileNotFoundError(f"domain sequence file not found: {sequence_path}")
    key_bytes = json.dumps(accession, ensure_ascii=True).encode("utf-8")
    try:
        with sequence_path.open("rb") as handle:
            with mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
                key_index = mapped.find(key_bytes)
                while key_index >= 0:
                    cursor = key_index + len(key_bytes)
                    while cursor < len(mapped) and mapped[cursor] in b" \t\r\n":
                        cursor += 1
                    if cursor < len(mapped) and mapped[cursor] == 58:
                        cursor += 1
                        while cursor < len(mapped) and mapped[cursor] in b" \t\r\n":
                            cursor += 1
                        if cursor < len(mapped) and mapped[cursor] == 34:
                            value_start = cursor
                            value_end = _json_string_end(mapped, value_start + 1)
                            return json.loads(mapped[value_start:value_end + 1].decode("utf-8"))
                    key_index = mapped.find(key_bytes, key_index + 1)
    except OSError as exc:
        raise RuntimeError(f"failed to read domain sequence file {sequence_path}: {exc}") from exc
    raise KeyError(f"protein {accession} was not found in {sequence_path.name}")


def slice_domain_sequence(full_sequence: str, fragment_key: str) -> tuple[str, list[int]]:
    sequence = str(full_sequence or "").strip().upper()
    if not sequence:
        raise ValueError("empty domain source sequence")
    residue_ids: list[int] = []
    sequence_parts: list[str] = []
    for start, end in fragment_ranges(fragment_key):
        if start < 1 or end < start:
            raise ValueError(f"invalid fragment range {start}-{end}")
        if end > len(sequence):
            raise ValueError(
                f"fragment {fragment_key} exceeds source sequence length {len(sequence)}"
            )
        residue_ids.extend(range(start, end + 1))
        sequence_parts.append(sequence[start - 1:end])
    domain_sequence = "".join(sequence_parts).replace("-", "").replace(".", "")
    if not domain_sequence:
        raise ValueError(f"empty sliced domain sequence for fragment {fragment_key}")
    return domain_sequence, residue_ids


def domain_sequence_from_sequence_file(
    sequence_dir: Path,
    pfam_id: str,
    accession: str,
    fragment_key: str,
) -> tuple[str, list[int]]:
    full_sequence = full_sequence_for_domain_accession(sequence_dir, pfam_id, accession)
    return slice_domain_sequence(full_sequence, fragment_key)


def write_fasta(path: Path, sequences: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for sequence in sequences:
            name = str(sequence["key"])
            residues = str(sequence["sequence"])
            handle.write(f">{name}\n")
            for index in range(0, len(residues), 60):
                handle.write(f"{residues[index:index + 60]}\n")


def optional_int(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def hmm_domain_coverage_payload(
    hmm_from: int | None,
    hmm_to: int | None,
    hmm_length: int | None,
    matched_hmm_covered: int | None = None,
    deleted_hmm_columns: int | None = None,
) -> dict[str, object]:
    matched_covered = max(0, int(matched_hmm_covered or 0))
    deleted_columns = max(0, int(deleted_hmm_columns or 0))
    matched_coverage = (
        min(1.0, matched_covered / hmm_length)
        if hmm_length is not None and hmm_length > 0
        else 0.0
    )
    if hmm_from is None or hmm_to is None or hmm_length is None or hmm_length <= 0:
        return {
            "hmm_from": hmm_from,
            "hmm_to": hmm_to,
            "hmm_length": hmm_length,
            "hmm_covered": 0,
            "hmm_coverage": 0.0,
            "hmm_coverage_percent": 0.0,
            "matched_hmm_covered": matched_covered,
            "matched_hmm_coverage": matched_coverage,
            "matched_hmm_coverage_percent": matched_coverage * 100.0,
            "deleted_hmm_columns": deleted_columns,
        }
    covered = max(0, hmm_to - hmm_from + 1)
    coverage = min(1.0, covered / hmm_length)
    return {
        "hmm_from": hmm_from,
        "hmm_to": hmm_to,
        "hmm_length": hmm_length,
        "hmm_covered": covered,
        "hmm_coverage": coverage,
        "hmm_coverage_percent": coverage * 100.0,
        "matched_hmm_covered": matched_covered,
        "matched_hmm_coverage": matched_coverage,
        "matched_hmm_coverage_percent": matched_coverage * 100.0,
        "deleted_hmm_columns": deleted_columns,
    }


HMMER_ALIGNMENT_ROW_RE = re.compile(r"^\s*(\S+)\s+(\d+)\s+([A-Za-z.\-]+)\s+(\d+)\s*$")


def empty_alignment_coverage() -> dict[str, object]:
    return {
        "matched_hmm_covered": 0,
        "matched_hmm_coverage": 0.0,
        "matched_hmm_coverage_percent": 0.0,
        "deleted_hmm_columns": 0,
        "aligned_hmm_columns": 0,
        "matched_hmm_positions": [],
    }


def parse_hmmsearch_alignment_coverages(
    output: str,
    hmm_name: str,
    hmm_length: int | None,
) -> dict[tuple[str, int], dict[str, object]]:
    coverages: dict[tuple[str, int], dict[str, object]] = {}
    if not output or not hmm_name:
        return coverages
    current_target: str | None = None
    current_domain: int | None = None
    pending_model_alignment: str | None = None
    pending_model_start: int | None = None
    for line in output.splitlines():
        if line.startswith(">> "):
            current_target = line[3:].strip()
            current_domain = None
            pending_model_alignment = None
            pending_model_start = None
            continue
        domain_match = re.match(r"^\s*== domain (\d+)\s+score:", line)
        if domain_match:
            current_domain = int(domain_match.group(1))
            pending_model_alignment = None
            pending_model_start = None
            if current_target is not None:
                coverages[(current_target, current_domain)] = empty_alignment_coverage()
            continue
        if current_target is None or current_domain is None:
            continue
        row_match = HMMER_ALIGNMENT_ROW_RE.match(line)
        if row_match is None:
            continue
        row_name, start_text, alignment, _end_text = row_match.groups()
        if row_name == hmm_name:
            pending_model_alignment = alignment
            pending_model_start = int(start_text)
            continue
        if (
            row_name != current_target
            or pending_model_alignment is None
            or pending_model_start is None
        ):
            continue
        coverage = coverages.setdefault(
            (current_target, current_domain),
            empty_alignment_coverage(),
        )
        matched_positions = coverage.setdefault("matched_hmm_positions", [])
        hmm_position = pending_model_start
        for hmm_char, query_char in zip(pending_model_alignment, alignment):
            if hmm_char == ".":
                continue
            coverage["aligned_hmm_columns"] = int(coverage["aligned_hmm_columns"]) + 1
            if query_char.isalpha():
                coverage["matched_hmm_covered"] = int(coverage["matched_hmm_covered"]) + 1
                if isinstance(matched_positions, list):
                    matched_positions.append(hmm_position)
            else:
                coverage["deleted_hmm_columns"] = int(coverage["deleted_hmm_columns"]) + 1
            hmm_position += 1
        pending_model_alignment = None
        pending_model_start = None
    normalized_length = optional_int(hmm_length)
    for coverage in coverages.values():
        matched = int(coverage.get("matched_hmm_covered") or 0)
        fraction = (min(1.0, matched / normalized_length) if normalized_length else 0.0)
        coverage["matched_hmm_coverage"] = fraction
        coverage["matched_hmm_coverage_percent"] = fraction * 100.0
        raw_positions = coverage.get("matched_hmm_positions")
        if isinstance(raw_positions, list):
            positions = []
            seen_positions: set[int] = set()
            for raw_position in raw_positions:
                position = optional_int(raw_position)
                if position is None or position in seen_positions:
                    continue
                seen_positions.add(position)
                positions.append(position)
            coverage["matched_hmm_positions"] = sorted(positions)
    return coverages


def parse_hmmsearch_domtblout(path: Path) -> dict[str, dict[str, object]]:
    domains: dict[str, dict[str, object]] = {}
    if not path.exists():
        return domains
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip() or line.startswith("#"):
                continue
            fields = line.split(maxsplit=22)
            if len(fields) < 22:
                continue
            target_name = fields[0]
            try:
                hmm_length = int(fields[5])
                domain_index = int(fields[9])
                domain_score = float(fields[13])
                domain_bias = float(fields[14])
                hmm_from = int(fields[15])
                hmm_to = int(fields[16])
                ali_from = int(fields[17])
                ali_to = int(fields[18])
                envelope_from = int(fields[19])
                envelope_to = int(fields[20])
                accuracy = float(fields[21])
            except ValueError:
                continue
            payload = {
                "domain_index": domain_index,
                "domain_score": domain_score,
                "domain_bias": domain_bias,
                "ali_from": ali_from,
                "ali_to": ali_to,
                "envelope_from": envelope_from,
                "envelope_to": envelope_to,
                "accuracy": accuracy,
                **hmm_domain_coverage_payload(hmm_from, hmm_to, hmm_length),
            }
            existing = domains.get(target_name)
            if existing is None or domain_score > float(existing.get("domain_score", float("-inf"))):
                domains[target_name] = payload
    return domains


def parse_hmmsearch_tblout(path: Path) -> dict[str, dict[str, object]]:
    scores: dict[str, dict[str, object]] = {}
    if not path.exists():
        return scores
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip() or line.startswith("#"):
                continue
            fields = line.split(maxsplit=18)
            if len(fields) < 9:
                continue
            target_name = fields[0]
            try:
                scores[target_name] = {
                    "evalue": float(fields[4]),
                    "full_score": float(fields[5]),
                    "full_bias": float(fields[6]),
                    "best_domain_evalue": float(fields[7]),
                    "best_domain_score": float(fields[8]),
                    "best_domain_bias": float(fields[9]) if len(fields) > 9 else None,
                    "reported": True,
                }
            except ValueError:
                continue
    return scores


def unreported_hmmer_score(hmm_length: int | None = None) -> dict[str, object]:
    return {
        "evalue": None,
        "full_score": 0.0,
        "full_bias": None,
        "best_domain_evalue": None,
        "best_domain_score": 0.0,
        "best_domain_bias": None,
        "reported": False,
        **hmm_domain_coverage_payload(None, None, hmm_length),
    }


def residue_span_for_sequence(sequence: dict[str, object]) -> tuple[int | None, int | None]:
    residue_ids = sequence.get("residue_ids")
    if not isinstance(residue_ids, list):
        return None, None
    normalized_ids: list[int] = []
    for residue_id in residue_ids:
        try:
            normalized_ids.append(int(residue_id))
        except (TypeError, ValueError):
            continue
    if not normalized_ids:
        return None, None
    return min(normalized_ids), max(normalized_ids)


def _residue_order_value(value: int | None) -> int:
    return value if value is not None else 10**12


def combined_sequence_record(sequences: list[dict[str, object]]) -> dict[str, object]:
    ordered_sequences = sorted(
        enumerate(sequences),
        key=lambda item: (
            _residue_order_value(residue_span_for_sequence(item[1])[0]),
            _residue_order_value(residue_span_for_sequence(item[1])[1]),
            item[0],
        ),
    )
    sequence_parts: list[str] = []
    residue_ids: list[int] = []
    sources: list[dict[str, object]] = []
    for _index, sequence in ordered_sequences:
        sequence_text = str(sequence.get("sequence") or "")
        start, end = residue_span_for_sequence(sequence)
        sequence_parts.append(sequence_text)
        raw_residue_ids = sequence.get("residue_ids")
        if isinstance(raw_residue_ids, list):
            for residue_id in raw_residue_ids:
                try:
                    residue_ids.append(int(residue_id))
                except (TypeError, ValueError):
                    continue
        sources.append(
            {
                "key": str(sequence.get("key") or ""),
                "fragment_key": str(sequence.get("fragment_key") or ""),
                "residue_start": start,
                "residue_end": end,
                "length": len(sequence_text),
            }
        )
    combined_sequence = "".join(sequence_parts)
    return {
        "key": "combined",
        "label": "Combined fragments",
        "sequence": combined_sequence,
        "length": len(combined_sequence),
        "residue_ids": residue_ids,
        "sequence_order": [str(source["key"]) for source in sources],
        "sources": sources,
    }


def optional_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def add_combined_coverage_gain(
    score: dict[str, object],
    combined_score: dict[str, object],
) -> None:
    full_score = optional_float(score.get("full_score"))
    combined_full_score = optional_float(combined_score.get("full_score"))
    full_score_gain = combined_full_score - full_score
    domain_score = optional_float(score.get("domain_score"))
    combined_domain_score = optional_float(combined_score.get("domain_score"))
    domain_score_gain = combined_domain_score - domain_score
    coverage = optional_float(score.get("hmm_coverage"))
    matched_coverage = optional_float(score.get("matched_hmm_coverage"))
    combined_coverage = optional_float(combined_score.get("hmm_coverage"))
    combined_matched_coverage = optional_float(
        combined_score.get("matched_hmm_coverage")
    )
    coverage_gain = combined_coverage - coverage
    matched_coverage_gain = combined_matched_coverage - matched_coverage
    score.update(
        {
            "combined_hmm_from": combined_score.get("hmm_from"),
            "combined_hmm_to": combined_score.get("hmm_to"),
            "combined_hmm_length": combined_score.get("hmm_length"),
            "combined_hmm_covered": combined_score.get("hmm_covered"),
            "combined_hmm_coverage": combined_coverage,
            "combined_hmm_coverage_percent": combined_coverage * 100.0,
            "combined_matched_hmm_covered": combined_score.get(
                "matched_hmm_covered"
            ),
            "combined_matched_hmm_coverage": combined_matched_coverage,
            "combined_matched_hmm_coverage_percent": (
                combined_matched_coverage * 100.0
            ),
            "combined_deleted_hmm_columns": combined_score.get(
                "deleted_hmm_columns"
            ),
            "combined_reported": combined_score.get("reported"),
            "combined_full_score": combined_score.get("full_score"),
            "combined_domain_score": combined_score.get("domain_score"),
            "domain_score_gain": domain_score_gain,
            "bit_score_gain": domain_score_gain,
            "score_gain": domain_score_gain,
            "full_score_gain": full_score_gain,
            "hmm_coverage_gain": coverage_gain,
            "hmm_coverage_gain_percent": coverage_gain * 100.0,
            "matched_hmm_coverage_gain": matched_coverage_gain,
            "matched_hmm_coverage_gain_percent": matched_coverage_gain * 100.0,
        }
    )


def matched_hmm_position_set(score: dict[str, object]) -> set[int]:
    positions = score.get("matched_hmm_positions")
    if not isinstance(positions, list):
        return set()
    normalized: set[int] = set()
    for raw_position in positions:
        position = optional_int(raw_position)
        if position is not None:
            normalized.add(position)
    return normalized


def hmm_position_overlap_payload(
    *,
    hmm_summary: dict[str, object],
    main_score: dict[str, object],
    partner_score: dict[str, object],
    main_pfam_id: str,
    partner_pfam_id: str,
) -> dict[str, object]:
    hmm_length = (
        optional_int(hmm_summary.get("length"))
        or optional_int(main_score.get("hmm_length"))
        or optional_int(partner_score.get("hmm_length"))
    )
    main_positions = matched_hmm_position_set(main_score)
    partner_positions = matched_hmm_position_set(partner_score)
    overlap_positions = main_positions & partner_positions
    union_positions = main_positions | partner_positions
    overlap_count = len(overlap_positions)
    union_count = len(union_positions)
    main_count = len(main_positions)
    partner_count = len(partner_positions)
    main_fraction = (
        min(1.0, main_count / hmm_length)
        if hmm_length is not None and hmm_length > 0
        else 0.0
    )
    partner_fraction = (
        min(1.0, partner_count / hmm_length)
        if hmm_length is not None and hmm_length > 0
        else 0.0
    )
    overlap_fraction = (
        min(1.0, overlap_count / hmm_length)
        if hmm_length is not None and hmm_length > 0
        else 0.0
    )
    union_fraction = (
        min(1.0, union_count / hmm_length)
        if hmm_length is not None and hmm_length > 0
        else 0.0
    )
    overlap_over_union = overlap_count / union_count if union_count > 0 else 0.0
    smaller_hit_count = min(main_count, partner_count)
    overlap_over_smaller_hit = (
        overlap_count / smaller_hit_count if smaller_hit_count > 0 else 0.0
    )
    main_union_gain = union_fraction - main_fraction
    partner_union_gain = union_fraction - partner_fraction
    return {
        "hmm_pfam_id": hmm_summary.get("pfam_id"),
        "hmm_name": hmm_summary.get("name", ""),
        "hmm_length": hmm_length,
        "main_pfam_id": main_pfam_id,
        "partner_pfam_id": partner_pfam_id,
        "main_matched_hmm_covered": main_count,
        "main_matched_hmm_coverage": main_fraction,
        "main_matched_hmm_coverage_percent": main_fraction * 100.0,
        "partner_matched_hmm_covered": partner_count,
        "partner_matched_hmm_coverage": partner_fraction,
        "partner_matched_hmm_coverage_percent": partner_fraction * 100.0,
        "overlap_hmm_covered": overlap_count,
        "union_hmm_covered": union_count,
        "overlap_hmm_coverage": overlap_fraction,
        "overlap_hmm_coverage_percent": overlap_fraction * 100.0,
        "union_hmm_coverage": union_fraction,
        "union_hmm_coverage_percent": union_fraction * 100.0,
        "main_union_hmm_coverage_gain": main_union_gain,
        "main_union_hmm_coverage_gain_percent": main_union_gain * 100.0,
        "partner_union_hmm_coverage_gain": partner_union_gain,
        "partner_union_hmm_coverage_gain_percent": partner_union_gain * 100.0,
        "overlap_over_union": overlap_over_union,
        "overlap_over_union_percent": overlap_over_union * 100.0,
        "overlap_over_smaller_hit": overlap_over_smaller_hit,
        "overlap_over_smaller_hit_percent": overlap_over_smaller_hit * 100.0,
    }


def score_sequences_against_hmm(
    *,
    hmmsearch_path: str,
    hmm_path: Path,
    sequences: list[dict[str, object]],
    cache_dir: Path,
    timeout_seconds: int = 120,
    hmm_length: int | None = None,
    hmm_name: str = "",
) -> dict[str, dict[str, object]]:
    tmp_root = cache_dir / "hmmer" / "tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hmmsearch-", dir=tmp_root) as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)
        fasta_path = tmp_dir / "domains.fasta"
        tblout_path = tmp_dir / "scores.tblout"
        domtblout_path = tmp_dir / "scores.domtblout"
        write_fasta(fasta_path, sequences)
        completed = subprocess.run(
            [
                hmmsearch_path,
                "--max",
                "--cpu",
                "1",
                "-T",
                "-100000",
                "--domT",
                "-100000",
                "--tblout",
                str(tblout_path),
                "--domtblout",
                str(domtblout_path),
                str(hmm_path),
                str(fasta_path),
            ],
            cwd=tmp_dir,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout or "hmmsearch failed").strip()
            raise RuntimeError(message)
        scores = parse_hmmsearch_tblout(tblout_path)
        domain_hits = parse_hmmsearch_domtblout(domtblout_path)
        fallback_hmm_length = optional_int(hmm_length)
        alignment_coverages = parse_hmmsearch_alignment_coverages(
            completed.stdout,
            hmm_name,
            fallback_hmm_length,
        )
        for target_name, score in scores.items():
            domain_hit = domain_hits.get(target_name)
            if domain_hit is None:
                score.update(hmm_domain_coverage_payload(None, None, fallback_hmm_length))
            else:
                domain_index = optional_int(domain_hit.get("domain_index"))
                alignment_coverage = (
                    alignment_coverages.get((target_name, domain_index))
                    if domain_index is not None
                    else None
                )
                if alignment_coverage is not None:
                    domain_hit = {**domain_hit, **alignment_coverage}
                score.update(domain_hit)
        return scores


def score_cache_key(payload: dict[str, object], pfam_hmm_path: Path) -> str:
    stat_payload = {}
    try:
        stat = pfam_hmm_path.stat()
        stat_payload = {
            "hmm_path": str(pfam_hmm_path.resolve()),
            "hmm_size": stat.st_size,
            "hmm_mtime_ns": stat.st_mtime_ns,
        }
    except OSError:
        stat_payload = {"hmm_path": str(pfam_hmm_path)}
    cache_payload = {
        **payload,
        **stat_payload,
        "version": 10,
    }
    return hashlib.sha1(
        json.dumps(cache_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()


def read_score_cache(cache_dir: Path, cache_key: str) -> dict[str, object] | None:
    path = cache_dir / "hmmer" / "scores" / f"{cache_key}.json"
    if not path.exists() or not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def write_score_cache(cache_dir: Path, cache_key: str, payload: dict[str, object]) -> None:
    path = cache_dir / "hmmer" / "scores" / f"{cache_key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    with temporary_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    temporary_path.replace(path)


def compute_domain_hmm_bit_scores(
    *,
    cache_dir: Path,
    accession: str,
    main_fragment_key: str,
    partner_fragment_key: str,
    main_pfam_id: str,
    partner_pfam_id: str,
    sequence_dir: Path = DEFAULT_SEQUENCE_BY_DOMAIN_DIR,
    pfam_hmm_path: Path = DEFAULT_PFAM_HMM_PATH,
    hmmer_bin_dir: Path = DEFAULT_HMMER_BIN_DIR,
) -> dict[str, object]:
    main_pfam_id = normalize_pfam_id(main_pfam_id)
    partner_pfam_id = normalize_pfam_id(partner_pfam_id)
    accession = str(accession or "").strip()
    main_sequence, main_residue_ids = domain_sequence_from_sequence_file(
        sequence_dir,
        main_pfam_id,
        accession,
        main_fragment_key,
    )
    partner_sequence, partner_residue_ids = domain_sequence_from_sequence_file(
        sequence_dir,
        partner_pfam_id,
        accession,
        partner_fragment_key,
    )
    sequences = [
        {
            "key": "main",
            "label": "Main domain",
            "pfam_id": main_pfam_id,
            "fragment_key": main_fragment_key,
            "sequence": main_sequence,
            "length": len(main_sequence),
            "residue_ids": main_residue_ids,
        },
        {
            "key": "partner",
            "label": "Interacting domain",
            "pfam_id": partner_pfam_id,
            "fragment_key": partner_fragment_key,
            "sequence": partner_sequence,
            "length": len(partner_sequence),
            "residue_ids": partner_residue_ids,
        },
    ]
    combined_sequence = combined_sequence_record(sequences)
    scoring_sequences = sequences + [combined_sequence]
    cache_payload = {
        "accession": accession,
        "sequence_dir": str(sequence_dir.resolve()),
        "domains": [
            {
                "key": sequence["key"],
                "pfam_id": sequence["pfam_id"],
                "fragment_key": sequence["fragment_key"],
                "sequence": sequence["sequence"],
            }
            for sequence in sequences
        ],
        "combined_sequence_order": combined_sequence["sequence_order"],
        "combined_sequence_sources": combined_sequence["sources"],
        "hmms": [main_pfam_id, partner_pfam_id],
    }
    cache_key = score_cache_key(cache_payload, pfam_hmm_path)
    cached = read_score_cache(cache_dir, cache_key)
    if cached is not None:
        return cached

    hmmsearch_path = hmmer_command("hmmsearch", hmmer_bin_dir)
    hmm_records = extract_pfam_hmms(
        [main_pfam_id, partner_pfam_id],
        cache_dir,
        pfam_hmm_path,
    )
    sequence_summaries = [
        {
            key: value
            for key, value in sequence.items()
            if key not in {"sequence", "residue_ids"}
        }
        for sequence in sequences
    ]
    hmm_summaries = []
    score_matrix: dict[str, dict[str, object]] = {
        str(sequence["key"]): {}
        for sequence in sequences
    }
    for pfam_id in [main_pfam_id, partner_pfam_id]:
        hmm_record = hmm_records[pfam_id]
        hmm_metadata = hmm_record["metadata"]
        hmm_summaries.append(
            {
                "pfam_id": pfam_id,
                "accession": hmm_metadata.get("accession", pfam_id),
                "name": hmm_metadata.get("name", ""),
                "description": hmm_metadata.get("description", ""),
                "length": optional_int(hmm_metadata.get("length")),
                "thresholds": hmm_metadata.get("thresholds", {}),
            }
        )
        hmm_length = optional_int(hmm_metadata.get("length"))
        scores = score_sequences_against_hmm(
            hmmsearch_path=hmmsearch_path,
            hmm_path=Path(str(hmm_record["path"])),
            sequences=scoring_sequences,
            cache_dir=cache_dir,
            hmm_length=hmm_length,
            hmm_name=str(hmm_metadata.get("name") or ""),
        )
        combined_score = scores.get("combined") or unreported_hmmer_score(hmm_length)
        for sequence in sequences:
            sequence_key = str(sequence["key"])
            score = scores.get(sequence_key) or unreported_hmmer_score(hmm_length)
            add_combined_coverage_gain(score, combined_score)
            score_matrix[sequence_key][pfam_id] = score

    coverage = []
    for sequence in sequences:
        sequence_key = str(sequence["key"])
        pfam_id = str(sequence["pfam_id"])
        score = score_matrix[sequence_key].get(pfam_id) or unreported_hmmer_score()
        coverage.append(
            {
                "key": sequence_key,
                "label": sequence["label"],
                "domain_pfam_id": pfam_id,
                "hmm_pfam_id": pfam_id,
                "hmm_from": score.get("hmm_from"),
                "hmm_to": score.get("hmm_to"),
                "hmm_length": score.get("hmm_length"),
                "hmm_covered": score.get("hmm_covered"),
                "coverage_fraction": score.get("hmm_coverage"),
                "coverage_percent": score.get("hmm_coverage_percent"),
                "matched_hmm_covered": score.get("matched_hmm_covered"),
                "matched_coverage_fraction": score.get("matched_hmm_coverage"),
                "matched_coverage_percent": score.get("matched_hmm_coverage_percent"),
                "deleted_hmm_columns": score.get("deleted_hmm_columns"),
                "combined_hmm_from": score.get("combined_hmm_from"),
                "combined_hmm_to": score.get("combined_hmm_to"),
                "combined_hmm_length": score.get("combined_hmm_length"),
                "combined_hmm_covered": score.get("combined_hmm_covered"),
                "combined_coverage_fraction": score.get("combined_hmm_coverage"),
                "combined_coverage_percent": score.get(
                    "combined_hmm_coverage_percent"
                ),
                "combined_matched_hmm_covered": score.get(
                    "combined_matched_hmm_covered"
                ),
                "combined_matched_coverage_fraction": score.get(
                    "combined_matched_hmm_coverage"
                ),
                "combined_matched_coverage_percent": score.get(
                    "combined_matched_hmm_coverage_percent"
                ),
                "combined_deleted_hmm_columns": score.get(
                    "combined_deleted_hmm_columns"
                ),
                "combined_reported": score.get("combined_reported"),
                "combined_full_score": score.get("combined_full_score"),
                "domain_score": score.get("domain_score"),
                "combined_domain_score": score.get("combined_domain_score"),
                "domain_score_gain": score.get("domain_score_gain"),
                "score_gain": score.get("score_gain"),
                "bit_score_gain": score.get("bit_score_gain"),
                "full_score_gain": score.get("full_score_gain"),
                "coverage_gain_fraction": score.get("hmm_coverage_gain"),
                "coverage_gain_percent": score.get("hmm_coverage_gain_percent"),
                "hmm_coverage_gain": score.get("hmm_coverage_gain"),
                "hmm_coverage_gain_percent": score.get(
                    "hmm_coverage_gain_percent"
                ),
                "matched_coverage_gain_fraction": score.get(
                    "matched_hmm_coverage_gain"
                ),
                "matched_coverage_gain_percent": score.get(
                    "matched_hmm_coverage_gain_percent"
                ),
                "matched_hmm_coverage_gain": score.get(
                    "matched_hmm_coverage_gain"
                ),
                "matched_hmm_coverage_gain_percent": score.get(
                    "matched_hmm_coverage_gain_percent"
                ),
                "reported": score.get("reported"),
                "full_score": score.get("full_score"),
            }
        )

    hmm_overlaps = []
    for hmm_summary in hmm_summaries:
        pfam_id = str(hmm_summary.get("pfam_id") or "")
        if not pfam_id:
            continue
        main_score = score_matrix.get("main", {}).get(pfam_id) or unreported_hmmer_score(
            optional_int(hmm_summary.get("length"))
        )
        partner_score = score_matrix.get("partner", {}).get(pfam_id) or unreported_hmmer_score(
            optional_int(hmm_summary.get("length"))
        )
        hmm_overlaps.append(
            hmm_position_overlap_payload(
                hmm_summary=hmm_summary,
                main_score=main_score,
                partner_score=partner_score,
                main_pfam_id=main_pfam_id,
                partner_pfam_id=partner_pfam_id,
            )
        )

    payload = {
        "domains": sequence_summaries,
        "hmms": hmm_summaries,
        "scores": score_matrix,
        "coverage": coverage,
        "hmm_overlaps": hmm_overlaps,
        "combined_sequence": {
            key: value
            for key, value in combined_sequence.items()
            if key not in {"sequence", "residue_ids"}
        },
    }
    write_score_cache(cache_dir, cache_key, payload)
    return payload
