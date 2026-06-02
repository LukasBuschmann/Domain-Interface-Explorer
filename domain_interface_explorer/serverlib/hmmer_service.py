from __future__ import annotations

import hashlib
import json
import mmap
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from .config import (
    DEFAULT_HMMER_BIN_DIR,
    DEFAULT_PFAM_HMM_PATH,
    DEFAULT_SEQUENCE_BY_DOMAIN_DIR,
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


def hmmer_command(command: str, hmmer_bin_dir: Path = DEFAULT_HMMER_BIN_DIR) -> str:
    configured_path = hmmer_bin_dir / command
    if configured_path.exists() and os.access(configured_path, os.X_OK):
        return str(configured_path)
    discovered = shutil.which(command)
    if discovered:
        return discovered
    raise RuntimeError(
        f"HMMER command {command!r} was not found. Install the 'hmmer' conda package "
        f"in {hmmer_bin_dir.parent}."
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


HMMER_ALIGNMENT_ROW_RE = re.compile(r"^\s*(\S+)\s+\d+\s+([A-Za-z.\-]+)\s+\d+\s*$")


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
    for line in output.splitlines():
        if line.startswith(">> "):
            current_target = line[3:].strip()
            current_domain = None
            pending_model_alignment = None
            continue
        domain_match = re.match(r"^\s*== domain (\d+)\s+score:", line)
        if domain_match:
            current_domain = int(domain_match.group(1))
            pending_model_alignment = None
            if current_target is not None:
                coverages[(current_target, current_domain)] = {
                    "matched_hmm_covered": 0,
                    "matched_hmm_coverage": 0.0,
                    "matched_hmm_coverage_percent": 0.0,
                    "deleted_hmm_columns": 0,
                    "aligned_hmm_columns": 0,
                }
            continue
        if current_target is None or current_domain is None:
            continue
        row_match = HMMER_ALIGNMENT_ROW_RE.match(line)
        if row_match is None:
            continue
        row_name, alignment = row_match.groups()
        if row_name == hmm_name:
            pending_model_alignment = alignment
            continue
        if row_name != current_target or pending_model_alignment is None:
            continue
        coverage = coverages.setdefault(
            (current_target, current_domain),
            {
                "matched_hmm_covered": 0,
                "matched_hmm_coverage": 0.0,
                "matched_hmm_coverage_percent": 0.0,
                "deleted_hmm_columns": 0,
                "aligned_hmm_columns": 0,
            },
        )
        for hmm_char, query_char in zip(pending_model_alignment, alignment):
            if hmm_char == ".":
                continue
            coverage["aligned_hmm_columns"] = int(coverage["aligned_hmm_columns"]) + 1
            if query_char.isalpha():
                coverage["matched_hmm_covered"] = int(coverage["matched_hmm_covered"]) + 1
            else:
                coverage["deleted_hmm_columns"] = int(coverage["deleted_hmm_columns"]) + 1
        pending_model_alignment = None
    normalized_length = optional_int(hmm_length)
    for coverage in coverages.values():
        matched = int(coverage.get("matched_hmm_covered") or 0)
        fraction = (min(1.0, matched / normalized_length) if normalized_length else 0.0)
        coverage["matched_hmm_coverage"] = fraction
        coverage["matched_hmm_coverage_percent"] = fraction * 100.0
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
        "version": 5,
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
            sequences=sequences,
            cache_dir=cache_dir,
            hmm_length=hmm_length,
            hmm_name=str(hmm_metadata.get("name") or ""),
        )
        for sequence in sequences:
            sequence_key = str(sequence["key"])
            score_matrix[sequence_key][pfam_id] = scores.get(sequence_key) or unreported_hmmer_score(hmm_length)

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
                "reported": score.get("reported"),
                "full_score": score.get("full_score"),
            }
        )

    payload = {
        "domains": sequence_summaries,
        "hmms": hmm_summaries,
        "scores": score_matrix,
        "coverage": coverage,
    }
    write_score_cache(cache_dir, cache_key, payload)
    return payload
