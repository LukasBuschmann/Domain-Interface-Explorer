from __future__ import annotations

from .interface_embedding import build_interface_alignment_rows_from_metadata
from .timing import timed_step

REPRESENTATIVE_METHOD_BALANCED = "balanced"
REPRESENTATIVE_METHOD_RESIDUE = "residue"
REPRESENTATIVE_METHODS = {
    REPRESENTATIVE_METHOD_BALANCED,
    REPRESENTATIVE_METHOD_RESIDUE,
}


def interaction_row_key(interface_row_key: object, partner_domain: object) -> str:
    row_key = str(interface_row_key or "")
    partner = str(partner_domain or "")
    return f"{row_key}@@{partner}" if partner else row_key


def interface_column_count(candidate: dict[str, object]) -> int:
    raw_columns = candidate.get("interface_msa_columns_a")
    if not isinstance(raw_columns, list):
        return 0
    columns: set[int] = set()
    for raw_column in raw_columns:
        try:
            columns.add(int(raw_column))
        except (TypeError, ValueError):
            continue
    return len(columns)


def _candidate_row_key(candidate: dict[str, object]) -> str:
    return interaction_row_key(
        candidate.get("interface_row_key"),
        candidate.get("partner_domain"),
    )


def _select_balanced_representative(
    normalized_candidates: list[dict[str, object]],
    matrix: object,
    alignment_length: int,
) -> tuple[dict[str, object], dict[str, object]]:
    import numpy as np

    gap_code = 26
    consensus_codes = np.empty(alignment_length, dtype=np.uint8)
    for column_index in range(alignment_length):
        consensus_codes[column_index] = int(
            np.bincount(matrix[:, column_index], minlength=27).argmax()
        )
    matches_by_row = np.count_nonzero(matrix == consensus_codes, axis=1)
    consensus_gap_columns = consensus_codes == gap_code
    if bool(np.any(consensus_gap_columns)):
        gap_matches_by_row = np.count_nonzero(
            matrix[:, consensus_gap_columns] == gap_code,
            axis=1,
        )
    else:
        gap_matches_by_row = np.zeros(len(normalized_candidates), dtype=np.int64)
    coverages = np.count_nonzero(matrix != gap_code, axis=1)
    median_coverage = float(np.median(coverages)) if len(coverages) else 0.0
    coverage_distances = np.abs(coverages.astype(np.float64) - median_coverage)
    match_fractions = matches_by_row.astype(np.float64) / alignment_length
    scores = match_fractions - (0.05 * (coverage_distances / max(1, alignment_length)))
    interface_sizes = [interface_column_count(candidate) for candidate in normalized_candidates]
    row_keys = [_candidate_row_key(candidate) for candidate in normalized_candidates]

    selected_index = min(
        range(len(normalized_candidates)),
        key=lambda index: (
            -float(scores[index]),
            -float(match_fractions[index]),
            float(coverage_distances[index]),
            -int(interface_sizes[index]),
            row_keys[index],
        ),
    )
    selected = normalized_candidates[selected_index]
    return selected, {
        "method": REPRESENTATIVE_METHOD_BALANCED,
        "score": float(scores[selected_index]),
        "match_fraction": float(match_fractions[selected_index]),
        "matches": int(matches_by_row[selected_index]),
        "gap_matches": int(gap_matches_by_row[selected_index]),
        "coverage": int(coverages[selected_index]),
        "median_coverage": median_coverage,
        "coverage_distance": float(coverage_distances[selected_index]),
        "interface_column_count": int(interface_sizes[selected_index]),
    }


def _select_residue_consensus_representative(
    normalized_candidates: list[dict[str, object]],
    matrix: object,
    alignment_length: int,
) -> tuple[dict[str, object], dict[str, object]]:
    import numpy as np

    gap_code = 26
    consensus_codes = np.full(alignment_length, gap_code, dtype=np.uint8)
    for column_index in range(alignment_length):
        column = matrix[:, column_index]
        residue_codes = column[column != gap_code]
        if residue_codes.size <= 0:
            continue
        consensus_codes[column_index] = int(np.bincount(residue_codes, minlength=26).argmax())

    consensus_columns = consensus_codes != gap_code
    consensus_column_count = int(np.count_nonzero(consensus_columns))
    if consensus_column_count > 0:
        consensus_view = consensus_codes[consensus_columns]
        matrix_view = matrix[:, consensus_columns]
        matches_by_row = np.count_nonzero(matrix_view == consensus_view, axis=1)
        coverages = np.count_nonzero(matrix_view != gap_code, axis=1)
    else:
        matches_by_row = np.zeros(len(normalized_candidates), dtype=np.int64)
        coverages = np.zeros(len(normalized_candidates), dtype=np.int64)

    match_fractions = matches_by_row.astype(np.float64) / max(1, consensus_column_count)
    interface_sizes = [interface_column_count(candidate) for candidate in normalized_candidates]
    row_keys = [_candidate_row_key(candidate) for candidate in normalized_candidates]
    selected_index = min(
        range(len(normalized_candidates)),
        key=lambda index: (
            -int(matches_by_row[index]),
            -int(coverages[index]),
            row_keys[index],
        ),
    )
    selected = normalized_candidates[selected_index]
    return selected, {
        "method": REPRESENTATIVE_METHOD_RESIDUE,
        "score": float(match_fractions[selected_index]),
        "match_fraction": float(match_fractions[selected_index]),
        "matches": int(matches_by_row[selected_index]),
        "coverage": int(coverages[selected_index]),
        "consensus_columns": consensus_column_count,
        "interface_column_count": int(interface_sizes[selected_index]),
    }


def compute_cluster_summary_payload(
    candidates: list[dict[str, object]],
    clustering_payload: dict[str, object],
) -> list[dict[str, object]]:
    candidate_by_key = {
        interaction_row_key(
            candidate.get("interface_row_key"),
            candidate.get("partner_domain"),
        ): candidate
        for candidate in candidates
    }
    points = clustering_payload.get("points")
    if not isinstance(points, list) or not candidate_by_key:
        return []

    summaries: dict[int, dict[str, object]] = {}
    total_partner_counts: dict[str, int] = {}
    for point in points:
        if not isinstance(point, dict):
            continue
        member_key = interaction_row_key(
            point.get("row_key"),
            point.get("partner_domain"),
        )
        candidate = candidate_by_key.get(member_key)
        if candidate is None:
            continue
        partner_domain = str(candidate.get("partner_domain") or "")
        total_partner_counts[partner_domain] = total_partner_counts.get(partner_domain, 0) + 1
        try:
            cluster_label = int(point.get("cluster_label", -1))
        except (TypeError, ValueError):
            continue
        if cluster_label < 0:
            continue
        summary = summaries.get(cluster_label)
        if summary is None:
            summary = {
                "cluster_label": cluster_label,
                "member_count": 0,
                "partner_counts": {},
                "column_counts": {},
            }
            summaries[cluster_label] = summary
        summary["member_count"] = int(summary["member_count"]) + 1
        partner_counts = summary["partner_counts"]
        if isinstance(partner_counts, dict):
            partner_counts[partner_domain] = int(partner_counts.get(partner_domain, 0)) + 1
        column_counts = summary["column_counts"]
        if isinstance(column_counts, dict):
            for column in sorted(set(candidate.get("interface_msa_columns_a") or [])):
                try:
                    column_index = int(column)
                except (TypeError, ValueError):
                    continue
                if column_index < 0:
                    continue
                column_key = str(column_index)
                column_counts[column_key] = int(column_counts.get(column_key, 0)) + 1

    return [
        {
            "cluster_label": cluster_label,
            "member_count": int(summary["member_count"]),
            "partner_counts": dict(summary["partner_counts"]),
            "total_partner_counts": total_partner_counts,
            "column_counts": [
                [int(column), int(count)]
                for column, count in sorted(
                    summary["column_counts"].items(),
                    key=lambda item: int(item[0]),
                )
            ],
        }
        for cluster_label, summary in sorted(summaries.items())
    ]


def compute_representative_payload(
    candidates: list[dict[str, object]],
    alignment_length: int,
    *,
    scope: str = "overall",
    cluster_label: int | None = None,
    method: str = REPRESENTATIVE_METHOD_BALANCED,
) -> dict[str, object]:
    method = method if method in REPRESENTATIVE_METHODS else REPRESENTATIVE_METHOD_BALANCED
    normalized_candidates = [
        candidate
        for candidate in candidates
        if interface_column_count(candidate) > 0
    ]
    with timed_step(
        "representative",
        "select representative row",
        representative_scope=scope,
        cluster_label=cluster_label if cluster_label is not None else "",
        representative_method=method,
        candidates=len(normalized_candidates),
        alignment_length=alignment_length,
    ) as timer:
        if alignment_length <= 0 or not normalized_candidates:
            timer.set(selected="")
            return {
                "scope": scope,
                "cluster_label": cluster_label,
                "representative_method": method,
                "candidate_count": len(normalized_candidates),
                "alignment_length": alignment_length,
                "representative_row_key": None,
                "row": None,
                "score": None,
            }

        import numpy as np

        gap_code = 26
        matrix = np.full(
            (len(normalized_candidates), alignment_length),
            gap_code,
            dtype=np.uint8,
        )
        for row_index, candidate in enumerate(normalized_candidates):
            sequence = str(candidate.get("aligned_sequence") or "").upper()
            if not sequence:
                continue
            encoded = sequence[:alignment_length].encode("ascii", "ignore")
            values = np.frombuffer(encoded, dtype=np.uint8)
            if values.size <= 0:
                continue
            row_view = matrix[row_index, :values.size]
            residue_mask = (values >= 65) & (values <= 90)
            row_view[residue_mask] = values[residue_mask] - 65

        if method == REPRESENTATIVE_METHOD_RESIDUE:
            selected, score_payload = _select_residue_consensus_representative(
                normalized_candidates,
                matrix,
                alignment_length,
            )
        else:
            selected, score_payload = _select_balanced_representative(
                normalized_candidates,
                matrix,
                alignment_length,
            )
        selected["_representative_score"] = score_payload

        row_payload, _alignment_length = build_interface_alignment_rows_from_metadata(
            [selected],
            alignment_length,
            row_offset=0,
            row_limit=None,
            include_total=False,
        )
        row = row_payload[0] if row_payload else None
        representative_row_key = row.get("row_key") if isinstance(row, dict) else None
        score_payload = selected.get("_representative_score")
        timer.set(
            selected=representative_row_key or "",
            score=round(float(score_payload.get("score", 0.0)), 6)
            if isinstance(score_payload, dict)
            else "",
        )
        return {
            "scope": scope,
            "cluster_label": cluster_label,
            "representative_method": method,
            "candidate_count": len(normalized_candidates),
            "alignment_length": alignment_length,
            "representative_row_key": representative_row_key,
            "row": row,
            "score": score_payload if isinstance(score_payload, dict) else None,
        }
