export function activeConservationVector(msa) {
  if (!msa) {
    return [];
  }
  return msa.clean_column_identity || [];
}

export function interactiveRowIndexes(msa, interfaceState, overlayStateForRow, useSelectedPartner = false) {
  if (!msa || !interfaceState) {
    return [];
  }
  const indexes = [];
  msa.rows.forEach((row, index) => {
    const overlay = useSelectedPartner
      ? overlayStateForRow(row)
      : interfaceState.overlayByInteractionRow?.get(row.row_key)?.all;
    if (overlay && overlay.interface.size > 0) {
      indexes.push(index);
    }
  });
  return indexes;
}

export function computeRepresentativeRowKey(msa, interfaceState, overlayStateForRow) {
  if (!msa || !interfaceState) {
    return null;
  }
  const candidateIndexes = interactiveRowIndexes(msa, interfaceState, overlayStateForRow, true);
  if (candidateIndexes.length === 0) {
    return null;
  }
  const consensus = new Array(msa.alignment_length).fill(null);
  for (let column = 0; column < msa.alignment_length; column += 1) {
    const counts = new Map();
    for (const rowIndex of candidateIndexes) {
      const residue = msa.rows[rowIndex].aligned_sequence[column] || "";
      if (!/^[A-Za-z]$/.test(residue)) {
        continue;
      }
      const key = residue.toUpperCase();
      counts.set(key, (counts.get(key) || 0) + 1);
    }
    if (counts.size === 0) {
      continue;
    }
    consensus[column] = [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0][0];
  }
  let bestRowKey = null;
  let bestMatches = -1;
  let bestCoverage = -1;
  for (const rowIndex of candidateIndexes) {
    const row = msa.rows[rowIndex];
    let matches = 0;
    let coverage = 0;
    for (let column = 0; column < msa.alignment_length; column += 1) {
      const consensusResidue = consensus[column];
      if (!consensusResidue) {
        continue;
      }
      const residue = row.aligned_sequence[column] || "";
      if (!/^[A-Za-z]$/.test(residue)) {
        continue;
      }
      coverage += 1;
      if (residue.toUpperCase() === consensusResidue) {
        matches += 1;
      }
    }
    if (
      matches > bestMatches ||
      (matches === bestMatches && coverage > bestCoverage) ||
      (matches === bestMatches &&
        coverage === bestCoverage &&
        (bestRowKey === null || row.row_key.localeCompare(bestRowKey) < 0))
    ) {
      bestMatches = matches;
      bestCoverage = coverage;
      bestRowKey = row.row_key;
    }
  }
  return bestRowKey;
}

export function buildStructureResidueLookup(row, conservationVector) {
  const lookup = new Map();
  for (let index = 0; index < row.residueIds.length; index += 1) {
    const residueId = row.residueIds[index];
    if (residueId === null) {
      continue;
    }
    lookup.set(Number(residueId), {
      residueId: Number(residueId),
      aminoAcid: row.aligned_sequence[index],
      conservedness: conservationVector[index] ?? "-",
      columnIndex: index,
    });
  }
  return lookup;
}

export function topResiduesForColumn(msa, filteredRowIndexes, columnIndex, selectedResidue) {
  if (!msa || columnIndex === null || columnIndex === undefined) {
    return [];
  }
  const counts = new Map();
  const denominator = filteredRowIndexes.length;
  if (denominator === 0) {
    return [];
  }
  for (const rowIndex of filteredRowIndexes) {
    const residue = msa.rows[rowIndex]?.aligned_sequence?.[columnIndex];
    if (!/^[A-Za-z]$/.test(residue || "")) {
      continue;
    }
    const key = residue.toUpperCase();
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  const selectedKey = String(selectedResidue || "").toUpperCase();
  const ranked = [...counts.entries()]
    .map(([residue, count]) => ({
      residue,
      count,
      percent: Math.round((count / denominator) * 100),
      isSelected: residue === selectedKey,
    }))
    .sort((a, b) => b.count - a.count || a.residue.localeCompare(b.residue));
  const topFive = ranked.slice(0, 5);
  if (selectedKey && !topFive.some((entry) => entry.residue === selectedKey)) {
    const selectedEntry = ranked.find((entry) => entry.residue === selectedKey);
    if (selectedEntry) {
      topFive.pop();
      topFive.push(selectedEntry);
      topFive.sort((a, b) => b.count - a.count || a.residue.localeCompare(b.residue));
    }
  }
  return topFive;
}

export function columnStateDistribution(msa, filteredRowIndexes, columnIndex, overlayStateForRow) {
  if (!msa || columnIndex === null || columnIndex === undefined) {
    return [];
  }
  const totals = { interface: 0, surface: 0, core: 0, gap: 0 };
  const denominator = filteredRowIndexes.length;
  if (denominator === 0) {
    return [];
  }
  for (const rowIndex of filteredRowIndexes) {
    const row = msa.rows[rowIndex];
    const residue = row?.aligned_sequence?.[columnIndex];
    if (!/^[A-Za-z]$/.test(residue || "")) {
      totals.gap += 1;
      continue;
    }
    const overlay = overlayStateForRow(row);
    if (overlay?.interface.has(columnIndex)) {
      totals.interface += 1;
      continue;
    }
    if (overlay?.surface.has(columnIndex)) {
      totals.surface += 1;
      continue;
    }
    totals.core += 1;
  }
  return [
    { key: "interface", label: "Interface", color: "#bc402d", count: totals.interface, fraction: totals.interface / denominator, percent: Math.round((totals.interface / denominator) * 100) },
    { key: "surface", label: "Surface", color: "#d7a84c", count: totals.surface, fraction: totals.surface / denominator, percent: Math.round((totals.surface / denominator) * 100) },
    { key: "core", label: "Core", color: "#817a71", count: totals.core, fraction: totals.core / denominator, percent: Math.round((totals.core / denominator) * 100) },
    { key: "gap", label: "Gap", color: "#d7d1c4", count: totals.gap, fraction: totals.gap / denominator, percent: Math.round((totals.gap / denominator) * 100) },
  ];
}
