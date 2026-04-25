export function interfaceFileStem(interfaceFile) {
  return String(interfaceFile || "")
    .replace(/\.json\.gz$/i, "")
    .replace(/\.json$/i, "");
}

export function interfaceFilePfamId(interfaceFile) {
  return interfaceFileStem(interfaceFile).split("_", 1)[0] || "";
}

export function buildPairs(files) {
  return (files.interface_files || [])
    .map((interfaceFile) => {
      const pfamId = interfaceFilePfamId(interfaceFile);
      return {
        pfamId,
        msaFile: `${pfamId}.json`,
        interfaceFile,
        label: `${pfamId} | ${interfaceFile}`,
      };
    })
    .sort((a, b) => a.label.localeCompare(b.label));
}

export function interactionRowKey(rowKey, partnerDomain) {
  const baseRowKey = String(rowKey || "");
  const domain = String(partnerDomain || "");
  return domain ? `${baseRowKey}@@${domain}` : baseRowKey;
}

export function parseInterfaceRowKey(rowKey) {
  const parts = String(rowKey || "").split("_", 3);
  const proteinId = parts[0] || "";
  const fragmentKey = parts[1] || "";
  const partnerFragmentKey = parts[2] || "";
  return {
    interfaceRowKey: String(rowKey || ""),
    proteinId,
    fragmentKey,
    partnerFragmentKey,
  };
}

export function parseInteractionRowKey(rowKey) {
  const [interfaceRowKey = "", partnerDomain = ""] = String(rowKey || "").split("@@", 2);
  return {
    interfaceRowKey,
    partnerDomain,
    ...parseInterfaceRowKey(interfaceRowKey),
  };
}

export function buildOverlayMaps(interfacePayload) {
  const overlayByRow = new Map();
  const overlayByInteractionRow = new Map();
  const partnerDomains = Object.keys(interfacePayload || {}).sort();
  const partnerColumnStats = new Map();
  const partnerInterfaceCounts = new Map();
  for (const partnerDomain of partnerDomains) {
    const entries = interfacePayload[partnerDomain];
    partnerInterfaceCounts.set(partnerDomain, Object.keys(entries || {}).length);
    const columnCounts = new Map();
    let denominator = 0;
    for (const [rowKey, payload] of Object.entries(entries)) {
      let rowState = overlayByRow.get(rowKey);
      if (!rowState) {
        rowState = {
          all: { interface: new Set(), surface: new Set() },
          byPartner: new Map(),
        };
        overlayByRow.set(rowKey, rowState);
      }
      const interactionKey = interactionRowKey(rowKey, partnerDomain);
      const partnerState = {
        interface: new Set(payload.interface_msa_columns_a || []),
        surface: new Set(payload.surface_msa_columns_a || []),
      };
      rowState.byPartner.set(partnerDomain, partnerState);
      overlayByInteractionRow.set(interactionKey, {
        all: partnerState,
        byPartner: new Map([[partnerDomain, partnerState]]),
      });
      if (partnerState.interface.size > 0) {
        denominator += 1;
        for (const col of partnerState.interface) {
          columnCounts.set(col, (columnCounts.get(col) || 0) + 1);
        }
      }
      for (const col of partnerState.interface) {
        rowState.all.interface.add(col);
      }
      for (const col of partnerState.surface) {
        rowState.all.surface.add(col);
      }
    }
    partnerColumnStats.set(partnerDomain, { denominator, columnCounts });
  }
  const allColumnCounts = new Map();
  let allDenominator = 0;
  for (const rowState of overlayByRow.values()) {
    if (rowState.all.interface.size === 0) {
      continue;
    }
    allDenominator += 1;
    for (const col of rowState.all.interface) {
      allColumnCounts.set(col, (allColumnCounts.get(col) || 0) + 1);
    }
  }
  return {
    overlayByRow,
    overlayByInteractionRow,
    partnerDomains,
    partnerInterfaceCounts,
    partnerColumnStats,
    allColumnStats: {
      denominator: allDenominator,
      columnCounts: allColumnCounts,
    },
  };
}
