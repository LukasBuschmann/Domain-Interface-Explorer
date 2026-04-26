type InterfaceFilesPayload = {
  interface_files?: string[];
};

type InterfaceResiduePayload = {
  interface_msa_columns_a?: Iterable<number>;
  surface_msa_columns_a?: Iterable<number>;
};

type InterfacePayload = Record<string, Record<string, InterfaceResiduePayload>>;

export function interfaceFileStem(interfaceFile: unknown) {
  return String(interfaceFile || "")
    .replace(/\.json\.gz$/i, "")
    .replace(/\.json$/i, "");
}

export function interfaceFilePfamId(interfaceFile: unknown) {
  return interfaceFileStem(interfaceFile).split("_", 1)[0] || "";
}

export function buildPairs(files: InterfaceFilesPayload = {}) {
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

export function interactionRowKey(rowKey: unknown, partnerDomain: unknown) {
  const baseRowKey = String(rowKey || "");
  const domain = String(partnerDomain || "");
  return domain ? `${baseRowKey}@@${domain}` : baseRowKey;
}

export function parseInterfaceRowKey(rowKey: unknown) {
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

export function parseInteractionRowKey(rowKey: unknown) {
  const [interfaceRowKey = "", partnerDomain = ""] = String(rowKey || "").split("@@", 2);
  return {
    interfaceRowKey,
    partnerDomain,
    ...parseInterfaceRowKey(interfaceRowKey),
  };
}

export function buildOverlayMaps(interfacePayload: InterfacePayload = {}) {
  const overlayByRow = new Map();
  const overlayByInteractionRow = new Map();
  const partnerDomains = Object.keys(interfacePayload || {}).sort();
  const partnerColumnStats = new Map();
  const partnerInterfaceCounts = new Map();
  for (const partnerDomain of partnerDomains) {
    const entries = interfacePayload[partnerDomain] || {};
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
