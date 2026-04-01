import { fetchJson, fetchText } from "./api.js";
import { appendSelectionSettingsToParams } from "./selectionSettings.js";

export function createStructureViewController({
  state,
  elements,
  THREE_TO_ONE,
  interfaceSelect,
  setLoading,
  hideLoading,
  buildStructureResidueLookup,
  columnResidueStyles,
  msaColumnMaxIndex,
  topResiduesForColumn,
  columnStateDistribution,
  syncColumnLegends,
  getSelectedRow,
}) {
  function structureRowKey(row) {
    return String(row?.interface_row_key || row?.row_key || "");
  }

  function structurePartnerForRow(row) {
    return row?.partner_domain || "__all__";
  }

  function structureRowLabel(row) {
    return row?.display_row_key || row?.row_key || "";
  }

  function resetStructurePanel(message = "Click a row name or use the button to open the structure.") {
    elements.structureStatus.textContent = message;
    elements.structureModalSubtitle.textContent = message;
    elements.structureModalStatus.textContent =
      "Whole protein: gray transparent. Main domain: gray. Main surface/interface: orange and red. Partner domain: muted blue with stronger blue interaction layers.";
    state.structureResidueLookup = null;
    state.structureData = null;
    elements.structureHoverCard.classList.add("hidden");
    setStructureHoverDetails(null);
    setStructureHoverHistogram(null);
    setStructureHoverDistribution(null);
    syncColumnLegends();
  }

  function handleStructureLoadFailure(error) {
    elements.loadingPanel.classList.remove("hidden");
    elements.loadingLabel.textContent = "Structure load failed";
    elements.loadingDetail.textContent = error.message;
    elements.progressBar.style.width = "100%";
    elements.structureStatus.textContent = error.message;
    elements.structureModalSubtitle.textContent = error.message;
  }

  function setStructureHoverDetails(payload) {
    const values = payload || {
      residueId: "-",
      aminoAcid: "-",
      conservedness: "-",
      columnIndex: null,
    };
    const items = [
      values.residueId,
      values.aminoAcid,
      values.conservedness,
      values.columnIndex === null || values.columnIndex === undefined ? "-" : values.columnIndex,
    ];
    [...elements.structureHoverDetails.querySelectorAll("dd")].forEach((el, index) => {
      el.textContent = String(items[index]);
    });
  }

  function setStructureHoverHistogram(entries) {
    elements.structureHoverHistogram.innerHTML = "";
    if (!entries?.length) {
      elements.structureHoverHistogram.innerHTML =
        '<p class="structure-hover-empty">No residue frequencies for this column.</p>';
      return;
    }

    const maxPercent = Math.max(...entries.map((entry) => entry.percent), 1);
    for (const entry of entries) {
      const row = document.createElement("div");
      row.className = `structure-hist-row${entry.isSelected ? " selected" : ""}`;
      const label = document.createElement("span");
      label.className = "structure-hist-label";
      label.textContent = entry.residue;
      const bar = document.createElement("div");
      bar.className = "structure-hist-bar";
      const fill = document.createElement("div");
      fill.className = "structure-hist-fill";
      fill.style.width = `${Math.max(4, (entry.percent / maxPercent) * 100)}%`;
      const value = document.createElement("span");
      value.className = "structure-hist-value";
      value.textContent = `${entry.percent}%`;
      bar.append(fill);
      row.append(label, bar, value);
      elements.structureHoverHistogram.append(row);
    }
  }

  function setStructureHoverDistribution(entries) {
    elements.structureHoverDistributionChart.innerHTML = "";
    elements.structureHoverDistributionLegend.innerHTML = "";
    if (!entries?.length) {
      elements.structureHoverDistributionChart.style.background = "none";
      elements.structureHoverDistributionLegend.innerHTML =
        '<p class="structure-hover-empty">No interaction-state data for this column.</p>';
      return;
    }

    const total = entries.reduce((sum, entry) => sum + entry.count, 0);
    const slices = [];
    let offset = 0;
    for (const entry of entries) {
      const share = total > 0 ? (entry.count / total) * 100 : 0;
      slices.push(`${entry.color} ${offset}% ${offset + share}%`);
      offset += share;

      const item = document.createElement("div");
      item.className = "structure-distribution-row";
      item.innerHTML = `
        <span class="structure-distribution-swatch" style="background:${entry.color}"></span>
        <span class="structure-distribution-label">${entry.label}</span>
        <span class="structure-distribution-value">${entry.percent}%</span>
      `;
      elements.structureHoverDistributionLegend.append(item);
    }
    elements.structureHoverDistributionChart.style.background = `conic-gradient(${slices.join(", ")})`;
  }

  function openStructureModal() {
    elements.structureModal.classList.remove("hidden");
    elements.structureModal.setAttribute("aria-hidden", "false");
  }

  function closeStructureModal() {
    elements.structureModal.classList.add("hidden");
    elements.structureModal.setAttribute("aria-hidden", "true");
  }

  function getStructureViewer() {
    if (!window.$3Dmol) {
      throw new Error("3Dmol.js is not available in the browser.");
    }
    if (!state.structureViewer) {
      state.structureViewer = window.$3Dmol.createViewer(elements.structureViewerRoot, {
        backgroundColor: "white",
      });
    }
    return state.structureViewer;
  }

  function mainFragmentResidues(structurePayload) {
    if (Array.isArray(structurePayload?.fragment_residue_ids) && structurePayload.fragment_residue_ids.length > 0) {
      return structurePayload.fragment_residue_ids;
    }
    return Array.from(
      { length: structurePayload.fragment_end - structurePayload.fragment_start + 1 },
      (_value, index) => structurePayload.fragment_start + index
    );
  }

  function applyStructureStyles(viewer, structurePayload, options = {}) {
    const columnView = Boolean(options.columnView ?? state.structureColumnView);
    const residueLookup = options.residueLookup || state.structureResidueLookup || new Map();
    const fragmentResidues = mainFragmentResidues(structurePayload);
    viewer.setStyle({}, { cartoon: { color: "#c7c3bc", opacity: 0.28 } });
    if (columnView) {
      const residuesByColor = new Map();
      for (const style of columnResidueStyles(residueLookup)) {
        const bucket = residuesByColor.get(style.color) || [];
        bucket.push(style.residueId);
        residuesByColor.set(style.color, bucket);
      }
      for (const [color, residueIds] of residuesByColor.entries()) {
        viewer.setStyle({ resi: residueIds }, { cartoon: { color, opacity: 1.0 } });
      }
      if (structurePayload.partner_fragment_residue_ids.length > 0) {
        viewer.setStyle(
          { resi: structurePayload.partner_fragment_residue_ids },
          { cartoon: { color: "#b8c9dc", opacity: 0.96 } }
        );
      }
      if (structurePayload.partner_surface_residue_ids.length > 0) {
        viewer.setStyle(
          { resi: structurePayload.partner_surface_residue_ids },
          { cartoon: { color: "#5b9fe3", opacity: 1.0 } }
        );
      }
      if (structurePayload.partner_interface_residue_ids.length > 0) {
        viewer.setStyle(
          { resi: structurePayload.partner_interface_residue_ids },
          { cartoon: { color: "#0b3f78", opacity: 1.0 } }
        );
      }
      viewer.addStyle(
        { resi: fragmentResidues, atom: "CA" },
        { sphere: { color: "#696157", opacity: 0.62, radius: 0.56 } }
      );
      return;
    }
    viewer.setStyle({ resi: fragmentResidues }, { cartoon: { color: "#8f8a82", opacity: 1.0 } });
    if (structurePayload.surface_residue_ids.length > 0) {
      viewer.setStyle(
        { resi: structurePayload.surface_residue_ids },
        { cartoon: { color: "#d7a84c", opacity: 1.0 } }
      );
    }
    if (structurePayload.interface_residue_ids.length > 0) {
      viewer.setStyle(
        { resi: structurePayload.interface_residue_ids },
        { cartoon: { color: "#bc402d", opacity: 1.0 } }
      );
    }
    if (structurePayload.partner_fragment_residue_ids.length > 0) {
      viewer.setStyle(
        { resi: structurePayload.partner_fragment_residue_ids },
        { cartoon: { color: "#b8c9dc", opacity: 0.96 } }
      );
    }
    if (structurePayload.partner_surface_residue_ids.length > 0) {
      viewer.setStyle(
        { resi: structurePayload.partner_surface_residue_ids },
        { cartoon: { color: "#5b9fe3", opacity: 1.0 } }
      );
    }
    if (structurePayload.partner_interface_residue_ids.length > 0) {
      viewer.setStyle(
        { resi: structurePayload.partner_interface_residue_ids },
        { cartoon: { color: "#0b3f78", opacity: 1.0 } }
      );
    }
    viewer.addStyle(
      { resi: fragmentResidues, atom: "CA" },
      { sphere: { color: "#696157", opacity: 0.62, radius: 0.56 } }
    );
  }

  function formatStructureHover(atom) {
    const residueId = Number(atom?.resi);
    const mapped = state.structureResidueLookup?.get(residueId);
    const residueName = String(atom?.resn || "").toUpperCase();
    const oneLetter = mapped?.aminoAcid || THREE_TO_ONE[residueName] || "?";

    return {
      residueId: Number.isFinite(residueId) ? residueId : atom?.resi ?? "-",
      aminoAcid: residueName ? `${oneLetter} (${residueName})` : oneLetter,
      conservedness:
        mapped?.conservedness === "-" || mapped?.conservedness === undefined
          ? "-"
          : `${mapped.conservedness}%`,
      columnIndex: mapped?.columnIndex ?? null,
      residueLetter: oneLetter,
    };
  }

  function attachStructureHover(viewer, structurePayload) {
    const fragmentResidues = mainFragmentResidues(structurePayload);

    viewer.setHoverable({}, false);
    viewer.setHoverable(
      { resi: fragmentResidues, atom: "CA" },
      true,
      (atom) => {
        const hover = formatStructureHover(atom);
        elements.structureHoverCard.classList.remove("hidden");
        setStructureHoverDetails(hover);
        setStructureHoverHistogram(topResiduesForColumn(hover.columnIndex, hover.residueLetter));
        setStructureHoverDistribution(columnStateDistribution(hover.columnIndex));
      },
      () => {
        elements.structureHoverCard.classList.add("hidden");
        setStructureHoverDetails(null);
        setStructureHoverHistogram(null);
        setStructureHoverDistribution(null);
      }
    );
  }

  function renderInteractiveStructure() {
    const structure = state.structureData;
    if (!structure) {
      return;
    }

    const { row, payload, modelText } = structure;
    const viewer = getStructureViewer();
    const shouldPreserveView =
      Boolean(state.structureRenderedRowKey) &&
      typeof viewer.getView === "function" &&
      typeof viewer.setView === "function" &&
      (
        row.row_key === state.structureRenderedRowKey ||
        (
          Boolean(state.structureAnchorRowKey) &&
          (
            structureRowKey(row) === state.structureAnchorRowKey ||
            payload.alignment_reference_row_key === state.structureAnchorRowKey
          )
        )
      );
    const previousView = shouldPreserveView ? viewer.getView() : null;
    const shouldReloadModel = state.structureRenderedRowKey !== row.row_key;
    if (shouldReloadModel) {
      viewer.clear();
      viewer.addModel(modelText, payload.model_format || "pdb");
    } else {
      viewer.setStyle({}, {});
    }
    state.structureResidueLookup = buildStructureResidueLookup(row);
    applyStructureStyles(viewer, payload);
    attachStructureHover(viewer, payload);
    viewer.resize();
    const domainSelection = { resi: mainFragmentResidues(payload) };
    if (previousView) {
      viewer.setView(previousView);
    } else {
      if (typeof viewer.center === "function") {
        viewer.center(domainSelection);
      }
      viewer.zoomTo(domainSelection, 8);
    }
    viewer.render();
    state.structureRenderedRowKey = row.row_key;
    if (!state.structureAnchorRowKey) {
      state.structureAnchorRowKey = structureRowKey(row);
    }

    const alignmentNote = payload.alignment_reference_row_key
      ? ` | Aligned to reference structure ${payload.alignment_reference_row_key} (${payload.alignment_method || "alignment"}).`
      : payload.alignment_error
        ? ` | Alignment fallback: ${payload.alignment_error}`
        : "";
    const lensNote = state.structureColumnView
      ? ` | Main domain hues follow MSA columns 0-${msaColumnMaxIndex()}.`
      : "";
    elements.structureStatus.textContent =
      `Interactive structure ready for ${structureRowLabel(row)}. Partners: ${payload.matched_partners.join(", ") || "none"}${lensNote}${alignmentNote}`;
    elements.structureModalTitle.textContent = `Interactive Structure: ${structureRowLabel(row)}`;
    const partnerRanges = payload.partner_fragment_ranges?.join(", ") || "none";
    elements.structureModalSubtitle.textContent =
      `${payload.uniprot_id} | fragment ${payload.fragment_key} | ` +
      `partners: ${payload.matched_partners.join(", ") || "none"} | ` +
      `partner range: ${partnerRanges}${alignmentNote}`;
    elements.structureModalStatus.textContent = state.structureColumnView
      ? `Whole protein: gray transparent. Main domain: rainbow by MSA column 0-${msaColumnMaxIndex()}. Partner domain keeps the blue context layers.`
      : `Main interface: ${payload.interface_residue_ids.length} | ` +
        `Main surface: ${payload.surface_residue_ids.length} | ` +
        `Partner interface: ${payload.partner_interface_residue_ids.length} | ` +
        `Partner surface: ${payload.partner_surface_residue_ids.length} | ` +
        `AlphaFold: ${payload.model_source || "unknown"}`;
    syncColumnLegends();
  }

  async function loadInteractiveStructure() {
    const row = getSelectedRow();
    if (!row || !interfaceSelect.value) {
      return;
    }

    setLoading(10, "Loading structure", `Preparing ${structureRowLabel(row)}`);
    elements.structureStatus.textContent = "Loading structure...";
    openStructureModal();
    await new Promise((resolve) => window.requestAnimationFrame(resolve));

    const requestId = state.structureRequestId + 1;
    state.structureRequestId = requestId;
    const requestRowKey = structureRowKey(row);
    const alignmentReferenceRowKey =
      state.structureAnchorRowKey && state.structureAnchorRowKey !== requestRowKey
        ? state.structureAnchorRowKey
        : "";

    const params = new URLSearchParams({
      interface_file: interfaceSelect.value,
      row_key: requestRowKey,
      uniprot_id: String(row.protein_id || ""),
      fragment_key: String(row.fragment_key || ""),
      partner: String(structurePartnerForRow(row)),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (alignmentReferenceRowKey) {
      params.set("align_to_row_key", alignmentReferenceRowKey);
    }
    const payload = await fetchJson(`/api/structure-preview?${params.toString()}`);

    setLoading(50, "Loading structure", `Fetching model for ${payload.uniprot_id}`);
    const modelText = await fetchText(payload.model_url);
    if (requestId !== state.structureRequestId) {
      return;
    }

    setLoading(80, "Rendering structure", `Applying cartoon styles for ${structureRowLabel(row)}`);
    state.structureData = {
      row,
      payload,
      modelText,
    };
    renderInteractiveStructure();
    setLoading(100, "Structure ready", structureRowLabel(row));
    window.setTimeout(hideLoading, 250);
  }

  return {
    applyStructureStyles,
    closeStructureModal,
    getStructureViewer,
    handleStructureLoadFailure,
    loadInteractiveStructure,
    openStructureModal,
    renderInteractiveStructure,
    resetStructurePanel,
  };
}
