import { fetchJson, fetchText } from "./api.js";
import { interfaceFilePfamId } from "./interfaceModel.js";
import { appendSelectionSettingsToParams } from "./selectionSettings.js";
import { createDomainMolstarViewer } from "./molstarView.js";

export function createRepresentativeViewController({
  state,
  elements,
  THREE_TO_ONE,
  interfaceSelect,
  syncColumnLegends,
  msaColumnMaxIndex,
  ensureEmbeddingClusteringLoaded,
  representativeClusterLensData,
  representativeResidueStyles,
  renderRepresentativeClusterLegend,
  partnerInteractionDistribution,
  buildStructureResidueLookup,
  representativeLens,
  getRepresentativeRow,
  clusteringMethodLabel,
  allRepresentativeClusterLabels,
  visibleRepresentativeClusters,
  partnerColor,
}) {
  function uniprotEntryUrl(accession) {
    return `https://www.uniprot.org/uniprotkb/${encodeURIComponent(String(accession || "").trim())}`;
  }

  function pfamEntryUrl(accession) {
    return `https://www.ebi.ac.uk/interpro/entry/pfam/${encodeURIComponent(String(accession || "").trim())}/`;
  }

  function createExternalLink(label, href, className) {
    const link = document.createElement("a");
    link.href = href;
    link.target = "_blank";
    link.rel = "noreferrer";
    link.className = className;
    link.textContent = label;
    return link;
  }

  function currentPfamId() {
    const fromState = String(state.interface?.pfam_id || "").trim();
    if (fromState) {
      return fromState;
    }
    return interfaceFilePfamId(interfaceSelect.value);
  }

  function partnerPfamId(row, payload) {
    const fromRow = String(row?.partner_domain || "").trim();
    if (fromRow && fromRow !== "__all__") {
      return fromRow;
    }
    const fromPayload = String(payload?.partner || "").trim();
    if (fromPayload && fromPayload !== "__all__") {
      return fromPayload;
    }
    const matchedPartner = Array.isArray(payload?.matched_partners) ? payload.matched_partners[0] : "";
    return String(matchedPartner || "").trim();
  }

  function renderRepresentativeCopyContent(row, payload) {
    const copy = elements.representativeCopy;
    const uniprotId = String(payload?.uniprot_id || row?.protein_id || "").trim();
    const mainPfamId = currentPfamId();
    const partnerId = partnerPfamId(row, payload);
    copy.replaceChildren();

    if (!uniprotId && !mainPfamId && !partnerId) {
      copy.textContent = representativeRowLabel(row);
      return;
    }

    const content = document.createElement("span");
    content.className = "structure-title-line";
    if (uniprotId) {
      content.appendChild(
        createExternalLink(
          uniprotId,
          uniprotEntryUrl(uniprotId),
          "structure-header-link structure-header-link-protein"
        )
      );
    }
    if (mainPfamId) {
      if (content.childNodes.length > 0) {
        content.appendChild(document.createTextNode(" | "));
      }
      content.appendChild(
        createExternalLink(
          mainPfamId,
          pfamEntryUrl(mainPfamId),
          "structure-header-link structure-header-link-main-pfam"
        )
      );
    }
    if (partnerId) {
      if (content.childNodes.length > 0) {
        content.appendChild(document.createTextNode(" | "));
      }
      content.appendChild(
        createExternalLink(
          partnerId,
          pfamEntryUrl(partnerId),
          "structure-header-link structure-header-link-partner-pfam"
        )
      );
    }
    copy.appendChild(content);
  }

  function representativeRowKey(row) {
    return String(row?.interface_row_key || row?.row_key || "");
  }

  function representativePartnerForRow(row) {
    return row?.partner_domain || "__all__";
  }

  function representativeRowLabel(row) {
    return row?.display_row_key || row?.row_key || "";
  }

  function setRepresentativeHoverDetails(payload, accentLabel = "Dominant") {
    const values = payload || {
      residueId: "-",
      aminoAcid: "-",
      conservedness: "-",
      dominant: "-",
    };
    const items = [
      values.residueId,
      values.aminoAcid,
      values.conservedness,
      values.dominant,
    ];
    [...elements.representativeHoverDetails.querySelectorAll("dd")].forEach((el, index) => {
      el.textContent = String(items[index]);
    });
    elements.representativeHoverAccentLabel.textContent = accentLabel;
  }

  function setRepresentativeHoverCardMode(mode, title = null) {
    const showDetails = mode === "partners";
    elements.representativeHoverDetails.classList.toggle("hidden", !showDetails);
    elements.representativeHoverDistributionLayout.classList.add("hidden");
    elements.representativeHoverTitle.textContent = title || (mode === "cluster" ? "Cluster Region" : "Representative Residue");
  }

  function setRepresentativeHoverDistribution(entries, title, emptyMessage = "Hover a representative residue dot.") {
    elements.representativeHoverDistributionTitle.textContent = title;
    elements.representativeHoverDistributionLayout.classList.add("hidden");
    elements.representativeHoverDistributionChart.style.background = "none";
    elements.representativeHoverDistributionLegend.innerHTML = "";
    elements.representativeHoverDistributionPieLegend.innerHTML = "";

    if (!entries || entries.length === 0) {
      elements.representativeHoverDistributionLegend.innerHTML =
        `<p class="structure-hover-empty">${emptyMessage}</p>`;
      return;
    }

    elements.representativeHoverDistributionLegend.innerHTML = entries
      .map(
        (entry) => `
          <div class="representative-bar-row">
            <span class="representative-bar-label" title="${entry.label}">${entry.label}</span>
            <div class="representative-bar-track">
              <div
                class="representative-bar-fill"
                style="width: ${entry.percent}%; background: ${entry.color};"
              ></div>
            </div>
            <span class="representative-bar-value">${entry.percent}%</span>
          </div>
        `
      )
      .join("");
  }

  function hideRepresentativeHoverCard() {
    elements.representativeHoverCard.classList.add("hidden");
  }

  function resetRepresentativePanel(message = "No representative row loaded.") {
    elements.representativeCopy.textContent = message;
    state.representativeStructure = null;
    state.representativeHoveredClusterLabel = null;
    state.representativeRenderedRowKey = null;
    hideRepresentativeHoverCard();
    renderRepresentativeClusterLegend();
    if (state.representativeViewer) {
      state.representativeViewer.clear();
      state.representativeViewer.render();
    }
    syncColumnLegends();
  }

  function shouldShowRepresentativePartnerFilter() {
    return (
      representativeLens() === "partners" &&
      state.selectedPartner === "__all__" &&
      Boolean(state.interface?.partnerDomains?.length)
    );
  }

  function renderRepresentativePartnerFilter() {
    const partners = state.interface?.partnerDomains || [];
    if (partners.length === 0) {
      elements.representativePartnerFilterList.innerHTML = "";
      return;
    }

    elements.representativePartnerFilterList.innerHTML = partners
      .map(
        (partner) => {
          const interfaceCount = Number(state.interface?.partnerInterfaceCounts?.get(partner) || 0);
          const partnerLabel = `${partner} (${interfaceCount})`;
          return `
          <button
            class="representative-partner-filter-option ${state.representativeVisiblePartners.has(partner) ? "active" : "inactive"}"
            type="button"
            data-partner-domain="${partner}"
            aria-pressed="${state.representativeVisiblePartners.has(partner) ? "true" : "false"}"
            title="${partnerLabel}"
          >
            <span class="representative-partner-filter-swatch" style="background: ${partnerColor(partner)};"></span>
            <span class="representative-partner-filter-name">${partnerLabel}</span>
          </button>
        `;
        }
      )
      .join("");
  }

  function syncRepresentativePartnerFilterVisibility() {
    const visible = shouldShowRepresentativePartnerFilter();
    elements.representativePartnerFilter.classList.toggle("hidden", !visible);
  }

  function positionRepresentativeHoverCard() {
    if (elements.representativeHoverCard.classList.contains("hidden")) {
      return;
    }

    const stage = elements.representativeHoverCard.parentElement;
    if (!elements.representativeViewerRoot || !stage) {
      return;
    }

    const stageRect = stage.getBoundingClientRect();
    const viewerRect = elements.representativeViewerRoot.getBoundingClientRect();
    const offset = 14;
    const viewerLeftInStage = viewerRect.left - stageRect.left;
    const viewerTopInStage = viewerRect.top - stageRect.top;
    if (representativeLens() === "cluster") {
      const left = Math.max(12, viewerLeftInStage + 12);
      const top = Math.max(
        12,
        viewerTopInStage + viewerRect.height - elements.representativeHoverCard.offsetHeight - 12
      );
      elements.representativeHoverCard.style.left = `${left}px`;
      elements.representativeHoverCard.style.top = `${top}px`;
      return;
    }
    if (!state.representativePointer) {
      return;
    }
    const maxLeft = Math.max(
      12,
      viewerLeftInStage + viewerRect.width - elements.representativeHoverCard.offsetWidth - 12
    );
    const maxTop = Math.max(
      12,
      viewerTopInStage + viewerRect.height - elements.representativeHoverCard.offsetHeight - 12
    );
    const left = Math.min(
      maxLeft,
      Math.max(12, viewerLeftInStage + state.representativePointer.x - viewerRect.left + offset)
    );
    const top = Math.min(
      maxTop,
      Math.max(12, viewerTopInStage + state.representativePointer.y - viewerRect.top + offset)
    );

    elements.representativeHoverCard.style.left = `${left}px`;
    elements.representativeHoverCard.style.top = `${top}px`;
  }

  function syncRepresentativeLensControls() {
    if (state.selectedPartner !== "__all__" && state.representativeLens === "partners") {
      state.representativeLens = "interface";
    }

    [...elements.representativeLensGroup.querySelectorAll("[data-lens]")].forEach((button) => {
      const lens = button.dataset.lens;
      const visible = lens !== "partners" || state.selectedPartner === "__all__";
      button.classList.toggle("hidden", !visible);
      const isActive = visible && lens === state.representativeLens;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
    syncRepresentativePartnerFilterVisibility();
    renderRepresentativeClusterLegend();
    syncColumnLegends();
  }

  function getRepresentativeViewer() {
    if (!state.representativeViewer) {
      state.representativeViewer = createDomainMolstarViewer(elements.representativeViewerRoot, {
        kind: "representative",
      });
    }
    return state.representativeViewer;
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

  function representativeDomainSelection(structurePayload) {
    return {
      resi: mainFragmentResidues(structurePayload),
    };
  }

  function formatRepresentativeHover(hover, residueLookup) {
    const residueId = Number(hover?.residueId);
    const mapped = residueLookup.get(residueId);
    const residueName = String(hover?.residueName || "").toUpperCase();
    const oneLetter = mapped?.aminoAcid || THREE_TO_ONE[residueName] || "?";

    return {
      residueId: Number.isFinite(residueId) ? residueId : hover?.residueId ?? "-",
      aminoAcid: residueName ? `${oneLetter} (${residueName})` : oneLetter,
      conservedness:
        mapped?.conservedness === "-" || mapped?.conservedness === undefined
          ? "-"
          : `${mapped.conservedness}%`,
      columnIndex: mapped?.columnIndex ?? null,
    };
  }

  function clearRepresentativeClusterHover(rerender = true) {
    const hadHoveredCluster = state.representativeHoveredClusterLabel !== null;
    state.representativeHoveredClusterLabel = null;
    state.representativeViewer?.clearHighlight?.();
    if (hadHoveredCluster && rerender && state.representativeStructure && representativeLens() === "cluster") {
      renderRepresentativeClusterLegend(
        representativeClusterLensData(state.representativeStructure.row)
      );
    }
  }

  function handleRepresentativeHover(hoverPayload, row, clusterLensData = null) {
    if (hoverPayload?.pointer) {
      state.representativePointer = hoverPayload.pointer;
    }
    if (representativeLens() === "cluster") {
      const residueId = Number(hoverPayload?.residueId);
      const clusterResidue = clusterLensData?.clusterByResidueId?.get(residueId);
      if (!clusterResidue) {
        clearRepresentativeClusterHover(true);
        hideRepresentativeHoverCard();
        return;
      }
      if (state.representativeHoveredClusterLabel !== clusterResidue.clusterLabel) {
        state.representativeHoveredClusterLabel = clusterResidue.clusterLabel;
        renderRepresentativeClusterLegend(clusterLensData);
      }
      const cluster = clusterLensData?.clusters?.find(
        (entry) => entry.clusterLabel === clusterResidue.clusterLabel
      );
      state.representativeViewer?.highlightResidues?.(cluster?.residueIds || [residueId]);
      elements.representativeHoverCard.classList.remove("hidden");
      setRepresentativeHoverCardMode("cluster", clusterResidue.label);
      setRepresentativeHoverDistribution(
        clusterResidue.distribution,
        "Percent Of Domain Rows In Cluster",
        "Hover a representative cluster region."
      );
      positionRepresentativeHoverCard();
      return;
    }

    if (representativeLens() !== "partners") {
      hideRepresentativeHoverCard();
      return;
    }
    const residueLookup = buildStructureResidueLookup(row);
    const hover = formatRepresentativeHover(hoverPayload, residueLookup);
    if (hover.columnIndex === null || hover.columnIndex === undefined) {
      hideRepresentativeHoverCard();
      return;
    }
    elements.representativeHoverCard.classList.remove("hidden");
    setRepresentativeHoverCardMode("partners");
    const distribution = partnerInteractionDistribution(hover.columnIndex);
    setRepresentativeHoverDetails({
      ...hover,
      dominant: distribution[0]?.partnerDomain || "None",
    });
    setRepresentativeHoverDistribution(distribution, "Partner Distribution");
    positionRepresentativeHoverCard();
  }

  function clearRepresentativeHover() {
    clearRepresentativeClusterHover(true);
    hideRepresentativeHoverCard();
  }

  async function renderRepresentativeStructure() {
    const representative = state.representativeStructure;
    if (!representative) {
      return;
    }

    if (representativeLens() === "cluster" && !state.embeddingClusteringLoading) {
      void ensureEmbeddingClusteringLoaded();
    }

    hideRepresentativeHoverCard();

    const viewer = getRepresentativeViewer();
    const clusterLensData =
      representativeLens() === "cluster" ? representativeClusterLensData(representative.row) : null;
    const residueStyles = representativeResidueStyles(representative.row, clusterLensData);
    const shouldPreserveView =
      state.representativeRenderedRowKey === representative.row.row_key &&
      typeof viewer.getView === "function" &&
      typeof viewer.setView === "function";
    const previousView = shouldPreserveView ? viewer.getView() : null;
    const domainSelection = representativeDomainSelection(representative.payload);
    await viewer.loadStructure({
      modelText: representative.modelText,
      payload: representative.payload,
      format: representative.payload.model_format || "pdb",
      label: representativeRowLabel(representative.row) || "Representative structure",
      mode: "representative",
      residueStyles,
      clusterLensData,
      representativeLens: representativeLens(),
      displaySettings: state.structureDisplaySettings,
      onHover: (hover) => handleRepresentativeHover(hover, representative.row, clusterLensData),
      onHoverEnd: clearRepresentativeHover,
    });
    viewer.resize();
    if (previousView) {
      viewer.setView(previousView);
    } else {
      if (typeof viewer.focusResiduesStable === "function") {
        viewer.focusResiduesStable(domainSelection.resi, 8);
      } else {
        viewer.focusResidues(domainSelection.resi, 8);
      }
    }
    viewer.render();
    state.representativeRenderedRowKey = representative.row.row_key;
    renderRepresentativeClusterLegend(clusterLensData);

    renderRepresentativeCopyContent(representative.row, representative.payload);
    syncColumnLegends();
  }

  async function loadRepresentativeStructure() {
    const row = getRepresentativeRow();
    if (!row || !interfaceSelect.value) {
      resetRepresentativePanel();
      return;
    }

    const requestedPartner = representativePartnerForRow(row);
    if (
      state.representativeStructure?.row?.row_key === row.row_key &&
      state.representativeStructure?.requestedPartner === requestedPartner
    ) {
      await renderRepresentativeStructure();
      return;
    }

    elements.representativeCopy.textContent = `Loading ${representativeRowLabel(row)}`;
    const requestId = state.representativeRequestId + 1;
    state.representativeRequestId = requestId;
    const requestRowKey = representativeRowKey(row);
    const alignmentReferenceRowKey =
      state.representativeAnchorRowKey && state.representativeAnchorRowKey !== requestRowKey
        ? state.representativeAnchorRowKey
        : "";

    const params = new URLSearchParams({
      interface_file: interfaceSelect.value,
      row_key: requestRowKey,
      uniprot_id: String(row.protein_id),
      fragment_key: String(row.fragment_key),
      partner: String(requestedPartner),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (alignmentReferenceRowKey) {
      params.set("align_to_row_key", alignmentReferenceRowKey);
    }
    const payload = await fetchJson(`/api/structure-preview?${params.toString()}`);
    const modelText = await fetchText(payload.model_url);
    if (requestId !== state.representativeRequestId) {
      return;
    }

    state.representativeStructure = {
      row,
      payload,
      modelText,
      requestedPartner,
    };
    if (!state.representativeAnchorRowKey) {
      state.representativeAnchorRowKey = requestRowKey;
    }
    await renderRepresentativeStructure();
  }

  return {
    clearRepresentativeClusterHover,
    getRepresentativeViewer,
    loadRepresentativeStructure,
    positionRepresentativeHoverCard,
    renderRepresentativePartnerFilter,
    renderRepresentativeStructure,
    resetRepresentativePanel,
    syncRepresentativeLensControls,
    syncRepresentativePartnerFilterVisibility,
  };
}
