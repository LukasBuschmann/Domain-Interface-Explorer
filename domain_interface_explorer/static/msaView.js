import {
  CELL_WIDTH,
  DEFAULT_CLUSTERING_SETTINGS,
  DEFAULT_EMBEDDING_SETTINGS,
  HEADER_HEIGHT,
  LABEL_WIDTH,
  ROW_HEIGHT,
  TEXT_FONT,
} from "./constants.js";
import { fetchJson } from "./api.js";
import { interactionRowKey } from "./interfaceModel.js";
import {
  appendSelectionSettingsToParams,
  normalizeSelectionSettings,
} from "./selectionSettings.js";

export function createMsaViewController({
  state,
  elements,
  buildPairs,
  activeConservationVector,
  conservationColor,
  overlayStateForRow,
  representativeLens,
  embeddingDistanceLabel,
  syncColumnLegends,
  syncRepresentativeLensControls,
  syncEmbeddingLoadingUi,
  syncEmbeddingSettingsUi,
  resizeEmbeddingCanvas,
  resizeDistanceCanvas,
  resizeColumnsCanvas,
  renderEmbeddingPlot,
  renderDistanceMatrixPlot,
  renderColumnsChart,
  renderColumnsClusterLegend,
  setEmbeddingInfo,
  setColumnsInfo,
  ensureEmbeddingDataLoaded,
  ensureDistanceMatrixLoaded,
  ensureEmbeddingClusteringLoaded,
  resetColumnsClusterSelection,
  resetEmbeddingPartnerSelection,
  resetEmbeddingClusterSelection,
  resetRepresentativePartnerSelection,
  resetRepresentativeClusterSelection,
  renderRepresentativePartnerFilter,
  renderEmbeddingLegend,
  refreshRepresentativeSelection,
  resetRepresentativePanel,
  resetStructurePanel,
  closeClusterCompareModal,
  closeStructureModal,
  resizeClusterCompareViewers,
  buildOverlayMaps,
  buildPartnerColorMap,
  embeddingClusterColor,
  embeddingClusterLabel,
  allColumnsClusterLabels,
  visibleColumnsClusters,
  updatePartnerOptions,
}) {
  const {
    appStatus,
    cellDetailsPanel,
    columnCount,
    columnsClusterLegend,
    detailsList,
    detailsBar,
    embeddingRoot,
    distanceRoot,
    columnsRoot,
    gridCanvas,
    gridScroll,
    gridSpacer,
    headerCanvas,
    interfaceSelect,
    labelsCanvas,
    loadingDetail,
    loadingLabel,
    loadingPanel,
    loadStructureButton,
    msaLegend,
    msaPanelTabs,
    msaClusterLegend,
    msaPickerButton,
    msaPickerFilters,
    msaPickerMenu,
    msaPickerOptions,
    msaPickerSearch,
    msaPickerSelection,
    msaSelect,
    selectionSettingsPanel,
    selectionSettingsToggle,
    selectionMinInterfaceSizeInput,
    partnerSelect,
    progressBar,
    representativeShell,
    representativeViewerRoot,
    rowCount,
    selectedRowCopy,
    statsPanel,
    structureModal,
    viewerPanel,
    viewerRoot,
  } = elements;

  let layoutSyncScheduled = false;
  let cachedMsaClusterSource = null;
  let cachedMsaRowClusterAssignments = new Map();
  let cachedMsaRowClusterMemberships = new Map();
  let cachedMsaClusterCounts = new Map();

  function activeMsaPanelView() {
    return state.msaPanelView;
  }

  function numericStyleValue(style, property) {
    const value = Number.parseFloat(style[property] || "0");
    return Number.isFinite(value) ? value : 0;
  }

  function outerHeight(element) {
    if (!element) {
      return 0;
    }
    if (
      element.classList?.contains("hidden") ||
      element.classList?.contains("panel-view-hidden")
    ) {
      return 0;
    }
    const style = window.getComputedStyle(element);
    return (
      element.offsetHeight +
      numericStyleValue(style, "marginTop") +
      numericStyleValue(style, "marginBottom")
    );
  }

  function activePanelRoot() {
    if (activeMsaPanelView() === "msa") {
      return viewerRoot;
    }
    if (activeMsaPanelView() === "embeddings") {
      return embeddingRoot;
    }
    if (activeMsaPanelView() === "distances") {
      return distanceRoot;
    }
    return columnsRoot;
  }

  function syncPaneHeights() {
    const panelRoots = [viewerRoot, embeddingRoot, distanceRoot, columnsRoot];
    panelRoots.forEach((root) => {
      if (root) {
        root.style.height = "";
      }
    });

    const activeRoot = activePanelRoot();
    if (viewerPanel && activeRoot) {
      const panelStyle = window.getComputedStyle(viewerPanel);
      const availableHeight =
        viewerPanel.clientHeight -
        numericStyleValue(panelStyle, "paddingTop") -
        numericStyleValue(panelStyle, "paddingBottom") -
        outerHeight(msaPanelTabs) -
        (activeMsaPanelView() === "msa" ? outerHeight(msaLegend) + outerHeight(detailsBar) : 0);
      activeRoot.style.height = `${Math.max(0, Math.floor(availableHeight))}px`;
    }

    if (representativeShell) {
      const stage = representativeShell.querySelector(".representative-stage");
      const title = representativeShell.querySelector("h2");
      const copy = representativeShell.querySelector("#representative-copy");
      const shellStyle = window.getComputedStyle(representativeShell);
      const shellChildren = [...representativeShell.children].filter(
        (child) =>
          !child.classList.contains("hidden") && !child.classList.contains("panel-view-hidden")
      );
      const shellGap =
        numericStyleValue(shellStyle, "rowGap") || numericStyleValue(shellStyle, "gap");
      const reservedHeight =
        outerHeight(title) +
        outerHeight(copy) +
        shellGap * Math.max(0, shellChildren.length - 1);
      const availableHeight =
        representativeShell.clientHeight -
        numericStyleValue(shellStyle, "paddingTop") -
        numericStyleValue(shellStyle, "paddingBottom") -
        reservedHeight;
      if (stage) {
        stage.style.height = `${Math.max(0, Math.floor(availableHeight))}px`;
      }
    }
  }

  function syncMsaPanelView() {
    const isMsaView = activeMsaPanelView() === "msa";
    const isEmbeddingView = activeMsaPanelView() === "embeddings";
    const isDistanceView = activeMsaPanelView() === "distances";
    const isColumnsView = activeMsaPanelView() === "columns";
    [...msaPanelTabs.querySelectorAll("[data-panel-view]")].forEach((button) => {
      const isActive = button.dataset.panelView === state.msaPanelView;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-selected", String(isActive));
    });
    detailsBar.classList.toggle("panel-view-hidden", !isMsaView);
    cellDetailsPanel.classList.toggle("panel-view-hidden", !isMsaView);
    statsPanel.classList.add("panel-view-hidden");
    msaLegend.classList.toggle("panel-view-hidden", !isMsaView);
    viewerRoot.classList.toggle("panel-view-hidden", !isMsaView);
    embeddingRoot.classList.toggle("panel-view-hidden", !isEmbeddingView);
    distanceRoot.classList.toggle("panel-view-hidden", !isDistanceView);
    columnsRoot.classList.toggle("panel-view-hidden", !isColumnsView);
  }

  function setOptions(select, options, value = null) {
    select.innerHTML = "";
    for (const option of options) {
      const el = document.createElement("option");
      el.value = option.value;
      el.textContent = option.label;
      select.appendChild(el);
    }
    if (value !== null) {
      select.value = value;
    }
  }

  function setLoading(progress, label, detail) {
    if (label) {
      loadingLabel.textContent = label;
    }
    if (detail) {
      loadingDetail.textContent = detail;
    }
    loadingPanel.classList.remove("hidden");
    progressBar.style.width = `${Math.max(0, Math.min(100, progress))}%`;
  }

  function hideLoading() {
    loadingPanel.classList.add("hidden");
  }

  function matchesFilterDirection(category, direction) {
    if (typeof category !== "string") {
      return false;
    }
    if (direction === "big") {
      return category === "big" || category === "very_big";
    }
    if (direction === "small") {
      return category === "small" || category === "very_small";
    }
    return false;
  }

  function optionMatchesBadgeFilters(option) {
    const stats = option.stats;
    return Object.entries(state.msaFilterState).every(([metricKey, directions]) => {
      if (!directions || directions.size === 0) {
        return true;
      }
      const category = stats?.[`${metricKey}_category`];
      for (const direction of directions) {
        if (matchesFilterDirection(category, direction)) {
          return true;
        }
      }
      return false;
    });
  }

  function badgeCategoryClass(key) {
    if (key === "big") {
      return "warm";
    }
    if (key === "very_big") {
      return "warm-strong";
    }
    if (key === "small") {
      return "cold";
    }
    if (key === "very_small") {
      return "cold-strong";
    }
    return null;
  }

  function badgeCategoryLabel(key) {
    if (key === "big") {
      return "larger than average";
    }
    if (key === "very_big") {
      return "much larger than average";
    }
    if (key === "small") {
      return "smaller than average";
    }
    if (key === "very_small") {
      return "much smaller than average";
    }
    return "near average";
  }

  function formatBadgeValue(metricKey, value) {
    if (metricKey === "avg_interface_residues_per_row") {
      return Number(value || 0).toFixed(1);
    }
    return String(value ?? 0);
  }

  function pfamBadges(stats) {
    if (!stats) {
      return [];
    }

    const specs = [
      {
        metricKey: "alignment_length",
        categoryKey: "alignment_length_category",
        symbol: "↔",
        label: "Alignment length",
      },
      {
        metricKey: "interface_rows",
        categoryKey: "interface_rows_category",
        symbol: "≣",
        label: "Interface rows",
      },
      {
        metricKey: "interaction_partners",
        categoryKey: "interaction_partners_category",
        symbol: "⋈",
        label: "Interaction partners",
      },
      {
        metricKey: "avg_interface_residues_per_row",
        categoryKey: "avg_interface_residues_per_row_category",
        symbol: "◍",
        label: "Interface residues per row",
      },
    ];

    return specs
      .map((spec) => {
        const category = stats[spec.categoryKey];
        const className = badgeCategoryClass(category);
        if (!className) {
          return null;
        }
        return {
          symbol: spec.symbol,
          className,
          title: `${spec.label}: ${badgeCategoryLabel(category)} (${formatBadgeValue(spec.metricKey, stats[spec.metricKey])})`,
        };
      })
      .filter(Boolean);
  }

  function createBadgeStrip(badges) {
    const strip = document.createElement("span");
    strip.className = "pfam-badge-strip";
    for (const badge of badges) {
      const item = document.createElement("span");
      item.className = `pfam-badge ${badge.className}`;
      item.textContent = badge.symbol;
      item.title = badge.title;
      strip.appendChild(item);
    }
    return strip;
  }

  function currentMsaOption() {
    return (state.msaOptions || []).find((option) => option.value === msaSelect.value) || null;
  }

  function syncMsaPickerSelection() {
    msaPickerSelection.innerHTML = "";
    const selected = currentMsaOption();
    if (!selected) {
      msaPickerSelection.textContent = "Select PFAM";
      return;
    }

    const name = document.createElement("span");
    name.className = "msa-picker-option-name";
    name.textContent = selected.pfamId;
    msaPickerSelection.appendChild(name);

    const badges = pfamBadges(selected.stats);
    if (badges.length > 0) {
      msaPickerSelection.appendChild(createBadgeStrip(badges));
    }
  }

  function syncSelectionSettingsUi() {
    selectionSettingsToggle?.setAttribute(
      "aria-expanded",
      String(state.selectionSettingsOpen)
    );
    selectionSettingsPanel?.classList.toggle("hidden", !state.selectionSettingsOpen);
    if (selectionMinInterfaceSizeInput) {
      selectionMinInterfaceSizeInput.value = String(
        normalizeSelectionSettings(state.selectionSettingsDraft).minInterfaceSize
      );
    }
  }

  function renderMsaPickerOptions(filterText = "") {
    const query = filterText.trim().toLowerCase();
    msaPickerOptions.innerHTML = "";

    const options = (state.msaOptions || []).filter((option) => {
      if (query && !option.pfamId.toLowerCase().includes(query)) {
        return false;
      }
      return optionMatchesBadgeFilters(option);
    });

    if (options.length === 0) {
      const empty = document.createElement("div");
      empty.className = "msa-picker-option empty";
      empty.textContent = "No PFAM matches the current filter.";
      msaPickerOptions.appendChild(empty);
      return;
    }

    for (const option of options) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "msa-picker-option";
      if (option.value === msaSelect.value) {
        button.classList.add("active");
      }
      button.dataset.value = option.value;

      const name = document.createElement("span");
      name.className = "msa-picker-option-name";
      name.textContent = option.pfamId;
      button.appendChild(name);

      const badges = pfamBadges(option.stats);
      if (badges.length > 0) {
        button.appendChild(createBadgeStrip(badges));
      }

      button.addEventListener("click", () => {
        msaSelect.value = option.value;
        syncMsaSelectionInUrl(option.value);
        updatePairedOptions();
        syncMsaPickerSelection();
        closeMsaPicker();
        void loadCurrentSelection();
      });
      msaPickerOptions.appendChild(button);
    }
  }

  function openMsaPicker() {
    msaPickerMenu.classList.remove("hidden");
    msaPickerButton.setAttribute("aria-expanded", "true");
    msaPickerSearch.value = "";
    updateMsaFilterButtons();
    renderMsaPickerOptions("");
    window.setTimeout(() => {
      msaPickerSearch.focus();
      msaPickerSearch.select();
    }, 0);
  }

  function closeMsaPicker() {
    msaPickerMenu.classList.add("hidden");
    msaPickerButton.setAttribute("aria-expanded", "false");
  }

  function toggleMsaPicker() {
    if (msaPickerMenu.classList.contains("hidden")) {
      openMsaPicker();
    } else {
      closeMsaPicker();
    }
  }

  function updateMsaFilterButtons() {
    [...msaPickerFilters.querySelectorAll(".msa-filter-chip")].forEach((button) => {
      const metricKey = button.dataset.filterKey;
      const direction = button.dataset.filterDirection;
      const active = state.msaFilterState[metricKey]?.has(direction);
      button.classList.toggle("active", Boolean(active));
    });
  }

  function normalizeResidueIds(residueIds, alignmentLength) {
    const normalized = Array.isArray(residueIds)
      ? residueIds.map((value) => {
          if (value === null || value === undefined || value === "") {
            return null;
          }
          const parsed = Number(value);
          return Number.isFinite(parsed) ? parsed : null;
        })
      : [];
    if (normalized.length >= alignmentLength) {
      return normalized.slice(0, alignmentLength);
    }
    return normalized.concat(new Array(alignmentLength - normalized.length).fill(null));
  }

  function normalizeInterfaceRows(rows, alignmentLength) {
    const normalizedLength = Math.max(0, Number(alignmentLength || 0));
    return (rows || []).map((row) => {
      const interfaceRowKey = String(row.interface_row_key || "");
      const proteinId = String(row.protein_id || "");
      const partnerDomain = String(row.partner_domain || "");
      const fullInteractionRowKey = String(row.row_key || interactionRowKey(interfaceRowKey, partnerDomain));
      const alignedSequenceRaw = String(row.aligned_sequence || "");
      const alignedSequence =
        alignedSequenceRaw.length < normalizedLength
          ? alignedSequenceRaw + "-".repeat(normalizedLength - alignedSequenceRaw.length)
          : alignedSequenceRaw;
      const effectiveLength = Math.max(normalizedLength, alignedSequence.length);
      const nextRow = {
        ...row,
        interface_row_key: interfaceRowKey,
        partner_domain: partnerDomain,
        row_key: fullInteractionRowKey,
        display_row_key:
          String(row.display_row_key || "") ||
          (partnerDomain ? `${proteinId} | ${partnerDomain}` : proteinId || interfaceRowKey),
        aligned_sequence: alignedSequence,
        alignment_fragment_key: String(row.alignment_fragment_key || row.fragment_key || ""),
        has_alignment: row.has_alignment !== false,
      };
      nextRow.residueIds = normalizeResidueIds(row.residue_ids, effectiveLength);
      return nextRow;
    });
  }

  function computeVisibleColumns(rows, alignmentLength) {
    const visible = [];
    for (let column = 0; column < alignmentLength; column += 1) {
      let keep = false;
      for (const row of rows) {
        if (/^[A-Za-z]$/.test(row.aligned_sequence[column] || "")) {
          keep = true;
          break;
        }
      }
      if (keep) {
        visible.push(column);
      }
    }
    if (visible.length > 0) {
      return visible;
    }
    return Array.from({ length: alignmentLength }, (_value, index) => index);
  }

  function displayAlignmentLength() {
    return state.msa?.visible_columns?.length || state.msa?.alignment_length || 0;
  }

  function msaUrlValue(msaFile) {
    return String(msaFile || "").replace(/\.json$/i, "");
  }

  function syncMsaSelectionInUrl(msaFile) {
    const url = new URL(window.location.href);
    const value = msaUrlValue(msaFile);
    if (value) {
      url.searchParams.set("msa", value);
    } else {
      url.searchParams.delete("msa");
    }
    window.history.replaceState({}, "", url);
  }

  function msaFileFromUrl() {
    const url = new URL(window.location.href);
    const requested = String(url.searchParams.get("msa") || "").trim();
    if (!requested) {
      return "";
    }
    const normalizedRequested = requested.replace(/\.json$/i, "");
    const match = (state.msaOptions || []).find(
      (option) => option.pfamId === normalizedRequested || option.value === `${normalizedRequested}.json`
    );
    return match?.value || "";
  }

  function normalizeFuzzyText(value) {
    return String(value || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "");
  }

  function isFuzzySubsequenceMatch(target, query) {
    if (!query) {
      return true;
    }
    let queryIndex = 0;
    for (const character of target) {
      if (character === query[queryIndex]) {
        queryIndex += 1;
        if (queryIndex === query.length) {
          return true;
        }
      }
    }
    return false;
  }

  function matchesRowSearch(rowKey, query = state.rowSearchQuery) {
    const rawQuery = String(query || "").trim().toLowerCase();
    if (!rawQuery) {
      return true;
    }
    const label = String(rowKey || "");
    const lowerLabel = label.toLowerCase();
    if (lowerLabel.includes(rawQuery)) {
      return true;
    }
    const condensedQuery = normalizeFuzzyText(rawQuery);
    if (!condensedQuery) {
      return true;
    }
    const condensedLabel = normalizeFuzzyText(label);
    return (
      condensedLabel.includes(condensedQuery) ||
      isFuzzySubsequenceMatch(condensedLabel, condensedQuery)
    );
  }

  function originalColumnForDisplay(displayColumn) {
    if (!state.msa?.visible_columns) {
      return null;
    }
    return state.msa.visible_columns[displayColumn] ?? null;
  }

  function compareMsaClusterKeys(leftKey, rightKey) {
    return Number(leftKey) - Number(rightKey);
  }

  function rebuildMsaClusterCache() {
    const source = state.embeddingClustering?.points || null;
    if (source === cachedMsaClusterSource) {
      return;
    }
    cachedMsaClusterSource = source;
    cachedMsaRowClusterAssignments = new Map();
    cachedMsaRowClusterMemberships = new Map();
    cachedMsaClusterCounts = new Map();
    if (!source || source.length === 0) {
      return;
    }

    const countsByRow = new Map();
    for (const point of source) {
      const fullRowKey = interactionRowKey(point.row_key, point.partner_domain);
      const clusterLabel = Number(point.cluster_label);
      if (!fullRowKey || !Number.isFinite(clusterLabel)) {
        continue;
      }
      const clusterKey = String(clusterLabel);
      cachedMsaClusterCounts.set(clusterKey, (cachedMsaClusterCounts.get(clusterKey) || 0) + 1);
      let memberships = cachedMsaRowClusterMemberships.get(fullRowKey);
      if (!memberships) {
        memberships = new Set();
        cachedMsaRowClusterMemberships.set(fullRowKey, memberships);
      }
      memberships.add(clusterKey);
      let rowCounts = countsByRow.get(fullRowKey);
      if (!rowCounts) {
        rowCounts = new Map();
        countsByRow.set(fullRowKey, rowCounts);
      }
      rowCounts.set(clusterLabel, (rowCounts.get(clusterLabel) || 0) + 1);
    }

    for (const [rowKey, rowCounts] of countsByRow.entries()) {
      let bestClusterLabel = null;
      let bestCount = -1;
      for (const [clusterLabel, count] of rowCounts.entries()) {
        if (
          count > bestCount ||
          (
            count === bestCount &&
            (
              bestClusterLabel === null ||
              (bestClusterLabel < 0 && clusterLabel >= 0) ||
              (
                (bestClusterLabel < 0) === (clusterLabel < 0) &&
                clusterLabel < bestClusterLabel
              )
            )
          )
        ) {
          bestClusterLabel = clusterLabel;
          bestCount = count;
        }
      }
      if (bestClusterLabel === null) {
        continue;
      }
      cachedMsaRowClusterAssignments.set(rowKey, String(bestClusterLabel));
    }
  }

  function rowClusterKey(rowKey) {
    rebuildMsaClusterCache();
    return cachedMsaRowClusterAssignments.get(String(rowKey)) || null;
  }

  function rowClusterMemberships(rowKey) {
    rebuildMsaClusterCache();
    return cachedMsaRowClusterMemberships.get(String(rowKey)) || null;
  }

  function allMsaClusterKeys() {
    rebuildMsaClusterCache();
    return [...cachedMsaClusterCounts.keys()].sort(compareMsaClusterKeys);
  }

  function visibleMsaClusterKeys() {
    const allKeys = allMsaClusterKeys();
    if (allKeys.length === 0) {
      return [];
    }
    if (state.msaVisibleClusters.size === 0) {
      state.msaVisibleClusters = new Set(allKeys);
      return allKeys;
    }
    const visible = allKeys.filter((key) => state.msaVisibleClusters.has(key));
    if (visible.length === 0) {
      state.msaVisibleClusters = new Set(allKeys);
      return allKeys;
    }
    return visible;
  }

  function clusterLabelForKey(clusterKey) {
    return embeddingClusterLabel(clusterKey);
  }

  function clusterColorForKey(clusterKey) {
    return embeddingClusterColor(clusterKey);
  }

  function renderMsaClusterLegend() {
    if (!msaClusterLegend) {
      return;
    }
    if (!state.msa || state.embeddingClusteringLoading) {
      msaClusterLegend.classList.toggle(
        "hidden",
        !(state.embeddingClusteringLoading && state.msa)
      );
      if (!msaClusterLegend.classList.contains("hidden")) {
        msaClusterLegend.innerHTML =
          '<div class="msa-cluster-legend-header"><span class="msa-cluster-legend-title">Clusters</span><span>Loading clustering...</span></div>';
      } else {
        msaClusterLegend.innerHTML = "";
      }
      return;
    }
    const clusterKeys = allMsaClusterKeys();
    const hasClustering = (state.embeddingClustering?.points || []).length > 0;
    if (!hasClustering || clusterKeys.length === 0) {
      msaClusterLegend.classList.add("hidden");
      msaClusterLegend.innerHTML = "";
      return;
    }
    const visibleKeys = new Set(visibleMsaClusterKeys());
    const legendEntries = clusterKeys
      .map((clusterKey) => {
        const active = visibleKeys.has(clusterKey);
        const count = cachedMsaClusterCounts.get(clusterKey) || 0;
        return `
          <button
            type="button"
            class="msa-cluster-chip ${active ? "active" : "inactive"}"
            data-msa-cluster-label="${clusterKey}"
            aria-pressed="${active}"
            title="${clusterLabelForKey(clusterKey)}"
          >
            <span class="representative-partner-filter-swatch" style="background:${clusterColorForKey(clusterKey)};"></span>
            <span class="msa-cluster-chip-label">${clusterLabelForKey(clusterKey)}</span>
            <span class="msa-cluster-chip-value">${count}</span>
          </button>
        `;
      })
      .join("");

    msaClusterLegend.innerHTML = `
      <div class="msa-cluster-legend-header">
        <span class="msa-cluster-legend-title">MSA Cluster Filter</span>
        <span>${visibleKeys.size}/${clusterKeys.length} selected</span>
      </div>
      <div class="msa-cluster-legend-list">${legendEntries}</div>
    `;
    msaClusterLegend.classList.remove("hidden");
  }

  function updateFilteredRows() {
    if (!state.msa) {
      state.filteredRowIndexes = [];
      state.visibleRows = [];
      return;
    }
    const visibleClusterKeys = new Set(visibleMsaClusterKeys());
    const useClusterFilter = visibleClusterKeys.size > 0 && (state.embeddingClustering?.points || []).length > 0;
    const indexes = [];
    state.msa.rows.forEach((row, index) => {
      if (!matchesRowSearch(row.display_row_key || row.row_key)) {
        return;
      }
      const overlay = overlayStateForRow(row);
      if (!overlay || overlay.interface.size === 0) {
        return;
      }
      if (useClusterFilter) {
        const clusterMemberships = rowClusterMemberships(row.row_key);
        if (!clusterMemberships) {
          return;
        }
        let isVisibleInCluster = false;
        for (const clusterKey of clusterMemberships) {
          if (visibleClusterKeys.has(clusterKey)) {
            isVisibleInCluster = true;
            break;
          }
        }
        if (!isVisibleInCluster) {
          return;
        }
      }
      indexes.push(index);
    });
    state.filteredRowIndexes = indexes;
  }

  function resizeCanvases() {
    const headerWidth = gridScroll.clientWidth;
    const bodyHeight = gridScroll.clientHeight;
    headerCanvas.width = headerWidth * window.devicePixelRatio;
    headerCanvas.height = HEADER_HEIGHT * window.devicePixelRatio;
    headerCanvas.style.width = `${headerWidth}px`;
    headerCanvas.style.height = `${HEADER_HEIGHT}px`;
    labelsCanvas.width = LABEL_WIDTH * window.devicePixelRatio;
    labelsCanvas.height = bodyHeight * window.devicePixelRatio;
    labelsCanvas.style.width = `${LABEL_WIDTH}px`;
    labelsCanvas.style.height = `${bodyHeight}px`;
    gridCanvas.width = headerWidth * window.devicePixelRatio;
    gridCanvas.height = bodyHeight * window.devicePixelRatio;
    gridCanvas.style.width = `${headerWidth}px`;
    gridCanvas.style.height = `${bodyHeight}px`;
    const totalWidth = displayAlignmentLength() * CELL_WIDTH;
    const totalHeight = state.filteredRowIndexes.length * ROW_HEIGHT;
    gridSpacer.style.width = `${totalWidth}px`;
    gridSpacer.style.height = `${totalHeight}px`;
  }

  function drawHeader() {
    const ctx = headerCanvas.getContext("2d");
    const dpr = window.devicePixelRatio;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, headerCanvas.width, headerCanvas.height);
    ctx.fillStyle = "#f0e8d6";
    ctx.fillRect(0, 0, headerCanvas.clientWidth, HEADER_HEIGHT);
    ctx.strokeStyle = "#d7ccb3";
    ctx.beginPath();
    ctx.moveTo(0, HEADER_HEIGHT - 0.5);
    ctx.lineTo(headerCanvas.clientWidth, HEADER_HEIGHT - 0.5);
    ctx.stroke();
    if (!state.msa) {
      return;
    }
    const scrollLeft = gridScroll.scrollLeft;
    const firstCol = Math.floor(scrollLeft / CELL_WIDTH);
    const visibleCols = Math.ceil(gridScroll.clientWidth / CELL_WIDTH) + 2;
    const labelStep = CELL_WIDTH >= 16 ? 5 : 10;
    const conservation = activeConservationVector();
    const displayedColumnCount = displayAlignmentLength();
    ctx.font = '11px "IBM Plex Mono", monospace';
    ctx.fillStyle = "#6f6658";
    for (
      let displayCol = firstCol;
      displayCol < Math.min(displayedColumnCount, firstCol + visibleCols);
      displayCol += 1
    ) {
      const originalCol = originalColumnForDisplay(displayCol);
      if (originalCol === null) {
        continue;
      }
      const x = displayCol * CELL_WIDTH - scrollLeft;
      const conservedness = conservation[originalCol];
      if (typeof conservedness === "number") {
        ctx.fillStyle = conservationColor(conservedness);
        ctx.fillRect(x, 22, CELL_WIDTH, HEADER_HEIGHT - 22);
      }
      if (displayCol % labelStep === 0) {
        ctx.fillStyle = "#6f6658";
        ctx.fillText(String(originalCol), x + 2, 15);
      }
      ctx.strokeStyle = originalCol === state.hover?.column ? "#2d6a4f" : "rgba(0,0,0,0.06)";
      ctx.beginPath();
      ctx.moveTo(x + 0.5, 18);
      ctx.lineTo(x + 0.5, HEADER_HEIGHT);
      ctx.stroke();
    }
  }

  function drawLabels(firstRowIndex, visibleRowCount, scrollTop) {
    const ctx = labelsCanvas.getContext("2d");
    const dpr = window.devicePixelRatio;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, labelsCanvas.clientWidth, labelsCanvas.clientHeight);
    ctx.fillStyle = "#fcfaf5";
    ctx.fillRect(0, 0, labelsCanvas.clientWidth, labelsCanvas.clientHeight);
    ctx.font = TEXT_FONT;
    ctx.textBaseline = "middle";
    for (let i = 0; i < visibleRowCount; i += 1) {
      const filteredIndex = firstRowIndex + i;
      if (filteredIndex >= state.filteredRowIndexes.length) {
        break;
      }
      const rowIndex = state.filteredRowIndexes[filteredIndex];
      const row = state.msa.rows[rowIndex];
      const y = filteredIndex * ROW_HEIGHT - scrollTop;
      const isHovered = state.hover?.filteredRowIndex === filteredIndex;
      const isSelected = state.selectedRowKey === row.row_key;
      const isRepresentative = state.representativeRowKey === row.row_key;
      if (isHovered) {
        ctx.fillStyle = "rgba(45, 106, 79, 0.12)";
        ctx.fillRect(0, y, LABEL_WIDTH, ROW_HEIGHT);
      }
      if (isSelected) {
        ctx.fillStyle = "rgba(127, 82, 40, 0.18)";
        ctx.fillRect(0, y, LABEL_WIDTH, ROW_HEIGHT);
      }
      if (isRepresentative) {
        ctx.fillStyle = "#d49a38";
        ctx.beginPath();
        ctx.arc(12, y + ROW_HEIGHT / 2, 4, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = "#fff9ee";
        ctx.beginPath();
        ctx.arc(12, y + ROW_HEIGHT / 2, 1.6, 0, Math.PI * 2);
        ctx.fill();
      }
      const clusterKey = rowClusterKey(row.row_key);
      ctx.fillStyle = clusterKey === null ? "#2e261d" : clusterColorForKey(clusterKey);
      ctx.fillText(row.display_row_key || row.row_key, 22, y + ROW_HEIGHT / 2);
    }
  }

  function drawGrid() {
    const ctx = gridCanvas.getContext("2d");
    const dpr = window.devicePixelRatio;
    const viewportWidth = gridScroll.clientWidth;
    const viewportHeight = gridScroll.clientHeight;
    const scrollLeft = gridScroll.scrollLeft;
    const scrollTop = gridScroll.scrollTop;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, viewportWidth, viewportHeight);
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, viewportWidth, viewportHeight);
    if (!state.msa) {
      drawHeader();
      return;
    }
    const firstCol = Math.floor(scrollLeft / CELL_WIDTH);
    const visibleCols = Math.ceil(viewportWidth / CELL_WIDTH) + 2;
    const displayedColumnCount = displayAlignmentLength();
    const firstRowIndex = Math.floor(scrollTop / ROW_HEIGHT);
    const visibleRowCount = Math.ceil(viewportHeight / ROW_HEIGHT) + 2;
    ctx.font = TEXT_FONT;
    ctx.textBaseline = "middle";
    ctx.textAlign = "center";
    for (let i = 0; i < visibleRowCount; i += 1) {
      const filteredIndex = firstRowIndex + i;
      if (filteredIndex >= state.filteredRowIndexes.length) {
        break;
      }
      const rowIndex = state.filteredRowIndexes[filteredIndex];
      const row = state.msa.rows[rowIndex];
      const y = filteredIndex * ROW_HEIGHT - scrollTop;
      const overlay = overlayStateForRow(row);
      const isHoveredRow = state.hover?.filteredRowIndex === filteredIndex;
      const isSelectedRow = state.selectedRowKey === row.row_key;
      if (isHoveredRow) {
        ctx.fillStyle = "rgba(45, 106, 79, 0.08)";
        ctx.fillRect(0, y, viewportWidth, ROW_HEIGHT);
      }
      if (isSelectedRow) {
        ctx.fillStyle = "rgba(127, 82, 40, 0.11)";
        ctx.fillRect(0, y, viewportWidth, ROW_HEIGHT);
      }
      for (
        let displayCol = firstCol;
        displayCol < Math.min(displayedColumnCount, firstCol + visibleCols);
        displayCol += 1
      ) {
        const originalCol = originalColumnForDisplay(displayCol);
        if (originalCol === null) {
          continue;
        }
        const x = displayCol * CELL_WIDTH - scrollLeft;
        const char = row.aligned_sequence[originalCol] || " ";
        const isHoveredCol = state.hover?.column === originalCol;
        if (overlay?.surface.has(originalCol)) {
          ctx.fillStyle = "rgba(215, 168, 76, 0.5)";
          ctx.fillRect(x, y, CELL_WIDTH, ROW_HEIGHT);
        }
        if (overlay?.interface.has(originalCol)) {
          ctx.fillStyle = "rgba(188, 64, 45, 0.72)";
          ctx.fillRect(x, y, CELL_WIDTH, ROW_HEIGHT);
        }
        if (isHoveredCol) {
          ctx.strokeStyle = "#2d6a4f";
          ctx.strokeRect(x + 0.5, y + 0.5, CELL_WIDTH - 1, ROW_HEIGHT - 1);
        }
        ctx.fillStyle = /^[A-Za-z]$/.test(char) ? "#1f1a14" : "#a7a092";
        ctx.fillText(char, x + CELL_WIDTH / 2, y + ROW_HEIGHT / 2 + 0.5);
      }
    }
    drawHeader();
    drawLabels(firstRowIndex, visibleRowCount, scrollTop);
  }

  function updateStats() {
    rowCount.textContent = String(state.filteredRowIndexes.length);
    columnCount.textContent = String(displayAlignmentLength());
  }

  async function loadInterface(filename) {
    state.interface = null;
    state.msa = null;
    state.embedding = null;
    state.embeddingClustering = null;
    state.columnsChart = null;
    state.columnsChartKey = null;
    closeClusterCompareModal();
    state.embeddingHoverRowKey = null;
    state.embeddingProjectedPoints = [];
    state.embeddingRequestId += 1;
    state.embeddingLoading = false;
    state.embeddingLoadingKey = null;
    state.embeddingPromise = null;
    state.embeddingClusteringRequestId += 1;
    state.embeddingClusteringLoading = false;
    state.embeddingClusteringLoadingKey = null;
    state.embeddingClusteringPromise = null;
    state.columnsVisibleClusters = new Set();
    syncEmbeddingLoadingUi();
    if (!filename) {
      return;
    }
    const params = new URLSearchParams({ file: filename });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    const payload = await fetchJson(`/api/interface?${params.toString()}`);
    setLoading(65, "Loading alignment", `Preparing rows from ${filename}`);
    const rows = normalizeInterfaceRows(payload.rows, payload.alignment_length);
    state.msa = {
      file: payload.file,
      pfam_id: payload.pfam_id,
      alignment_length: Number(payload.alignment_length || 0),
      row_count: Number(payload.row_count || rows.length),
      clean_column_identity: payload.clean_column_identity || [],
      rows,
    };
    state.msa.visible_columns = computeVisibleColumns(state.msa.rows, state.msa.alignment_length);
    state.msa.hidden_gap_only_columns = Math.max(
      0,
      state.msa.alignment_length - state.msa.visible_columns.length
    );
    syncColumnLegends();
    setLoading(78, "Loading interface", `Preparing overlays for ${filename}`);
    const maps = buildOverlayMaps(payload.data);
    state.interface = {
      ...payload,
      ...maps,
      partnerColors: buildPartnerColorMap(maps.partnerDomains),
    };
    resetEmbeddingPartnerSelection();
    resetEmbeddingClusterSelection();
    resetColumnsClusterSelection();
    resetRepresentativePartnerSelection();
    resetRepresentativeClusterSelection();
    renderRepresentativePartnerFilter();
    renderEmbeddingLegend();
    renderColumnsClusterLegend();
  }

  async function refreshData() {
    if (!msaSelect.value || !interfaceSelect.value) {
      return;
    }
    setLoading(10, "Loading selection", "Fetching interface data");
    state.representativeAnchorRowKey = null;
    state.representativeRenderedRowKey = null;
    state.representativeStructure = null;
    state.representativeRequestId += 1;
    state.structureAnchorRowKey = null;
    state.structureRenderedRowKey = null;
    state.structureRequestId += 1;
    await loadInterface(interfaceSelect.value);
    updatePartnerOptions();
    setLoading(85, "Loading selection", "Finding representative row");
    state.hover = null;
    updateFilteredRows();
    void ensureEmbeddingDataLoaded();
    void ensureEmbeddingClusteringLoaded().then(() => {
      if (
        state.msa &&
        (activeMsaPanelView() === "msa" || activeMsaPanelView() === "columns")
      ) {
        render();
      }
    });
    void ensureDistanceMatrixLoaded();
    render();
    await new Promise((resolve) =>
      window.requestAnimationFrame(() => window.requestAnimationFrame(resolve))
    );
    render();
    setLoading(96, "Loading selection", "Loading representative structure");
    await refreshRepresentativeSelection("No representative row found.");
    setLoading(100, "Loaded", interfaceSelect.value);
    window.setTimeout(hideLoading, 250);
  }

  async function loadCurrentSelection() {
    syncMsaSelectionInUrl(msaSelect.value);
    if (!msaSelect.value || !interfaceSelect.value) {
      clearViewer();
      return;
    }
    clearViewer();
    try {
      await refreshData();
    } catch (error) {
      loadingPanel.classList.remove("hidden");
      loadingLabel.textContent = "Load failed";
      loadingDetail.textContent = error.message;
      progressBar.style.width = "100%";
    }
  }

  function render() {
    updateFilteredRows();
    renderMsaClusterLegend();
    renderColumnsClusterLegend();
    updateStats();
    syncMsaPanelView();
    syncPaneHeights();
    if (activeMsaPanelView() === "msa") {
      resizeCanvases();
      drawGrid();
    } else if (activeMsaPanelView() === "embeddings") {
      resizeEmbeddingCanvas();
      renderEmbeddingPlot();
    } else if (activeMsaPanelView() === "distances") {
      resizeDistanceCanvas();
      renderDistanceMatrixPlot();
    } else {
      resizeColumnsCanvas();
      renderColumnsChart();
    }
    setDetails(null);
  }

  function syncLayout() {
    syncPaneHeights();
    if (state.msa && activeMsaPanelView() === "msa") {
      resizeCanvases();
      drawGrid();
    }
    if (activeMsaPanelView() === "embeddings") {
      resizeEmbeddingCanvas();
      renderEmbeddingPlot();
    }
    if (activeMsaPanelView() === "distances") {
      resizeDistanceCanvas();
      renderDistanceMatrixPlot();
    }
    if (activeMsaPanelView() === "columns") {
      resizeColumnsCanvas();
      renderColumnsChart();
    }
    if (state.representativeViewer) {
      state.representativeViewer.resize();
      state.representativeViewer.render();
    }
    if (state.structureViewer && !structureModal.classList.contains("hidden")) {
      state.structureViewer.resize();
      state.structureViewer.render();
    }
    if (!elements.clusterCompareModal.classList.contains("hidden")) {
      resizeClusterCompareViewers();
    }
  }

  function scheduleLayoutSync() {
    if (layoutSyncScheduled) {
      return;
    }
    layoutSyncScheduled = true;
    window.requestAnimationFrame(() => {
      layoutSyncScheduled = false;
      syncLayout();
    });
  }

  function clearViewer() {
    closeClusterCompareModal();
    state.msa = null;
    state.interface = null;
    state.embedding = null;
    state.distanceMatrix = null;
    state.columnsChart = null;
    state.columnsChartKey = null;
    state.embeddingClustering = null;
    state.embeddingHoverRowKey = null;
    state.embeddingProjectedPoints = [];
    state.embeddingDrag = null;
    state.embeddingRequestId += 1;
    state.distanceMatrixRequestId += 1;
    state.embeddingLoading = false;
    state.distanceMatrixLoading = false;
    state.distanceMatrixLoadingKey = null;
    state.distanceMatrixPromise = null;
    state.embeddingLoadingKey = null;
    state.embeddingPromise = null;
    state.embeddingClusteringRequestId += 1;
    state.embeddingClusteringLoading = false;
    state.embeddingClusteringLoadingKey = null;
    state.embeddingClusteringPromise = null;
    state.embeddingSettingsOpen = false;
    state.selectionSettingsOpen = false;
    state.selectionSettingsDraft = {
      ...state.selectionSettings,
    };
    state.embeddingSettingsSection = "clustering";
    state.embeddingColorMode = "cluster";
    state.embeddingVisiblePartners = new Set();
    state.embeddingVisibleClusters = new Set();
    state.columnsVisibleClusters = new Set();
    state.msaVisibleClusters = new Set();
    state.embeddingSettings = { ...DEFAULT_EMBEDDING_SETTINGS };
    state.embeddingSettingsDraft = { ...DEFAULT_EMBEDDING_SETTINGS };
    state.embeddingClusteringSettings = { ...DEFAULT_CLUSTERING_SETTINGS };
    state.embeddingClusteringSettingsDraft = { ...DEFAULT_CLUSTERING_SETTINGS };
    state.embeddingHierarchicalTargetMemory = {
      nClusters: String(DEFAULT_CLUSTERING_SETTINGS.nClusters),
      distanceThreshold: String(DEFAULT_CLUSTERING_SETTINGS.distanceThreshold),
    };
    state.selectedPartner = "__all__";
    state.selectedRowKey = null;
    state.representativeRowKey = null;
    state.representativeAnchorRowKey = null;
    state.representativeVisiblePartners = new Set();
    state.representativeVisibleClusters = new Set();
    state.representativeHoveredClusterLabel = null;
    state.representativeRenderedRowKey = null;
    state.representativeStructure = null;
    state.representativeRequestId = 0;
    state.representativePointer = null;
    state.embeddingView = {
      yaw: -0.7,
      pitch: 0.45,
      zoom: 1.0,
    };
    state.structureData = null;
    state.structureAnchorRowKey = null;
    state.structureRenderedRowKey = null;
    state.structureRequestId = 0;
    state.structureColumnView = false;
    state.hover = null;
    closeStructureModal();
    updatePartnerOptions();
    syncRepresentativeLensControls();
    renderRepresentativePartnerFilter();
    renderEmbeddingLegend();
    renderColumnsClusterLegend();
    renderMsaClusterLegend();
    updateSelectedRowUi();
    resetRepresentativePanel();
    resetStructurePanel();
    setEmbeddingInfo(
      `3D t-SNE on ${embeddingDistanceLabel(DEFAULT_EMBEDDING_SETTINGS.distance)} interface distance. Drag to rotate.`
    );
    setColumnsInfo("Stacked per-column cluster interaction profile.");
    syncEmbeddingLoadingUi();
    syncEmbeddingSettingsUi();
    syncSelectionSettingsUi();
    syncColumnLegends();
    render();
  }

  function updatePairedOptions() {
    const selectedMsa = msaSelect.value;
    const matchingPair = state.files.pairs.find((pair) => pair.msaFile === selectedMsa);
    interfaceSelect.value = matchingPair ? matchingPair.interfaceFile : "";
  }

  function updateSelectedRowUi() {
    const row = getSelectedRow();
    if (!row) {
      selectedRowCopy.textContent = "Select a row in the alignment.";
      loadStructureButton.disabled = true;
      return;
    }
    selectedRowCopy.textContent = `Selected row: ${row.display_row_key || row.row_key}`;
    loadStructureButton.disabled = !interfaceSelect.value;
  }

  function getSelectedRow() {
    if (!state.msa || !state.selectedRowKey) {
      return null;
    }
    return state.msa.rows.find((row) => row.row_key === state.selectedRowKey) || null;
  }

  function getRowByKey(rowKey) {
    if (!state.msa) {
      return null;
    }
    return state.msa.rows.find((row) => row.row_key === rowKey) || null;
  }

  function selectFilteredRow(filteredRowIndex) {
    if (!state.msa) {
      return null;
    }
    if (filteredRowIndex < 0 || filteredRowIndex >= state.filteredRowIndexes.length) {
      return null;
    }
    const rowIndex = state.filteredRowIndexes[filteredRowIndex];
    const row = state.msa.rows[rowIndex];
    state.selectedRowKey = row.row_key;
    updateSelectedRowUi();
    resetStructurePanel("Click a row name or use the button to open the structure.");
    drawGrid();
    return row;
  }

  function selectRowByKey(rowKey) {
    if (!state.msa || !rowKey) {
      return null;
    }
    const row = state.msa.rows.find((entry) => entry.row_key === rowKey) || null;
    if (!row) {
      return null;
    }
    state.selectedRowKey = row.row_key;
    updateSelectedRowUi();
    resetStructurePanel("Click a row name or use the button to open the structure.");
    if (activeMsaPanelView() === "msa") {
      drawGrid();
    } else {
      renderEmbeddingPlot();
    }
    return row;
  }

  function setDetails(payload) {
    const values = payload || {
      row: "-",
      protein: "-",
      fragment: "-",
      column: "-",
      conservedness: "-",
      residue: "-",
      residueId: "-",
      state: "-",
    };
    const items = [
      values.row,
      values.protein,
      values.fragment,
      values.column,
      values.conservedness,
      values.residue,
      values.residueId,
      values.state,
    ];
    [...detailsList.querySelectorAll("dd")].forEach((el, index) => {
      el.textContent = String(items[index]);
    });
  }

  async function initialize() {
    state.files = await fetchJson("/api/files");
    state.files.pairs = buildPairs(state.files);
    state.msaOptions = [...new Set(state.files.pairs.map((pair) => pair.msaFile))]
      .sort()
      .map((name) => ({
        value: name,
        pfamId: name.replace(/\.json$/i, ""),
        stats: state.files.pfam_option_stats?.[name.replace(/\.json$/i, "")] || null,
      }));
    setOptions(
      msaSelect,
      [{ value: "", label: "Select MSA" }].concat(
        state.msaOptions.map((option) => ({
          value: option.value,
          label: option.pfamId,
        }))
      )
    );
    setOptions(partnerSelect, [{ value: "__all__", label: "All partners" }], "__all__");
    const msaFromUrl = msaFileFromUrl();
    if (msaFromUrl) {
      msaSelect.value = msaFromUrl;
      updatePairedOptions();
    }
    syncMsaPickerSelection();
    renderMsaPickerOptions();
    syncRepresentativeLensControls();
    syncMsaPanelView();
    syncSelectionSettingsUi();
    setEmbeddingInfo(
      `3D t-SNE on ${embeddingDistanceLabel(DEFAULT_EMBEDDING_SETTINGS.distance)} interface distance. Drag to rotate.`
    );
    syncEmbeddingSettingsUi();
    clearViewer();
    if (msaFromUrl) {
      await loadCurrentSelection();
    }

    msaClusterLegend?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-msa-cluster-label]");
      if (!button || !state.msa) {
        return;
      }
      const clusterKey = button.dataset.msaClusterLabel;
      if (!clusterKey) {
        return;
      }
      const allKeys = allMsaClusterKeys();
      if (allKeys.length === 0) {
        return;
      }
      const isModifier = event.ctrlKey || event.metaKey;
      const isCurrentlyOnlyOne =
        state.msaVisibleClusters.size === 1 && state.msaVisibleClusters.has(clusterKey);
      if (isModifier) {
        state.msaVisibleClusters = isCurrentlyOnlyOne ? new Set(allKeys) : new Set([clusterKey]);
      } else if (state.msaVisibleClusters.has(clusterKey)) {
        state.msaVisibleClusters.delete(clusterKey);
      } else {
        state.msaVisibleClusters.add(clusterKey);
      }
      render();
    });

    columnsClusterLegend?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-columns-cluster-label]");
      if (!button) {
        return;
      }
      const clusterKey = button.dataset.columnsClusterLabel;
      if (!clusterKey) {
        return;
      }
      const allKeys = allColumnsClusterLabels();
      if (allKeys.length === 0) {
        return;
      }
      const currentlyVisible = new Set(visibleColumnsClusters());
      const isModifier = event.ctrlKey || event.metaKey;
      const isCurrentlyOnlyOne =
        currentlyVisible.size === 1 && currentlyVisible.has(clusterKey);
      if (isModifier) {
        state.columnsVisibleClusters = isCurrentlyOnlyOne
          ? new Set(allKeys)
          : new Set([clusterKey]);
      } else if (state.columnsVisibleClusters.has(clusterKey)) {
        state.columnsVisibleClusters.delete(clusterKey);
      } else {
        state.columnsVisibleClusters.add(clusterKey);
      }
      renderColumnsClusterLegend();
      renderColumnsChart();
    });
  }

  function handleInitializeError(error) {
    appStatus.textContent = error.message;
    loadingPanel.classList.remove("hidden");
    loadingLabel.textContent = "Startup failed";
    loadingDetail.textContent = error.message;
    progressBar.style.width = "100%";
  }

  return {
    clearViewer,
    closeMsaPicker,
    displayAlignmentLength,
    drawGrid,
    hideLoading,
    initialize,
    handleInitializeError,
    loadCurrentSelection,
    openMsaPicker,
    originalColumnForDisplay,
    render,
    renderMsaPickerOptions,
    scheduleLayoutSync,
    selectFilteredRow,
    selectRowByKey,
    setDetails,
    setLoading,
    setOptions,
    syncMsaPanelView,
    syncMsaPickerSelection,
    syncSelectionSettingsUi,
    toggleMsaPicker,
    updateMsaFilterButtons,
    updateSelectedRowUi,
  };
}
