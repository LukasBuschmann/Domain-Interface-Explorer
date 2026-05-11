// @ts-nocheck
import {
  CELL_WIDTH,
  CLUSTER_COLOR_PALETTE,
  DEFAULT_CLUSTERING_SETTINGS,
  DEFAULT_EMBEDDING_SETTINGS,
  DEFAULT_STRUCTURE_DISPLAY_SETTINGS,
  HEADER_HEIGHT,
  LABEL_WIDTH,
  PARTNER_COLOR_PALETTE,
  ROW_HEIGHT,
  TEXT_FONT,
  THREE_TO_ONE,
} from "./constants.js";
import { elements } from "./dom.js";
import { createClusterCompareController } from "./clusterCompare.js";
import { fetchJson, fetchText } from "./api.js";
import { state } from "./state.js";
import { createStructureViewController } from "./structureView.js";
import { createRepresentativeViewController } from "./representativeView.js";
import { createEmbeddingViewController } from "./embeddingView.js";
import { createDendrogramViewController } from "./dendrogramView.js";
import { createMsaViewController } from "./msaView.js";
import {
  buildPartnerColorMap,
  clusterHoverColor as clusterHoverColorForLabel,
  clusterLensColor as clusterLensColorForLabel,
  colorToRgb as parseColorToRgb,
  columnColor as colorColumn,
  conservationColor,
  interpolateColor as mixColor,
  nonZeroRoundedPercent,
  partnerColorFromMap,
  partnerLensColor as colorPartnerLens,
} from "./colors.js";
import {
  activeConservationVector as getActiveConservationVector,
  buildStructureResidueLookup as buildStructureResidueLookupFromModel,
  columnStateDistribution as getColumnStateDistribution,
  topResiduesForColumn as getTopResiduesForColumn,
} from "./msaModel.js";
import { buildOverlayMaps, buildPairs, interactionRowKey, parseInteractionRowKey } from "./interfaceModel.js";
import { appendSelectionSettingsToParams, parseSelectionSettingsDraft } from "./selectionSettings.js";

const {
  appStatus,
  closeClusterCompareModalButton,
  closeStructureModalButton,
  clusterCompareGrid,
  clusterCompareModal,
  columnCount,
  columnsRoot,
  clusteringSettingsToggle,
  detailsList,
  embeddingCanvas,
  embeddingRoot,
  embeddingClusterDistanceInput,
  embeddingClusterDistanceThresholdInput,
  embeddingClusterEpsilonInput,
  embeddingClusterHierarchicalMinSizeInput,
  embeddingClusterLifetimeThresholdInput,
  embeddingClusterLinkageInput,
  embeddingClusterMinSamplesInput,
  embeddingClusterMinSizeInput,
  embeddingClusterNClustersInput,
  embeddingClusteringApply,
  embeddingEarlyExaggerationInput,
  embeddingDistanceInput,
  embeddingInfo,
  embeddingLearningRateInput,
  embeddingLoading,
  embeddingLoadingLabel,
  embeddingMemberNext,
  embeddingMemberPrev,
  embeddingMaxIterInput,
  embeddingPartnerLegend,
  embeddingPerplexityInput,
  embeddingSettingsPanel,
  embeddingSettingsToggle,
  embeddingTsneApply,
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
  msaPickerButton,
  msaPickerFilters,
  msaPickerMenu,
  msaPickerOptions,
  msaPickerSearch,
  msaPickerSelection,
  msaSelect,
  selectionSettingsApply,
  selectionSettingsPanel,
  selectionSettingsToggle,
  selectionMinInterfaceSizeInput,
  partnerSelect,
  progressBar,
  representativeClusterLegend,
  representativeColumnLegend,
  representativeColumnLegendEnd,
  representativeColumnLegendMid,
  representativeColumnLegendStart,
  representativeCopy,
  representativeHoverAccentLabel,
  representativeHoverCard,
  representativeHoverDetails,
  representativeHoverDistributionChart,
  representativeHoverDistributionLayout,
  representativeHoverDistributionLegend,
  representativeHoverDistributionPieLegend,
  representativeHoverDistributionTitle,
  representativeHoverTitle,
  representativeClusterGridButton,
  representativeLensGroup,
  representativeMethodButton,
  representativeMethodLabel,
  representativeMethodMenu,
  representativePartnerFilterList,
  representativeScopeControl,
  representativeScopeButton,
  representativeScopeLabel,
  representativeScopeMenu,
  representativeScopeSwatch,
  representativeViewerRoot,
  rowCount,
  rowSearchInput,
  selectedRowCopy,
  structureColumnLegend,
  structureColumnLegendEnd,
  structureColumnLegendMid,
  structureColumnLegendStart,
  structureColumnViewToggle,
  structureHoverCard,
  structureHoverDetails,
  structureHoverDistributionChart,
  structureHoverDistributionLegend,
  structureHoverHistogram,
  structureModal,
  structureMemberNext,
  structureMemberPrev,
  structureContactViewToggle,
  structureDisplaySettingsClose,
  structureDisplaySettingsPanel,
  structureRecenterDomainButton,
  structureModalStatus,
  structureModalSubtitle,
  structureModalTitle,
  structureStatus,
  structureViewerRoot,
  clusterCompareRerollButton,
  viewerRoot,
} = elements;

function activeConservationVector() {
  return getActiveConservationVector(state.msa);
}

function activeMsaPanelView() {
  return state.msaPanelView;
}

function parseInteractionRowKeyParts(rowKey) {
  const parsed = parseInteractionRowKey(rowKey);
  return {
    interfaceRowKey: parsed.interfaceRowKey,
    partnerDomain: parsed.partnerDomain,
    proteinId: parsed.proteinId,
    fragmentKey: parsed.fragmentKey,
    interactingFragmentKey: parsed.partnerFragmentKey,
  };
}

function nearestEmbeddingPoint(clientX, clientY, maxDistance = 14) {
  const rect = embeddingCanvas.getBoundingClientRect();
  const canvasX = clientX - rect.left;
  const canvasY = clientY - rect.top;
  let bestPoint = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const point of state.embeddingProjectedPoints || []) {
    const distance = Math.hypot(canvasX - point.screenX, canvasY - point.screenY);
    if (distance <= maxDistance && distance < bestDistance) {
      bestPoint = point;
      bestDistance = distance;
    }
  }
  return bestPoint;
}

function normalizedEmbeddingPointMembers(point) {
  const members = Array.isArray(point?.members) ? point.members : [];
  if (members.length > 0) {
    return members
      .map((member) => ({
        row_key: String(member?.row_key || ""),
        partner_domain: String(member?.partner_domain || ""),
      }))
      .filter((member) => member.row_key && member.partner_domain);
  }
  const rowKey = String(point?.row_key || "");
  const partnerDomain = String(point?.partner_domain || "");
  return rowKey && partnerDomain
    ? [{ row_key: rowKey, partner_domain: partnerDomain }]
    : [];
}

function normalizedClusterPointMembers(point) {
  const members = normalizedEmbeddingPointMembers(point);
  const rowKey = String(point?.row_key || "");
  const partnerDomain = String(point?.partner_domain || "");
  if (!rowKey || !partnerDomain) {
    return members;
  }
  const directKey = interactionRowKey(rowKey, partnerDomain);
  if (members.some((member) => embeddingMemberKey(member) === directKey)) {
    return members;
  }
  return [{ row_key: rowKey, partner_domain: partnerDomain }].concat(members);
}

function embeddingMemberKey(member) {
  return interactionRowKey(member?.row_key || "", member?.partner_domain || "");
}

function activeEmbeddingMemberKey() {
  const selection = state.embeddingMemberSelection;
  const member = selection?.members?.[selection.index];
  return member ? embeddingMemberKey(member) : "";
}

function setEmbeddingMemberSelectionFromPoint(point) {
  const members = normalizedEmbeddingPointMembers(point);
  if (members.length <= 1) {
    state.embeddingMemberSelection = null;
    syncEmbeddingMemberControls();
    return point?.interactionRowKey || "";
  }

  state.embeddingMemberSelection = {
    pointKey: String(point.group_id || point.interactionRowKey || ""),
    members,
    index: 0,
  };
  syncEmbeddingMemberControls();
  return activeEmbeddingMemberKey();
}

async function selectActiveEmbeddingMember() {
  const targetRowKey = activeEmbeddingMemberKey();
  if (!targetRowKey) {
    return;
  }
  const row = selectRowByKey(targetRowKey);
  if (!row) {
    return;
  }
  requestEmbeddingRender();
  try {
    await loadInteractiveStructure();
  } catch (error) {
    handleStructureLoadFailure(error);
  }
}

async function cycleEmbeddingMember(delta) {
  const selection = state.embeddingMemberSelection;
  const members = Array.isArray(selection?.members) ? selection.members : [];
  if (members.length <= 1) {
    return;
  }
  const currentIndex = Number.isInteger(selection.index) ? selection.index : 0;
  selection.index = (currentIndex + delta + members.length) % members.length;
  syncEmbeddingMemberControls();
  await selectActiveEmbeddingMember();
}

async function activateEmbeddingPoint(event, source = "unknown") {
  if (activeMsaPanelView() !== "embeddings" || !interfaceSelect.value) {
    console.debug("[embedding-click] ignored", {
      reason: "inactive panel or missing interface",
      source,
      panel: activeMsaPanelView(),
      interfaceFile: interfaceSelect.value || "",
    });
    return;
  }
  const point =
    embeddingPointAt(event.clientX, event.clientY) ||
    nearestEmbeddingPoint(event.clientX, event.clientY);
  const targetRowKey = point
    ? setEmbeddingMemberSelectionFromPoint(point)
    : state.embeddingHoverRowKey;
  if (!targetRowKey) {
    console.debug("[embedding-click] no point hit", {
      source,
      clientX: event.clientX,
      clientY: event.clientY,
      hoverRowKey: state.embeddingHoverRowKey,
      projectedPointCount: (state.embeddingProjectedPoints || []).length,
    });
    return;
  }
  const row = selectRowByKey(targetRowKey);
  if (!row) {
    const parsedTarget = parseInteractionRowKeyParts(targetRowKey);
    const allRows = state.msa?.rows || [];
    const exactRowExists = allRows.some((entry) => entry.row_key === targetRowKey);
    const interfaceRowExists = allRows.some(
      (entry) => String(entry.interface_row_key || "") === parsedTarget.interfaceRowKey
    );
    const relatedRows = allRows
      .filter((entry) => String(entry.interface_row_key || "") === parsedTarget.interfaceRowKey)
      .slice(0, 12)
      .map((entry) => ({
        rowKey: entry.row_key,
        interfaceRowKey: entry.interface_row_key || "",
        partnerDomain: entry.partner_domain || "",
        interactingFragmentKey: entry.interacting_fragment_key || "",
      }));
    console.warn("[embedding-click] point hit but no row found", {
      source,
      targetRowKey,
      pointRowKey: point?.row_key || "",
      partnerDomain: point?.partner_domain || "",
      exactRowExists,
      interfaceRowExists,
      interfaceRowKey: parsedTarget.interfaceRowKey,
      msaRowCount: allRows.length,
      relatedRows,
    });
    return;
  }
  try {
    await loadInteractiveStructure();
  } catch (error) {
    handleStructureLoadFailure(error);
  }
}

let renderRepresentativeStructure = () => {};
let setLoading = () => {};
let hideLoading = () => {};
let setOptions = () => {};

const embeddingViewController = createEmbeddingViewController({
  state,
  elements,
  interfaceSelect,
  partnerColor,
  renderRepresentativeClusterLegend,
  renderRepresentativeStructure: () => {
    void renderRepresentativeStructure();
  },
  syncRepresentativeScopeControls,
  representativeLens,
});
const {
  allEmbeddingClusterLabels,
  allColumnsClusterLabels,
  allRepresentativeClusterLabels,
  clusteringMethodLabel,
  currentClusterCompareQuery,
  currentEmbeddingClusteringQuery,
  currentEmbeddingClusteringRequestKey,
  currentEmbeddingQuery,
  currentEmbeddingRequestKey,
  currentHierarchicalTarget,
  embeddingClusterColor,
  embeddingClusterLabel,
  embeddingClusteringSettingsKey,
  embeddingDistanceLabel,
  embeddingLegendMode,
  embeddingPointAt,
  embeddingSettingsKey,
  ensureEmbeddingClusteringLoaded,
  ensureEmbeddingDataLoaded,
  ensureHierarchyStatusLoaded,
  normalizeHierarchicalDraft,
  parseEmbeddingClusteringSettingsDraft,
  parseEmbeddingSettingsDraft,
  readEmbeddingClusteringDraftInputs,
  renderEmbeddingLegend,
  renderEmbeddingPlot,
  requestEmbeddingRender,
  renderColumnsChart,
  renderColumnsClusterLegend,
  resetColumnsClusterSelection,
  resetEmbeddingClusterSelection,
  resetEmbeddingPartnerSelection,
  resetRepresentativeClusterSelection,
  resizeColumnsCanvas,
  resizeEmbeddingCanvas,
  setEmbeddingInfo,
  setColumnsInfo,
  syncEmbeddingLoadingUi,
  syncEmbeddingMemberControls,
  syncDistanceThresholdValueUi,
  syncPersistenceMinLifetimeValueUi,
  syncEmbeddingSettingsUi,
  syncHierarchicalTargetMemoryFromDraft,
  syncHierarchicalTargetUi,
  visibleColumnsClusters,
  visibleRepresentativeClusters,
} = embeddingViewController;

const dendrogramViewController = createDendrogramViewController({
  state,
  elements,
  interfaceSelect,
  appendClusteringSettingsToParams,
  embeddingClusteringSettingsKey,
  embeddingClusterColor,
  embeddingClusterLabel,
  partnerColor,
});
const {
  allDendrogramClusterLabels,
  clearDendrogram,
  ensureDendrogramLoaded,
  handleDendrogramPointerDown,
  handleDendrogramPointerMove,
  handleDendrogramPointerUp,
  handleDendrogramWheel,
  renderDendrogramLegend,
  renderDendrogram,
  requestDendrogramRender,
  resetDendrogramClusterSelection,
  resetDendrogramPartnerSelection,
  resizeDendrogramCanvas,
  scheduleDendrogramLoad,
  syncDendrogramControls,
} = dendrogramViewController;

function representativeLens() {
  return state.representativeLens;
}

function hasCurrentEmbeddingClustering() {
  return Boolean(
    interfaceSelect.value &&
    state.embeddingClustering?.file === interfaceSelect.value &&
    state.embeddingClustering?.settingsKey === embeddingClusteringSettingsKey() &&
    !state.embeddingClustering?.error &&
    Array.isArray(state.embeddingClustering?.points)
  );
}

function msaColumnMaxIndex() {
  return Math.max(0, Number(state.msa?.alignment_length || 1) - 1);
}

function columnColor(columnIndex) {
  return colorColumn(columnIndex, msaColumnMaxIndex());
}

function syncColumnLegend(element, startElement, midElement, endElement, visible) {
  if (!element || !startElement || !midElement || !endElement) {
    return;
  }
  const maxIndex = msaColumnMaxIndex();
  element.classList.toggle("hidden", !visible);
  startElement.textContent = "0";
  midElement.textContent = String(Math.round(maxIndex / 2));
  endElement.textContent = String(maxIndex);
}

function syncColumnLegends() {
  syncColumnLegend(
    structureColumnLegend,
    structureColumnLegendStart,
    structureColumnLegendMid,
    structureColumnLegendEnd,
    state.structureColumnView
  );
  syncColumnLegend(
    representativeColumnLegend,
    representativeColumnLegendStart,
    representativeColumnLegendMid,
    representativeColumnLegendEnd,
    representativeLens() === "column"
  );
  if (structureColumnViewToggle) {
    structureColumnViewToggle.checked = state.structureColumnView;
  }
  if (structureContactViewToggle) {
    structureContactViewToggle.checked = state.structureContactsVisible;
  }
}



function setRepresentativeHoverDetails(payload, accentLabel = "Dominant") {
  const values = payload || {
    residueId: "-",
    aminoAcid: "-",
    conservedness: "-",
    dominant: "-",
  };
  const items = [values.residueId, values.aminoAcid, values.conservedness, values.dominant];
  if (representativeHoverAccentLabel) {
    representativeHoverAccentLabel.textContent = accentLabel;
  }
  [...representativeHoverDetails.querySelectorAll("dd")].forEach((el, index) => {
    el.textContent = String(items[index]);
  });
}

function setRepresentativeHoverCardMode(mode, title = null) {
  const isCluster = mode === "cluster";
  if (representativeHoverTitle) {
    representativeHoverTitle.textContent = title || (isCluster ? "Representative Cluster" : "Representative Residue");
  }
  representativeHoverDetails?.classList.toggle("hidden", isCluster);
}

function setRepresentativeHoverDistribution(entries, title, emptyMessage = "Hover a representative residue dot.") {
  representativeHoverDistributionTitle.textContent = title || "Partner Distribution";
  representativeHoverDistributionLegend.classList.remove("hidden");
  representativeHoverDistributionLayout?.classList.add("hidden");
  if (!entries || entries.length === 0) {
    representativeHoverDistributionLegend.innerHTML = `<p class="structure-hover-empty">${emptyMessage}</p>`;
    return;
  }

  representativeHoverDistributionLegend.innerHTML = entries
    .map(
      (entry) => `
        <div class="representative-bar-row">
          <span class="representative-bar-label" title="${entry.label}">${entry.label}</span>
          <div class="structure-hist-bar">
            <div class="structure-hist-fill" style="width: ${entry.percent}%; background: ${entry.color};"></div>
          </div>
          <span class="representative-bar-value">${entry.percent}%</span>
        </div>
      `
    )
    .join("");
}

function hideRepresentativeHoverCard() {
  representativeHoverCard.classList.add("hidden");
  setRepresentativeHoverCardMode("partners");
  setRepresentativeHoverDetails(null, "Dominant");
  setRepresentativeHoverDistribution(null, "");
}

function resetRepresentativePartnerSelection() {
  const partners = state.interface?.partnerDomains || [];
  state.representativeVisiblePartners = new Set(partners);
}

function enabledRepresentativePartners() {
  const partners = state.interface?.partnerDomains || [];
  return partners.filter((partner) => state.representativeVisiblePartners.has(partner));
}

function invalidateRepresentativePartnerCache() {
  return;
}

function shouldShowRepresentativePartnerFilter() {
  return (
    representativeLens() === "partners" &&
    state.selectedPartner === "__all__" &&
    Boolean(state.interface?.partnerDomains?.length)
  );
}


function representativeScopeClusterLabels() {
  const clusterLabels = allRepresentativeClusterLabels();
  if (state.selectedPartner === "__all__") {
    return clusterLabels;
  }
  const labelsForPartner = new Set(
    (state.embeddingClustering?.points || [])
      .filter((point) => String(point.partner_domain || "") === state.selectedPartner)
      .map((point) => String(point.cluster_label))
      .filter((clusterLabel) => Number(clusterLabel) >= 0)
  );
  return clusterLabels.filter((clusterLabel) => labelsForPartner.has(String(clusterLabel)));
}

function activeRepresentativeClusterLabel() {
  const clusterLabels = representativeScopeClusterLabels();
  if (clusterLabels.length === 0) {
    return null;
  }
  const currentLabel = String(state.representativeClusterLabel ?? "");
  if (clusterLabels.includes(currentLabel)) {
    return currentLabel;
  }
  state.representativeClusterLabel = clusterLabels[0];
  return clusterLabels[0];
}

function syncRepresentativeScopeControls() {
  if (!representativeScopeControl || !representativeScopeButton || !representativeScopeMenu) {
    return;
  }
  const clusterLabels = representativeScopeClusterLabels();
  const hasClusters = clusterLabels.length > 0;
  representativeScopeControl.classList.toggle("hidden", !hasClusters);
  if (!hasClusters) {
    if (!state.embeddingClusteringLoading) {
      state.representativeScope = "overall";
      state.representativeClusterLabel = null;
    }
    if (representativeScopeSwatch) {
      representativeScopeSwatch.style.background = "#817a71";
    }
    if (representativeScopeLabel) {
      representativeScopeLabel.textContent = "Overall";
      representativeScopeLabel.style.color = "#817a71";
    }
    representativeScopeControl.style.borderColor = "rgba(129, 122, 113, 0.32)";
    representativeScopeButton.disabled = Boolean(state.embeddingClusteringLoading);
    setRepresentativeScopeMenuOpen(false);
    return;
  }

  if (state.representativeScope === "cluster") {
    activeRepresentativeClusterLabel();
  }
  const optionsKey = clusterLabels.join("|");
  if (representativeScopeMenu.dataset.optionsKey !== optionsKey) {
    representativeScopeMenu.innerHTML = [
      `<button
        type="button"
        class="representative-scope-option"
        data-representative-scope-value="overall"
        role="menuitemradio"
        aria-checked="false"
      >
        <span class="representative-scope-swatch representative-scope-option-swatch" style="background: #817a71;"></span>
        <span>Overall</span>
      </button>`,
      ...clusterLabels.map(
        (clusterLabel) =>
          `<button
            type="button"
            class="representative-scope-option"
            data-representative-scope-value="cluster:${clusterLabel}"
            role="menuitemradio"
            aria-checked="false"
          >
            <span class="representative-scope-swatch representative-scope-option-swatch" style="background: ${embeddingClusterColor(clusterLabel)};"></span>
            <span>${embeddingClusterLabel(clusterLabel)}</span>
          </button>`
      ),
    ].join("");
    representativeScopeMenu.dataset.optionsKey = optionsKey;
  }
  const selectedValue =
    state.representativeScope === "cluster"
      ? `cluster:${activeRepresentativeClusterLabel()}`
      : "overall";
  representativeScopeButton.disabled = Boolean(state.embeddingClusteringLoading);
  const selectedClusterLabel =
    state.representativeScope === "cluster" ? activeRepresentativeClusterLabel() : null;
  const swatchColor =
    selectedClusterLabel === null ? "#817a71" : embeddingClusterColor(selectedClusterLabel);
  if (representativeScopeSwatch) {
    representativeScopeSwatch.style.background = swatchColor;
  }
  if (representativeScopeLabel) {
    representativeScopeLabel.textContent =
      selectedClusterLabel === null ? "Overall" : embeddingClusterLabel(selectedClusterLabel);
    representativeScopeLabel.style.color = selectedClusterLabel === null ? "#817a71" : "";
  }
  representativeScopeControl.style.borderColor =
    selectedClusterLabel === null ? "rgba(129, 122, 113, 0.32)" : swatchColor;
  [...representativeScopeMenu.querySelectorAll("[data-representative-scope-value]")].forEach(
    (button) => {
      const isActive = button.dataset.representativeScopeValue === selectedValue;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-checked", isActive ? "true" : "false");
    }
  );
}

function setRepresentativeScopeFromValue(value) {
  const normalized = String(value || "overall");
  if (normalized.startsWith("cluster:")) {
    state.representativeScope = "cluster";
    state.representativeClusterLabel = normalized.slice("cluster:".length);
  } else {
    state.representativeScope = "overall";
    state.representativeClusterLabel = null;
  }
  syncRepresentativeScopeControls();
}

function setRepresentativeScopeMenuOpen(open) {
  if (!representativeScopeMenu || !representativeScopeButton) {
    return;
  }
  representativeScopeMenu.classList.toggle("hidden", !open);
  representativeScopeButton.setAttribute("aria-expanded", open ? "true" : "false");
  fitRepresentativeDropdownToViewport(representativeScopeMenu, open);
}

function fitRepresentativeDropdownToViewport(menu, open) {
  if (!menu) {
    return;
  }
  menu.classList.remove("open-above");
  menu.style.maxHeight = "";
  if (!open) {
    return;
  }
  const control = menu.parentElement;
  if (!control) {
    return;
  }
  const viewportPadding = 12;
  const menuGap = 4;
  const controlRect = control.getBoundingClientRect();
  const spaceBelow = window.innerHeight - controlRect.bottom - viewportPadding - menuGap;
  const spaceAbove = controlRect.top - viewportPadding - menuGap;
  const openAbove = spaceBelow < 160 && spaceAbove > spaceBelow;
  const availableHeight = Math.max(72, Math.floor(openAbove ? spaceAbove : spaceBelow));
  menu.classList.toggle("open-above", openAbove);
  menu.style.maxHeight = `${availableHeight}px`;
}

function syncRepresentativeMethodControls() {
  const activeMethod = state.representativeMethod === "residue" ? "residue" : "balanced";
  if (representativeMethodLabel) {
    representativeMethodLabel.textContent = activeMethod === "residue" ? "Residue" : "Balanced";
  }
  if (representativeMethodButton) {
    const menuOpen = representativeMethodMenu
      ? !representativeMethodMenu.classList.contains("hidden")
      : false;
    representativeMethodButton.setAttribute(
      "aria-expanded",
      menuOpen ? "true" : "false"
    );
  }
  if (!representativeMethodMenu) {
    return;
  }
  [...representativeMethodMenu.querySelectorAll("[data-representative-method]")].forEach(
    (button) => {
      const isActive = button.dataset.representativeMethod === activeMethod;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-checked", isActive ? "true" : "false");
    }
  );
}

function setRepresentativeMethodFromValue(value) {
  const nextMethod = String(value || "") === "residue" ? "residue" : "balanced";
  if (state.representativeMethod === nextMethod) {
    syncRepresentativeMethodControls();
    return false;
  }
  state.representativeMethod = nextMethod;
  syncRepresentativeMethodControls();
  return true;
}

function setRepresentativeMethodMenuOpen(open) {
  if (!representativeMethodMenu || !representativeMethodButton) {
    return;
  }
  representativeMethodMenu.classList.toggle("hidden", !open);
  representativeMethodButton.setAttribute("aria-expanded", open ? "true" : "false");
  fitRepresentativeDropdownToViewport(representativeMethodMenu, open);
}

function appendClusteringSettingsToParams(params) {
  params.set("method", String(state.embeddingClusteringSettings.method));
  params.set("distance", String(state.embeddingClusteringSettings.distance));
  if (state.embeddingClusteringSettings.method === "hierarchical") {
    const hierarchicalTarget = currentHierarchicalTarget(state.embeddingClusteringSettings);
    params.set("linkage", String(state.embeddingClusteringSettings.linkage));
    params.set("hierarchical_target", hierarchicalTarget);
    const minClusterSize = String(
      state.embeddingClusteringSettings.hierarchicalMinClusterSize ??
        DEFAULT_CLUSTERING_SETTINGS.hierarchicalMinClusterSize
    ).trim();
    if (minClusterSize !== "") {
      params.set(
        "hierarchical_min_cluster_size",
        minClusterSize
      );
    }
    if (
      hierarchicalTarget === "n_clusters" &&
      String(state.embeddingClusteringSettings.nClusters).trim() !== ""
    ) {
      params.set("n_clusters", String(state.embeddingClusteringSettings.nClusters));
    }
    if (
      hierarchicalTarget === "distance_threshold" &&
      String(state.embeddingClusteringSettings.distanceThreshold).trim() !== ""
    ) {
      params.set("distance_threshold", String(state.embeddingClusteringSettings.distanceThreshold));
    }
    if (hierarchicalTarget === "persistence") {
      const minLifetime = String(state.embeddingClusteringSettings.persistenceMinLifetime ?? "").trim();
      params.set(
        "persistence_min_lifetime",
        minLifetime || String(DEFAULT_CLUSTERING_SETTINGS.persistenceMinLifetime)
      );
    }
  } else {
    params.set("min_cluster_size", String(state.embeddingClusteringSettings.minClusterSize));
    params.set(
      "cluster_selection_epsilon",
      String(state.embeddingClusteringSettings.clusterSelectionEpsilon)
    );
    if (String(state.embeddingClusteringSettings.minSamples).trim() !== "") {
      params.set("min_samples", String(state.embeddingClusteringSettings.minSamples));
    }
  }
  return params;
}

async function representativeSelectionUrl() {
  if (state.representativeScope === "cluster") {
    await ensureEmbeddingClusteringLoaded();
    syncRepresentativeScopeControls();
  }
  const params = new URLSearchParams({
    file: interfaceSelect.value,
    partner: String(state.selectedPartner || "__all__"),
    representative_scope: String(state.representativeScope || "overall"),
    representative_method: String(state.representativeMethod || "balanced"),
  });
  appendSelectionSettingsToParams(params, state.selectionSettings);
  if (state.representativeScope === "cluster") {
    const clusterLabel = activeRepresentativeClusterLabel();
    if (clusterLabel !== null) {
      params.set("cluster_label", String(clusterLabel));
      appendClusteringSettingsToParams(params);
    } else {
      params.set("representative_scope", "overall");
      state.representativeScope = "overall";
    }
  }
  return `/api/representative?${params.toString()}`;
}

function normalizeRepresentativeResidueIds(row, alignmentLength) {
  const rawResidueIds = Array.isArray(row?.residueIds)
    ? row.residueIds
    : Array.isArray(row?.residue_ids)
      ? row.residue_ids
      : [];
  const normalized = rawResidueIds.map((value) => {
    if (value === null || value === undefined || value === "") {
      return null;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  });
  if (normalized.length >= alignmentLength) {
    return normalized.slice(0, alignmentLength);
  }
  return normalized.concat(new Array(alignmentLength - normalized.length).fill(null));
}

function normalizeRepresentativeRow(row, alignmentLength) {
  if (!row) {
    return null;
  }
  const normalizedLength = Math.max(
    0,
    Number(alignmentLength || state.msa?.alignment_length || row?.aligned_sequence?.length || 0)
  );
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
  return {
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
    residueIds: normalizeRepresentativeResidueIds(row, effectiveLength),
  };
}

async function refreshRepresentativeSelection(emptyMessage = "No representative row found.") {
  if (!interfaceSelect.value) {
    state.representativeRowKey = null;
    state.representativeRowSnapshot = null;
    state.representativeClusterSummaries = null;
    resetRepresentativePanel(emptyMessage);
    return;
  }

  const requestId = state.representativeSelectionRequestId + 1;
  state.representativeSelectionRequestId = requestId;
  state.representativeClusterSummaries = null;
  state.representativeHoveredClusterLabel = null;
  representativeCopy.textContent = "Finding representative";
  renderRepresentativeClusterLegend();

  try {
    const payload = await fetchJson(await representativeSelectionUrl());
    if (requestId !== state.representativeSelectionRequestId) {
      return;
    }
    const row = normalizeRepresentativeRow(payload.row || null, payload.alignment_length);
    const nextRowKey = row?.row_key || payload.representative_row_key || null;
    if (nextRowKey !== state.representativeRowKey) {
      state.representativeStructure = null;
      state.representativeRenderedRowKey = null;
      state.representativeHoveredClusterLabel = null;
    }
    state.representativeRowKey = nextRowKey;
    state.representativeRowSnapshot = row;
    state.representativeClusterSummaries = Array.isArray(payload.cluster_summaries)
      ? payload.cluster_summaries
      : null;
    if (state.representativeStructure?.row?.row_key === state.representativeRowKey) {
      state.representativeStructure.row = row;
    }
    drawGrid();

    if (!state.representativeRowKey || !state.representativeRowSnapshot) {
      resetRepresentativePanel(emptyMessage);
      return;
    }

    await loadRepresentativeStructure();
  } catch (error) {
    representativeCopy.textContent = String(state.representativeRowKey || "");
    appStatus.textContent = error.message;
  }
}

function buildStructureResidueLookup(row) {
  const normalizedRow =
    !Array.isArray(row?.residueIds) && Array.isArray(row?.residue_ids)
      ? normalizeRepresentativeRow(row, state.msa?.alignment_length)
      : row;
  if (!Array.isArray(normalizedRow?.residueIds) || !normalizedRow.residueIds.length) {
    return new Map();
  }
  return buildStructureResidueLookupFromModel(normalizedRow, activeConservationVector());
}

function columnResidueStyles(residueLookup) {
  const styles = [];
  for (const entry of residueLookup.values()) {
    styles.push({
      residueId: entry.residueId,
      color: columnColor(entry.columnIndex),
      intensity: 1,
    });
  }
  return styles;
}

function activeStructureColumnMarker() {
  const marker = state.embeddingMemberSelection?.columnMarker;
  const columnIndex = Number(marker?.columnIndex);
  if (!Number.isInteger(columnIndex) || columnIndex < 0) {
    return null;
  }
  return {
    ...marker,
    columnIndex,
    color: marker.color || "#2d6a4f",
  };
}

function structureMarkerResidueStyles(residueLookup) {
  const marker = activeStructureColumnMarker();
  if (!marker || !(residueLookup instanceof Map)) {
    return [];
  }
  const styles = [];
  for (const entry of residueLookup.values()) {
    if (entry.columnIndex !== marker.columnIndex) {
      continue;
    }
    styles.push({
      residueId: entry.residueId,
      color: marker.color,
      intensity: 1,
      columnIndex: marker.columnIndex,
    });
  }
  return styles;
}

function interpolateColor(start, end, fraction) {
  return mixColor(start, end, fraction);
}

function colorToRgb(color) {
  return parseColorToRgb(color);
}

function partnerLensColor(partnerDomain, fraction) {
  return colorPartnerLens(partnerColor, partnerDomain, fraction);
}

function partnerColor(partnerDomain) {
  return partnerColorFromMap(state.interface?.partnerColors, partnerDomain);
}

function activeRepresentativeInterfaceStats() {
  if (!state.interface) {
    return null;
  }
  if (state.selectedPartner === "__all__") {
    return state.interface.allColumnStats || null;
  }
  return state.interface.partnerColumnStats?.get(state.selectedPartner) || null;
}

function topResiduesForColumn(columnIndex, selectedResidue) {
  return getTopResiduesForColumn(state.msa, state.filteredRowIndexes, columnIndex, selectedResidue);
}

function columnStateDistribution(columnIndex, rowIndexes = state.filteredRowIndexes) {
  return getColumnStateDistribution(state.msa, rowIndexes, columnIndex, overlayStateForRow);
}

function representativeResidueStyles(row, clusterLensData = null) {
  if (!row) {
    return [];
  }

  const residueLookup = buildStructureResidueLookup(row);
  const mode = representativeLens();
  const baseColor = [143, 138, 130];
  const interfaceColor = [188, 64, 45];
  const conservedColor = [47, 125, 90];
  const styles = [];
  const interfaceStats = mode === "interface" ? activeRepresentativeInterfaceStats() : null;
  const interfaceDenominator = interfaceStats?.denominator || 0;
  const clusterByResidueId = clusterLensData?.clusterByResidueId || new Map();
  for (const entry of residueLookup.values()) {
    let color = `rgb(${baseColor[0]}, ${baseColor[1]}, ${baseColor[2]})`;
    let intensity = 0;
    if (mode === "interface") {
      const interfaceCount = interfaceStats?.columnCounts.get(entry.columnIndex) || 0;
      intensity = interfaceDenominator > 0 ? interfaceCount / interfaceDenominator : 0;
      color = interpolateColor(baseColor, interfaceColor, intensity);
    } else if (mode === "conservedness") {
      intensity = Math.max(
        0,
        Math.min(1, Number(activeConservationVector()[entry.columnIndex] || 0) / 100)
      );
      color = interpolateColor(baseColor, conservedColor, intensity);
    } else if (mode === "partners") {
      const partnerDistribution = partnerInteractionDistribution(entry.columnIndex);
      const dominantPartner = partnerDistribution[0];
      intensity = dominantPartner?.fraction || 0;
      color = dominantPartner
        ? partnerLensColor(dominantPartner.partnerDomain, intensity)
        : color;
    } else if (mode === "column") {
      intensity = 1;
      color = columnColor(entry.columnIndex);
    } else if (mode === "cluster") {
      const clusterResidue = clusterByResidueId.get(entry.residueId);
      if (!clusterResidue) {
        continue;
      }
      intensity = clusterResidue.supportFraction || 0;
      color = clusterLensColor(clusterResidue.clusterLabel, intensity);
    }
    styles.push({
      residueId: entry.residueId,
      color,
      intensity,
    });
  }
  return styles;
}

function partnerInteractionDistribution(columnIndex) {
  if (
    !state.msa ||
    !state.interface ||
    columnIndex === null ||
    columnIndex === undefined ||
    state.selectedPartner !== "__all__"
  ) {
    return [];
  }

  const enabledPartners = enabledRepresentativePartners();
  if (enabledPartners.length === 0) {
    return [];
  }

  const entries = [];
  for (const partnerDomain of enabledPartners) {
    const stats = state.interface.partnerColumnStats?.get(partnerDomain);
    const denominator = stats?.denominator || 0;
    const count = stats?.columnCounts.get(columnIndex) || 0;

    if (denominator === 0 || count === 0) {
      continue;
    }

    const rawPercent = (count / denominator) * 100;
    const roundedPercent = Math.round(rawPercent);

    entries.push({
      key: partnerDomain,
      partnerDomain,
      label: partnerDomain,
      color: partnerColor(partnerDomain),
      count,
      denominator,
      fraction: count / denominator,
      percent: Math.max(1, roundedPercent),
    });
  }

  entries.sort(
    (a, b) =>
      b.fraction - a.fraction ||
      b.count - a.count ||
      a.partnerDomain.localeCompare(b.partnerDomain)
  );

  const totalFraction = entries.reduce((sum, entry) => sum + entry.fraction, 0);
  const distribution = entries.map((entry) => ({
    ...entry,
    chartPercent: totalFraction > 0 ? (entry.fraction / totalFraction) * 100 : 0,
  }));
  return distribution;
}

function clusterLensColor(clusterLabel, supportFraction = 0) {
  return clusterLensColorForLabel(clusterLabel, supportFraction);
}

function clusterHoverColor(clusterLabel) {
  return clusterHoverColorForLabel(clusterLabel);
}

function representativeClusterCompareSummaries() {
  const allowedLabels = new Set(representativeScopeClusterLabels().map(String));
  const summaries = representativeClusterSummaries()
    .filter((summary) => allowedLabels.size === 0 || allowedLabels.has(String(summary.clusterLabel)))
    .map((summary) => {
      const compareMemberCount =
        state.selectedPartner === "__all__"
          ? Number(summary.memberCount || 0)
          : Number(summary.partnerCounts?.get?.(state.selectedPartner) || 0);
      return {
        ...summary,
        compareMemberCount,
      };
    })
    .filter((summary) => summary.compareMemberCount > 0);
  const totalMemberCount = summaries.reduce(
    (sum, summary) => sum + Number(summary.compareMemberCount || 0),
    0
  );
  return summaries
    .map((summary) => {
      const coverageFraction =
        totalMemberCount > 0 ? Number(summary.compareMemberCount || 0) / totalMemberCount : 0;
      return {
        ...summary,
        totalMemberCount,
        coverageFraction,
        coveragePercent: coverageFraction * 100,
      };
    })
    .sort(
      (left, right) =>
        right.compareMemberCount - left.compareMemberCount ||
        left.clusterLabel - right.clusterLabel
    );
}

function representativeClusterCompareUrl(clusterLabel, method = state.representativeClusterCompareMethod) {
  const params = new URLSearchParams({
    file: interfaceSelect.value,
    partner: String(state.selectedPartner || "__all__"),
    representative_scope: "cluster",
    representative_method: String(method || "balanced"),
    cluster_label: String(clusterLabel),
  });
  appendSelectionSettingsToParams(params, state.selectionSettings);
  appendClusteringSettingsToParams(params);
  return `/api/representative?${params.toString()}`;
}

function representativeClusterCompareTileStyles(row, clusterSummary) {
  if (!row || !clusterSummary) {
    return {
      residueStyles: [],
      clusterLensData: { clusters: [], clusterByResidueId: new Map() },
    };
  }
  const residueLookup = buildStructureResidueLookup(row);
  const clusterLabel = Number(clusterSummary.clusterLabel);
  const memberCount = Number(clusterSummary.memberCount || 0);
  const minSupportFraction = 0.04;
  const clusterByResidueId = new Map();
  const residueIds = [];
  const residueStyles = [];
  for (const entry of residueLookup.values()) {
    const columnCount = Number(clusterSummary.columnCounts?.get?.(entry.columnIndex) || 0);
    if (columnCount <= 0 || memberCount <= 0) {
      continue;
    }
    const supportFraction = columnCount / memberCount;
    if (supportFraction < minSupportFraction) {
      continue;
    }
    const color = clusterLensColor(clusterLabel, supportFraction);
    const residueCluster = {
      clusterLabel,
      label: clusterSummary.label,
      residueId: entry.residueId,
      columnIndex: entry.columnIndex,
      memberCount,
      columnCount,
      supportFraction,
      color,
      hoverColor: clusterHoverColor(clusterLabel),
      distribution: clusterSummary.partnerDistribution,
    };
    clusterByResidueId.set(entry.residueId, residueCluster);
    residueIds.push(entry.residueId);
    residueStyles.push({
      residueId: entry.residueId,
      color,
      intensity: supportFraction,
    });
  }
  return {
    residueStyles,
    clusterLensData: {
      clusters: [
        {
          ...clusterSummary,
          residueIds: residueIds.sort((left, right) => left - right),
        },
      ],
      clusterByResidueId,
    },
  };
}

function representativeClusterSummariesFromPayload() {
  const payloadSummaries = state.representativeClusterSummaries;
  if (!Array.isArray(payloadSummaries)) {
    return null;
  }
  return payloadSummaries
    .map(normalizeRepresentativeClusterSummaryPayload)
    .filter(Boolean)
    .sort((left, right) => left.clusterLabel - right.clusterLabel);
}

function normalizeRepresentativeClusterSummaryPayload(summary) {
  const clusterLabel = Number(summary?.cluster_label);
  if (!Number.isFinite(clusterLabel) || clusterLabel < 0) {
    return null;
  }
  const partnerCounts = summary?.partner_counts || {};
  const totalPartnerCounts = summary?.total_partner_counts || {};
  const partnerDistribution = Object.entries(partnerCounts)
    .map(([partnerDomain, count]) => {
      const numericCount = Number(count || 0);
      const totalPartnerCount = Number(totalPartnerCounts[partnerDomain] || 0);
      const rawPercent = totalPartnerCount > 0 ? (numericCount / totalPartnerCount) * 100 : 0;
      return {
        label: partnerDomain,
        color: partnerColor(partnerDomain),
        count: numericCount,
        totalCount: totalPartnerCount,
        percent: nonZeroRoundedPercent(rawPercent),
        chartPercent: rawPercent,
      };
    })
    .sort(
      (left, right) =>
        right.chartPercent - left.chartPercent ||
        right.count - left.count ||
        left.label.localeCompare(right.label)
    );
  const columnCounts = new Map();
  for (const entry of summary?.column_counts || []) {
    if (!Array.isArray(entry) || entry.length < 2) {
      continue;
    }
    const columnIndex = Number(entry[0]);
    const count = Number(entry[1]);
    if (!Number.isInteger(columnIndex) || columnIndex < 0 || !Number.isFinite(count)) {
      continue;
    }
    columnCounts.set(columnIndex, count);
  }
  return {
    clusterLabel,
    label: embeddingClusterLabel(clusterLabel),
    color: embeddingClusterColor(clusterLabel),
    memberCount: Number(summary?.member_count || 0),
    columnCounts,
    partnerCounts: new Map(Object.entries(partnerCounts)),
    partnerDistribution,
  };
}

function representativeClusterSummaryFromPayload(payload, clusterLabel, fallbackSummary = null) {
  const summaries = Array.isArray(payload?.cluster_summaries)
    ? payload.cluster_summaries
      .map(normalizeRepresentativeClusterSummaryPayload)
      .filter(Boolean)
    : [];
  return (
    summaries.find((summary) => String(summary.clusterLabel) === String(clusterLabel)) ||
    fallbackSummary
  );
}

function representativeClusterSummaries() {
  const payloadSummaries = representativeClusterSummariesFromPayload();
  if (payloadSummaries) {
    return payloadSummaries;
  }
  if (!state.interface || !state.embeddingClustering?.points?.length) {
    return [];
  }
  if (!hasCurrentEmbeddingClustering()) {
    return [];
  }

  const summaries = new Map();
  const totalPartnerCounts = new Map();
  for (const point of state.embeddingClustering.points) {
    const partnerDomain = String(point.partner_domain);
    totalPartnerCounts.set(partnerDomain, (totalPartnerCounts.get(partnerDomain) || 0) + 1);

    const clusterLabel = Number(point.cluster_label);
    if (!Number.isFinite(clusterLabel) || clusterLabel < 0) {
      continue;
    }

    let summary = summaries.get(clusterLabel);
    if (!summary) {
      summary = {
        clusterLabel,
        label: embeddingClusterLabel(clusterLabel),
        color: embeddingClusterColor(clusterLabel),
        memberCount: 0,
        columnCounts: new Map(),
        partnerCounts: new Map(),
      };
      summaries.set(clusterLabel, summary);
    }

    summary.memberCount += 1;
    summary.partnerCounts.set(partnerDomain, (summary.partnerCounts.get(partnerDomain) || 0) + 1);

    const interfaceColumns = state.interface.overlayByRow
      .get(point.row_key)
      ?.byPartner.get(partnerDomain)
      ?.interface;
    if (!interfaceColumns) {
      continue;
    }
    for (const columnIndex of interfaceColumns) {
      summary.columnCounts.set(columnIndex, (summary.columnCounts.get(columnIndex) || 0) + 1);
    }
  }

  return [...summaries.values()]
    .map((summary) => {
      const partnerDistribution = [...summary.partnerCounts.entries()]
        .map(([partnerDomain, count]) => {
          const totalPartnerCount = totalPartnerCounts.get(partnerDomain) || 0;
          const rawPercent = totalPartnerCount > 0 ? (count / totalPartnerCount) * 100 : 0;
          return {
            label: partnerDomain,
            color: partnerColor(partnerDomain),
            count,
            totalCount: totalPartnerCount,
            percent: nonZeroRoundedPercent(rawPercent),
            chartPercent: rawPercent,
          };
        })
        .sort(
          (left, right) =>
            right.chartPercent - left.chartPercent ||
            right.count - left.count ||
            left.label.localeCompare(right.label)
        );

      return {
        ...summary,
        partnerDistribution,
      };
    })
    .sort((left, right) => left.clusterLabel - right.clusterLabel);
}

function representativeClusterLensData(row) {
  if (!row) {
    return {
      clusters: [],
      clusterByResidueId: new Map(),
    };
  }

  const residueLookup = buildStructureResidueLookup(row);
  const visibleClusterLabels = new Set(visibleRepresentativeClusters());
  const clusterSummaries = representativeClusterSummaries().filter((summary) =>
    visibleClusterLabels.has(String(summary.clusterLabel))
  );
  const dominantClusterByColumn = new Map();
  const minSupportFraction = 0.04;

  for (const summary of clusterSummaries) {
    for (const [columnIndex, columnCount] of summary.columnCounts.entries()) {
      const supportFraction =
        summary.memberCount > 0
          ? columnCount / summary.memberCount
          : 0;
      if (supportFraction < minSupportFraction) {
        continue;
      }
      const existing = dominantClusterByColumn.get(columnIndex);
      if (
        !existing ||
        supportFraction > existing.supportFraction ||
        (
          supportFraction === existing.supportFraction &&
          columnCount > existing.columnCount
        ) ||
        (
          supportFraction === existing.supportFraction &&
          columnCount === existing.columnCount &&
          summary.clusterLabel < existing.clusterLabel
        )
      ) {
        dominantClusterByColumn.set(columnIndex, {
          clusterLabel: summary.clusterLabel,
          columnCount,
          memberCount: summary.memberCount,
          supportFraction,
        });
      }
    }
  }

  const clusters = new Map(
    clusterSummaries.map((summary) => [
      summary.clusterLabel,
      {
        ...summary,
        residueIds: [],
      },
    ])
  );
  const clusterByResidueId = new Map();

  for (const entry of residueLookup.values()) {
    const clusterAssignment = dominantClusterByColumn.get(entry.columnIndex);
    if (!clusterAssignment) {
      continue;
    }
    const clusterSummary = clusters.get(clusterAssignment.clusterLabel);
    if (!clusterSummary) {
      continue;
    }

    const supportFraction =
      clusterAssignment.memberCount > 0
        ? clusterAssignment.columnCount / clusterAssignment.memberCount
        : 0;
    const residueCluster = {
      clusterLabel: clusterSummary.clusterLabel,
      label: clusterSummary.label,
      residueId: entry.residueId,
      columnIndex: entry.columnIndex,
      memberCount: clusterSummary.memberCount,
      columnCount: clusterAssignment.columnCount,
      supportFraction,
      color: clusterLensColor(clusterSummary.clusterLabel, supportFraction),
      hoverColor: clusterHoverColor(clusterSummary.clusterLabel),
      distribution: clusterSummary.partnerDistribution,
    };
    clusterByResidueId.set(entry.residueId, residueCluster);
    clusterSummary.residueIds.push(entry.residueId);
  }

  const visibleClusters = [...clusters.values()].map((cluster) => ({
    ...cluster,
    residueIds: [...cluster.residueIds].sort((left, right) => left - right),
  }));

  return {
    clusters: visibleClusters,
    clusterByResidueId,
  };
}

function syncRepresentativeClusterCompareButton(clusters = null) {
  if (!representativeClusterGridButton) {
    return;
  }
  const clusterCount = Array.isArray(clusters)
    ? clusters.length
    : representativeClusterCompareSummaries().length;
  const visible =
    representativeLens() === "cluster" &&
    clusterCount > 0 &&
    !state.embeddingClustering?.error;
  representativeClusterGridButton.classList.toggle("hidden", !visible);
  representativeClusterGridButton.disabled =
    visible && state.embeddingClusteringLoading && !hasCurrentEmbeddingClustering();
}

function renderRepresentativeClusterLegend(clusterLensData = null) {
  if (!representativeClusterLegend) {
    return;
  }

  const shouldShow = representativeLens() === "cluster";
  representativeClusterLegend.classList.toggle("hidden", !shouldShow);
  if (!shouldShow) {
    representativeClusterLegend.innerHTML = "";
    syncRepresentativeClusterCompareButton([]);
    return;
  }

  if (state.embeddingClusteringLoading && !hasCurrentEmbeddingClustering()) {
    representativeClusterLegend.innerHTML =
      '<span class="representative-cluster-legend-title">Clusters</span><p class="embedding-legend-empty">Loading clustering…</p>';
    syncRepresentativeClusterCompareButton([]);
    return;
  }

  if (state.embeddingClustering?.error) {
    representativeClusterLegend.innerHTML =
      `<span class="representative-cluster-legend-title">Clusters</span><p class="embedding-legend-empty">${state.embeddingClustering.error}</p>`;
    syncRepresentativeClusterCompareButton([]);
    return;
  }

  const clusters = representativeClusterSummaries();
  if (clusters.length === 0) {
    representativeClusterLegend.innerHTML =
      '<span class="representative-cluster-legend-title">Clusters</span><p class="embedding-legend-empty">No cluster regions available.</p>';
    syncRepresentativeClusterCompareButton([]);
    return;
  }
  syncRepresentativeClusterCompareButton(representativeClusterCompareSummaries());

  representativeClusterLegend.innerHTML = `
    <span class="representative-cluster-legend-title">Clusters</span>
    <div class="representative-cluster-legend-list">
      ${clusters
        .map(
          (cluster) => `
            <button
              type="button"
              class="representative-cluster-chip ${state.representativeVisibleClusters.has(String(cluster.clusterLabel)) ? "active" : "inactive"}${cluster.clusterLabel === state.representativeHoveredClusterLabel ? " hovered" : ""}"
              data-cluster-label="${cluster.clusterLabel}"
              aria-pressed="${state.representativeVisibleClusters.has(String(cluster.clusterLabel)) ? "true" : "false"}"
              title="${cluster.label}"
            >
              <span class="representative-partner-filter-swatch" style="background: ${cluster.color};"></span>
              <span class="representative-cluster-chip-label">${cluster.label}</span>
              <span class="representative-cluster-chip-value">${cluster.memberCount}</span>
            </button>
          `
        )
        .join("")}
    </div>
  `;
}

function updatePartnerOptions() {
  const partners = state.interface ? state.interface.partnerDomains : [];
  const options = [{ value: "__all__", label: "All partners" }].concat(
    partners.map((partner) => ({ value: partner, label: partner }))
  );
  setOptions(partnerSelect, options, "__all__");
  state.selectedPartner = "__all__";
  syncRepresentativeLensControls();
}

function getSelectedRow() {
  if (!state.msa || !state.selectedRowKey) {
    return null;
  }
  return (
    state.msa.rows.find((row) => row.row_key === state.selectedRowKey) ||
    (state.selectedRowSnapshot?.row_key === state.selectedRowKey ? state.selectedRowSnapshot : null)
  );
}

function getRepresentativeRow() {
  if (!state.msa || !state.representativeRowKey) {
    return null;
  }
  return (
    state.msa.rows.find((row) => row.row_key === state.representativeRowKey) ||
    (state.representativeRowSnapshot?.row_key === state.representativeRowKey
      ? state.representativeRowSnapshot
      : null)
  );
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

function getRowByKey(rowKey) {
  if (!state.msa) {
    return null;
  }
  return (
    state.msa.rows.find((row) => row.row_key === rowKey) ||
    (state.selectedRowSnapshot?.row_key === rowKey ? state.selectedRowSnapshot : null)
  );
}

function nextBrowserPaint() {
  return new Promise((resolve) => {
    window.requestAnimationFrame(() => {
      window.setTimeout(resolve, 0);
    });
  });
}

function clearEmbeddingMemberSelection() {
  state.embeddingMemberSelection = null;
  syncEmbeddingMemberControls([]);
}

function getStructurePreloadRows() {
  const members = Array.isArray(state.embeddingMemberSelection?.members)
    ? state.embeddingMemberSelection.members
    : [];
  if (members.length <= 1) {
    return [];
  }
  const rows = [];
  const seen = new Set();
  for (const member of members) {
    const rowKey = embeddingMemberKey(member);
    if (!rowKey || seen.has(rowKey)) {
      continue;
    }
    const row = getRowByKey(rowKey);
    if (row) {
      seen.add(rowKey);
      rows.push(row);
    }
  }
  return rows;
}

function clusterOverviewResidueMembers(clusterLabel, columnIndex) {
  const numericClusterLabel = Number(clusterLabel);
  const numericColumnIndex = Number(columnIndex);
  if (
    !Number.isFinite(numericClusterLabel) ||
    !Number.isInteger(numericColumnIndex) ||
    !(state.interface?.overlayByRow instanceof Map)
  ) {
    return [];
  }
  const selectedPartner = String(state.selectedPartner || "__all__");
  const members = [];
  const seen = new Set();
  for (const point of state.embeddingClustering?.points || []) {
    if (Number(point?.cluster_label) !== numericClusterLabel) {
      continue;
    }
    for (const member of normalizedClusterPointMembers(point)) {
      const rowKey = String(member?.row_key || "");
      const partnerDomain = String(member?.partner_domain || "");
      if (!rowKey || !partnerDomain) {
        continue;
      }
      if (selectedPartner !== "__all__" && partnerDomain !== selectedPartner) {
        continue;
      }
      const rowState = state.interface.overlayByRow.get(rowKey);
      const interfaceColumns = rowState?.byPartner?.get?.(partnerDomain)?.interface;
      const hasColumn =
        interfaceColumns instanceof Set &&
        (
          interfaceColumns.has(numericColumnIndex) ||
          interfaceColumns.has(String(numericColumnIndex))
        );
      if (!hasColumn) {
        continue;
      }
      const fullRowKey = interactionRowKey(rowKey, partnerDomain);
      if (!fullRowKey || seen.has(fullRowKey) || !getRowByKey(fullRowKey)) {
        continue;
      }
      seen.add(fullRowKey);
      members.push({
        row_key: rowKey,
        partner_domain: partnerDomain,
      });
    }
  }
  return members.sort(
    (left, right) =>
      left.partner_domain.localeCompare(right.partner_domain) ||
      left.row_key.localeCompare(right.row_key)
  );
}

async function openClusterOverviewResidueMembers({
  clusterLabel,
  columnIndex,
  residueId,
  residueName = "",
  entry = null,
} = {}) {
  const members = clusterOverviewResidueMembers(clusterLabel, columnIndex);
  if (members.length === 0) {
    const residueLabel = residueName ? `${residueName} ${residueId}` : `residue ${residueId}`;
    throw new Error(
      `No ${embeddingClusterLabel(clusterLabel)} interfaces interact at ${residueLabel} / MSA column ${columnIndex}.`
    );
  }

  const preferredKey = interactionRowKey(entry?.rowKey || "", entry?.partnerDomain || "");
  const preferredIndex = members.findIndex((member) => embeddingMemberKey(member) === preferredKey);
  state.embeddingMemberSelection = {
    pointKey: `cluster-overview:${clusterLabel}:${columnIndex}`,
    members,
    index: preferredIndex >= 0 ? preferredIndex : 0,
    columnMarker: {
      columnIndex: Number(columnIndex),
      residueId: Number.isFinite(Number(residueId)) ? Number(residueId) : null,
      residueName: String(residueName || ""),
      color: "#2d6a4f",
    },
  };
  syncEmbeddingMemberControls([]);
  appStatus.textContent =
    `Opening ${members.length} ${embeddingClusterLabel(clusterLabel)} interfaces at MSA column ${columnIndex}.`;
  await selectActiveEmbeddingMember();
}

async function openStructureForInteractionEntry(entry, loadedStructure = {}) {
  const rowKey = interactionRowKey(
    entry?.rowKey || entry?.row_key || "",
    entry?.partnerDomain || entry?.partner_domain || ""
  );
  clearEmbeddingMemberSelection();
  const row = selectRowByKey(rowKey);
  if (!row) {
    return;
  }
  if (loadedStructure.payload && typeof loadedStructure.modelText === "string") {
    await renderLoadedStructure(row, loadedStructure.payload, loadedStructure.modelText, {
      previewUrl: loadedStructure.previewUrl || "",
      initialView: loadedStructure.initialView || null,
      modelKey: loadedStructure.modelKey || "",
    });
    return;
  }
  try {
    await loadInteractiveStructure();
  } catch (error) {
    handleStructureLoadFailure(error);
  }
}

const structureViewController = createStructureViewController({
  state,
  elements,
  THREE_TO_ONE,
  interfaceSelect,
  setLoading,
  hideLoading,
  buildStructureResidueLookup,
  columnResidueStyles,
  structureMarkerResidueStyles,
  msaColumnMaxIndex,
  topResiduesForColumn,
  columnStateDistribution,
  syncColumnLegends,
  getSelectedRow,
  getStructurePreloadRows,
  clearEmbeddingMemberSelection,
});
const {
  closeStructureModal,
  getStructureViewer,
  handleStructureLoadFailure,
  loadInteractiveStructure,
  openStructureModal,
  recenterStructureDomain,
  renderLoadedStructure,
  renderInteractiveStructure,
  resetStructurePanel,
} = structureViewController;

const representativeViewController = createRepresentativeViewController({
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
});
const {
  clearRepresentativeClusterHover,
  getRepresentativeViewer,
  loadRepresentativeStructure,
  positionRepresentativeHoverCard,
  renderRepresentativePartnerFilter,
  resetRepresentativePanel,
  syncRepresentativeLensControls,
  syncRepresentativePartnerFilterVisibility,
} = representativeViewController;
renderRepresentativeStructure = representativeViewController.renderRepresentativeStructure;

const clusterCompareController = createClusterCompareController({
  state,
  elements,
  interfaceSelect,
  currentClusterCompareQuery,
  getRowByKey,
  embeddingClusterLabel,
  embeddingDistanceLabel,
  nextBrowserPaint,
  openStructureForEntry: openStructureForInteractionEntry,
  openClusterResidueMembers: openClusterOverviewResidueMembers,
  representativeClusterCompareSummaries,
  representativeClusterCompareUrl,
  normalizeRepresentativeRow,
  representativeClusterSummaryFromPayload,
  representativeClusterCompareTileStyles,
});
const {
  closeClusterCompareModal,
  openClusterCompareForLabel,
  openRepresentativeClusterCompare,
  resizeClusterCompareViewers,
} = clusterCompareController;

const STRUCTURE_DISPLAY_PRESETS = {
  soft: {
    ...DEFAULT_STRUCTURE_DISPLAY_SETTINGS,
    preset: "soft",
  },
  crisp: {
    ...DEFAULT_STRUCTURE_DISPLAY_SETTINGS,
    preset: "crisp",
    shadows: true,
    lightIntensity: 0.96,
    ambientIntensity: 0.36,
    contextAlpha: 0.2,
    quality: "higher",
    antialiasSampleLevel: 4,
  },
  illustrative: {
    ...DEFAULT_STRUCTURE_DISPLAY_SETTINGS,
    preset: "illustrative",
    outline: true,
    ambientOcclusion: true,
    shadows: false,
    contextAlpha: 0.3,
    roughness: 0.88,
    antialiasSampleLevel: 3,
  },
  performance: {
    ...DEFAULT_STRUCTURE_DISPLAY_SETTINGS,
    preset: "performance",
    ambientOcclusion: false,
    shadows: false,
    outline: false,
    depthOfField: false,
    fog: false,
    sharpen: false,
    quality: "medium",
    antialiasSampleLevel: 1,
    contextAlpha: 0.18,
  },
};

const STRUCTURE_REPRESENTATION_SETTINGS = new Set([
  "contextAlpha",
  "contactOpacity",
  "contactRadius",
  "roughness",
  "metalness",
  "bumpiness",
  "quality",
]);

let structureDisplayRefreshFrame = 0;

function structureDisplayControls() {
  return structureDisplaySettingsPanel
    ? [...structureDisplaySettingsPanel.querySelectorAll("[data-structure-display-setting]")]
    : [];
}

function setStructureDisplaySettingsOpen(open) {
  state.structureDisplaySettingsOpen = Boolean(open);
  structureDisplaySettingsPanel?.classList.toggle("hidden", !state.structureDisplaySettingsOpen);
  structureDisplaySettingsPanel?.setAttribute(
    "aria-hidden",
    state.structureDisplaySettingsOpen ? "false" : "true"
  );
}

function syncStructureDisplaySettingsUi() {
  const settings = state.structureDisplaySettings || {};
  for (const control of structureDisplayControls()) {
    const key = control.dataset.structureDisplaySetting;
    if (!key) {
      continue;
    }
    if (control.type === "checkbox") {
      control.checked = Boolean(settings[key]);
    } else if (settings[key] !== undefined) {
      control.value = String(settings[key]);
    }
  }
  setStructureDisplaySettingsOpen(state.structureDisplaySettingsOpen);
}

function controlValue(control) {
  if (control.type === "checkbox") {
    return control.checked;
  }
  if (control.type === "range" || control.type === "number") {
    const value = Number(control.value);
    return Number.isFinite(value) ? value : control.value;
  }
  return control.value;
}

function applyStructureDisplaySettingsToViewers() {
  const settings = state.structureDisplaySettings;
  state.structureViewer?.applyDisplaySettings?.(settings);
  state.representativeViewer?.applyDisplaySettings?.(settings);
  for (const tile of state.clusterCompareTiles || []) {
    tile.viewer?.applyDisplaySettings?.(settings);
  }
  resizeClusterCompareViewers();
}

function scheduleStructureDisplayRepresentationRefresh() {
  if (structureDisplayRefreshFrame) {
    window.cancelAnimationFrame(structureDisplayRefreshFrame);
  }
  structureDisplayRefreshFrame = window.requestAnimationFrame(() => {
    structureDisplayRefreshFrame = 0;
    if (state.structureData && structureModal && !structureModal.classList.contains("hidden")) {
      void renderInteractiveStructure();
    }
    if (state.representativeStructure) {
      void renderRepresentativeStructure();
    }
  });
}

function updateStructureDisplaySetting(control) {
  const key = control?.dataset?.structureDisplaySetting;
  if (!key) {
    return;
  }
  if (key === "preset") {
    const preset = control.value;
    state.structureDisplaySettings = {
      ...DEFAULT_STRUCTURE_DISPLAY_SETTINGS,
      ...(STRUCTURE_DISPLAY_PRESETS[preset] || {}),
      preset,
    };
    syncStructureDisplaySettingsUi();
    applyStructureDisplaySettingsToViewers();
    scheduleStructureDisplayRepresentationRefresh();
    return;
  }
  state.structureDisplaySettings = {
    ...state.structureDisplaySettings,
    preset: "custom",
    [key]: controlValue(control),
  };
  syncStructureDisplaySettingsUi();
  applyStructureDisplaySettingsToViewers();
  if (
    control.dataset.representationSetting === "true" ||
    STRUCTURE_REPRESENTATION_SETTINGS.has(key)
  ) {
    scheduleStructureDisplayRepresentationRefresh();
  }
}

function isTextEntryTarget(target) {
  const element = target instanceof Element ? target : null;
  if (!element) {
    return false;
  }
  return Boolean(
    element.closest("input, textarea, select, [contenteditable='true']")
  );
}

const msaViewController = createMsaViewController({
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
  syncRepresentativeScopeControls,
  syncEmbeddingLoadingUi,
  syncEmbeddingMemberControls,
  syncEmbeddingSettingsUi,
  resizeEmbeddingCanvas,
  resizeColumnsCanvas,
  resizeDendrogramCanvas,
  renderEmbeddingPlot,
  renderColumnsChart,
  renderDendrogram,
  renderDendrogramLegend,
  renderColumnsClusterLegend,
  setEmbeddingInfo,
  setColumnsInfo,
  ensureEmbeddingDataLoaded,
  ensureEmbeddingClusteringLoaded,
  ensureDendrogramLoaded,
  resetColumnsClusterSelection,
  resetDendrogramPartnerSelection,
  resetDendrogramClusterSelection,
  resetEmbeddingPartnerSelection,
  resetEmbeddingClusterSelection,
  resetRepresentativePartnerSelection,
  resetRepresentativeClusterSelection,
  renderRepresentativePartnerFilter,
  renderEmbeddingLegend,
  refreshRepresentativeSelection,
  loadInteractiveStructure,
  handleStructureLoadFailure,
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
  syncDendrogramControls,
});
const {
  clearViewer,
  closeMsaPicker,
  displayAlignmentLength,
  drawGrid,
  ensurePfamInfoLoaded,
  initialize,
  handleInitializeError,
  loadCurrentSelection,
  originalColumnForDisplay,
  render,
  renderMsaPickerOptions,
  scheduleLayoutSync,
  selectFilteredRow,
  labelHitTargetAtClientPoint,
  selectRowByKey,
  setDetails,
  syncMsaPanelView,
  syncMsaPickerSelection,
  syncSelectionSettingsUi,
  toggleMsaPicker,
  updateMsaFilterButtons,
} = msaViewController;
setLoading = msaViewController.setLoading;
hideLoading = msaViewController.hideLoading;
setOptions = msaViewController.setOptions;

function overlayStateForRow(rowOrKey) {
  if (!state.interface) {
    return null;
  }
  const rowKey = typeof rowOrKey === "string" ? rowOrKey : rowOrKey?.row_key;
  if (!rowKey) {
    return null;
  }
  const interactionState = state.interface.overlayByInteractionRow?.get(rowKey);
  if (interactionState) {
    if (state.selectedPartner === "__all__") {
      return interactionState.all;
    }
    return interactionState.byPartner.get(state.selectedPartner) || null;
  }
  const rowState = state.interface.overlayByRow.get(rowKey);
  if (!rowState) {
    return null;
  }
  if (state.selectedPartner === "__all__") {
    return rowState.all;
  }
  return rowState.byPartner.get(state.selectedPartner) || null;
}

function onGridHover(event) {
  if (!state.msa) {
    return;
  }
  const rect = gridScroll.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const displayColumn = Math.floor((x + gridScroll.scrollLeft) / CELL_WIDTH);
  const column = originalColumnForDisplay(displayColumn);
  const filteredRowIndex = Math.floor((y + gridScroll.scrollTop) / ROW_HEIGHT);

  if (
    filteredRowIndex < 0 ||
    filteredRowIndex >= state.filteredRowIndexes.length ||
    displayColumn < 0 ||
    displayColumn >= displayAlignmentLength() ||
    column === null
  ) {
    state.hover = null;
    setDetails(null);
    drawGrid();
    return;
  }

  const rowIndex = state.filteredRowIndexes[filteredRowIndex];
  const row = state.msa.rows[rowIndex];
  const overlay = overlayStateForRow(row);
  let residueState = "None";
  if (overlay?.surface.has(column)) {
    residueState = "Surface";
  }
  if (overlay?.interface.has(column)) {
    residueState = "Interface";
  }

  state.hover = { filteredRowIndex, column };
  setDetails({
    row: row.display_row_key || row.row_key,
    protein: row.protein_id,
    fragment: row.fragment_key,
    column,
    conservedness:
      activeConservationVector()[column] === undefined
        ? "-"
        : `${activeConservationVector()[column]}%`,
    residue: row.aligned_sequence[column] || "-",
    residueId: row.residueIds[column] ?? "-",
    state: residueState,
  });
  drawGrid();
}

function onGridLeave() {
  state.hover = null;
  setDetails(null);
  drawGrid();
}

msaPickerButton.addEventListener("click", (event) => {
  event.stopPropagation();
  if (state.selectionSettingsOpen) {
    state.selectionSettingsOpen = false;
    syncSelectionSettingsUi();
  }
  toggleMsaPicker();
});

selectionSettingsToggle?.addEventListener("click", (event) => {
  event.stopPropagation();
  closeMsaPicker();
  state.selectionSettingsOpen = !state.selectionSettingsOpen;
  state.selectionSettingsDraft = {
    ...state.selectionSettings,
  };
  syncSelectionSettingsUi();
});

msaPickerSearch.addEventListener("input", () => {
  renderMsaPickerOptions(msaPickerSearch.value);
});

msaPickerFilters.addEventListener("click", (event) => {
  const button = event.target.closest(".msa-filter-chip");
  if (!button) {
    return;
  }
  const metricKey = button.dataset.filterKey;
  const direction = button.dataset.filterDirection;
  const activeDirections = state.msaFilterState[metricKey];
  if (!activeDirections) {
    return;
  }

  if (activeDirections.has(direction)) {
    activeDirections.delete(direction);
  } else {
    activeDirections.add(direction);
  }
  updateMsaFilterButtons();
  renderMsaPickerOptions(msaPickerSearch.value);
});

partnerSelect.addEventListener("change", async () => {
  state.selectedPartner = partnerSelect.value;
  state.representativeHoveredClusterLabel = null;
  syncRepresentativeLensControls();
  syncRepresentativeScopeControls();
  state.hover = null;
  resetStructurePanel("Partner filter changed. Reload the interactive structure if needed.");
  render();
  await refreshRepresentativeSelection("No representative row found for the selected partner.");
  drawGrid();
});

selectionSettingsApply?.addEventListener("click", async () => {
  try {
    const nextSettings = parseSelectionSettingsDraft({
      minInterfaceSize: selectionMinInterfaceSizeInput?.value,
    });
    const settingsChanged =
      nextSettings.minInterfaceSize !== state.selectionSettings.minInterfaceSize;
    state.selectionSettings = nextSettings;
    state.selectionSettingsDraft = {
      ...nextSettings,
    };
    state.selectionSettingsOpen = false;
    syncSelectionSettingsUi();
    if (settingsChanged && msaSelect.value && interfaceSelect.value) {
      await loadCurrentSelection();
    }
  } catch (error) {
    appStatus.textContent = error.message;
  }
});

msaPanelTabs.addEventListener("click", (event) => {
  const button = event.target.closest("[data-panel-view]");
  if (!button) {
    return;
  }
  const nextView = button.dataset.panelView;
  if (!nextView || nextView === state.msaPanelView) {
    return;
  }
  state.msaPanelView = nextView;
  render();
  scheduleLayoutSync();
  if (nextView === "info") {
    void ensurePfamInfoLoaded();
  } else if (nextView === "embeddings") {
    void ensureEmbeddingDataLoaded();
    void ensureEmbeddingClusteringLoaded();
  } else if (nextView === "columns") {
    void ensureEmbeddingClusteringLoaded();
  } else if (nextView === "dendrogram") {
    void ensureDendrogramLoaded();
  }
});

let liveHierarchicalClusteringTimer = 0;
let hierarchyStatusTimer = 0;

function hierarchyDraftMatchesApplied() {
  return (
    state.embeddingClusteringSettings.method === "hierarchical" &&
    state.embeddingClusteringSettingsDraft.method === "hierarchical" &&
    state.embeddingClusteringSettings.distance === state.embeddingClusteringSettingsDraft.distance &&
    state.embeddingClusteringSettings.linkage === state.embeddingClusteringSettingsDraft.linkage
  );
}

function scheduleLiveHierarchicalClusteringUpdate() {
  if (!hierarchyDraftMatchesApplied()) {
    return;
  }
  window.clearTimeout(liveHierarchicalClusteringTimer);
  liveHierarchicalClusteringTimer = window.setTimeout(async () => {
    try {
      const nextSettings = parseEmbeddingClusteringSettingsDraft({
        preserveAppliedHierarchy: true,
      });
      state.embeddingClusteringSettings = nextSettings;
      const representativeDependsOnClustering =
        state.representativeScope === "cluster" || representativeLens() === "cluster";
      if (
        activeMsaPanelView() === "embeddings" ||
        activeMsaPanelView() === "columns" ||
        representativeDependsOnClustering
      ) {
        await ensureEmbeddingClusteringLoaded();
      }
      if (activeMsaPanelView() === "dendrogram") {
        await ensureDendrogramLoaded({ force: true });
      }
      if (state.representativeScope === "cluster") {
        await refreshRepresentativeSelection("No representative row found for the selected scope.");
      }
      render();
    } catch (error) {
      setEmbeddingInfo(error.message);
    }
  }, 250);
}

function scheduleHierarchyStatusUpdate() {
  window.clearTimeout(hierarchyStatusTimer);
  hierarchyStatusTimer = window.setTimeout(() => {
    state.embeddingClusteringSettingsDraft = readEmbeddingClusteringDraftInputs();
    void ensureHierarchyStatusLoaded();
  }, 250);
}

function placeEmbeddingSettingsPanel() {
  if (state.embeddingSettingsSection === "clustering") {
    if (embeddingSettingsPanel.parentElement !== msaPanelTabs.parentElement) {
      msaPanelTabs.insertAdjacentElement("afterend", embeddingSettingsPanel);
    }
    embeddingSettingsPanel.classList.remove("points-settings-panel");
    embeddingSettingsPanel.classList.add("clustering-settings-panel");
    return;
  }
  if (embeddingSettingsPanel.parentElement !== embeddingRoot) {
    embeddingRoot.appendChild(embeddingSettingsPanel);
  }
  embeddingSettingsPanel.classList.remove("clustering-settings-panel");
  embeddingSettingsPanel.classList.add("points-settings-panel");
}

function openEmbeddingSettingsSection(section) {
  const wasOpenOnSection =
    state.embeddingSettingsOpen && state.embeddingSettingsSection === section;
  state.embeddingSettingsOpen = !wasOpenOnSection;
  state.embeddingSettingsSection = section;
  state.embeddingSettingsDraft = {
    ...state.embeddingSettings,
  };
  state.embeddingClusteringSettingsDraft = {
    ...state.embeddingClusteringSettings,
  };
  if (String(state.embeddingClusteringSettings.nClusters).trim() !== "") {
    state.embeddingHierarchicalTargetMemory.nClusters = String(state.embeddingClusteringSettings.nClusters).trim();
  }
  if (String(state.embeddingClusteringSettings.distanceThreshold).trim() !== "") {
    state.embeddingHierarchicalTargetMemory.distanceThreshold = String(
      state.embeddingClusteringSettings.distanceThreshold
    ).trim();
  }
  if (String(state.embeddingClusteringSettings.persistenceMinLifetime).trim() !== "") {
    state.embeddingHierarchicalTargetMemory.persistenceMinLifetime = String(
      state.embeddingClusteringSettings.persistenceMinLifetime
    ).trim();
  }
  state.embeddingClusteringSettingsDraft = normalizeHierarchicalDraft(state.embeddingClusteringSettingsDraft);
  placeEmbeddingSettingsPanel();
  syncEmbeddingSettingsUi();
  if (state.embeddingSettingsOpen && section === "clustering") {
    void ensureHierarchyStatusLoaded();
  }
}

embeddingSettingsToggle.addEventListener("click", () => {
  openEmbeddingSettingsSection("points");
});

clusteringSettingsToggle?.addEventListener("click", () => {
  openEmbeddingSettingsSection("clustering");
});

embeddingSettingsPanel.addEventListener("click", (event) => {
  const pointsMethodButton = event.target.closest("[data-points-method]");
  if (pointsMethodButton) {
    const nextMethod = pointsMethodButton.dataset.pointsMethod;
    if (!nextMethod || nextMethod === state.embeddingSettingsDraft.method) {
      return;
    }
    state.embeddingSettingsDraft = {
      ...state.embeddingSettingsDraft,
      method: nextMethod,
    };
    syncEmbeddingSettingsUi();
    return;
  }

  const methodButton = event.target.closest("[data-clustering-method]");
  if (methodButton) {
    const nextMethod = methodButton.dataset.clusteringMethod;
    if (!nextMethod || nextMethod === state.embeddingClusteringSettingsDraft.method) {
      return;
    }
    state.embeddingClusteringSettingsDraft = {
      ...readEmbeddingClusteringDraftInputs(),
      method: nextMethod,
    };
    if (nextMethod === "hierarchical") {
      state.embeddingClusteringSettingsDraft = normalizeHierarchicalDraft(state.embeddingClusteringSettingsDraft);
    }
    syncEmbeddingSettingsUi();
    void ensureHierarchyStatusLoaded();
    return;
  }

  const hierarchicalTargetButton = event.target.closest("[data-hierarchical-target]");
  if (hierarchicalTargetButton) {
    const nextTarget = hierarchicalTargetButton.dataset.hierarchicalTarget;
    if (!nextTarget || nextTarget === currentHierarchicalTarget()) {
      return;
    }
    syncHierarchicalTargetMemoryFromDraft();
    state.embeddingClusteringSettingsDraft = normalizeHierarchicalDraft({
      ...readEmbeddingClusteringDraftInputs(),
      hierarchicalTarget: nextTarget,
    });
    syncEmbeddingSettingsUi();
    scheduleLiveHierarchicalClusteringUpdate();
    scheduleHierarchyStatusUpdate();
    return;
  }

  const button = event.target.closest("[data-settings-section]");
  if (!button) {
    return;
  }
  const nextSection = button.dataset.settingsSection;
  if (!nextSection || nextSection === state.embeddingSettingsSection) {
    return;
  }
  state.embeddingSettingsSection = nextSection;
  syncEmbeddingSettingsUi();
});

embeddingClusterNClustersInput.addEventListener("input", () => {
  const value = embeddingClusterNClustersInput.value.trim();
  if (value !== "") {
    state.embeddingHierarchicalTargetMemory.nClusters = value;
  }
  scheduleHierarchyStatusUpdate();
  scheduleLiveHierarchicalClusteringUpdate();
});

embeddingClusterDistanceThresholdInput.addEventListener("input", () => {
  syncDistanceThresholdValueUi();
  const value = embeddingClusterDistanceThresholdInput.value.trim();
  if (value !== "") {
    state.embeddingHierarchicalTargetMemory.distanceThreshold = value;
  }
  scheduleHierarchyStatusUpdate();
  scheduleLiveHierarchicalClusteringUpdate();
});

embeddingClusterLifetimeThresholdInput.addEventListener("input", () => {
  syncPersistenceMinLifetimeValueUi();
  const value = embeddingClusterLifetimeThresholdInput.value.trim();
  if (value !== "") {
    state.embeddingHierarchicalTargetMemory.persistenceMinLifetime = value;
  }
  scheduleHierarchyStatusUpdate();
  scheduleLiveHierarchicalClusteringUpdate();
});

embeddingClusterHierarchicalMinSizeInput.addEventListener("input", () => {
  scheduleHierarchyStatusUpdate();
  scheduleLiveHierarchicalClusteringUpdate();
});

elements.dendrogramDepthSlider?.addEventListener("input", () => {
  state.dendrogramDepth = Number(elements.dendrogramDepthSlider.value || 5);
  syncDendrogramControls();
  scheduleDendrogramLoad();
});

elements.dendrogramRadiusMode?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-dendrogram-radius-mode]");
  if (!button) {
    return;
  }
  const nextMode = button.dataset.dendrogramRadiusMode;
  if (!nextMode || nextMode === state.dendrogramRadiusMode) {
    return;
  }
  state.dendrogramRadiusMode = nextMode;
  syncDendrogramControls();
  requestDendrogramRender();
});

elements.dendrogramCanvas?.addEventListener("pointerdown", handleDendrogramPointerDown);
elements.dendrogramCanvas?.addEventListener("pointermove", handleDendrogramPointerMove);
elements.dendrogramCanvas?.addEventListener("pointerup", handleDendrogramPointerUp);
elements.dendrogramCanvas?.addEventListener("pointercancel", handleDendrogramPointerUp);
elements.dendrogramCanvas?.addEventListener("wheel", handleDendrogramWheel, { passive: false });

embeddingClusterDistanceInput.addEventListener("change", () => {
  state.embeddingClusteringSettingsDraft = readEmbeddingClusteringDraftInputs();
  void ensureHierarchyStatusLoaded();
});

embeddingClusterLinkageInput.addEventListener("change", () => {
  state.embeddingClusteringSettingsDraft = readEmbeddingClusteringDraftInputs();
  void ensureHierarchyStatusLoaded();
});

embeddingTsneApply.addEventListener("click", async () => {
  try {
    const nextSettings = parseEmbeddingSettingsDraft();
    state.embeddingSettings = nextSettings;
    state.embeddingSettingsDraft = {
      ...nextSettings,
    };
    state.embeddingSettingsOpen = false;
    syncEmbeddingSettingsUi();
    if (activeMsaPanelView() === "embeddings") {
      await ensureEmbeddingDataLoaded();
    }
  } catch (error) {
    setEmbeddingInfo(error.message);
  }
});

embeddingClusteringApply.addEventListener("click", async () => {
  try {
    const nextSettings = parseEmbeddingClusteringSettingsDraft();
    state.embeddingClusteringSettings = nextSettings;
    state.embeddingClusteringSettingsDraft = {
      ...nextSettings,
    };
    if (nextSettings.method === "hierarchical") {
      state.hierarchyStatus = null;
      state.hierarchyStatusLoadingKey = null;
      state.hierarchyStatusPromise = null;
    }
    state.embeddingSettingsOpen = false;
    syncEmbeddingSettingsUi();
    const representativeDependsOnClustering =
      state.representativeScope === "cluster" || representativeLens() === "cluster";
    if (
      activeMsaPanelView() === "embeddings" ||
      activeMsaPanelView() === "columns" ||
      representativeDependsOnClustering
    ) {
      await ensureEmbeddingClusteringLoaded();
    }
    if (activeMsaPanelView() === "dendrogram") {
      await ensureDendrogramLoaded({ force: true });
    }
    if (state.representativeScope === "cluster") {
      await refreshRepresentativeSelection("No representative row found for the selected scope.");
    }
    render();
  } catch (error) {
    setEmbeddingInfo(error.message);
  }
});

embeddingPartnerLegend.addEventListener("click", (event) => {
  const modeButton = event.target.closest("[data-legend-mode]");
  if (modeButton) {
    const nextMode = modeButton.dataset.legendMode;
    if (!nextMode || nextMode === state.embeddingColorMode) {
      return;
    }
    state.embeddingColorMode = nextMode;
    renderEmbeddingLegend();
    requestEmbeddingRender();
    if (nextMode === "cluster") {
      void ensureEmbeddingClusteringLoaded();
    }
    return;
  }

  const clusterButton = event.target.closest("[data-cluster-label]");
  if (clusterButton) {
    const clusterLabel = clusterButton.dataset.clusterLabel;
    if (!clusterLabel) {
      return;
    }
    const allClusterLabels = allEmbeddingClusterLabels();
    if (event.ctrlKey || event.metaKey) {
      const isIsolated =
        state.embeddingVisibleClusters.size === 1 &&
        state.embeddingVisibleClusters.has(clusterLabel);
      state.embeddingVisibleClusters = isIsolated
        ? new Set(allClusterLabels)
        : new Set([clusterLabel]);
    } else if (state.embeddingVisibleClusters.has(clusterLabel)) {
      state.embeddingVisibleClusters.delete(clusterLabel);
    } else {
      state.embeddingVisibleClusters.add(clusterLabel);
    }
    renderEmbeddingLegend();
    requestEmbeddingRender();
    return;
  }

  const partnerButton = event.target.closest("[data-partner-domain]");
  if (!partnerButton) {
    return;
  }
  const partnerDomain = partnerButton.dataset.partnerDomain;
  if (!partnerDomain) {
    return;
  }
  const allPartners = state.interface?.partnerDomains || [];
  if (event.ctrlKey || event.metaKey) {
    const isIsolated =
      state.embeddingVisiblePartners.size === 1 &&
      state.embeddingVisiblePartners.has(partnerDomain);
    state.embeddingVisiblePartners = isIsolated
      ? new Set(allPartners)
      : new Set([partnerDomain]);
  } else if (state.embeddingVisiblePartners.has(partnerDomain)) {
    state.embeddingVisiblePartners.delete(partnerDomain);
  } else {
    state.embeddingVisiblePartners.add(partnerDomain);
  }
  renderEmbeddingLegend();
  requestEmbeddingRender();
});

elements.dendrogramPartnerLegend?.addEventListener("click", (event) => {
  const modeButton = event.target.closest("[data-legend-mode]");
  if (modeButton) {
    const nextMode = modeButton.dataset.legendMode;
    if (!nextMode || nextMode === state.dendrogramColorMode) {
      return;
    }
    state.dendrogramColorMode = nextMode;
    renderDendrogramLegend();
    requestDendrogramRender();
    if (nextMode === "cluster") {
      void ensureDendrogramLoaded();
    }
    return;
  }

  const clusterButton = event.target.closest("[data-cluster-label]");
  if (clusterButton) {
    const clusterLabel = clusterButton.dataset.clusterLabel;
    if (!clusterLabel) {
      return;
    }
    const allClusterLabels = allDendrogramClusterLabels();
    if (event.ctrlKey || event.metaKey) {
      const isIsolated =
        state.dendrogramVisibleClusters.size === 1 &&
        state.dendrogramVisibleClusters.has(clusterLabel);
      state.dendrogramVisibleClusters = isIsolated
        ? new Set(allClusterLabels)
        : new Set([clusterLabel]);
    } else if (state.dendrogramVisibleClusters.has(clusterLabel)) {
      state.dendrogramVisibleClusters.delete(clusterLabel);
    } else {
      state.dendrogramVisibleClusters.add(clusterLabel);
    }
    renderDendrogramLegend();
    requestDendrogramRender();
    return;
  }

  const partnerButton = event.target.closest("[data-partner-domain]");
  if (!partnerButton) {
    return;
  }
  const partnerDomain = partnerButton.dataset.partnerDomain;
  if (!partnerDomain) {
    return;
  }
  const allPartners = state.interface?.partnerDomains || [];
  if (event.ctrlKey || event.metaKey) {
    const isIsolated =
      state.dendrogramVisiblePartners.size === 1 &&
      state.dendrogramVisiblePartners.has(partnerDomain);
    state.dendrogramVisiblePartners = isIsolated
      ? new Set(allPartners)
      : new Set([partnerDomain]);
  } else if (state.dendrogramVisiblePartners.has(partnerDomain)) {
    state.dendrogramVisiblePartners.delete(partnerDomain);
  } else {
    state.dendrogramVisiblePartners.add(partnerDomain);
  }
  renderDendrogramLegend();
  requestDendrogramRender();
});

representativeClusterLegend?.addEventListener("click", (event) => {
  const clusterButton = event.target.closest("[data-cluster-label]");
  if (!clusterButton) {
    return;
  }
  const clusterLabel = clusterButton.dataset.clusterLabel;
  if (!clusterLabel) {
    return;
  }

  const allClusterLabels = allRepresentativeClusterLabels();
  if (event.ctrlKey || event.metaKey) {
    const isIsolated =
      state.representativeVisibleClusters.size === 1 &&
      state.representativeVisibleClusters.has(clusterLabel);
    state.representativeVisibleClusters = isIsolated
      ? new Set(allClusterLabels)
      : new Set([clusterLabel]);
  } else if (state.representativeVisibleClusters.has(clusterLabel)) {
    state.representativeVisibleClusters.delete(clusterLabel);
  } else {
    state.representativeVisibleClusters.add(clusterLabel);
  }

  if (!state.representativeVisibleClusters.has(String(state.representativeHoveredClusterLabel))) {
    state.representativeHoveredClusterLabel = null;
  }

  renderRepresentativeClusterLegend();
  if (state.representativeStructure && representativeLens() === "cluster") {
    void renderRepresentativeStructure();
  }
});

representativeClusterGridButton?.addEventListener("click", async () => {
  if (representativeLens() !== "cluster") {
    return;
  }
  representativeClusterGridButton.disabled = true;
  try {
    await ensureEmbeddingClusteringLoaded();
    await openRepresentativeClusterCompare();
  } catch (error) {
    appStatus.textContent = error.message || "Unable to load Cluster Overview.";
    console.error(error);
  } finally {
    syncRepresentativeClusterCompareButton();
  }
});

rowSearchInput.addEventListener("input", () => {
  state.rowSearchQuery = rowSearchInput.value;
  render();
});

representativeLensGroup.addEventListener("click", (event) => {
  const button = event.target.closest("[data-lens]");
  if (!button || button.classList.contains("hidden")) {
    return;
  }
  if (button.dataset.lens === state.representativeLens) {
    return;
  }
  state.representativeLens = button.dataset.lens;
  state.representativeHoveredClusterLabel = null;
  syncRepresentativeLensControls();
  if (state.representativeLens === "cluster") {
    void ensureEmbeddingClusteringLoaded();
  }
  if (state.representativeStructure) {
    void renderRepresentativeStructure();
  }
});

representativeScopeButton?.addEventListener("click", () => {
  setRepresentativeScopeMenuOpen(representativeScopeMenu?.classList.contains("hidden"));
});

representativeScopeMenu?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-representative-scope-value]");
  if (!button) {
    return;
  }
  setRepresentativeScopeMenuOpen(false);
  setRepresentativeScopeFromValue(button.dataset.representativeScopeValue);
  state.representativeHoveredClusterLabel = null;
  void refreshRepresentativeSelection("No representative row found for the selected scope.");
});

representativeMethodButton?.addEventListener("click", () => {
  setRepresentativeMethodMenuOpen(representativeMethodMenu?.classList.contains("hidden"));
});

representativeMethodMenu?.addEventListener("click", (event) => {
  if (event.target.closest(".representative-method-item-help")) {
    return;
  }
  const button = event.target.closest("[data-representative-method]");
  if (!button) {
    return;
  }
  setRepresentativeMethodMenuOpen(false);
  if (!setRepresentativeMethodFromValue(button.dataset.representativeMethod)) {
    return;
  }
  state.representativeHoveredClusterLabel = null;
  void refreshRepresentativeSelection("No representative row found for the selected method.");
});

structureColumnViewToggle.addEventListener("change", () => {
  state.structureColumnView = structureColumnViewToggle.checked;
  syncColumnLegends();
  if (state.structureData) {
    void renderInteractiveStructure();
  }
});

structureContactViewToggle?.addEventListener("change", () => {
  state.structureContactsVisible = structureContactViewToggle.checked;
  syncColumnLegends();
  if (state.structureData) {
    void renderInteractiveStructure();
  }
});

structureDisplaySettingsPanel?.addEventListener("input", (event) => {
  const control = event.target.closest("[data-structure-display-setting]");
  if (!control || !structureDisplaySettingsPanel.contains(control)) {
    return;
  }
  updateStructureDisplaySetting(control);
});

structureDisplaySettingsPanel?.addEventListener("change", (event) => {
  const control = event.target.closest("[data-structure-display-setting]");
  if (!control || !structureDisplaySettingsPanel.contains(control)) {
    return;
  }
  updateStructureDisplaySetting(control);
});

structureDisplaySettingsClose?.addEventListener("click", () => {
  setStructureDisplaySettingsOpen(false);
});

representativePartnerFilterList.addEventListener("click", (event) => {
  const button = event.target.closest("[data-partner-domain]");
  if (!button) {
    return;
  }
  const partnerDomain = button.dataset.partnerDomain;
  if (!partnerDomain) {
    return;
  }
  const allPartners = state.interface?.partnerDomains || [];
  if (event.ctrlKey || event.metaKey) {
    const isIsolated =
      state.representativeVisiblePartners.size === 1 &&
      state.representativeVisiblePartners.has(partnerDomain);
    state.representativeVisiblePartners = isIsolated
      ? new Set(allPartners)
      : new Set([partnerDomain]);
  } else if (state.representativeVisiblePartners.has(partnerDomain)) {
    state.representativeVisiblePartners.delete(partnerDomain);
  } else {
    state.representativeVisiblePartners.add(partnerDomain);
  }
  if (state.representativeVisiblePartners.size === 0) {
    state.representativeVisiblePartners = new Set(allPartners);
  }

  invalidateRepresentativePartnerCache();
  renderRepresentativePartnerFilter();
  if (state.representativeStructure && representativeLens() === "partners") {
    void renderRepresentativeStructure();
  }
});

representativeViewerRoot.addEventListener("mousemove", (event) => {
  state.representativePointer = {
    x: event.clientX,
    y: event.clientY,
  };
  positionRepresentativeHoverCard();
});

representativeViewerRoot.addEventListener("mouseleave", () => {
  state.representativePointer = null;
  clearRepresentativeClusterHover();
  hideRepresentativeHoverCard();
});

representativeViewerRoot.addEventListener("dblclick", async () => {
  if (representativeLens() !== "cluster" || state.representativeHoveredClusterLabel === null) {
    return;
  }
  try {
    await openClusterCompareForLabel(state.representativeHoveredClusterLabel);
  } catch (error) {
    appStatus.textContent = error.message;
  }
});

embeddingCanvas.addEventListener("mousedown", (event) => {
  if (activeMsaPanelView() !== "embeddings") {
    return;
  }
  if (event.button !== 0) {
    return;
  }
  state.embeddingDrag = {
    x: event.clientX,
    y: event.clientY,
    yaw: state.embeddingView.yaw,
    pitch: state.embeddingView.pitch,
  };
});

window.addEventListener("mousemove", (event) => {
  if (state.embeddingDrag) {
    const deltaX = event.clientX - state.embeddingDrag.x;
    const deltaY = event.clientY - state.embeddingDrag.y;
    state.embeddingView.yaw = state.embeddingDrag.yaw + deltaX * 0.01;
    state.embeddingView.pitch = Math.max(
      -1.35,
      Math.min(1.35, state.embeddingDrag.pitch + deltaY * 0.01)
    );
    requestEmbeddingRender();
    return;
  }
  if (activeMsaPanelView() !== "embeddings") {
    return;
  }
  const hoveredPoint = embeddingPointAt(event.clientX, event.clientY);
  const nextRowKey = hoveredPoint?.interactionRowKey || null;
  if (nextRowKey !== state.embeddingHoverRowKey) {
    state.embeddingHoverRowKey = nextRowKey;
    requestEmbeddingRender();
  }
});

window.addEventListener("mouseup", () => {
  state.embeddingDrag = null;
});

embeddingCanvas.addEventListener("mouseleave", () => {
  if (state.embeddingDrag) {
    return;
  }
  if (state.embeddingHoverRowKey !== null) {
    state.embeddingHoverRowKey = null;
    requestEmbeddingRender();
  }
});

embeddingRoot.addEventListener("click", async (event) => {
  if (event.detail !== 2) {
    return;
  }
  await activateEmbeddingPoint(event, "click-detail-2");
});

embeddingRoot.addEventListener("dblclick", async (event) => {
  await activateEmbeddingPoint(event, "dblclick");
});

embeddingMemberPrev?.addEventListener("click", async (event) => {
  event.preventDefault();
  event.stopPropagation();
  await cycleEmbeddingMember(-1);
});

embeddingMemberNext?.addEventListener("click", async (event) => {
  event.preventDefault();
  event.stopPropagation();
  await cycleEmbeddingMember(1);
});

structureMemberPrev?.addEventListener("click", async (event) => {
  event.preventDefault();
  event.stopPropagation();
  await cycleEmbeddingMember(-1);
});

structureMemberNext?.addEventListener("click", async (event) => {
  event.preventDefault();
  event.stopPropagation();
  await cycleEmbeddingMember(1);
});

structureRecenterDomainButton?.addEventListener("click", (event) => {
  event.preventDefault();
  event.stopPropagation();
  recenterStructureDomain();
});

embeddingCanvas.addEventListener(
  "wheel",
  (event) => {
    if (activeMsaPanelView() !== "embeddings") {
      return;
    }
    event.preventDefault();
    const zoomFactor = event.deltaY > 0 ? 1 / 1.12 : 1.12;
    state.embeddingView.zoom = Math.max(0.55, state.embeddingView.zoom * zoomFactor);
    requestEmbeddingRender();
  },
  { passive: false }
);

loadStructureButton.addEventListener("click", async () => {
  try {
    await loadInteractiveStructure();
  } catch (error) {
    handleStructureLoadFailure(error);
  }
});

gridScroll.addEventListener("scroll", drawGrid);
gridScroll.addEventListener("mousemove", onGridHover);
gridScroll.addEventListener("mouseleave", onGridLeave);
gridScroll.addEventListener("click", (event) => {
  if (!state.msa) {
    return;
  }
  const rect = gridScroll.getBoundingClientRect();
  const y = event.clientY - rect.top;
  const filteredRowIndex = Math.floor((y + gridScroll.scrollTop) / ROW_HEIGHT);
  selectFilteredRow(filteredRowIndex);
});
labelsCanvas.addEventListener("click", async (event) => {
  if (!state.msa) {
    return;
  }
  const target = labelHitTargetAtClientPoint(event.clientX, event.clientY);
  if (!target) {
    return;
  }
  if (target.href) {
    window.open(target.href, "_blank", "noopener,noreferrer");
    return;
  }
  const row = selectRowByKey(target.row?.row_key || "");
  if (!row || !interfaceSelect.value) {
    return;
  }
  try {
    await loadInteractiveStructure();
  } catch (error) {
    handleStructureLoadFailure(error);
  }
});
closeStructureModalButton.addEventListener("click", closeStructureModal);
closeClusterCompareModalButton?.addEventListener("click", closeClusterCompareModal);
clusterCompareRerollButton?.addEventListener("click", async () => {
  if (state.clusterCompareClusterLabel === null || state.clusterCompareClusterLabel === undefined) {
    return;
  }
  try {
    await openClusterCompareForLabel(state.clusterCompareClusterLabel, { reroll: true });
  } catch (error) {
    console.error(error);
  }
});
structureModal.addEventListener("click", (event) => {
  if (event.target.classList.contains("structure-modal-backdrop")) {
    closeStructureModal();
  }
});
clusterCompareModal?.addEventListener("click", (event) => {
  if (event.target.classList.contains("structure-modal-backdrop")) {
    closeClusterCompareModal();
  }
});
document.addEventListener("click", (event) => {
  if (!event.target.closest(".msa-picker")) {
    closeMsaPicker();
  }
  if (
    state.selectionSettingsOpen &&
    !event.target.closest("#selection-settings-panel") &&
    !event.target.closest("#selection-settings-toggle")
  ) {
    state.selectionSettingsOpen = false;
    syncSelectionSettingsUi();
  }
  if (
    state.embeddingSettingsOpen &&
    !event.target.closest("#embedding-settings-panel") &&
    !event.target.closest("#embedding-settings-toggle") &&
    !event.target.closest("#clustering-settings-toggle")
  ) {
    state.embeddingSettingsOpen = false;
    syncEmbeddingSettingsUi();
  }
  if (!event.target.closest("#representative-method-control")) {
    setRepresentativeMethodMenuOpen(false);
  }
  if (!event.target.closest("#representative-scope-control")) {
    setRepresentativeScopeMenuOpen(false);
  }
  if (
    state.structureDisplaySettingsOpen &&
    !event.target.closest("#structure-display-settings-panel")
  ) {
    setStructureDisplaySettingsOpen(false);
  }
});
window.addEventListener("keydown", (event) => {
  if (
    !event.ctrlKey &&
    !event.metaKey &&
    !event.altKey &&
    event.key.toLowerCase() === "d" &&
    structureModal &&
    !structureModal.classList.contains("hidden") &&
    !isTextEntryTarget(event.target)
  ) {
    event.preventDefault();
    setStructureDisplaySettingsOpen(!state.structureDisplaySettingsOpen);
    syncStructureDisplaySettingsUi();
    return;
  }
  if (event.key === "Escape" && state.structureDisplaySettingsOpen) {
    setStructureDisplaySettingsOpen(false);
    return;
  }
  if (event.key === "Escape" && !structureModal.classList.contains("hidden")) {
    closeStructureModal();
    return;
  }
  if (event.key === "Escape" && clusterCompareModal && !clusterCompareModal.classList.contains("hidden")) {
    closeClusterCompareModal();
    return;
  }
  if (event.key === "Escape" && !msaPickerMenu.classList.contains("hidden")) {
    closeMsaPicker();
    return;
  }
  if (event.key === "Escape" && state.selectionSettingsOpen) {
    state.selectionSettingsOpen = false;
    syncSelectionSettingsUi();
  }
});
window.addEventListener("resize", render);
window.addEventListener("resize", () => {
  fitRepresentativeDropdownToViewport(
    representativeScopeMenu,
    Boolean(representativeScopeMenu && !representativeScopeMenu.classList.contains("hidden"))
  );
  fitRepresentativeDropdownToViewport(
    representativeMethodMenu,
    Boolean(representativeMethodMenu && !representativeMethodMenu.classList.contains("hidden"))
  );
  if (state.representativeViewer) {
    state.representativeViewer.resize();
    state.representativeViewer.render();
  }
  if (state.structureViewer && !structureModal.classList.contains("hidden")) {
    state.structureViewer.resize();
    state.structureViewer.render();
  }
  resizeClusterCompareViewers();
});

if (window.ResizeObserver) {
  const layoutObserver = new window.ResizeObserver(() => {
    scheduleLayoutSync();
  });
  layoutObserver.observe(viewerRoot);
  layoutObserver.observe(gridScroll);
  layoutObserver.observe(embeddingRoot);
  layoutObserver.observe(columnsRoot);
  layoutObserver.observe(elements.dendrogramRoot);
  layoutObserver.observe(representativeViewerRoot);
}

syncStructureDisplaySettingsUi();
syncRepresentativeMethodControls();
initialize().catch(handleInitializeError);
