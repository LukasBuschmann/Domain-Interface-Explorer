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
  computeRepresentativeRowKey as getRepresentativeRowKey,
  interactiveRowIndexes as getInteractiveRowIndexes,
  topResiduesForColumn as getTopResiduesForColumn,
} from "./msaModel.js";
import { buildOverlayMaps, buildPairs, interactionRowKey, parseInteractionRowKey } from "./interfaceModel.js";
import { parseSelectionSettingsDraft } from "./selectionSettings.js";

const {
  appStatus,
  closeClusterCompareModalButton,
  closeStructureModalButton,
  clusterCompareGrid,
  clusterCompareModal,
  columnCount,
  columnsRoot,
  detailsList,
  embeddingCanvas,
  embeddingRoot,
  embeddingClusterDistanceInput,
  embeddingClusterDistanceThresholdInput,
  embeddingClusterEpsilonInput,
  embeddingClusterHierarchicalMinSizeInput,
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
  representativeLensGroup,
  representativePartnerFilterList,
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
  syncEmbeddingSettingsUi,
  syncHierarchicalTargetMemoryFromDraft,
  syncHierarchicalTargetUi,
  visibleColumnsClusters,
  visibleRepresentativeClusters,
} = embeddingViewController;

function representativeLens() {
  return state.representativeLens;
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


function interactiveRowIndexes(useSelectedPartner = false) {
  return getInteractiveRowIndexes(state.msa, state.interface, overlayStateForRow, useSelectedPartner);
}

function computeRepresentativeRowKey() {
  return getRepresentativeRowKey(state.msa, state.interface, overlayStateForRow);
}

async function refreshRepresentativeSelection(emptyMessage = "No representative row found.") {
  state.representativeRowKey = computeRepresentativeRowKey();
  drawGrid();

  if (!state.representativeRowKey) {
    resetRepresentativePanel(emptyMessage);
    return;
  }

  try {
    await loadRepresentativeStructure();
  } catch (error) {
    representativeCopy.textContent = String(state.representativeRowKey || "");
    appStatus.textContent = error.message;
  }
}

function buildStructureResidueLookup(row) {
  if (!Array.isArray(row?.residueIds) || !row.residueIds.length) {
    return new Map();
  }
  return buildStructureResidueLookupFromModel(row, activeConservationVector());
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

function representativeClusterSummaries() {
  if (!state.interface || !state.embeddingClustering?.points?.length) {
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

function renderRepresentativeClusterLegend(clusterLensData = null) {
  if (!representativeClusterLegend) {
    return;
  }

  const shouldShow = representativeLens() === "cluster";
  representativeClusterLegend.classList.toggle("hidden", !shouldShow);
  if (!shouldShow) {
    representativeClusterLegend.innerHTML = "";
    return;
  }

  if (state.embeddingClusteringLoading && !(state.embeddingClustering?.points || []).length) {
    representativeClusterLegend.innerHTML =
      '<span class="representative-cluster-legend-title">Clusters</span><p class="embedding-legend-empty">Loading clustering…</p>';
    return;
  }

  if (state.embeddingClustering?.error) {
    representativeClusterLegend.innerHTML =
      `<span class="representative-cluster-legend-title">Clusters</span><p class="embedding-legend-empty">${state.embeddingClustering.error}</p>`;
    return;
  }

  const clusters = representativeClusterSummaries();
  if (clusters.length === 0) {
    representativeClusterLegend.innerHTML =
      '<span class="representative-cluster-legend-title">Clusters</span><p class="embedding-legend-empty">No cluster regions available.</p>';
    return;
  }

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
  return state.msa.rows.find((row) => row.row_key === state.representativeRowKey) || null;
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
});
const {
  closeClusterCompareModal,
  openClusterCompareForLabel,
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
  syncEmbeddingLoadingUi,
  syncEmbeddingMemberControls,
  syncEmbeddingSettingsUi,
  resizeEmbeddingCanvas,
  resizeColumnsCanvas,
  renderEmbeddingPlot,
  renderColumnsChart,
  renderColumnsClusterLegend,
  setEmbeddingInfo,
  setColumnsInfo,
  ensureEmbeddingDataLoaded,
  ensureEmbeddingClusteringLoaded,
  resetColumnsClusterSelection,
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
      if (activeMsaPanelView() === "embeddings" || activeMsaPanelView() === "columns") {
        await ensureEmbeddingClusteringLoaded();
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

embeddingSettingsToggle.addEventListener("click", () => {
  state.embeddingSettingsOpen = !state.embeddingSettingsOpen;
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
  state.embeddingClusteringSettingsDraft = normalizeHierarchicalDraft(state.embeddingClusteringSettingsDraft);
  syncEmbeddingSettingsUi();
  if (state.embeddingSettingsOpen) {
    void ensureHierarchyStatusLoaded();
  }
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

embeddingClusterHierarchicalMinSizeInput.addEventListener("input", () => {
  scheduleHierarchyStatusUpdate();
  scheduleLiveHierarchicalClusteringUpdate();
});

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
    if (activeMsaPanelView() === "embeddings" || activeMsaPanelView() === "columns") {
      await ensureEmbeddingClusteringLoaded();
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
    !event.target.closest("#embedding-settings-toggle")
  ) {
    state.embeddingSettingsOpen = false;
    syncEmbeddingSettingsUi();
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
  layoutObserver.observe(representativeViewerRoot);
}

syncStructureDisplaySettingsUi();
initialize().catch(handleInitializeError);
