// @ts-nocheck
import {
  CLUSTER_COLOR_PALETTE,
  DEFAULT_CLUSTERING_SETTINGS,
  DEFAULT_EMBEDDING_SETTINGS,
} from "./constants.js";
import { fetchJson } from "./api.js";
import { interactionRowKey, interfaceFilePfamId, parseInteractionRowKey } from "./interfaceModel.js";
import {
  appendSelectionSettingsToParams,
  selectionSettingsKey,
} from "./selectionSettings.js";

export function createEmbeddingViewController({
  state,
  elements,
  interfaceSelect,
  partnerColor,
  openColumnsCellStructures = () => {},
  renderRepresentativeClusterLegend,
  renderRepresentativeStructure,
  syncRepresentativeScopeControls = () => {},
  representativeLens,
}) {
  let columnsRenderFrameId = 0;
  let columnsHoverTimer = 0;
  let columnsHoverTargetKey = "";
  let embeddingRenderFrameId = 0;
  const SIZE_RAINBOW_PALETTE = [
    "#d7191c",
    "#fdae61",
    "#ffffbf",
    "#66bd63",
    "#00a6ca",
    "#2c7bb6",
    "#7b3294",
  ];
  const emptyEmbeddingPoints = [];
  const emptyClusteringPoints = [];
  const embeddingAnnotationCache = {
    embeddingPoints: null,
    clusteringPoints: null,
    clusterByRowKey: null,
    annotatedPoints: [],
  };
  const embeddingPointSpriteCache = new Map();
  const columnsInterfaceColumnCache = {
    chart: null,
    key: "",
    columns: [],
  };

  function embeddingSettingsKey(settings = state.embeddingSettings) {
    return JSON.stringify(settings);
  }

  function embeddingClusteringSettingsKey(settings = state.embeddingClusteringSettings) {
    return JSON.stringify(normalizedClusteringSettingsForRequest(settings));
  }

  function currentHierarchicalTarget(settings = state.embeddingClusteringSettingsDraft) {
    if (settings?.hierarchicalTarget === "distance_threshold") {
      return "distance_threshold";
    }
    if (settings?.hierarchicalTarget === "n_clusters") {
      return "n_clusters";
    }
    if (settings?.hierarchicalTarget === "persistence") {
      return "persistence";
    }
    const distanceThresholdValue = String(settings?.distanceThreshold ?? "").trim();
    const nClustersValue = String(settings?.nClusters ?? "").trim();
    const persistenceMinLifetimeValue = String(settings?.persistenceMinLifetime ?? "").trim();
    if (distanceThresholdValue !== "" && nClustersValue === "") {
      return "distance_threshold";
    }
    if (nClustersValue !== "") {
      return "n_clusters";
    }
    return persistenceMinLifetimeValue !== "" ? "persistence" : "distance_threshold";
  }

  function currentPersistenceScoreMode(settings = state.embeddingClusteringSettingsDraft) {
    const settingsMode = String(settings?.persistenceScoreMode ?? "").trim().toLowerCase();
    if (settingsMode === "rectangle" || settingsMode === "integral") {
      return settingsMode;
    }
    const memoryMode = String(
      state.embeddingHierarchicalTargetMemory.persistenceScoreMode ??
        DEFAULT_CLUSTERING_SETTINGS.persistenceScoreMode
    ).trim().toLowerCase();
    return memoryMode === "integral" ? "integral" : "rectangle";
  }

  function normalizeDomainLengthHistogram(entries) {
    if (!Array.isArray(entries)) {
      return [];
    }
    return entries
      .map((entry) => ({
        size: Number(entry?.size ?? entry?.length),
        count: Number(entry?.count),
      }))
      .filter((entry) => Number.isFinite(entry.size) && entry.size > 0 && Number.isFinite(entry.count) && entry.count > 0)
      .sort((left, right) => left.size - right.size);
  }

  function normalizePfamRowCoverageHistogram(entries) {
    if (!Array.isArray(entries)) {
      return [];
    }
    return entries
      .map((entry) => ({
        coverage: Number(entry?.coverage ?? entry?.percent ?? entry?.value ?? entry?.size),
        count: Number(entry?.count),
      }))
      .filter((entry) =>
        Number.isFinite(entry.coverage) &&
        entry.coverage >= 0 &&
        entry.coverage <= 100 &&
        Number.isFinite(entry.count) &&
        entry.count > 0
      )
      .sort((left, right) => left.coverage - right.coverage);
  }

  function fragmentLength(fragmentKey) {
    return String(fragmentKey || "")
      .split(";")
      .reduce((total, fragmentPart) => {
        const [startText, endText] = fragmentPart.split("-", 2);
        const start = Number(startText);
        const end = Number(endText);
        return Number.isInteger(start) && Number.isInteger(end) && end >= start
          ? total + end - start + 1
          : total;
      }, 0);
  }

  function domainLengthFromRowKey(rowKey) {
    return fragmentLength(parseInteractionRowKey(rowKey).fragmentKey);
  }

  function positiveInteger(value) {
    const numericValue = Number(value);
    return Number.isFinite(numericValue) && numericValue > 0
      ? Math.round(numericValue)
      : null;
  }

  function coveragePercentInteger(value) {
    const numericValue = Number(value);
    return Number.isFinite(numericValue) && numericValue >= 0 && numericValue <= 100
      ? Math.round(numericValue)
      : null;
  }

  function metricMemberFromPayload(member) {
    const normalizedMember = {
      row_key: String(member?.row_key || ""),
      partner_domain: String(member?.partner_domain || ""),
    };
    const domainLength = positiveInteger(member?.domain_length ?? member?.domainLength);
    if (domainLength !== null) {
      normalizedMember.domain_length = domainLength;
    }
    const pfamRowCoverage = coveragePercentInteger(
      member?.pfam_row_coverage ?? member?.pfamRowCoverage ?? member?.coverage
    );
    if (pfamRowCoverage !== null) {
      normalizedMember.pfam_row_coverage = pfamRowCoverage;
    }
    return normalizedMember;
  }

  function embeddingPointDomainSize(point, members = null) {
    const pointMembers = members || embeddingPointMembers(point);
    const rows = [
      ...pointMembers,
      metricMemberFromPayload(point),
    ];
    const seenRows = new Set();
    const sizes = rows
      .filter((member) => {
        const rowKey = String(member?.row_key || "");
        if (!rowKey || seenRows.has(rowKey)) {
          return false;
        }
        seenRows.add(rowKey);
        return true;
      })
      .map((member) => domainLengthFromRowKey(member.row_key))
      .filter((size) => Number.isFinite(size) && size > 0)
      .sort((left, right) => left - right);
    if (sizes.length === 0) {
      return domainLengthFromRowKey(point?.row_key);
    }
    return sizes[Math.floor((sizes.length - 1) / 2)];
  }

  function embeddingPointPfamRowCoverage(point, members = null) {
    const pointMembers = members || embeddingPointMembers(point);
    const rows = [
      ...pointMembers,
      metricMemberFromPayload(point),
    ];
    const seenRows = new Set();
    const coverageValues = rows
      .filter((member) => {
        const rowKey = String(member?.row_key || "");
        const partnerDomain = String(member?.partner_domain || "");
        const key = interactionRowKey(rowKey, partnerDomain);
        if (!rowKey || !partnerDomain || seenRows.has(key)) {
          return false;
        }
        seenRows.add(key);
        return true;
      })
      .map((member) => coveragePercentInteger(member?.pfam_row_coverage ?? member?.pfamRowCoverage ?? member?.coverage))
      .filter((coverage) => coverage !== null)
      .sort((left, right) => left - right);
    if (coverageValues.length === 0) {
      return null;
    }
    return coverageValues[Math.floor((coverageValues.length - 1) / 2)];
  }

  function currentDomainLengthBounds() {
    const pfamId = state.interface?.pfam_id || interfaceFilePfamId(interfaceSelect.value);
    const optionStats = state.files?.pfam_option_stats?.[pfamId] || null;
    const summary = state.interface?.pfam_id === pfamId ? state.interface?.interface_summary : null;
    const histogram = normalizeDomainLengthHistogram(
      summary?.domain_length_histogram || optionStats?.domain_length_histogram
    );
    if (histogram.length === 0) {
      return { min: 1, max: 1, available: false };
    }
    return {
      min: histogram[0].size,
      max: histogram[histogram.length - 1].size,
      available: true,
    };
  }

  function currentPfamRowCoverageBounds() {
    const pfamId = state.interface?.pfam_id || interfaceFilePfamId(interfaceSelect.value);
    const optionStats = state.files?.pfam_option_stats?.[pfamId] || null;
    const summary = state.interface?.pfam_id === pfamId ? state.interface?.interface_summary : null;
    const histogram = normalizePfamRowCoverageHistogram(
      summary?.pfam_row_coverage_histogram || optionStats?.pfam_row_coverage_histogram
    );
    if (histogram.length === 0) {
      return { min: 0, max: 100, available: false };
    }
    return {
      min: histogram[0].coverage,
      max: histogram[histogram.length - 1].coverage,
      available: true,
    };
  }

  function domainSizeDisplayRange(settings = state.embeddingClusteringSettingsDraft) {
    const bounds = currentDomainLengthBounds();
    const rawMin = String(settings?.domainSizeMin ?? "").trim();
    const rawMax = String(settings?.domainSizeMax ?? "").trim();
    let minValue = rawMin === "" ? bounds.min : Number.parseInt(rawMin, 10);
    let maxValue = rawMax === "" ? bounds.max : Number.parseInt(rawMax, 10);
    if (!Number.isFinite(minValue)) {
      minValue = bounds.min;
    }
    if (!Number.isFinite(maxValue)) {
      maxValue = bounds.max;
    }
    minValue = Math.max(bounds.min, Math.min(bounds.max, minValue));
    maxValue = Math.max(bounds.min, Math.min(bounds.max, maxValue));
    if (minValue > maxValue) {
      [minValue, maxValue] = [maxValue, minValue];
    }
    return {
      ...bounds,
      minValue,
      maxValue,
      active: bounds.available && (minValue > bounds.min || maxValue < bounds.max),
    };
  }

  function pfamRowCoverageDisplayRange(settings = state.embeddingClusteringSettingsDraft) {
    const bounds = currentPfamRowCoverageBounds();
    const rawMin = String(settings?.pfamRowCoverageMin ?? "").trim();
    const rawMax = String(settings?.pfamRowCoverageMax ?? "").trim();
    let minValue = rawMin === "" ? bounds.min : Number.parseInt(rawMin, 10);
    let maxValue = rawMax === "" ? bounds.max : Number.parseInt(rawMax, 10);
    if (!Number.isFinite(minValue)) {
      minValue = bounds.min;
    }
    if (!Number.isFinite(maxValue)) {
      maxValue = bounds.max;
    }
    minValue = Math.max(bounds.min, Math.min(bounds.max, minValue));
    maxValue = Math.max(bounds.min, Math.min(bounds.max, maxValue));
    if (minValue > maxValue) {
      [minValue, maxValue] = [maxValue, minValue];
    }
    return {
      ...bounds,
      minValue,
      maxValue,
      active: bounds.available && (minValue > bounds.min || maxValue < bounds.max),
    };
  }

  function domainSizeFilterFromInputs() {
    const range = domainSizeDisplayRange({
      domainSizeMin: elements.embeddingClusterDomainSizeMinInput?.value || "",
      domainSizeMax: elements.embeddingClusterDomainSizeMaxInput?.value || "",
    });
    if (!range.available || !range.active) {
      return { domainSizeMin: "", domainSizeMax: "" };
    }
    return {
      domainSizeMin: String(range.minValue),
      domainSizeMax: String(range.maxValue),
    };
  }

  function pfamRowCoverageFilterFromInputs() {
    const range = pfamRowCoverageDisplayRange({
      pfamRowCoverageMin: elements.embeddingClusterPfamCoverageMinInput?.value || "",
      pfamRowCoverageMax: elements.embeddingClusterPfamCoverageMaxInput?.value || "",
    });
    if (!range.available || !range.active) {
      return { pfamRowCoverageMin: "", pfamRowCoverageMax: "" };
    }
    return {
      pfamRowCoverageMin: String(range.minValue),
      pfamRowCoverageMax: String(range.maxValue),
    };
  }

  function normalizedClusteringSettingsForRequest(settings = state.embeddingClusteringSettings) {
    const normalizedSettings = { ...settings };
    if (String(normalizedSettings.method || "") !== "hierarchical") {
      normalizedSettings.domainSizeMin = "";
      normalizedSettings.domainSizeMax = "";
      normalizedSettings.pfamRowCoverageMin = "";
      normalizedSettings.pfamRowCoverageMax = "";
      return normalizedSettings;
    }

    const domainRange = domainSizeDisplayRange(settings);
    if (domainRange.available && domainRange.active) {
      normalizedSettings.domainSizeMin = String(domainRange.minValue);
      normalizedSettings.domainSizeMax = String(domainRange.maxValue);
    } else {
      normalizedSettings.domainSizeMin = "";
      normalizedSettings.domainSizeMax = "";
    }

    const coverageRange = pfamRowCoverageDisplayRange(settings);
    if (coverageRange.available && coverageRange.active) {
      normalizedSettings.pfamRowCoverageMin = String(coverageRange.minValue);
      normalizedSettings.pfamRowCoverageMax = String(coverageRange.maxValue);
    } else {
      normalizedSettings.pfamRowCoverageMin = "";
      normalizedSettings.pfamRowCoverageMax = "";
    }

    return normalizedSettings;
  }

  function normalizedRepresentativeDomainSizeFilter(settings = state.clusterCompareDomainSizeFilter || {}) {
    let domainSizeMin = positiveInteger(settings.domainSizeMin);
    let domainSizeMax = positiveInteger(settings.domainSizeMax);
    if (
      domainSizeMin !== null &&
      domainSizeMax !== null &&
      domainSizeMin > domainSizeMax
    ) {
      [domainSizeMin, domainSizeMax] = [domainSizeMax, domainSizeMin];
    }
    return {
      domainSizeMin: domainSizeMin === null ? "" : String(domainSizeMin),
      domainSizeMax: domainSizeMax === null ? "" : String(domainSizeMax),
    };
  }

  function appendRepresentativeDomainSizeFilterParams(params) {
    const range = normalizedRepresentativeDomainSizeFilter();
    if (range.domainSizeMin !== "") {
      params.set("representative_domain_size_min", range.domainSizeMin);
    }
    if (range.domainSizeMax !== "") {
      params.set("representative_domain_size_max", range.domainSizeMax);
    }
  }

  function appendDomainSizeFilterParams(params, settings) {
    const normalizedSettings = normalizedClusteringSettingsForRequest(settings);
    const minSize = String(normalizedSettings?.domainSizeMin ?? "").trim();
    const maxSize = String(normalizedSettings?.domainSizeMax ?? "").trim();
    if (minSize !== "") {
      params.set("domain_size_min", minSize);
    }
    if (maxSize !== "") {
      params.set("domain_size_max", maxSize);
    }
  }

  function appendPfamRowCoverageFilterParams(params, settings) {
    const normalizedSettings = normalizedClusteringSettingsForRequest(settings);
    const minCoverage = String(normalizedSettings?.pfamRowCoverageMin ?? "").trim();
    const maxCoverage = String(normalizedSettings?.pfamRowCoverageMax ?? "").trim();
    if (minCoverage !== "") {
      params.set("pfam_row_coverage_min", minCoverage);
    }
    if (maxCoverage !== "") {
      params.set("pfam_row_coverage_max", maxCoverage);
    }
  }

  function appendHierarchicalClusteringParams(params, settings) {
    const hierarchicalTarget = currentHierarchicalTarget(settings);
    params.set("linkage", String(settings.linkage));
    params.set("hierarchical_target", hierarchicalTarget);
    const minClusterSize = String(
      settings.hierarchicalMinClusterSize ?? DEFAULT_CLUSTERING_SETTINGS.hierarchicalMinClusterSize
    ).trim();
    if (minClusterSize !== "") {
      params.set(
        "hierarchical_min_cluster_size",
        minClusterSize
      );
    }
    if (hierarchicalTarget === "n_clusters" && String(settings.nClusters).trim() !== "") {
      params.set("n_clusters", String(settings.nClusters));
    }
    if (
      hierarchicalTarget === "distance_threshold" &&
      String(settings.distanceThreshold).trim() !== ""
    ) {
      params.set("distance_threshold", String(settings.distanceThreshold));
    }
    if (hierarchicalTarget === "persistence") {
      const minLifetime = String(settings.persistenceMinLifetime ?? "").trim();
      const lifetimeWeight = String(settings.persistenceLifetimeWeight ?? "").trim();
      const scoreMode = currentPersistenceScoreMode(settings);
      params.set(
        "persistence_min_lifetime",
        minLifetime || String(DEFAULT_CLUSTERING_SETTINGS.persistenceMinLifetime)
      );
      params.set(
        "persistence_lifetime_weight",
        lifetimeWeight || String(DEFAULT_CLUSTERING_SETTINGS.persistenceLifetimeWeight)
      );
      params.set("persistence_score_mode", scoreMode);
    }
    appendDomainSizeFilterParams(params, settings);
    appendPfamRowCoverageFilterParams(params, settings);
  }

  function currentEmbeddingQuery() {
    const params = new URLSearchParams({
      file: interfaceSelect.value,
      embedding_method: String(state.embeddingSettings.method),
      distance: String(state.embeddingSettings.distance),
      learning_rate: String(state.embeddingSettings.learningRate),
      max_iter: String(state.embeddingSettings.maxIter),
      early_exaggeration_iter: String(state.embeddingSettings.earlyExaggerationIter),
      early_exaggeration: String(state.embeddingSettings.earlyExaggeration),
      neighbors: String(state.embeddingSettings.neighbors),
      theta: String(state.embeddingSettings.theta),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (state.embeddingSettings.perplexity !== "auto") {
      params.set("perplexity", String(state.embeddingSettings.perplexity));
    }
    return `/api/embedding?${params.toString()}`;
  }

  function currentEmbeddingRequestKey() {
    return `${interfaceSelect.value}|${selectionSettingsKey(state.selectionSettings)}|${embeddingSettingsKey()}`;
  }

  function currentEmbeddingClusteringQuery() {
    const params = new URLSearchParams({
      file: interfaceSelect.value,
      method: String(state.embeddingClusteringSettings.method),
      distance: String(state.embeddingClusteringSettings.distance),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (state.embeddingClusteringSettings.method === "hierarchical") {
      appendHierarchicalClusteringParams(params, state.embeddingClusteringSettings);
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
    return `/api/clustering?${params.toString()}`;
  }

  function currentColumnsChartQuery() {
    return currentEmbeddingClusteringQuery().replace("/api/clustering?", "/api/columns-chart?");
  }

  function currentColumnsChartRequestKey() {
    return `${currentEmbeddingClusteringRequestKey()}|columns-chart`;
  }

  function currentClusterCompareQuery(clusterLabel) {
    const params = new URLSearchParams({
      file: interfaceSelect.value,
      cluster_label: String(clusterLabel),
      method: String(state.embeddingClusteringSettings.method),
      distance: String(state.embeddingClusteringSettings.distance),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (state.embeddingClusteringSettings.method === "hierarchical") {
      appendHierarchicalClusteringParams(params, state.embeddingClusteringSettings);
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
    appendRepresentativeDomainSizeFilterParams(params);
    return `/api/cluster-compare?${params.toString()}`;
  }

  function currentEmbeddingClusteringRequestKey() {
    return `${interfaceSelect.value}|${selectionSettingsKey(state.selectionSettings)}|${embeddingClusteringSettingsKey()}`;
  }

  function currentHierarchyStatusQuery() {
    const settings = normalizeHierarchicalDraft(readEmbeddingClusteringDraftInputs());
    const params = new URLSearchParams({
      file: interfaceSelect.value,
      method: String(settings.method),
      distance: String(settings.distance),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (settings.method === "hierarchical") {
      appendHierarchicalClusteringParams(params, settings);
    }
    return `/api/hierarchy-status?${params.toString()}`;
  }

  function currentHierarchyStatusRequestKey() {
    const settings = readEmbeddingClusteringDraftInputs();
    return [
      interfaceSelect.value,
      selectionSettingsKey(state.selectionSettings),
      settings.method,
      settings.distance,
      settings.linkage,
      currentHierarchicalTarget(settings),
      settings.nClusters,
      settings.distanceThreshold,
      settings.persistenceMinLifetime,
      settings.persistenceLifetimeWeight,
      currentPersistenceScoreMode(settings),
      settings.hierarchicalMinClusterSize,
      settings.domainSizeMin,
      settings.domainSizeMax,
      settings.pfamRowCoverageMin,
      settings.pfamRowCoverageMax,
    ].join("|");
  }

  function domainSizeFilterIsActive(settings = state.embeddingClusteringSettingsDraft) {
    return Boolean(domainSizeDisplayRange(settings).active);
  }

  function pfamRowCoverageFilterIsActive(settings = state.embeddingClusteringSettingsDraft) {
    return Boolean(pfamRowCoverageDisplayRange(settings).active);
  }

  function hierarchyRangeFilterIsActive(settings = state.embeddingClusteringSettingsDraft) {
    return domainSizeFilterIsActive(settings) || pfamRowCoverageFilterIsActive(settings);
  }

  function currentHierarchyStatus() {
    const requestKey = currentHierarchyStatusRequestKey();
    return state.hierarchyStatus?.requestKey === requestKey ? state.hierarchyStatus : null;
  }

  function hierarchyLoadingLabel() {
    if (state.embeddingClusteringSettings.method !== "hierarchical") {
      return "Loading clustering...";
    }
    const status = currentHierarchyStatus();
    if (status?.local_calculation_required) {
      return "Creating hierarchy...";
    }
    if (status && !status.local_calculation_required) {
      return "Applying hierarchy cutthrough...";
    }
    if (state.hierarchyStatusLoadingKey === currentHierarchyStatusRequestKey()) {
      return "Checking hierarchy cache...";
    }
    if (hierarchyRangeFilterIsActive(state.embeddingClusteringSettings)) {
      return "Preparing filtered hierarchy...";
    }
    return "Applying hierarchy cutthrough...";
  }

  function hierarchyLoadingInfoMessage() {
    if (state.embeddingClusteringSettings.method !== "hierarchical") {
      return `Loading ${clusteringMethodLabel(state.embeddingClusteringSettings.method)} clustering (${embeddingDistanceLabel(state.embeddingClusteringSettings.distance)} distance)...`;
    }
    const status = currentHierarchyStatus();
    const distanceLabel = embeddingDistanceLabel(state.embeddingClusteringSettings.distance);
    if (status?.local_calculation_required) {
      return `Creating the ${distanceLabel} hierarchy, then applying the selected cutthrough.`;
    }
    if (status && !status.local_calculation_required) {
      return `Applying the selected hierarchy cutthrough using the cached ${distanceLabel} hierarchy.`;
    }
    if (hierarchyRangeFilterIsActive(state.embeddingClusteringSettings)) {
      return `Preparing a filtered ${distanceLabel} hierarchy for the selected range filters.`;
    }
    return `Applying the selected hierarchy cutthrough using ${distanceLabel} distances.`;
  }

  function syncEmbeddingLoadingUi() {
    const showEmbeddingLoading = state.embeddingLoading;
    const showClusteringLoading = state.embeddingClusteringLoading;
    const isVisible = showEmbeddingLoading || showClusteringLoading;
    elements.embeddingLoading.classList.toggle("hidden", !isVisible);
    elements.embeddingLoadingLabel.textContent = showClusteringLoading
      ? hierarchyLoadingLabel()
      : "Loading embeddings...";
    elements.embeddingTsneApply.disabled = state.embeddingLoading;
    elements.embeddingClusteringApply.disabled = state.embeddingClusteringLoading;
  }

  function formatDuration(seconds) {
    if (!Number.isFinite(seconds) || seconds <= 0) {
      return "unknown time";
    }
    if (seconds < 60) {
      return `${Math.max(1, Math.round(seconds))} s`;
    }
    if (seconds < 3600) {
      return `${Math.max(1, Math.round(seconds / 60))} min`;
    }
    return `${(seconds / 3600).toFixed(seconds < 36000 ? 1 : 0)} h`;
  }

  function formatBytes(bytes) {
    if (!Number.isFinite(bytes) || bytes <= 0) {
      return "unknown RAM";
    }
    const gib = bytes / (1024 ** 3);
    if (gib >= 1) {
      return `${gib.toFixed(gib >= 10 ? 0 : 1)} GiB RAM`;
    }
    const mib = bytes / (1024 ** 2);
    return `${Math.max(1, Math.round(mib))} MiB RAM`;
  }

  function hierarchyWarningMessage(status = state.hierarchyStatus) {
    if (!status?.local_calculation_required) {
      return "";
    }
    const estimate = status.estimate || {};
    const seconds = Number(estimate.estimated_total_seconds);
    const bytes = Number(estimate.estimated_peak_rss_delta_bytes);
    if (Number.isFinite(seconds) || Number.isFinite(bytes)) {
      return `Local hierarchy build estimated at ${formatDuration(seconds)} and ${formatBytes(bytes)}.`;
    }
    return "Local hierarchy build required; this may be expensive.";
  }

  function syncHierarchyWarningUi() {
    if (!elements.embeddingClusteringApplyWarning || !elements.embeddingHierarchyWarning) {
      return;
    }
    const currentRequestKey =
      state.embeddingClusteringSettingsDraft.method === "hierarchical"
        ? currentHierarchyStatusRequestKey()
        : "";
    const showWarning =
      state.embeddingClusteringSettingsDraft.method === "hierarchical" &&
      (
        (
          state.hierarchyStatus?.requestKey === currentRequestKey &&
          Boolean(state.hierarchyStatus?.local_calculation_required)
        ) ||
        state.hierarchyStatusLoadingKey === currentRequestKey ||
        (
          state.hierarchyStatus?.requestKey !== currentRequestKey &&
          hierarchyRangeFilterIsActive(state.embeddingClusteringSettingsDraft)
        )
      );
    let message = "";
    if (showWarning) {
      if (
        state.hierarchyStatus?.requestKey === currentRequestKey &&
        state.hierarchyStatus?.local_calculation_required
      ) {
        message = hierarchyWarningMessage();
      } else if (state.hierarchyStatusLoadingKey === currentRequestKey) {
        message = "Checking whether this hierarchy is already cached.";
      } else {
        message = "Range filtering may require building a filtered local hierarchy.";
      }
    }
    elements.embeddingClusteringApplyWarning.classList.toggle("hidden", !showWarning);
    elements.embeddingClusteringApplyWarning.title = message;
    elements.embeddingHierarchyWarning.classList.toggle("hidden", !showWarning);
    elements.embeddingHierarchyWarning.textContent = message;
  }

  function syncDistanceThresholdValueUi() {
    if (!elements.embeddingClusterDistanceThresholdValue) {
      return;
    }
    const value = Number.parseFloat(elements.embeddingClusterDistanceThresholdInput.value);
    elements.embeddingClusterDistanceThresholdValue.textContent = Number.isFinite(value)
      ? value.toFixed(2)
      : "";
  }

  function syncPersistenceMinLifetimeValueUi() {
    if (!elements.embeddingClusterLifetimeThresholdValue) {
      return;
    }
    const value = Number.parseFloat(elements.embeddingClusterLifetimeThresholdInput.value);
    elements.embeddingClusterLifetimeThresholdValue.textContent = Number.isFinite(value)
      ? value.toFixed(2)
      : "";
  }

  function syncPersistenceStabilityWeightValueUi() {
    if (!elements.embeddingClusterStabilityWeightInput) {
      return;
    }
    const value = Number.parseFloat(elements.embeddingClusterStabilityWeightInput.value);
    if (!Number.isFinite(value)) {
      if (elements.embeddingClusterStabilitySizeValue) {
        elements.embeddingClusterStabilitySizeValue.textContent = "";
      }
      if (elements.embeddingClusterStabilityLifetimeValue) {
        elements.embeddingClusterStabilityLifetimeValue.textContent = "";
      }
      return;
    }
    const lifetimePercent = Math.round(value * 100);
    const sizePercent = 100 - lifetimePercent;
    if (elements.embeddingClusterStabilitySizeValue) {
      elements.embeddingClusterStabilitySizeValue.textContent = `${sizePercent}%`;
    }
    if (elements.embeddingClusterStabilityLifetimeValue) {
      elements.embeddingClusterStabilityLifetimeValue.textContent = `${lifetimePercent}%`;
    }
    elements.embeddingClusterStabilityWeightInput.setAttribute(
      "aria-valuetext",
      `Size ${sizePercent} percent, lifetime ${lifetimePercent} percent`
    );
  }

  function syncDomainSizeRangeUi() {
    const minInput = elements.embeddingClusterDomainSizeMinInput;
    const maxInput = elements.embeddingClusterDomainSizeMaxInput;
    const valueOutput = elements.embeddingClusterDomainSizeValue;
    if (!minInput || !maxInput || !valueOutput) {
      return;
    }
    const range = domainSizeDisplayRange();
    for (const input of [minInput, maxInput]) {
      input.min = String(range.min);
      input.max = String(range.max);
      input.disabled = !range.available || range.min === range.max;
    }
    minInput.value = String(range.minValue);
    maxInput.value = String(range.maxValue);
    const denominator = Math.max(1, range.max - range.min);
    const startPercent = ((range.minValue - range.min) / denominator) * 100;
    const endPercent = ((range.maxValue - range.min) / denominator) * 100;
    const control = minInput.closest(".embedding-settings-dual-range");
    control?.style.setProperty("--range-start", `${Math.max(0, Math.min(100, startPercent))}%`);
    control?.style.setProperty("--range-end", `${Math.max(0, Math.min(100, endPercent))}%`);
    valueOutput.textContent = range.available
      ? `${range.minValue}-${range.maxValue}`
      : "n/a";
    minInput.setAttribute("aria-valuetext", `${range.minValue} residues`);
    maxInput.setAttribute("aria-valuetext", `${range.maxValue} residues`);
  }

  function syncPfamRowCoverageRangeUi() {
    const minInput = elements.embeddingClusterPfamCoverageMinInput;
    const maxInput = elements.embeddingClusterPfamCoverageMaxInput;
    const valueOutput = elements.embeddingClusterPfamCoverageValue;
    if (!minInput || !maxInput || !valueOutput) {
      return;
    }
    const range = pfamRowCoverageDisplayRange();
    for (const input of [minInput, maxInput]) {
      input.min = String(range.min);
      input.max = String(range.max);
      input.disabled = !range.available || range.min === range.max;
    }
    minInput.value = String(range.minValue);
    maxInput.value = String(range.maxValue);
    const denominator = Math.max(1, range.max - range.min);
    const startPercent = ((range.minValue - range.min) / denominator) * 100;
    const endPercent = ((range.maxValue - range.min) / denominator) * 100;
    const control = minInput.closest(".embedding-settings-dual-range");
    control?.style.setProperty("--range-start", `${Math.max(0, Math.min(100, startPercent))}%`);
    control?.style.setProperty("--range-end", `${Math.max(0, Math.min(100, endPercent))}%`);
    valueOutput.textContent = range.available
      ? `${range.minValue}-${range.maxValue}%`
      : "n/a";
    minInput.setAttribute("aria-valuetext", `${range.minValue} percent`);
    maxInput.setAttribute("aria-valuetext", `${range.maxValue} percent`);
  }

  function readEmbeddingClusteringDraftInputs() {
    const domainSizeFilter = domainSizeFilterFromInputs();
    const pfamRowCoverageFilter = pfamRowCoverageFilterFromInputs();
    return {
      ...state.embeddingClusteringSettingsDraft,
      distance:
        elements.embeddingClusterDistanceInput.value.trim().toLowerCase() ||
        DEFAULT_CLUSTERING_SETTINGS.distance,
      minClusterSize: elements.embeddingClusterMinSizeInput.value.trim(),
      minSamples: elements.embeddingClusterMinSamplesInput.value.trim(),
      clusterSelectionEpsilon: elements.embeddingClusterEpsilonInput.value.trim(),
      linkage:
        elements.embeddingClusterLinkageInput.value.trim().toLowerCase() ||
        DEFAULT_CLUSTERING_SETTINGS.linkage,
      nClusters: elements.embeddingClusterNClustersInput.value.trim(),
      distanceThreshold: elements.embeddingClusterDistanceThresholdInput.value.trim(),
      persistenceMinLifetime: elements.embeddingClusterLifetimeThresholdInput.value.trim(),
      persistenceLifetimeWeight: elements.embeddingClusterStabilityWeightInput.value.trim(),
      persistenceScoreMode: currentPersistenceScoreMode(),
      hierarchicalMinClusterSize:
        elements.embeddingClusterHierarchicalMinSizeInput.value.trim(),
      ...domainSizeFilter,
      ...pfamRowCoverageFilter,
    };
  }

  function syncHierarchicalTargetMemoryFromDraft() {
    const nClustersValue = elements.embeddingClusterNClustersInput.value.trim();
    const distanceThresholdValue = elements.embeddingClusterDistanceThresholdInput.value.trim();
    const persistenceMinLifetimeValue =
      elements.embeddingClusterLifetimeThresholdInput.value.trim();
    const persistenceLifetimeWeightValue =
      elements.embeddingClusterStabilityWeightInput.value.trim();
    if (nClustersValue !== "") {
      state.embeddingHierarchicalTargetMemory.nClusters = nClustersValue;
    }
    if (distanceThresholdValue !== "") {
      state.embeddingHierarchicalTargetMemory.distanceThreshold = distanceThresholdValue;
    }
    if (persistenceMinLifetimeValue !== "") {
      state.embeddingHierarchicalTargetMemory.persistenceMinLifetime = persistenceMinLifetimeValue;
    }
    if (persistenceLifetimeWeightValue !== "") {
      state.embeddingHierarchicalTargetMemory.persistenceLifetimeWeight =
        persistenceLifetimeWeightValue;
    }
    state.embeddingHierarchicalTargetMemory.persistenceScoreMode =
      currentPersistenceScoreMode(state.embeddingClusteringSettingsDraft);
  }

  function hierarchicalTargetFallbackValue(target) {
    if (target === "distance_threshold") {
      return (
        state.embeddingHierarchicalTargetMemory.distanceThreshold ||
        String(DEFAULT_CLUSTERING_SETTINGS.distanceThreshold)
      );
    }
    if (target === "persistence") {
      return (
        state.embeddingHierarchicalTargetMemory.persistenceMinLifetime ||
        String(DEFAULT_CLUSTERING_SETTINGS.persistenceMinLifetime)
      );
    }
    return (
      state.embeddingHierarchicalTargetMemory.nClusters ||
      String(DEFAULT_CLUSTERING_SETTINGS.nClusters)
    );
  }

  function normalizeHierarchicalDraft(settings) {
    const target = currentHierarchicalTarget(settings);
    const nClustersValue = String(settings?.nClusters ?? "").trim();
    const distanceThresholdValue = String(settings?.distanceThreshold ?? "").trim();
    const persistenceMinLifetimeValue = String(settings?.persistenceMinLifetime ?? "").trim();
    const persistenceLifetimeWeightValue = String(settings?.persistenceLifetimeWeight ?? "").trim();
    const persistenceScoreModeValue = currentPersistenceScoreMode(settings);
    return {
      ...settings,
      hierarchicalTarget: target,
      nClusters:
        target === "n_clusters"
          ? nClustersValue || hierarchicalTargetFallbackValue("n_clusters")
          : "",
      distanceThreshold:
        target === "distance_threshold"
          ? distanceThresholdValue || hierarchicalTargetFallbackValue("distance_threshold")
          : "",
      persistenceMinLifetime:
        target === "persistence"
          ? persistenceMinLifetimeValue || hierarchicalTargetFallbackValue("persistence")
          : "",
      persistenceLifetimeWeight:
        target === "persistence"
          ? persistenceLifetimeWeightValue ||
            state.embeddingHierarchicalTargetMemory.persistenceLifetimeWeight ||
            String(DEFAULT_CLUSTERING_SETTINGS.persistenceLifetimeWeight)
          : "",
      persistenceScoreMode:
        target === "persistence"
          ? persistenceScoreModeValue ||
            state.embeddingHierarchicalTargetMemory.persistenceScoreMode ||
            DEFAULT_CLUSTERING_SETTINGS.persistenceScoreMode
          : "",
    };
  }

  function syncHierarchicalTargetUi() {
    const isHierarchical = state.embeddingClusteringSettingsDraft.method === "hierarchical";
    const hierarchicalTarget = currentHierarchicalTarget();

    [...elements.embeddingSettingsPanel.querySelectorAll("[data-hierarchical-target]")].forEach((button) => {
      const isActive = isHierarchical && button.dataset.hierarchicalTarget === hierarchicalTarget;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-hierarchical-target-panel]")].forEach((element) => {
      element.classList.toggle(
        "embedding-settings-section-hidden",
        !isHierarchical || element.dataset.hierarchicalTargetPanel !== hierarchicalTarget
      );
    });
  }

  function syncEmbeddingSettingsUi() {
    if (elements.embeddingSettingsPanel && elements.msaPanelTabs && elements.embeddingRoot) {
      if (state.embeddingSettingsSection === "clustering") {
        if (elements.embeddingSettingsPanel.parentElement !== elements.msaPanelTabs.parentElement) {
          elements.msaPanelTabs.insertAdjacentElement("afterend", elements.embeddingSettingsPanel);
        }
        elements.embeddingSettingsPanel.classList.remove("points-settings-panel");
        elements.embeddingSettingsPanel.classList.add("clustering-settings-panel");
      } else {
        if (elements.embeddingSettingsPanel.parentElement !== elements.embeddingRoot) {
          elements.embeddingRoot.appendChild(elements.embeddingSettingsPanel);
        }
        elements.embeddingSettingsPanel.classList.remove("clustering-settings-panel");
        elements.embeddingSettingsPanel.classList.add("points-settings-panel");
      }
    }
    const pointsOpen = state.embeddingSettingsOpen && state.embeddingSettingsSection === "points";
    const clusteringOpen = state.embeddingSettingsOpen && state.embeddingSettingsSection === "clustering";
    elements.embeddingSettingsToggle.setAttribute(
      "aria-expanded",
      String(pointsOpen)
    );
    elements.embeddingSettingsToggle.classList.toggle("active", pointsOpen);
    elements.clusteringSettingsToggle?.setAttribute(
      "aria-expanded",
      String(clusteringOpen)
    );
    elements.clusteringSettingsToggle?.classList.toggle("active", clusteringOpen);
    elements.embeddingSettingsPanel.classList.toggle("hidden", !state.embeddingSettingsOpen);
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-settings-section]")].forEach((button) => {
      const isActive = button.dataset.settingsSection === state.embeddingSettingsSection;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-settings-section-panel]")].forEach((section) => {
      section.classList.toggle(
        "embedding-settings-section-hidden",
        section.dataset.settingsSectionPanel !== state.embeddingSettingsSection
      );
    });
    elements.embeddingDistanceInput.value = String(state.embeddingSettingsDraft.distance);
    elements.embeddingPerplexityInput.value = String(state.embeddingSettingsDraft.perplexity);
    elements.embeddingLearningRateInput.value = String(state.embeddingSettingsDraft.learningRate);
    elements.embeddingMaxIterInput.value = String(state.embeddingSettingsDraft.maxIter);
    elements.embeddingEarlyExaggerationIterInput.value = String(
      state.embeddingSettingsDraft.earlyExaggerationIter
    );
    elements.embeddingEarlyExaggerationInput.value = String(
      state.embeddingSettingsDraft.earlyExaggeration
    );
    elements.embeddingNeighborsInput.value = String(state.embeddingSettingsDraft.neighbors);
    elements.embeddingThetaInput.value = String(state.embeddingSettingsDraft.theta);
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-points-method]")].forEach((button) => {
      const isActive = button.dataset.pointsMethod === state.embeddingSettingsDraft.method;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-points-method-panel]")].forEach((element) => {
      element.classList.toggle(
        "embedding-settings-section-hidden",
        element.dataset.pointsMethodPanel !== state.embeddingSettingsDraft.method
      );
    });
    elements.embeddingClusterDistanceInput.value = String(
      state.embeddingClusteringSettingsDraft.distance
    );
    elements.embeddingClusterMinSizeInput.value = String(
      state.embeddingClusteringSettingsDraft.minClusterSize
    );
    elements.embeddingClusterMinSamplesInput.value = String(
      state.embeddingClusteringSettingsDraft.minSamples
    );
    elements.embeddingClusterEpsilonInput.value = String(
      state.embeddingClusteringSettingsDraft.clusterSelectionEpsilon
    );
    elements.embeddingClusterLinkageInput.value = String(
      state.embeddingClusteringSettingsDraft.linkage
    );
    elements.embeddingClusterNClustersInput.value = String(
      state.embeddingClusteringSettingsDraft.nClusters
    );
    elements.embeddingClusterDistanceThresholdInput.value = String(
      state.embeddingClusteringSettingsDraft.distanceThreshold ||
        hierarchicalTargetFallbackValue("distance_threshold")
    );
    syncDistanceThresholdValueUi();
    elements.embeddingClusterLifetimeThresholdInput.value = String(
      state.embeddingClusteringSettingsDraft.persistenceMinLifetime ||
        hierarchicalTargetFallbackValue("persistence")
    );
    syncPersistenceMinLifetimeValueUi();
    elements.embeddingClusterStabilityWeightInput.value = String(
      state.embeddingClusteringSettingsDraft.persistenceLifetimeWeight ||
        state.embeddingHierarchicalTargetMemory.persistenceLifetimeWeight ||
        DEFAULT_CLUSTERING_SETTINGS.persistenceLifetimeWeight
    );
    syncPersistenceStabilityWeightValueUi();
    const persistenceScoreMode = currentPersistenceScoreMode(
      state.embeddingClusteringSettingsDraft
    );
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-persistence-score-mode]")].forEach((button) => {
      const isActive = button.dataset.persistenceScoreMode === persistenceScoreMode;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
    elements.embeddingClusterHierarchicalMinSizeInput.value = String(
      state.embeddingClusteringSettingsDraft.hierarchicalMinClusterSize
    );
    syncDomainSizeRangeUi();
    syncPfamRowCoverageRangeUi();
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-clustering-method]")].forEach((button) => {
      const isActive =
        button.dataset.clusteringMethod === state.embeddingClusteringSettingsDraft.method;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", String(isActive));
    });
    [...elements.embeddingSettingsPanel.querySelectorAll("[data-clustering-panel]")].forEach((element) => {
      element.classList.toggle(
        "embedding-settings-section-hidden",
        element.dataset.clusteringPanel !== state.embeddingClusteringSettingsDraft.method
      );
    });
    syncHierarchicalTargetUi();
    syncHierarchyWarningUi();
  }

  function parseEmbeddingSettingsDraft() {
    const method =
      state.embeddingSettingsDraft.method || DEFAULT_EMBEDDING_SETTINGS.method;
    const distance = elements.embeddingDistanceInput.value.trim().toLowerCase();
    const perplexityRaw = elements.embeddingPerplexityInput.value.trim();
    const learningRateRaw = elements.embeddingLearningRateInput.value.trim().toLowerCase();
    const maxIterRaw = elements.embeddingMaxIterInput.value.trim();
    const earlyExaggerationIterRaw = elements.embeddingEarlyExaggerationIterInput.value.trim();
    const earlyExaggerationRaw = elements.embeddingEarlyExaggerationInput.value.trim();
    const neighbors = elements.embeddingNeighborsInput.value.trim().toLowerCase();
    const thetaRaw = elements.embeddingThetaInput.value.trim();
    if (!["tsne", "pca"].includes(method)) {
      throw new Error("Point method must be openTSNE or PCA.");
    }
    if (!["binary", "jaccard", "dice", "overlap"].includes(distance)) {
      throw new Error("Point distance must be Binary Columns, Jaccard, Dice, or Overlap.");
    }
    const perplexity = perplexityRaw === "" ? "auto" : Number.parseFloat(perplexityRaw);
    if (perplexity !== "auto" && (!Number.isFinite(perplexity) || perplexity <= 0)) {
      throw new Error("Perplexity must be positive or blank for auto.");
    }
    const learningRate =
      learningRateRaw === "" || learningRateRaw === "auto"
        ? "auto"
        : Number.parseFloat(learningRateRaw);
    if (learningRate !== "auto" && (!Number.isFinite(learningRate) || learningRate <= 0)) {
      throw new Error("Learning rate must be positive or 'auto'.");
    }
    const maxIter = Number.parseInt(maxIterRaw, 10);
    if (!Number.isFinite(maxIter) || maxIter <= 0) {
      throw new Error("Iterations must be a positive integer.");
    }
    const earlyExaggerationIter = Number.parseInt(earlyExaggerationIterRaw, 10);
    if (!Number.isFinite(earlyExaggerationIter) || earlyExaggerationIter <= 0) {
      throw new Error("Early exaggeration iterations must be a positive integer.");
    }
    const earlyExaggeration = Number.parseFloat(earlyExaggerationRaw);
    if (!Number.isFinite(earlyExaggeration) || earlyExaggeration <= 0) {
      throw new Error("Early exaggeration must be positive.");
    }
    if (!["approx", "auto", "exact"].includes(neighbors)) {
      throw new Error("Neighbors must be approx, auto, or exact.");
    }
    const theta = Number.parseFloat(thetaRaw);
    if (!Number.isFinite(theta) || theta < 0 || theta > 1) {
      throw new Error("Theta must be between 0 and 1.");
    }
    return {
      method,
      distance,
      perplexity,
      learningRate,
      maxIter,
      earlyExaggerationIter,
      earlyExaggeration,
      neighbors,
      theta,
    };
  }

  function parseEmbeddingClusteringSettingsDraft(options = {}) {
    const method = state.embeddingClusteringSettingsDraft.method;
    const hierarchicalTarget = currentHierarchicalTarget();
    const distance = elements.embeddingClusterDistanceInput.value.trim().toLowerCase();
    if (!["jaccard", "dice", "overlap"].includes(distance)) {
      throw new Error("Clustering distance must be Jaccard, Dice, or Overlap.");
    }
    const linkage = elements.embeddingClusterLinkageInput.value.trim().toLowerCase();
    if (!["single", "complete", "average", "average_deduplicated", "weighted"].includes(linkage)) {
      throw new Error(
        "Hierarchical linkage must be single, complete, average, average deduplicated, or weighted."
      );
    }
    const minClusterSize = Number.parseInt(
      elements.embeddingClusterMinSizeInput.value.trim(),
      10
    );
    if (!Number.isFinite(minClusterSize) || minClusterSize <= 0) {
      throw new Error("Min cluster size must be a positive integer.");
    }
    const minSamplesRaw = elements.embeddingClusterMinSamplesInput.value.trim();
    let minSamples = "";
    if (minSamplesRaw !== "") {
      minSamples = Number.parseInt(minSamplesRaw, 10);
      if (!Number.isFinite(minSamples) || minSamples <= 0) {
        throw new Error("Min samples must be blank or a positive integer.");
      }
    }
    const clusterSelectionEpsilon = Number.parseFloat(
      elements.embeddingClusterEpsilonInput.value.trim()
    );
    if (!Number.isFinite(clusterSelectionEpsilon) || clusterSelectionEpsilon < 0) {
      throw new Error("Cluster selection epsilon must be a non-negative number.");
    }
    let nClusters = elements.embeddingClusterNClustersInput.value.trim();
    let distanceThreshold = elements.embeddingClusterDistanceThresholdInput.value.trim();
    let persistenceMinLifetime = elements.embeddingClusterLifetimeThresholdInput.value.trim();
    let persistenceLifetimeWeight = elements.embeddingClusterStabilityWeightInput.value.trim();
    let persistenceScoreMode = currentPersistenceScoreMode();
    const hierarchicalMinClusterSize = Number.parseInt(
      elements.embeddingClusterHierarchicalMinSizeInput.value.trim(),
      10
    );
    if (!Number.isFinite(hierarchicalMinClusterSize) || hierarchicalMinClusterSize <= 0) {
      throw new Error("Minimal hierarchical cluster size must be a positive integer.");
    }
    const domainSizeFilter = method === "hierarchical"
      ? domainSizeFilterFromInputs()
      : { domainSizeMin: "", domainSizeMax: "" };
    const pfamRowCoverageFilter = method === "hierarchical"
      ? pfamRowCoverageFilterFromInputs()
      : { pfamRowCoverageMin: "", pfamRowCoverageMax: "" };
    if (method === "hierarchical") {
      if (hierarchicalTarget === "n_clusters") {
        if (nClusters === "") {
          throw new Error("Hierarchical clustering needs a number of clusters.");
        }
        nClusters = Number.parseInt(nClusters, 10);
        if (!Number.isFinite(nClusters) || nClusters <= 0) {
          throw new Error("Number of clusters must be a positive integer.");
        }
        distanceThreshold = "";
        persistenceMinLifetime = "";
        persistenceLifetimeWeight = "";
        persistenceScoreMode = "";
      } else if (hierarchicalTarget === "persistence") {
        if (persistenceMinLifetime === "") {
          throw new Error("Persistent clustering needs a minimum lifetime.");
        }
        persistenceMinLifetime = Number.parseFloat(persistenceMinLifetime);
        if (!Number.isFinite(persistenceMinLifetime) || persistenceMinLifetime < 0) {
          throw new Error("Minimum lifetime must be a non-negative number.");
        }
        if (persistenceLifetimeWeight === "") {
          persistenceLifetimeWeight = String(DEFAULT_CLUSTERING_SETTINGS.persistenceLifetimeWeight);
        }
        persistenceLifetimeWeight = Number.parseFloat(persistenceLifetimeWeight);
        if (
          !Number.isFinite(persistenceLifetimeWeight) ||
          persistenceLifetimeWeight < 0 ||
          persistenceLifetimeWeight > 1
        ) {
          throw new Error("Stability balance must be between 0 and 1.");
        }
        if (!["rectangle", "integral"].includes(persistenceScoreMode)) {
          throw new Error("Stability score must be rectangle or integral.");
        }
        nClusters = "";
        distanceThreshold = "";
      } else {
        if (distanceThreshold === "") {
          throw new Error("Hierarchical clustering needs a cutoff distance.");
        }
        distanceThreshold = Number.parseFloat(distanceThreshold);
        if (!Number.isFinite(distanceThreshold) || distanceThreshold < 0) {
          throw new Error("Cutoff distance must be a non-negative number.");
        }
        nClusters = "";
        persistenceMinLifetime = "";
        persistenceLifetimeWeight = "";
        persistenceScoreMode = "";
      }
    }
    const parsedSettings = {
      method,
      distance,
      minClusterSize,
      minSamples,
      clusterSelectionEpsilon,
      linkage,
      hierarchicalTarget,
      nClusters: hierarchicalTarget === "n_clusters" ? nClusters : "",
      distanceThreshold:
        hierarchicalTarget === "distance_threshold" ? distanceThreshold : "",
      persistenceMinLifetime:
        hierarchicalTarget === "persistence" ? persistenceMinLifetime : "",
      persistenceLifetimeWeight:
        hierarchicalTarget === "persistence" ? persistenceLifetimeWeight : "",
      persistenceScoreMode:
        hierarchicalTarget === "persistence" ? persistenceScoreMode : "",
      hierarchicalMinClusterSize,
      ...domainSizeFilter,
      ...pfamRowCoverageFilter,
    };
    if (
      options.preserveAppliedHierarchy &&
      state.embeddingClusteringSettings.method === "hierarchical" &&
      parsedSettings.method === "hierarchical"
    ) {
      parsedSettings.method = state.embeddingClusteringSettings.method;
      parsedSettings.distance = state.embeddingClusteringSettings.distance;
      parsedSettings.linkage = state.embeddingClusteringSettings.linkage;
    }
    return parsedSettings;
  }

  function embeddingDistanceLabel(distance = state.embeddingSettings.distance) {
    if (distance === "binary") {
      return "Binary columns";
    }
    if (distance === "dice") {
      return "Dice";
    }
    if (distance === "overlap") {
      return "Overlap";
    }
    return "Jaccard";
  }

  function pointMethodLabel(method = state.embeddingSettings.method) {
    return method === "pca" ? "PCA" : "openTSNE";
  }

  function clusteringMethodLabel(method) {
    return method === "hierarchical" ? "Hierarchical" : "HDBSCAN";
  }

  function embeddingLegendMode() {
    return state.embeddingColorMode;
  }

  function setEmbeddingInfo(message) {
    elements.embeddingInfo.textContent = message;
  }

  function setColumnsInfo(message) {
    elements.columnsInfo.textContent = message;
  }

  function resetEmbeddingPartnerSelection() {
    const partners = state.interface?.partnerDomains || [];
    state.embeddingVisiblePartners = new Set(partners);
  }

  function allEmbeddingClusterLabels() {
    return Array.from(
      new Set((state.embeddingClustering?.points || []).map((point) => String(point.cluster_label)))
    ).sort((left, right) => Number(left) - Number(right));
  }

  function resetEmbeddingClusterSelection() {
    state.embeddingVisibleClusters = new Set(allEmbeddingClusterLabels());
  }

  function allEmbeddingSizeBracketKeys() {
    return embeddingSizeBrackets().map((bracket) => bracket.key);
  }

  function resetEmbeddingSizeSelection() {
    state.embeddingVisibleSizeBrackets = new Set(allEmbeddingSizeBracketKeys());
  }

  function allEmbeddingCoverageBracketKeys() {
    return embeddingCoverageBrackets().map((bracket) => bracket.key);
  }

  function resetEmbeddingCoverageSelection() {
    state.embeddingVisibleCoverageBrackets = new Set(allEmbeddingCoverageBracketKeys());
  }

  function visibleEmbeddingSizeBracketKeys(sizeBrackets = embeddingSizeBrackets()) {
    const bracketKeys = sizeBrackets.map((bracket) => bracket.key);
    if (bracketKeys.length === 0) {
      return [];
    }
    if (!(state.embeddingVisibleSizeBrackets instanceof Set)) {
      state.embeddingVisibleSizeBrackets = new Set(bracketKeys);
    }
    return bracketKeys.filter((bracketKey) => state.embeddingVisibleSizeBrackets.has(bracketKey));
  }

  function visibleEmbeddingCoverageBracketKeys(coverageBrackets = embeddingCoverageBrackets()) {
    const bracketKeys = coverageBrackets.map((bracket) => bracket.key);
    if (bracketKeys.length === 0) {
      return [];
    }
    if (!(state.embeddingVisibleCoverageBrackets instanceof Set)) {
      state.embeddingVisibleCoverageBrackets = new Set(bracketKeys);
    }
    return bracketKeys.filter((bracketKey) => state.embeddingVisibleCoverageBrackets.has(bracketKey));
  }

  function resetEmbeddingMetricSelections() {
    resetEmbeddingSizeSelection();
    resetEmbeddingCoverageSelection();
  }

  function allColumnsClusterLabels() {
    if (!state.columnsChart?.clusters?.length) {
      return [];
    }
    const clusterKeys = state.columnsChart.clusters.map((clusterLabel) => String(clusterLabel));
    const clusterSet = new Set(clusterKeys);
    const ordered = Array.isArray(state.columnsClusterOrder)
      ? state.columnsClusterOrder.filter((clusterLabel) => clusterSet.has(String(clusterLabel))).map(String)
      : [];
    const orderedSet = new Set(ordered);
    return [
      ...ordered,
      ...clusterKeys.filter((clusterLabel) => !orderedSet.has(clusterLabel)),
    ];
  }

  function resetColumnsClusterSelection() {
    const clusterKeys = allColumnsClusterLabels();
    state.columnsClusterOrder = [...clusterKeys];
    state.columnsVisibleClusters = new Set(clusterKeys);
    state.columnsInterfaceView = { start: 0, end: null };
  }

  function allRepresentativeClusterLabels() {
    if (
      !interfaceSelect.value ||
      state.embeddingClustering?.file !== interfaceSelect.value ||
      state.embeddingClustering?.settingsKey !== embeddingClusteringSettingsKey() ||
      state.embeddingClustering?.error
    ) {
      return [];
    }
    return Array.from(
      new Set(
        (state.embeddingClustering?.points || [])
          .map((point) => String(point.cluster_label))
          .filter((clusterLabel) => Number(clusterLabel) >= 0)
      )
    ).sort((left, right) => Number(left) - Number(right));
  }

  function resetRepresentativeClusterSelection() {
    state.representativeVisibleClusters = new Set(allRepresentativeClusterLabels());
  }

  function visibleEmbeddingPartners() {
    const partners = state.interface?.partnerDomains || [];
    if (partners.length === 0) {
      return [];
    }
    return partners.filter((partner) => state.embeddingVisiblePartners.has(partner));
  }

  function visibleEmbeddingClusters() {
    const clusterKeys = allEmbeddingClusterLabels();
    if (clusterKeys.length === 0) {
      return [];
    }
    return clusterKeys.filter((clusterKey) => state.embeddingVisibleClusters.has(clusterKey));
  }

  function visibleRepresentativeClusters() {
    const clusterKeys = allRepresentativeClusterLabels();
    if (clusterKeys.length === 0) {
      return [];
    }
    const visible = clusterKeys.filter((clusterKey) => state.representativeVisibleClusters.has(clusterKey));
    if (visible.length > 0) {
      return visible;
    }
    state.representativeVisibleClusters = new Set(clusterKeys);
    return clusterKeys;
  }

  function visibleColumnsClusters() {
    const clusterKeys = allColumnsClusterLabels();
    if (clusterKeys.length === 0) {
      return [];
    }
    state.columnsVisibleClusters = new Set(clusterKeys);
    return clusterKeys;
  }

  function embeddingClusterLabel(clusterLabel) {
    if (clusterLabel === null || clusterLabel === undefined || Number.isNaN(Number(clusterLabel))) {
      return "Unclustered";
    }
    return Number(clusterLabel) < 0 ? "Noise" : `Cluster ${Number(clusterLabel) + 1}`;
  }

  function columnsSourceMode() {
    return state.columnsSource === "domains" ? "domains" : "clusters";
  }

  function columnsSourceTitle() {
    return columnsSourceMode() === "domains" ? "Domain" : "Cluster";
  }

  function columnsSourcePlural() {
    return columnsSourceMode() === "domains" ? "domains" : "clusters";
  }

  function columnsClusterShortLabel(clusterLabel) {
    if (clusterLabel === null || clusterLabel === undefined || Number.isNaN(Number(clusterLabel))) {
      return "?";
    }
    return Number(clusterLabel) < 0 ? "N" : `C${Number(clusterLabel) + 1}`;
  }

  function columnsSeriesShortLabel(seriesLabel) {
    if (columnsSourceMode() !== "domains") {
      return columnsClusterShortLabel(seriesLabel);
    }
    const label = String(seriesLabel || "?");
    return label.length > 8 ? label.slice(0, 8) : label;
  }

  function embeddingClusterColor(clusterLabel) {
    const numericLabel = Number(clusterLabel);
    if (!Number.isFinite(numericLabel)) {
      return "#8a847a";
    }
    if (numericLabel < 0) {
      return "#8a847a";
    }
    return CLUSTER_COLOR_PALETTE[numericLabel % CLUSTER_COLOR_PALETTE.length];
  }

  function columnsSeriesColor(seriesLabel) {
    return columnsSourceMode() === "domains"
      ? partnerColor(seriesLabel)
      : embeddingClusterColor(seriesLabel);
  }

  function sizeBracketColor(index, bracketCount) {
    if (bracketCount <= 1) {
      return SIZE_RAINBOW_PALETTE[Math.floor(SIZE_RAINBOW_PALETTE.length / 2)];
    }
    const paletteIndex = Math.round(
      (Math.max(0, Math.min(bracketCount - 1, index)) * (SIZE_RAINBOW_PALETTE.length - 1)) /
        (bracketCount - 1)
    );
    return SIZE_RAINBOW_PALETTE[paletteIndex];
  }

  function sizeBracketLabel(bracket) {
    if (!bracket || bracket.key === "unknown") {
      return "Unknown size";
    }
    return bracket.start === bracket.end
      ? `${bracket.start} residues`
      : `${bracket.start}-${bracket.end} residues`;
  }

  function embeddingSizeBrackets(points = annotatedEmbeddingPoints()) {
    const sizes = points
      .map((point) => Number(point.domainSize))
      .filter((size) => Number.isFinite(size) && size > 0)
      .sort((left, right) => left - right);
    if (sizes.length === 0) {
      return [
        {
          key: "unknown",
          start: null,
          end: null,
          color: "#8a847a",
          count: points.length,
        },
      ];
    }
    const minSize = sizes[0];
    const maxSize = sizes[sizes.length - 1];
    const uniqueCount = new Set(sizes).size;
    const bracketCount = minSize === maxSize
      ? 1
      : Math.min(SIZE_RAINBOW_PALETTE.length, Math.max(2, uniqueCount));
    const width = Math.max(1, Math.ceil((maxSize - minSize + 1) / bracketCount));
    const brackets = [];
    for (let index = 0; index < bracketCount; index += 1) {
      const start = minSize + index * width;
      const end = index === bracketCount - 1
        ? maxSize
        : Math.min(maxSize, start + width - 1);
      if (start > maxSize) {
        break;
      }
      brackets.push({
        key: `size:${start}:${end}`,
        start,
        end,
        color: sizeBracketColor(index, bracketCount),
        count: 0,
      });
    }
    for (const point of points) {
      const bracket = sizeBracketForPoint(point, brackets);
      if (bracket) {
        bracket.count += Number(point.memberCount || 1);
      }
    }
    return brackets;
  }

  function sizeBracketForPoint(point, brackets) {
    const size = Number(point?.domainSize);
    if (!Number.isFinite(size) || size <= 0) {
      return brackets.find((bracket) => bracket.key === "unknown") || null;
    }
    return brackets.find((bracket) =>
      bracket.key !== "unknown" &&
      size >= Number(bracket.start) &&
      size <= Number(bracket.end)
    ) || null;
  }

  function coverageBracketLabel(bracket) {
    if (!bracket || bracket.key === "unknown") {
      return "Unknown coverage";
    }
    return bracket.start === bracket.end
      ? `${bracket.start}% coverage`
      : `${bracket.start}-${bracket.end}% coverage`;
  }

  function embeddingCoverageBrackets(points = annotatedEmbeddingPoints()) {
    const coverageValues = points
      .map((point) => Number(point.pfamRowCoverage))
      .filter((coverage) => Number.isFinite(coverage) && coverage >= 0 && coverage <= 100)
      .sort((left, right) => left - right);
    if (coverageValues.length === 0) {
      return [
        {
          key: "unknown",
          start: null,
          end: null,
          color: "#8a847a",
          count: points.length,
        },
      ];
    }
    const minCoverage = coverageValues[0];
    const maxCoverage = coverageValues[coverageValues.length - 1];
    const uniqueCount = new Set(coverageValues).size;
    const bracketCount = minCoverage === maxCoverage
      ? 1
      : Math.min(SIZE_RAINBOW_PALETTE.length, Math.max(2, uniqueCount));
    const width = Math.max(1, Math.ceil((maxCoverage - minCoverage + 1) / bracketCount));
    const brackets = [];
    for (let index = 0; index < bracketCount; index += 1) {
      const start = minCoverage + index * width;
      const end = index === bracketCount - 1
        ? maxCoverage
        : Math.min(maxCoverage, start + width - 1);
      if (start > maxCoverage) {
        break;
      }
      brackets.push({
        key: `coverage:${start}:${end}`,
        start,
        end,
        color: sizeBracketColor(index, bracketCount),
        count: 0,
      });
    }
    for (const point of points) {
      const bracket = coverageBracketForPoint(point, brackets);
      if (bracket) {
        bracket.count += Number(point.memberCount || 1);
      }
    }
    return brackets;
  }

  function coverageBracketForPoint(point, brackets) {
    const coverage = Number(point?.pfamRowCoverage);
    if (!Number.isFinite(coverage) || coverage < 0 || coverage > 100) {
      return brackets.find((bracket) => bracket.key === "unknown") || null;
    }
    return brackets.find((bracket) =>
      bracket.key !== "unknown" &&
      coverage >= Number(bracket.start) &&
      coverage <= Number(bracket.end)
    ) || null;
  }

  function embeddingPointColor(point, colorMode, sizeBrackets, coverageBrackets) {
    if (colorMode === "cluster") {
      return embeddingClusterColor(point.clusterLabel);
    }
    if (colorMode === "size") {
      return sizeBracketForPoint(point, sizeBrackets)?.color || "#8a847a";
    }
    if (colorMode === "coverage") {
      return coverageBracketForPoint(point, coverageBrackets)?.color || "#8a847a";
    }
    return partnerColor(point.partner_domain);
  }

  function colorToRgb(color) {
    if (typeof color !== "string") {
      return [129, 122, 113];
    }
    const match = color.trim().match(/^#([0-9a-f]{6})$/i);
    if (!match) {
      return [129, 122, 113];
    }
    const value = match[1];
    return [
      Number.parseInt(value.slice(0, 2), 16),
      Number.parseInt(value.slice(2, 4), 16),
      Number.parseInt(value.slice(4, 6), 16),
    ];
  }

  function rgbToCss(channels) {
    return `rgb(${channels[0]}, ${channels[1]}, ${channels[2]})`;
  }

  function columnsEmptyBinRgb() {
    return state.columnsEmptyBinsWhite ? [255, 255, 255] : [234, 223, 207];
  }

  function columnsBarcodeRgb(clusterLabel, fraction) {
    const clamped = Math.max(0, Math.min(1, Number(fraction) || 0));
    const base = colorToRgb(columnsSeriesColor(clusterLabel));
    const background = [234, 223, 207];
    const strength = clamped * clamped * (3 - (2 * clamped));
    const high = base.map((channel) => Math.round(channel + (255 - channel) * 0.38));
    return background.map((channel, index) =>
      Math.round(channel + (high[index] - channel) * strength)
    );
  }

  function columnsGapMergedColor(baseRgb, fraction) {
    const clamped = Math.max(0, Math.min(1, Number(fraction) || 0));
    const blackness = Math.min(0.78, clamped * 0.78);
    return rgbToCss(baseRgb.map((channel) => Math.round(channel * (1 - blackness))));
  }

  function niceColumnTickStep(span, targetTicks = 8) {
    const roughStep = Math.max(1, Number(span) / Math.max(1, targetTicks));
    const power = 10 ** Math.floor(Math.log10(roughStep));
    for (const multiplier of [1, 2, 5, 10]) {
      const step = multiplier * power;
      if (step >= roughStep) {
        return Math.max(1, Math.round(step));
      }
    }
    return Math.max(1, Math.round(roughStep));
  }

  function renderEmbeddingLegend() {
    const colorMode = embeddingLegendMode();
    const partners = state.interface?.partnerDomains || [];
    const clusterKeys = Array.from(
      new Set((state.embeddingClustering?.points || []).map((point) => String(point.cluster_label)))
    ).sort((left, right) => Number(left) - Number(right));
    const sizeBrackets = colorMode === "size" ? embeddingSizeBrackets() : [];
    const coverageBrackets = colorMode === "coverage" ? embeddingCoverageBrackets() : [];
    const visibleSizeBracketKeys = new Set(
      colorMode === "size" ? visibleEmbeddingSizeBracketKeys(sizeBrackets) : []
    );
    const visibleCoverageBracketKeys = new Set(
      colorMode === "coverage" ? visibleEmbeddingCoverageBracketKeys(coverageBrackets) : []
    );
    if (partners.length === 0 && colorMode !== "size" && colorMode !== "coverage") {
      elements.embeddingPartnerLegend.innerHTML = "";
      return;
    }
    const modeControls = `
      <div class="embedding-legend-header">
        <div class="embedding-legend-mode" role="tablist" aria-label="Embedding color mode">
          <button type="button" class="embedding-legend-mode-button ${colorMode === "domain" ? "active" : ""}" data-legend-mode="domain" aria-pressed="${colorMode === "domain"}">Domains</button>
          <button type="button" class="embedding-legend-mode-button ${colorMode === "cluster" ? "active" : ""}" data-legend-mode="cluster" aria-pressed="${colorMode === "cluster"}">Clusters</button>
          <button type="button" class="embedding-legend-mode-button ${colorMode === "size" ? "active" : ""}" data-legend-mode="size" aria-pressed="${colorMode === "size"}">Size</button>
          <button type="button" class="embedding-legend-mode-button ${colorMode === "coverage" ? "active" : ""}" data-legend-mode="coverage" aria-pressed="${colorMode === "coverage"}">Coverage</button>
        </div>
      </div>
    `;
    const legendEntries =
      colorMode === "cluster"
        ? clusterKeys.length === 0
          ? '<p class="embedding-legend-empty">Clustering not loaded yet.</p>'
          : clusterKeys
              .map(
                (clusterKey) => `
          <button class="embedding-partner-chip ${state.embeddingVisibleClusters.has(clusterKey) ? "active" : "inactive"}" type="button" data-cluster-label="${clusterKey}" aria-pressed="${state.embeddingVisibleClusters.has(clusterKey)}" title="${embeddingClusterLabel(clusterKey)}">
            <span class="representative-partner-filter-swatch" style="background: ${embeddingClusterColor(clusterKey)};"></span>
            <span class="embedding-partner-chip-label">${embeddingClusterLabel(clusterKey)}</span>
          </button>
        `
              )
              .join("")
        : colorMode === "size"
          ? (state.embedding?.points || []).length === 0
            ? '<p class="embedding-legend-empty">Embedding not loaded yet.</p>'
            : sizeBrackets
                .map(
                  (bracket) => `
          <button class="embedding-partner-chip embedding-size-chip ${visibleSizeBracketKeys.has(bracket.key) ? "active" : "inactive"}" type="button" data-size-bracket-key="${bracket.key}" aria-pressed="${visibleSizeBracketKeys.has(bracket.key)}" title="${sizeBracketLabel(bracket)}">
            <span class="representative-partner-filter-swatch" style="background: ${bracket.color};"></span>
            <span class="embedding-partner-chip-label">${sizeBracketLabel(bracket)}</span>
            <span class="embedding-partner-chip-value">${bracket.count}</span>
          </button>
        `
                )
                .join("")
        : colorMode === "coverage"
          ? (state.embedding?.points || []).length === 0
            ? '<p class="embedding-legend-empty">Embedding not loaded yet.</p>'
            : coverageBrackets
                .map(
                  (bracket) => `
          <button class="embedding-partner-chip embedding-size-chip ${visibleCoverageBracketKeys.has(bracket.key) ? "active" : "inactive"}" type="button" data-coverage-bracket-key="${bracket.key}" aria-pressed="${visibleCoverageBracketKeys.has(bracket.key)}" title="${coverageBracketLabel(bracket)}">
            <span class="representative-partner-filter-swatch" style="background: ${bracket.color};"></span>
            <span class="embedding-partner-chip-label">${coverageBracketLabel(bracket)}</span>
            <span class="embedding-partner-chip-value">${bracket.count}</span>
          </button>
        `
                )
                .join("")
        : partners
            .map(
              (partner) => `
          <button class="embedding-partner-chip ${state.embeddingVisiblePartners.has(partner) ? "active" : "inactive"}" type="button" data-partner-domain="${partner}" aria-pressed="${state.embeddingVisiblePartners.has(partner)}" title="${partner}">
            <span class="representative-partner-filter-swatch" style="background: ${partnerColor(partner)};"></span>
            <span class="embedding-partner-chip-label">${partner}</span>
          </button>
        `
            )
            .join("");
    elements.embeddingPartnerLegend.innerHTML = `${modeControls}<div class="embedding-legend-list">${legendEntries}</div>`;
  }

  function rotateEmbeddingPoint(point) {
    const yaw = state.embeddingView.yaw;
    const pitch = state.embeddingView.pitch;
    const cosYaw = Math.cos(yaw);
    const sinYaw = Math.sin(yaw);
    const cosPitch = Math.cos(pitch);
    const sinPitch = Math.sin(pitch);
    const xYaw = point.x * cosYaw + point.z * sinYaw;
    const zYaw = point.z * cosYaw - point.x * sinYaw;
    const yPitch = point.y * cosPitch - zYaw * sinPitch;
    const zPitch = point.y * sinPitch + zYaw * cosPitch;
    return { x: xYaw, y: yPitch, z: zPitch };
  }

  function resizeEmbeddingCanvas() {
    const width = Math.max(1, elements.embeddingRoot.clientWidth);
    const height = Math.max(1, elements.embeddingRoot.clientHeight);
    const dpr = window.devicePixelRatio || 1;
    elements.embeddingCanvas.width = Math.round(width * dpr);
    elements.embeddingCanvas.height = Math.round(height * dpr);
    elements.embeddingCanvas.style.width = `${width}px`;
    elements.embeddingCanvas.style.height = `${height}px`;
  }

  function requestEmbeddingRender() {
    if (embeddingRenderFrameId) {
      return;
    }
    embeddingRenderFrameId = window.requestAnimationFrame(() => {
      embeddingRenderFrameId = 0;
      if (state.msaPanelView === "embeddings") {
        renderEmbeddingPlot();
      }
    });
  }

  function columnsAlignmentLength() {
    return Math.max(
      0,
      Number(
        state.msa?.alignment_length ||
          state.interface?.alignment_length ||
          state.interface?.alignmentLength ||
          0
      )
    );
  }

  function columnsDomainChartSignature(alignmentLength) {
    const partners = state.interface?.partnerDomains || [];
    return [
      interfaceSelect.value || "",
      columnsSourceMode(),
      alignmentLength,
      partners
        .map((partner) => {
          const stats = state.interface?.partnerColumnStats?.get(partner);
          const columnCountTotal = stats?.columnCounts instanceof Map
            ? Array.from(stats.columnCounts.values()).reduce((total, count) => total + Number(count || 0), 0)
            : 0;
          return `${partner}:${Number(stats?.denominator || 0)}:${Number(stats?.columnCounts?.size || 0)}:${columnCountTotal}`;
        })
        .join(","),
    ].join("|");
  }

  function columnsChartCacheKey() {
    if (columnsSourceMode() === "domains") {
      return columnsDomainChartSignature(columnsAlignmentLength());
    }
    const serverChart = state.embeddingClustering?.columns_chart;
    return [
      interfaceSelect.value || "",
      state.embeddingClustering?.settingsKey || "",
      columnsSourceMode(),
      serverChart ? Number(serverChart.alignmentLength || 0) : "missing",
      serverChart ? (serverChart.clusters || []).join(",") : "",
      Number(state.embeddingClustering?.points?.length || 0),
    ].join("|");
  }

  function normalizedServerColumnsChart() {
    const chart = state.embeddingClustering?.columns_chart;
    if (!chart || !Array.isArray(chart.clusters)) {
      return null;
    }
    const alignmentLength = Math.max(0, Number(chart.alignmentLength || 0));
    const clusters = chart.clusters.map((clusterLabel) => String(clusterLabel));
    const clusterSizes = {};
    const rawClusterSizes = chart.clusterSizes || {};
    for (const clusterLabel of clusters) {
      clusterSizes[clusterLabel] = Number(rawClusterSizes[clusterLabel] || 0);
    }
    const relativeByCluster = {};
    const rawRelativeByCluster = chart.relativeByCluster || {};
    const gapByCluster = {};
    const rawGapByCluster = chart.gapByCluster || {};
    for (const clusterLabel of clusters) {
      const values = rawRelativeByCluster[clusterLabel];
      relativeByCluster[clusterLabel] = Array.isArray(values) ? values : [];
      const gapValues = rawGapByCluster[clusterLabel];
      gapByCluster[clusterLabel] = Array.isArray(gapValues) ? gapValues : [];
    }
    return {
      file: interfaceSelect.value,
      alignmentLength,
      clusters,
      clusterSizes,
      relativeByCluster,
      gapByCluster,
      maxStackValue: Number(chart.maxStackValue || 0),
      source: "clusters",
    };
  }

  function ensureColumnsChartLoaded() {
    if (
      columnsSourceMode() !== "clusters" ||
      !interfaceSelect.value ||
      !state.embeddingClustering ||
      state.embeddingClustering.error ||
      state.embeddingClustering.columns_chart
    ) {
      return null;
    }
    const requestKey = currentColumnsChartRequestKey();
    if (state.columnsChartErrorKey === requestKey) {
      return null;
    }
    if (
      state.columnsChartLoading &&
      state.columnsChartLoadingKey === requestKey &&
      state.columnsChartPromise
    ) {
      return state.columnsChartPromise;
    }
    state.columnsChartLoading = true;
    state.columnsChartLoadingKey = requestKey;
    state.columnsChartErrorKey = null;
    const requestId = state.embeddingClusteringRequestId;
    const promise = (async () => {
      try {
        const payload = await fetchJson(currentColumnsChartQuery());
        if (
          requestId !== state.embeddingClusteringRequestId ||
          state.columnsChartLoadingKey !== requestKey ||
          !state.embeddingClustering ||
          state.embeddingClustering.file !== interfaceSelect.value
        ) {
          return;
        }
        state.embeddingClustering = {
          ...state.embeddingClustering,
          columns_chart: payload.columns_chart || null,
        };
        state.columnsChartKey = null;
      } catch (error) {
        if (state.columnsChartLoadingKey === requestKey) {
          state.columnsChartErrorKey = requestKey;
          if (state.embeddingClustering) {
            state.embeddingClustering = {
              ...state.embeddingClustering,
              columns_chart_error: error.message,
            };
          }
        }
      } finally {
        if (state.columnsChartLoadingKey === requestKey) {
          state.columnsChartLoading = false;
          state.columnsChartLoadingKey = null;
          state.columnsChartPromise = null;
          renderColumnsClusterLegend();
          renderColumnsChart();
        }
      }
    })();
    state.columnsChartPromise = promise;
    renderColumnsChart();
    return promise;
  }

  function columnsDomainOverlayRequestKey() {
    return [
      interfaceSelect.value || "",
      selectionSettingsKey(state.selectionSettings),
    ].join("|");
  }

  function columnsDomainOverlayUrl() {
    const params = new URLSearchParams({ file: interfaceSelect.value || "" });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    params.set("include_rows", "0");
    params.set("include_data", "1");
    params.set("include_clean_column_identity", "0");
    params.set("include_summary", "0");
    params.set("data_offset", "0");
    return `/api/interface?${params.toString()}`;
  }

  function ensureInterfaceOverlayContainers(interfaceState) {
    if (!interfaceState.data || typeof interfaceState.data !== "object") {
      interfaceState.data = {};
    }
    if (!(interfaceState.overlayByRow instanceof Map)) {
      interfaceState.overlayByRow = new Map();
    }
    if (!(interfaceState.overlayByInteractionRow instanceof Map)) {
      interfaceState.overlayByInteractionRow = new Map();
    }
    if (!(interfaceState.partnerColumnStats instanceof Map)) {
      interfaceState.partnerColumnStats = new Map();
    }
    if (!interfaceState.allColumnStats) {
      interfaceState.allColumnStats = { denominator: 0, columnCounts: new Map() };
    }
    if (!(interfaceState.allColumnStats.columnCounts instanceof Map)) {
      interfaceState.allColumnStats.columnCounts = new Map();
    }
  }

  function mergeColumnsDomainOverlayPayload(payload) {
    if (!state.interface || !payload?.data || payload.file !== state.interface.file) {
      return false;
    }
    const interfaceState = state.interface;
    ensureInterfaceOverlayContainers(interfaceState);
    const existingPartners = new Set(interfaceState.partnerDomains || []);
    const payloadPartners = Array.isArray(payload.interface_partner_domains)
      ? payload.interface_partner_domains.map((partner) => String(partner || "")).filter(Boolean)
      : [];
    for (const partner of payloadPartners) {
      existingPartners.add(partner);
    }
    if (payload.interface_partner_counts && typeof payload.interface_partner_counts === "object") {
      const counts = interfaceState.partnerInterfaceCounts instanceof Map
        ? interfaceState.partnerInterfaceCounts
        : new Map();
      for (const [partner, count] of Object.entries(payload.interface_partner_counts)) {
        counts.set(String(partner), Number(count || 0));
      }
      interfaceState.partnerInterfaceCounts = counts;
    }

    for (const [partnerDomain, rowsByKey] of Object.entries(payload.data || {})) {
      if (!rowsByKey || typeof rowsByKey !== "object") {
        continue;
      }
      existingPartners.add(partnerDomain);
      if (!interfaceState.data[partnerDomain]) {
        interfaceState.data[partnerDomain] = {};
      }
      for (const [rowKey, rowPayload] of Object.entries(rowsByKey)) {
        if (!rowPayload || typeof rowPayload !== "object") {
          continue;
        }
        if (interfaceState.data[partnerDomain][rowKey]) {
          continue;
        }
        interfaceState.data[partnerDomain][rowKey] = rowPayload;
        const partnerState = {
          interface: new Set(rowPayload.interface_msa_columns_a || []),
          surface: new Set(rowPayload.surface_msa_columns_a || []),
        };
        let rowState = interfaceState.overlayByRow.get(rowKey);
        if (!rowState) {
          rowState = {
            all: { interface: new Set(), surface: new Set() },
            byPartner: new Map(),
          };
          interfaceState.overlayByRow.set(rowKey, rowState);
        }
        rowState.byPartner.set(partnerDomain, partnerState);
        interfaceState.overlayByInteractionRow.set(interactionRowKey(rowKey, partnerDomain), {
          all: partnerState,
          byPartner: new Map([[partnerDomain, partnerState]]),
        });

        const partnerColumnStats = interfaceState.partnerColumnStats.get(partnerDomain) || {
          denominator: 0,
          columnCounts: new Map(),
        };
        if (partnerState.interface.size > 0) {
          partnerColumnStats.denominator += 1;
          for (const columnIndex of partnerState.interface) {
            partnerColumnStats.columnCounts.set(
              columnIndex,
              (partnerColumnStats.columnCounts.get(columnIndex) || 0) + 1
            );
          }
        }
        interfaceState.partnerColumnStats.set(partnerDomain, partnerColumnStats);

        const hadRowInterface = rowState.all.interface.size > 0;
        for (const columnIndex of partnerState.interface) {
          if (!rowState.all.interface.has(columnIndex)) {
            rowState.all.interface.add(columnIndex);
            interfaceState.allColumnStats.columnCounts.set(
              columnIndex,
              (interfaceState.allColumnStats.columnCounts.get(columnIndex) || 0) + 1
            );
          }
        }
        for (const columnIndex of partnerState.surface) {
          rowState.all.surface.add(columnIndex);
        }
        if (!hadRowInterface && rowState.all.interface.size > 0) {
          interfaceState.allColumnStats.denominator += 1;
        }
      }
    }
    interfaceState.partnerDomains = [...existingPartners].sort();
    interfaceState.overlayRowsTotal = Number(
      payload.data_row_count || interfaceState.overlayRowsTotal || state.msaRowsTotal || 0
    );
    interfaceState.overlayRowsLoaded = Math.max(
      Number(interfaceState.overlayRowsLoaded || 0),
      Number(payload.data_offset || 0) + Number(payload.data_loaded || 0)
    );
    interfaceState.overlayComplete =
      Boolean(payload.data_complete) ||
      (
        interfaceState.overlayRowsTotal > 0 &&
        interfaceState.overlayRowsLoaded >= interfaceState.overlayRowsTotal
      );
    return true;
  }

  function ensureColumnsDomainOverlayLoaded() {
    if (
      !interfaceSelect.value ||
      !state.interface ||
      state.interface.overlayComplete
    ) {
      return null;
    }
    const requestKey = columnsDomainOverlayRequestKey();
    if (state.columnsDomainOverlayErrorKey === requestKey) {
      return null;
    }
    if (
      state.columnsDomainOverlayPromise &&
      state.columnsDomainOverlayRequestKey === requestKey
    ) {
      return state.columnsDomainOverlayPromise;
    }
    state.columnsDomainOverlayLoading = true;
    state.columnsDomainOverlayRequestKey = requestKey;
    state.columnsDomainOverlayErrorKey = null;
    const promise = (async () => {
      try {
        const payload = await fetchJson(columnsDomainOverlayUrl());
        if (state.columnsDomainOverlayRequestKey !== requestKey) {
          return;
        }
        if (mergeColumnsDomainOverlayPayload(payload)) {
          state.columnsChartKey = null;
          state.columnsHoverCell = null;
          state.columnsInteractionLayout = null;
        }
      } catch (error) {
        if (state.columnsDomainOverlayRequestKey === requestKey) {
          state.columnsDomainOverlayErrorKey = requestKey;
          setColumnsInfo(`Could not load all interacting domains: ${error.message}`);
        }
      } finally {
        if (state.columnsDomainOverlayRequestKey === requestKey) {
          state.columnsDomainOverlayLoading = false;
          state.columnsDomainOverlayPromise = null;
          renderColumnsChart();
        }
      }
    })();
    state.columnsDomainOverlayPromise = promise;
    return promise;
  }

  function normalizedDomainColumnsChart() {
    const alignmentLength = columnsAlignmentLength();
    if (!state.interface || alignmentLength <= 0) {
      return null;
    }
    const partners = (state.interface.partnerDomains || [])
      .map((partner) => String(partner || ""))
      .filter((partner) => partner);
    if (partners.length === 0) {
      return null;
    }
    const clusters = [];
    const clusterSizes = {};
    const relativeByCluster = {};
    const gapByCluster = {};
    for (const partner of partners) {
      const stats = state.interface.partnerColumnStats?.get(partner);
      const denominator = Math.max(0, Number(stats?.denominator || 0));
      if (denominator <= 0) {
        continue;
      }
      clusters.push(partner);
      clusterSizes[partner] = denominator;
      const values = new Array(alignmentLength).fill(0);
      if (stats?.columnCounts instanceof Map) {
        for (const [columnIndex, count] of stats.columnCounts.entries()) {
          const index = Number(columnIndex);
          if (!Number.isInteger(index) || index < 0 || index >= alignmentLength) {
            continue;
          }
          values[index] = Math.max(0, Number(count || 0)) / denominator;
        }
      }
      relativeByCluster[partner] = values;
      gapByCluster[partner] = new Array(alignmentLength).fill(0);
    }
    if (clusters.length === 0) {
      return null;
    }
    return {
      file: interfaceSelect.value,
      alignmentLength,
      clusters,
      clusterSizes,
      relativeByCluster,
      gapByCluster,
      maxStackValue: 1,
      source: "domains",
    };
  }

  function rebuildColumnsChartIfNeeded() {
    const nextKey = columnsChartCacheKey();
    if (state.columnsChartKey === nextKey) {
      return;
    }

    const columnsChart = columnsSourceMode() === "domains"
      ? normalizedDomainColumnsChart()
      : normalizedServerColumnsChart();
    if (!columnsChart) {
      state.columnsChart = null;
      state.columnsChartKey = nextKey;
      state.columnsView = { start: 0, end: null };
      state.columnsInterfaceView = { start: 0, end: null };
      state.columnsVisibleClusters = new Set();
      state.columnsClusterOrder = [];
      state.columnsDrag = null;
      state.columnsHoverCell = null;
      state.columnsInteractionLayout = null;
      return;
    }

    state.columnsChart = columnsChart;
    state.columnsChartKey = nextKey;
    state.columnsView = {
      start: 0,
      end: Math.max(1, Number(columnsChart.alignmentLength || 1)),
    };
    state.columnsInterfaceView = { start: 0, end: null };
    const clusterKeys = columnsChart.clusters.map((clusterLabel) => String(clusterLabel));
    const clusterSet = new Set(clusterKeys);
    const ordered = Array.isArray(state.columnsClusterOrder)
      ? state.columnsClusterOrder.filter((clusterLabel) => clusterSet.has(String(clusterLabel))).map(String)
      : [];
    const orderedSet = new Set(ordered);
    state.columnsClusterOrder = [
      ...ordered,
      ...clusterKeys.filter((clusterLabel) => !orderedSet.has(clusterLabel)),
    ];
    const stillVisible = columnsChart.clusters.filter((clusterLabel) =>
      state.columnsVisibleClusters.has(String(clusterLabel))
    );
    state.columnsVisibleClusters = new Set(
      stillVisible.length > 0 ? stillVisible : columnsChart.clusters
    );
  }

  function renderColumnsClusterLegend() {
    if (!elements.columnsClusterLegend) {
      return;
    }
    elements.columnsClusterLegend.classList.add("hidden");
    elements.columnsClusterLegend.innerHTML = "";
  }

  function resizeColumnsCanvas() {
    const scrollElement = elements.columnsScroll || elements.columnsRoot;
    const width = Math.max(1, Math.round(scrollElement.clientWidth || elements.columnsRoot.clientWidth));
    const height = Math.max(1, Math.round(scrollElement.clientHeight || elements.columnsRoot.clientHeight));
    const dpr = window.devicePixelRatio || 1;
    elements.columnsCanvas.width = Math.round(width * dpr);
    elements.columnsCanvas.height = Math.round(height * dpr);
    elements.columnsCanvas.style.width = `${width}px`;
    elements.columnsCanvas.style.height = `${height}px`;
  }

  function requestColumnsRenderNextFrame() {
    if (columnsRenderFrameId) {
      return;
    }
    columnsRenderFrameId = window.requestAnimationFrame(() => {
      columnsRenderFrameId = 0;
      if (state.msaPanelView !== "columns") {
        return;
      }
      resizeColumnsCanvas();
      renderColumnsChart();
    });
  }

  function normalizedColumnsRange(startValue, endValue, alignmentLength) {
    const total = Math.max(1, Number(alignmentLength) || 1);
    let start = Number(startValue ?? 0);
    let end = Number(endValue ?? total);
    if (!Number.isFinite(start)) {
      start = 0;
    }
    if (!Number.isFinite(end)) {
      end = total;
    }
    start = Math.max(0, Math.min(total - 1, start));
    end = Math.max(start + 1, Math.min(total, end));
    if (end - start > total) {
      start = 0;
      end = total;
    }
    return { start, end, span: end - start };
  }

  function columnsVisibleRange(alignmentLength) {
    const total = Math.max(1, Number(alignmentLength) || 1);
    const range = normalizedColumnsRange(
      state.columnsView?.start ?? 0,
      state.columnsView?.end ?? total,
      total
    );
    state.columnsView = { start: range.start, end: range.end };
    return range;
  }

  function columnsChartArea(width) {
    return {
      chartLeft: 58,
      chartRight: width - 12,
    };
  }

  function columnHasVisibleInteraction(columnIndex, visibleClusters) {
    for (const clusterLabel of visibleClusters) {
      const values = state.columnsChart?.relativeByCluster?.[clusterLabel] || [];
      if (Number(values[columnIndex] || 0) > 0) {
        return true;
      }
    }
    return false;
  }

  function visibleInterfaceColumns(visibleClusters, range, alignmentLength) {
    const startColumn = Math.max(0, Math.floor(range.start));
    const endColumn = Math.max(
      startColumn + 1,
      Math.min(alignmentLength, Math.ceil(range.end))
    );
    const columns = [];
    for (let columnIndex = startColumn; columnIndex < endColumn; columnIndex += 1) {
      if (columnHasVisibleInteraction(columnIndex, visibleClusters)) {
        columns.push(columnIndex);
      }
    }
    return columns;
  }

  function allVisibleInterfaceColumns(visibleClusters, alignmentLength) {
    const key = `${alignmentLength}|${visibleClusters.join(",")}`;
    if (
      columnsInterfaceColumnCache.chart === state.columnsChart &&
      columnsInterfaceColumnCache.key === key
    ) {
      return columnsInterfaceColumnCache.columns;
    }
    const columns = visibleInterfaceColumns(
      visibleClusters,
      {
        start: 0,
        end: alignmentLength,
      },
      alignmentLength
    );
    columnsInterfaceColumnCache.chart = state.columnsChart;
    columnsInterfaceColumnCache.key = key;
    columnsInterfaceColumnCache.columns = columns;
    return columns;
  }

  function normalizedInterfaceOrdinalRange(startValue, endValue, totalCount) {
    const total = Math.max(0, Math.floor(Number(totalCount) || 0));
    if (total <= 0) {
      return { start: 0, end: 0, span: 0 };
    }
    let start = Number(startValue ?? 0);
    let end = Number(endValue ?? total);
    if (!Number.isFinite(start)) {
      start = 0;
    }
    if (!Number.isFinite(end)) {
      end = total;
    }
    start = Math.max(0, Math.min(total - 1, start));
    end = Math.max(start + 1, Math.min(total, end));
    if (end - start > total) {
      start = 0;
      end = total;
    }
    return { start, end, span: end - start };
  }

  function columnsInterfaceOrdinalRange(interfaceColumns) {
    if (!interfaceColumns.length) {
      state.columnsInterfaceView = { start: 0, end: null };
      return { start: 0, end: 0, span: 0 };
    }
    const range = normalizedInterfaceOrdinalRange(
      state.columnsInterfaceView?.start ?? 0,
      state.columnsInterfaceView?.end ?? interfaceColumns.length,
      interfaceColumns.length
    );
    state.columnsInterfaceView = { start: range.start, end: range.end };
    return range;
  }

  function setColumnsInterfaceOrdinalRange(start, end, interfaceColumns) {
    if (!interfaceColumns.length) {
      return false;
    }
    const range = normalizedInterfaceOrdinalRange(start, end, interfaceColumns.length);
    state.columnsInterfaceView = {
      start: range.start,
      end: range.end,
    };
    return true;
  }

  function interfaceColumnRangeFromOrdinalRange(interfaceColumns, ordinalRange, alignmentLength) {
    if (!interfaceColumns.length || ordinalRange.span <= 0) {
      return normalizedColumnsRange(0, alignmentLength, alignmentLength);
    }
    const startIndex = Math.max(
      0,
      Math.min(interfaceColumns.length - 1, Math.floor(ordinalRange.start))
    );
    const endIndex = Math.max(
      startIndex + 1,
      Math.min(interfaceColumns.length, Math.ceil(ordinalRange.end))
    );
    const firstColumn = interfaceColumns[startIndex];
    const lastColumn = interfaceColumns[endIndex - 1];
    const leftBoundary = startIndex > 0
      ? (interfaceColumns[startIndex - 1] + firstColumn) / 2
      : 0;
    const rightBoundary = endIndex < interfaceColumns.length
      ? (lastColumn + interfaceColumns[endIndex]) / 2
      : alignmentLength;
    return normalizedColumnsRange(leftBoundary, rightBoundary, alignmentLength);
  }

  function setColumnsViewRange(start, end, alignmentLength) {
    const range = normalizedColumnsRange(start, end, alignmentLength);
    state.columnsView = {
      start: range.start,
      end: range.end,
    };
    return true;
  }

  function columnsRowGap(clusterCount) {
    return clusterCount > 80 ? 0 : clusterCount > 36 ? 1 : 2;
  }

  function columnsCanvasContentHeight(viewportHeight) {
    const clusterCount = visibleColumnsClusters().length;
    if (!clusterCount) {
      return viewportHeight;
    }
    const rowsTop = 62;
    const bottomInset = 42;
    const minRowHeight = 10;
    const gap = columnsRowGap(clusterCount);
    return Math.ceil(
      rowsTop +
        bottomInset +
        clusterCount * minRowHeight +
        gap * Math.max(0, clusterCount - 1)
    );
  }

  function columnsVirtualWidth(viewportWidth, alignmentLength, span) {
    const total = Math.max(1, Number(alignmentLength) || 1);
    const visibleSpan = Math.max(1, Number(span) || total);
    const scaledWidth = Math.ceil((Math.max(1, viewportWidth) * total) / visibleSpan);
    return Math.max(viewportWidth, Math.min(2_000_000, scaledWidth));
  }

  function columnsScrollLeftForRange(range, viewportWidth, virtualWidth, alignmentLength) {
    const maxScrollLeft = Math.max(0, virtualWidth - viewportWidth);
    const maxStart = Math.max(0, alignmentLength - range.span);
    if (maxScrollLeft <= 0 || maxStart <= 0) {
      return 0;
    }
    return (Math.max(0, Math.min(maxStart, range.start)) / maxStart) * maxScrollLeft;
  }

  function columnsRangeStartForScroll(scrollLeft, viewportWidth, virtualWidth, alignmentLength, span) {
    const maxScrollLeft = Math.max(0, virtualWidth - viewportWidth);
    const maxStart = Math.max(0, alignmentLength - span);
    if (maxScrollLeft <= 0 || maxStart <= 0) {
      return 0;
    }
    return (Math.max(0, Math.min(maxScrollLeft, scrollLeft)) / maxScrollLeft) * maxStart;
  }

  function syncColumnsScrollSpace(
    alignmentLength,
    range,
    contentHeight,
    scrollAxisLength = alignmentLength,
    scrollRange = range
  ) {
    const scrollElement = elements.columnsScroll;
    const spacer = elements.columnsSpacer;
    if (!scrollElement || !spacer) {
      return;
    }
    const viewportWidth = Math.max(1, scrollElement.clientWidth);
    const viewportHeight = Math.max(1, scrollElement.clientHeight);
    const virtualWidth = columnsVirtualWidth(viewportWidth, scrollAxisLength, scrollRange.span);
    const virtualHeight = Math.max(viewportHeight, Math.ceil(contentHeight));
    spacer.style.width = `${Math.ceil(virtualWidth)}px`;
    spacer.style.height = `${virtualHeight}px`;
    const targetScrollLeft = columnsScrollLeftForRange(
      scrollRange,
      viewportWidth,
      virtualWidth,
      scrollAxisLength
    );
    if (Math.abs(scrollElement.scrollLeft - targetScrollLeft) > 1) {
      scrollElement.scrollLeft = targetScrollLeft;
    }
  }

  function drawColumnsRuler(ctx, ticks, chartLeft, chartWidth, y, labelsAbove) {
    ctx.strokeStyle = "rgba(62, 51, 39, 0.36)";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(chartLeft, y + 0.5);
    ctx.lineTo(chartLeft + chartWidth, y + 0.5);
    ctx.stroke();
    ctx.fillStyle = "#6f6658";
    ctx.font = '10px "IBM Plex Sans", sans-serif';
    ctx.textAlign = "center";
    ctx.textBaseline = labelsAbove ? "bottom" : "top";
    for (const tick of ticks) {
      const x = chartLeft + tick.ratio * chartWidth;
      ctx.beginPath();
      ctx.moveTo(x + 0.5, y - 4);
      ctx.lineTo(x + 0.5, y + 4);
      ctx.stroke();
      ctx.fillText(String(tick.label), x, labelsAbove ? y - 7 : y + 7);
    }
  }

  function drawColumnsHoverCell(ctx, hoverCell) {
    const rect = hoverCell?.rect;
    if (!rect) {
      return;
    }
    const x = Math.floor(Number(rect.x || 0)) + 0.5;
    const y = Math.floor(Number(rect.y || 0)) + 0.5;
    const width = Math.max(2, Math.round(Number(rect.width || 0)) - 1);
    const height = Math.max(2, Math.round(Number(rect.height || 0)) - 1);
    ctx.save();
    ctx.fillStyle = "rgba(45, 106, 79, 0.08)";
    ctx.fillRect(x, y, width, height);
    ctx.strokeStyle = "#2d6a4f";
    ctx.lineWidth = 2;
    ctx.strokeRect(x, y, width, height);
    ctx.restore();
  }

  function panColumnsByPixels(pixelDelta, chartWidth, alignmentLength, visibleClusters = null) {
    if (state.columnsInterfaceOnly) {
      const interfaceColumns = allVisibleInterfaceColumns(
        visibleClusters || visibleColumnsClusters(),
        alignmentLength
      );
      const range = columnsInterfaceOrdinalRange(interfaceColumns);
      if (!interfaceColumns.length || range.span <= 0) {
        return false;
      }
      const deltaColumns = (Number(pixelDelta) / Math.max(1, chartWidth)) * range.span;
      let nextStart = range.start + deltaColumns;
      nextStart = Math.max(0, Math.min(interfaceColumns.length - range.span, nextStart));
      return setColumnsInterfaceOrdinalRange(
        nextStart,
        nextStart + range.span,
        interfaceColumns
      );
    }
    const range = columnsVisibleRange(alignmentLength);
    const deltaColumns = (Number(pixelDelta) / Math.max(1, chartWidth)) * range.span;
    let nextStart = range.start + deltaColumns;
    nextStart = Math.max(0, Math.min(alignmentLength - range.span, nextStart));
    return setColumnsViewRange(nextStart, nextStart + range.span, alignmentLength);
  }

  function columnsCanvasPoint(event) {
    const rect = elements.columnsCanvas.getBoundingClientRect();
    return {
      x: event.clientX - rect.left,
      y: event.clientY - rect.top,
    };
  }

  function columnsRowAtPoint(point) {
    const layout = state.columnsInteractionLayout;
    if (!layout?.rows?.length) {
      return null;
    }
    return layout.rows.find((row) => point.y >= row.y && point.y <= row.y + row.height) || null;
  }

  function columnsCellAtPoint(point) {
    const layout = state.columnsInteractionLayout;
    if (
      !layout ||
      point.x < layout.chartLeft ||
      point.x > layout.chartRight ||
      point.y < layout.rowsTop ||
      point.y > layout.rowsBottom
    ) {
      return null;
    }
    const row = columnsRowAtPoint(point);
    if (!row) {
      return null;
    }
    const ratio = Math.max(0, Math.min(1, (point.x - layout.chartLeft) / Math.max(1, layout.chartWidth)));
    const binCount = Math.max(1, Number(layout.binCount || 1));
    const binIndex = Math.max(0, Math.min(binCount - 1, Math.floor(ratio * binCount)));
    const binWidth = layout.chartWidth / binCount;
    const rect = {
      x: layout.chartLeft + binIndex * binWidth,
      y: row.y,
      width: binWidth,
      height: row.height,
    };
    if (layout.interfaceOnly) {
      const interfaceColumns = Array.isArray(layout.interfaceColumns) ? layout.interfaceColumns : [];
      if (interfaceColumns.length === 0) {
        return null;
      }
      const startOrdinal = Math.floor((binIndex * interfaceColumns.length) / binCount);
      const endOrdinal = Math.max(
        startOrdinal + 1,
        Math.ceil(((binIndex + 1) * interfaceColumns.length) / binCount)
      );
      const pointerOrdinal = Math.max(
        0,
        Math.min(interfaceColumns.length - 1, Math.floor(ratio * interfaceColumns.length))
      );
      const columnIndex = Number(interfaceColumns[pointerOrdinal]);
      if (!Number.isInteger(columnIndex)) {
        return null;
      }
      return {
        row,
        columnIndex,
        binIndex,
        rect,
        bucketColumns: interfaceColumns.slice(startOrdinal, endOrdinal),
      };
    }

    const alignmentLength = Math.max(1, Number(layout.alignmentLength || 1));
    const range = layout.range || { start: 0, end: alignmentLength, span: alignmentLength };
    const startColumn = Math.max(
      0,
      Math.min(alignmentLength - 1, Math.floor(range.start + (binIndex * range.span) / binCount))
    );
    const endColumn = Math.max(
      startColumn + 1,
      Math.min(alignmentLength, Math.ceil(range.start + ((binIndex + 1) * range.span) / binCount))
    );
    const columnIndex = Math.max(
      0,
      Math.min(alignmentLength - 1, Math.floor(range.start + ratio * range.span))
    );
    return {
      row,
      columnIndex,
      binIndex,
      rect,
      bucketColumns: Array.from(
        { length: Math.max(0, endColumn - startColumn) },
        (_value, index) => startColumn + index
      ),
    };
  }

  function moveColumnsCluster(clusterLabel, targetLabel, insertAfter = false) {
    const labels = allColumnsClusterLabels();
    const fromIndex = labels.indexOf(clusterLabel);
    if (fromIndex < 0) {
      return;
    }
    labels.splice(fromIndex, 1);
    let targetIndex = targetLabel ? labels.indexOf(targetLabel) : labels.length;
    if (targetIndex < 0) {
      targetIndex = labels.length;
    }
    if (insertAfter) {
      targetIndex += 1;
    }
    targetIndex = Math.max(0, Math.min(labels.length, targetIndex));
    labels.splice(targetIndex, 0, clusterLabel);
    state.columnsClusterOrder = labels;
  }

  function clearColumnsHoverCell() {
    if (columnsHoverTimer) {
      window.clearTimeout(columnsHoverTimer);
      columnsHoverTimer = 0;
    }
    columnsHoverTargetKey = "";
    if (state.columnsHoverCell) {
      state.columnsHoverCell = null;
      requestColumnsRenderNextFrame();
    }
  }

  function columnsHoverCellKey(cell) {
    if (!cell) {
      return "";
    }
    return [
      columnsSourceMode(),
      cell.row?.clusterLabel ?? "",
      cell.binIndex ?? "",
      cell.columnIndex ?? "",
    ].join("|");
  }

  function scheduleColumnsHoverCell(cell) {
    const key = columnsHoverCellKey(cell);
    if (!key) {
      clearColumnsHoverCell();
      return;
    }
    if (state.columnsHoverCell?.key === key) {
      return;
    }
    if (columnsHoverTargetKey === key && columnsHoverTimer) {
      return;
    }
    if (columnsHoverTimer) {
      window.clearTimeout(columnsHoverTimer);
    }
    columnsHoverTargetKey = key;
    columnsHoverTimer = window.setTimeout(() => {
      columnsHoverTimer = 0;
      if (columnsHoverTargetKey !== key) {
        return;
      }
      state.columnsHoverCell = {
        key,
        source: columnsSourceMode(),
        seriesLabel: String(cell.row?.clusterLabel ?? ""),
        columnIndex: cell.columnIndex,
        rect: cell.rect,
      };
      requestColumnsRenderNextFrame();
    }, 200);
  }

  function columnsMemberKey(member) {
    return interactionRowKey(member?.row_key || "", member?.partner_domain || "");
  }

  function columnsPointMembers(point) {
    const members = embeddingPointMembers(point);
    const rowKey = String(point?.row_key || "");
    const partnerDomain = String(point?.partner_domain || "");
    if (!rowKey || !partnerDomain) {
      return members;
    }
    const directMember = { row_key: rowKey, partner_domain: partnerDomain };
    const directKey = columnsMemberKey(directMember);
    return members.some((member) => columnsMemberKey(member) === directKey)
      ? members
      : [directMember].concat(members);
  }

  function payloadHasInterfaceColumn(rowPayload, columnIndex) {
    if (!rowPayload || typeof rowPayload !== "object") {
      return false;
    }
    const columns = rowPayload.interface_msa_columns_a || [];
    if (columns instanceof Set) {
      return columns.has(columnIndex) || columns.has(String(columnIndex));
    }
    return Array.isArray(columns) && columns.some((value) => Number(value) === columnIndex);
  }

  function interactionPayloadForMember(member) {
    const rowKey = String(member?.row_key || "");
    const partnerDomain = String(member?.partner_domain || "");
    if (!rowKey || !partnerDomain) {
      return null;
    }
    return state.interface?.data?.[partnerDomain]?.[rowKey] || null;
  }

  function uniqueSortedColumns(columns, preferredColumn) {
    const preferred = Number(preferredColumn);
    const uniqueColumns = Array.from(
      new Set(
        [preferred].concat(columns || [])
          .map((column) => Number(column))
          .filter((column) => Number.isInteger(column) && column >= 0)
      )
    );
    return uniqueColumns.sort(
      (left, right) =>
        Math.abs(left - preferred) - Math.abs(right - preferred) ||
        left - right
    );
  }

  function domainMembersForColumn(partnerDomain, columnIndex) {
    const rowsByKey = state.interface?.data?.[partnerDomain];
    if (!rowsByKey || typeof rowsByKey !== "object") {
      return [];
    }
    return Object.entries(rowsByKey)
      .filter(([_rowKey, rowPayload]) => payloadHasInterfaceColumn(rowPayload, columnIndex))
      .map(([rowKey]) => ({
        row_key: String(rowKey),
        partner_domain: String(partnerDomain),
      }));
  }

  function clusterMembersForColumn(clusterLabel, columnIndex) {
    const numericClusterLabel = Number(clusterLabel);
    if (!Number.isFinite(numericClusterLabel)) {
      return [];
    }
    const members = [];
    const seen = new Set();
    for (const point of state.embeddingClustering?.points || []) {
      if (Number(point?.cluster_label) !== numericClusterLabel) {
        continue;
      }
      for (const member of columnsPointMembers(point)) {
        if (!payloadHasInterfaceColumn(interactionPayloadForMember(member), columnIndex)) {
          continue;
        }
        const key = columnsMemberKey(member);
        if (!key || seen.has(key)) {
          continue;
        }
        seen.add(key);
        members.push({
          row_key: String(member.row_key),
          partner_domain: String(member.partner_domain),
        });
      }
    }
    return members;
  }

  function membersForColumnsCell(cell) {
    const candidateColumns = uniqueSortedColumns(cell.bucketColumns, cell.columnIndex);
    for (const columnIndex of candidateColumns) {
      const members = columnsSourceMode() === "domains"
        ? domainMembersForColumn(String(cell.row.clusterLabel), columnIndex)
        : clusterMembersForColumn(cell.row.clusterLabel, columnIndex);
      if (members.length > 0) {
        return {
          columnIndex,
          members: members.sort(
            (left, right) =>
              left.partner_domain.localeCompare(right.partner_domain) ||
              left.row_key.localeCompare(right.row_key)
          ),
        };
      }
    }
    return { columnIndex: cell.columnIndex, members: [] };
  }

  async function handleColumnsDoubleClick(event) {
    if (!state.columnsChart?.alignmentLength) {
      return;
    }
    const point = columnsCanvasPoint(event);
    const cell = columnsCellAtPoint(point);
    if (!cell) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    const overlayPromise = ensureColumnsDomainOverlayLoaded();
    if (overlayPromise) {
      setColumnsInfo("Loading interface examples for the selected column...");
      await overlayPromise;
    }
    const resolved = membersForColumnsCell(cell);
    if (resolved.members.length === 0) {
      setColumnsInfo(
        `No interfaces found for ${columnsSourceTitle().toLowerCase()} ${cell.row.clusterLabel} at MSA column ${cell.columnIndex}.`
      );
      return;
    }
    await openColumnsCellStructures({
      source: columnsSourceMode(),
      seriesLabel: String(cell.row.clusterLabel),
      columnIndex: resolved.columnIndex,
      members: resolved.members,
    });
    setColumnsInfo(
      `Opening ${resolved.members.length} ${columnsSourcePlural()} examples at MSA column ${resolved.columnIndex}.`
    );
  }

  function handleColumnsPointerDown(event) {
    if (event.button !== 0 || !state.columnsChart?.alignmentLength) {
      return;
    }
    clearColumnsHoverCell();
    const point = columnsCanvasPoint(event);
    const layout = state.columnsInteractionLayout;
    if (!layout) {
      return;
    }
    const row = columnsRowAtPoint(point);
    if (row && point.x < layout.chartLeft) {
      event.preventDefault();
      state.columnsDrag = {
        type: "reorder",
        clusterLabel: row.clusterLabel,
      };
      elements.columnsCanvas.setPointerCapture?.(event.pointerId);
      return;
    }
    if (
      point.x >= layout.chartLeft &&
      point.x <= layout.chartRight &&
      point.y >= 0 &&
      point.y <= elements.columnsCanvas.clientHeight
    ) {
      event.preventDefault();
      state.columnsDrag = {
        type: "scroll",
        startX: point.x,
        startY: point.y,
        scrollLeft: elements.columnsScroll?.scrollLeft || 0,
        scrollTop: elements.columnsScroll?.scrollTop || 0,
      };
      elements.columnsCanvas.setPointerCapture?.(event.pointerId);
    }
  }

  function handleColumnsPointerMove(event) {
    if (!state.columnsDrag || !state.columnsChart?.alignmentLength) {
      if (state.columnsChart?.alignmentLength) {
        scheduleColumnsHoverCell(columnsCellAtPoint(columnsCanvasPoint(event)));
      }
      return;
    }
    const point = columnsCanvasPoint(event);
    const layout = state.columnsInteractionLayout;
    if (!layout) {
      return;
    }
    if (state.columnsDrag.type === "reorder") {
      event.preventDefault();
      const targetRow = columnsRowAtPoint(point);
      if (!targetRow || targetRow.clusterLabel === state.columnsDrag.clusterLabel) {
        return;
      }
      const insertAfter = point.y > targetRow.y + targetRow.height / 2;
      moveColumnsCluster(state.columnsDrag.clusterLabel, targetRow.clusterLabel, insertAfter);
      renderColumnsClusterLegend();
      requestColumnsRenderNextFrame();
      return;
    }
    if (state.columnsDrag.type === "scroll") {
      event.preventDefault();
      if (!elements.columnsScroll) {
        return;
      }
      elements.columnsScroll.scrollLeft = state.columnsDrag.scrollLeft + state.columnsDrag.startX - point.x;
      elements.columnsScroll.scrollTop = state.columnsDrag.scrollTop + state.columnsDrag.startY - point.y;
      requestColumnsRenderNextFrame();
    }
  }

  function handleColumnsPointerUp(event) {
    state.columnsDrag = null;
    elements.columnsCanvas?.releasePointerCapture?.(event.pointerId);
  }

  function handleColumnsPointerLeave() {
    clearColumnsHoverCell();
  }

  function handleColumnsWheel(event) {
    if (!state.columnsChart?.alignmentLength) {
      return;
    }
    clearColumnsHoverCell();
    if (!event.ctrlKey && !event.metaKey && !event.altKey) {
      if (!elements.columnsScroll) {
        return;
      }
      event.preventDefault();
      const horizontalDelta = event.shiftKey ? Number(event.deltaY || 0) : Number(event.deltaX || 0);
      const verticalDelta = event.shiftKey ? 0 : Number(event.deltaY || 0);
      elements.columnsScroll.scrollLeft += horizontalDelta;
      elements.columnsScroll.scrollTop += verticalDelta;
      requestColumnsRenderNextFrame();
      return;
    }
    event.preventDefault();
    const width = Math.max(
      1,
      Math.round((elements.columnsScroll || elements.columnsRoot).clientWidth)
    );
    const { chartLeft, chartRight } = columnsChartArea(width);
    const chartWidth = Math.max(1, chartRight - chartLeft);
    const alignmentLength = Math.max(1, Number(state.columnsChart.alignmentLength || 1));
    const visibleClusters = visibleColumnsClusters();
    const point = columnsCanvasPoint(event);
    const pointerX = Math.max(chartLeft, Math.min(chartRight, point.x));
    const pointerRatio = Math.max(0, Math.min(1, (pointerX - chartLeft) / chartWidth));
    if (event.shiftKey || Math.abs(Number(event.deltaX || 0)) > Math.abs(Number(event.deltaY || 0))) {
      if (
        panColumnsByPixels(
          Number(event.deltaX || event.deltaY || 0),
          chartWidth,
          alignmentLength,
          visibleClusters
        )
      ) {
        requestColumnsRenderNextFrame();
      }
      return;
    }
    const zoomFactor = Math.exp(Number(event.deltaY || 0) * 0.0015);
    if (state.columnsInterfaceOnly) {
      const interfaceColumns = allVisibleInterfaceColumns(visibleClusters, alignmentLength);
      const range = columnsInterfaceOrdinalRange(interfaceColumns);
      if (!interfaceColumns.length || range.span <= 0) {
        return;
      }
      const minSpan = Math.min(interfaceColumns.length, 8);
      const nextSpan = Math.max(minSpan, Math.min(interfaceColumns.length, range.span * zoomFactor));
      const anchorColumn = range.start + pointerRatio * range.span;
      let nextStart = anchorColumn - pointerRatio * nextSpan;
      nextStart = Math.max(0, Math.min(interfaceColumns.length - nextSpan, nextStart));
      if (setColumnsInterfaceOrdinalRange(nextStart, nextStart + nextSpan, interfaceColumns)) {
        requestColumnsRenderNextFrame();
      }
      return;
    }
    const range = columnsVisibleRange(alignmentLength);
    const minSpan = Math.min(alignmentLength, 8);
    const nextSpan = Math.max(minSpan, Math.min(alignmentLength, range.span * zoomFactor));
    const anchorColumn = range.start + pointerRatio * range.span;
    let nextStart = anchorColumn - pointerRatio * nextSpan;
    nextStart = Math.max(0, Math.min(alignmentLength - nextSpan, nextStart));
    if (setColumnsViewRange(nextStart, nextStart + nextSpan, alignmentLength)) {
      requestColumnsRenderNextFrame();
    }
  }

  function handleColumnsScroll() {
    clearColumnsHoverCell();
    const scrollElement = elements.columnsScroll;
    if (!scrollElement || !state.columnsChart?.alignmentLength) {
      requestColumnsRenderNextFrame();
      return;
    }
    const alignmentLength = Math.max(1, Number(state.columnsChart.alignmentLength || 1));
    const viewportWidth = Math.max(1, scrollElement.clientWidth);
    const visibleClusters = visibleColumnsClusters();
    if (state.columnsInterfaceOnly) {
      const interfaceColumns = allVisibleInterfaceColumns(visibleClusters, alignmentLength);
      const range = columnsInterfaceOrdinalRange(interfaceColumns);
      if (!interfaceColumns.length || range.span <= 0) {
        requestColumnsRenderNextFrame();
        return;
      }
      const virtualWidth = columnsVirtualWidth(viewportWidth, interfaceColumns.length, range.span);
      const nextStart = columnsRangeStartForScroll(
        scrollElement.scrollLeft,
        viewportWidth,
        virtualWidth,
        interfaceColumns.length,
        range.span
      );
      setColumnsInterfaceOrdinalRange(nextStart, nextStart + range.span, interfaceColumns);
      requestColumnsRenderNextFrame();
      return;
    }
    const range = columnsVisibleRange(alignmentLength);
    const virtualWidth = columnsVirtualWidth(viewportWidth, alignmentLength, range.span);
    const nextStart = columnsRangeStartForScroll(
      scrollElement.scrollLeft,
      viewportWidth,
      virtualWidth,
      alignmentLength,
      range.span
    );
    setColumnsViewRange(nextStart, nextStart + range.span, alignmentLength);
    requestColumnsRenderNextFrame();
  }

  function renderColumnsChart() {
    const ctx = elements.columnsCanvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    const scrollElement = elements.columnsScroll || elements.columnsRoot;
    const width = Math.max(1, Math.round(scrollElement.clientWidth || elements.columnsRoot.clientWidth));
    const height = Math.max(1, Math.round(scrollElement.clientHeight || elements.columnsRoot.clientHeight));
    if (!ctx || width <= 0 || height <= 0) {
      return;
    }
    const expectedCanvasWidth = Math.round(width * dpr);
    const expectedCanvasHeight = Math.round(height * dpr);
    if (
      elements.columnsCanvas.width !== expectedCanvasWidth ||
      elements.columnsCanvas.height !== expectedCanvasHeight
    ) {
      resizeColumnsCanvas();
      requestColumnsRenderNextFrame();
      return;
    }

    const sourceMode = columnsSourceMode();
    const sourceTitle = columnsSourceTitle();
    const sourcePlural = columnsSourcePlural();
    const clusterSource = sourceMode === "clusters";
    const domainSource = sourceMode === "domains";
    if (domainSource && state.interface && !state.interface.overlayComplete) {
      void ensureColumnsDomainOverlayLoaded();
    }
    if (clusterSource && state.embeddingClustering && !state.embeddingClustering.columns_chart) {
      void ensureColumnsChartLoaded();
    }
    rebuildColumnsChartIfNeeded();
    renderColumnsClusterLegend();
    const showClusterLoading =
      clusterSource &&
      (state.embeddingClusteringLoading || state.columnsChartLoading) &&
      !(state.columnsChart?.clusters || []).length;
    const showDomainLoading =
      domainSource && state.columnsDomainOverlayLoading && !state.interface?.overlayComplete;
    elements.columnsLoading.classList.toggle(
      "hidden",
      !(showClusterLoading || showDomainLoading)
    );
    elements.columnsLoadingLabel.textContent = showDomainLoading
      ? "Loading interacting domains..."
      : state.columnsChartLoading
        ? "Loading column histogram..."
        : "Loading clustering data...";

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#fffdf8";
    ctx.fillRect(0, 0, width, height);
    state.columnsInteractionLayout = null;

    const centerX = width / 2;
    const centerY = height / 2;

    if (!interfaceSelect.value) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Load an interface selection to view columns.", centerX, centerY);
      setColumnsInfo(`${sourceTitle}-column interaction barcode.`);
      return;
    }
    if (clusterSource && state.embeddingClustering?.error) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(state.embeddingClustering.error, centerX, centerY);
      setColumnsInfo(state.embeddingClustering.error);
      return;
    }
    if (
      clusterSource &&
      state.embeddingClustering &&
      !state.embeddingClusteringLoading &&
      !state.columnsChartLoading &&
      !state.embeddingClustering.columns_chart
    ) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Columns histogram unavailable.", centerX, centerY);
      setColumnsInfo("Clustering response did not include server-side columns histogram data.");
      return;
    }
    if (!(state.columnsChart?.clusters || []).length) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(
        clusterSource && state.embeddingClusteringLoading
          ? "Preparing clustering..."
          : clusterSource && state.columnsChartLoading
            ? "Preparing column histogram..."
          : clusterSource
            ? "Load clustering to inspect cluster-column interactions."
            : "No interacting domains available.",
        centerX,
        centerY
      );
      setColumnsInfo(
        clusterSource
          ? "Shows for each MSA column the fraction of each cluster that interacts at that position."
          : "Shows for each MSA column the fraction of each interacting domain that contacts that position."
      );
      return;
    }

    const visibleClusters = visibleColumnsClusters();
    if (visibleClusters.length === 0) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(`Select at least one ${sourceMode === "domains" ? "domain" : "cluster"} in the legend.`, centerX, centerY);
      setColumnsInfo(`${sourceTitle} filter hides all columns.`);
      return;
    }

    if (elements.columnsInterfaceOnlyToggle) {
      elements.columnsInterfaceOnlyToggle.checked = Boolean(state.columnsInterfaceOnly);
    }
    if (elements.columnsEmptyBinsWhiteToggle) {
      elements.columnsEmptyBinsWhiteToggle.checked = Boolean(state.columnsEmptyBinsWhite);
    }
    if (elements.columnsGapShadingToggle) {
      elements.columnsGapShadingToggle.checked = Boolean(state.columnsGapShading);
    }
    const { chartLeft, chartRight } = columnsChartArea(width);
    const scrollTop = elements.columnsScroll?.scrollTop || 0;
    const contentHeight = columnsCanvasContentHeight(height);
    const rowsTopContent = 62;
    const rowsBottomContent = contentHeight - 42;
    const rowsTop = rowsTopContent - scrollTop;
    const rowsBottom = rowsBottomContent - scrollTop;
    const chartWidth = Math.max(0, chartRight - chartLeft);
    const chartHeight = Math.max(0, rowsBottomContent - rowsTopContent);
    if (chartWidth < 20 || chartHeight < 1) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Columns view is too small.", centerX, centerY);
      setColumnsInfo(`Expand the panel to render the ${sourceMode === "domains" ? "domain" : "cluster"}-column barcode.`);
      return;
    }

    const alignmentLength = Math.max(1, Number(state.columnsChart.alignmentLength || 1));
    const interfaceOnly = Boolean(state.columnsInterfaceOnly);
    const allInterfaceColumns = interfaceOnly
      ? allVisibleInterfaceColumns(visibleClusters, alignmentLength)
      : [];
    let range = columnsVisibleRange(alignmentLength);
    let ordinalRange = null;
    if (interfaceOnly) {
      if (allInterfaceColumns.length === 0) {
        ctx.fillStyle = "#6f6658";
        ctx.font = '13px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "center";
        ctx.fillText(`No interface residues found for selected ${sourcePlural}.`, centerX, centerY);
        setColumnsInfo("Interface-only mode hides columns without interactions.");
        return;
      }
      ordinalRange = columnsInterfaceOrdinalRange(allInterfaceColumns);
      range = interfaceColumnRangeFromOrdinalRange(allInterfaceColumns, ordinalRange, alignmentLength);
      state.columnsView = {
        start: range.start,
        end: range.end,
      };
    }
    syncColumnsScrollSpace(
      alignmentLength,
      range,
      contentHeight,
      interfaceOnly ? allInterfaceColumns.length : alignmentLength,
      interfaceOnly ? ordinalRange : range
    );
    if (
      Math.round(scrollElement.clientWidth || width) !== width ||
      Math.round(scrollElement.clientHeight || height) !== height
    ) {
      resizeColumnsCanvas();
      requestColumnsRenderNextFrame();
      return;
    }
    const interfaceColumns = interfaceOnly
      ? allInterfaceColumns.slice(
          Math.floor(ordinalRange.start),
          Math.ceil(ordinalRange.end)
        )
      : [];
    const visibleColumnCount = interfaceOnly
      ? interfaceColumns.length
      : Math.max(1, Math.ceil(range.end) - Math.floor(range.start));
    if (interfaceOnly && visibleColumnCount === 0) {
      state.columnsInterfaceView = { start: 0, end: null };
      requestColumnsRenderNextFrame();
      return;
    }
    const binCount = Math.max(1, Math.min(visibleColumnCount, Math.floor(chartWidth)));
    const binWidth = chartWidth / binCount;
    const binnedValues = new Map(visibleClusters.map((clusterLabel) => [clusterLabel, new Float64Array(binCount)]));
    const binnedGapValues = new Map(visibleClusters.map((clusterLabel) => [clusterLabel, new Float64Array(binCount)]));
    const gapShading = Boolean(state.columnsGapShading);
    let maxValue = 0;
    for (let binIndex = 0; binIndex < binCount; binIndex += 1) {
      if (interfaceOnly) {
        const startOrdinal = Math.floor((binIndex * interfaceColumns.length) / binCount);
        const endOrdinal = Math.max(
          startOrdinal + 1,
          Math.ceil(((binIndex + 1) * interfaceColumns.length) / binCount)
        );
        for (const clusterLabel of visibleClusters) {
          const values = state.columnsChart.relativeByCluster?.[clusterLabel] || [];
          const gapValues = state.columnsChart.gapByCluster?.[clusterLabel] || [];
          let sum = 0;
          let gapSum = 0;
          for (let ordinal = startOrdinal; ordinal < endOrdinal; ordinal += 1) {
            sum += Number(values[interfaceColumns[ordinal]] || 0);
            gapSum += Number(gapValues[interfaceColumns[ordinal]] || 0);
          }
          const averageValue = sum / Math.max(1, endOrdinal - startOrdinal);
          const averageGapValue = gapSum / Math.max(1, endOrdinal - startOrdinal);
          binnedValues.get(clusterLabel)[binIndex] = averageValue;
          binnedGapValues.get(clusterLabel)[binIndex] = averageGapValue;
          if (averageValue > maxValue) {
            maxValue = averageValue;
          }
        }
      } else {
        const startColumn = Math.max(
          0,
          Math.min(alignmentLength - 1, Math.floor(range.start + (binIndex * range.span) / binCount))
        );
        const endColumn = Math.max(
          startColumn + 1,
          Math.min(alignmentLength, Math.ceil(range.start + ((binIndex + 1) * range.span) / binCount))
        );
        for (const clusterLabel of visibleClusters) {
          const values = state.columnsChart.relativeByCluster?.[clusterLabel] || [];
          const gapValues = state.columnsChart.gapByCluster?.[clusterLabel] || [];
          let sum = 0;
          let gapSum = 0;
          for (let columnIndex = startColumn; columnIndex < endColumn; columnIndex += 1) {
            sum += Number(values[columnIndex] || 0);
            gapSum += Number(gapValues[columnIndex] || 0);
          }
          const averageValue = sum / Math.max(1, endColumn - startColumn);
          const averageGapValue = gapSum / Math.max(1, endColumn - startColumn);
          binnedValues.get(clusterLabel)[binIndex] = averageValue;
          binnedGapValues.get(clusterLabel)[binIndex] = averageGapValue;
          if (averageValue > maxValue) {
            maxValue = averageValue;
          }
        }
      }
    }

    if (maxValue <= 0) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(`No interface residues found for selected ${sourcePlural}.`, centerX, centerY);
      setColumnsInfo(`Selected ${sourcePlural} have no interacting columns.`);
      return;
    }

    const rowGap = columnsRowGap(visibleClusters.length);
    const rowHeight = Math.max(
      1,
      (chartHeight - rowGap * Math.max(0, visibleClusters.length - 1)) / visibleClusters.length
    );
    const rows = visibleClusters.map((clusterLabel, clusterIndex) => ({
      clusterLabel,
      y: rowsTopContent + clusterIndex * (rowHeight + rowGap) - scrollTop,
      height: rowHeight,
    }));
    state.columnsInteractionLayout = {
      chartLeft,
      chartRight,
      chartWidth,
      rowsTop,
      rowsBottom,
      range,
      alignmentLength,
      interfaceOnly,
      interfaceColumns,
      binCount,
      rows,
    };

    const targetTickCount = Math.max(2, Math.min(10, Math.floor(chartWidth / 92)));
    const rulerTicks = [];
    if (interfaceOnly) {
      const maxOrdinal = Math.max(0, interfaceColumns.length - 1);
      const seen = new Set();
      for (let tickIndex = 0; tickIndex < targetTickCount; tickIndex += 1) {
        const ratio = targetTickCount === 1 ? 0 : tickIndex / (targetTickCount - 1);
        const ordinal = Math.round(ratio * maxOrdinal);
        if (seen.has(ordinal)) {
          continue;
        }
        seen.add(ordinal);
        rulerTicks.push({
          ratio: maxOrdinal <= 0 ? 0 : ordinal / maxOrdinal,
          label: interfaceColumns[ordinal],
        });
      }
    } else {
      const tickStep = niceColumnTickStep(range.span, targetTickCount);
      const firstTick = Math.ceil(range.start / tickStep) * tickStep;
      for (let tick = firstTick; tick <= range.end; tick += tickStep) {
        const ratio = (tick - range.start) / range.span;
        if (ratio < -0.001 || ratio > 1.001) {
          continue;
        }
        rulerTicks.push({
          ratio: Math.max(0, Math.min(1, ratio)),
          label: Math.round(tick),
        });
      }
    }
    drawColumnsRuler(ctx, rulerTicks, chartLeft, chartWidth, rowsTop - 7, true);
    drawColumnsRuler(ctx, rulerTicks, chartLeft, chartWidth, rowsBottom + 7, false);

    for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
      const row = rows[rowIndex];
      const clusterLabel = row.clusterLabel;
      const y = row.y;
      const rowY = Math.floor(y);
      const drawHeight = Math.max(1, Math.ceil(rowHeight));
      ctx.fillStyle = rowIndex % 2 === 0 ? "#fffaf1" : "#f8f1e6";
      ctx.fillRect(0, rowY, chartRight, drawHeight);
      const emptyBinRgb = columnsEmptyBinRgb();
      ctx.fillStyle = rgbToCss(emptyBinRgb);
      ctx.fillRect(chartLeft, rowY, chartWidth, drawHeight);
      if (rowHeight >= 8) {
        ctx.fillStyle = "#b0a491";
        ctx.font = '10px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "left";
        ctx.textBaseline = "middle";
        ctx.fillText("::", 7, y + rowHeight / 2);
        ctx.fillText("::", 13, y + rowHeight / 2);
        ctx.fillStyle = columnsSeriesColor(clusterLabel);
        ctx.fillRect(chartLeft - 34, y + Math.max(1, (rowHeight - 9) / 2), 9, Math.min(9, rowHeight - 2));
        ctx.fillStyle = "#6f6658";
        ctx.font = '10px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        ctx.fillText(columnsSeriesShortLabel(clusterLabel), chartLeft - 7, y + rowHeight / 2);
      } else {
        ctx.fillStyle = columnsSeriesColor(clusterLabel);
        ctx.fillRect(chartLeft - 10, rowY, 5, drawHeight);
      }
      const values = binnedValues.get(clusterLabel);
      const gapValues = binnedGapValues.get(clusterLabel);
      for (let binIndex = 0; binIndex < binCount; binIndex += 1) {
        const value = values[binIndex];
        const gapValue = gapValues?.[binIndex] || 0;
        const x0 = chartLeft + binIndex * binWidth;
        const x1 = chartLeft + (binIndex + 1) * binWidth;
        const drawWidth = Math.max(1, Math.ceil(x1 - x0));
        const x = Math.floor(x0);
        if (value <= 0) {
          if (gapShading && gapValue > 0) {
            ctx.fillStyle = columnsGapMergedColor(emptyBinRgb, gapValue);
            ctx.fillRect(x, rowY, drawWidth, drawHeight);
          }
          continue;
        }
        const baseRgb = columnsBarcodeRgb(clusterLabel, value);
        ctx.fillStyle = gapShading && gapValue > 0
          ? columnsGapMergedColor(baseRgb, gapValue)
          : rgbToCss(baseRgb);
        ctx.fillRect(x, rowY, drawWidth, drawHeight);
      }
      if (rowGap > 0) {
        ctx.fillStyle = "rgba(114, 98, 76, 0.13)";
        ctx.fillRect(0, rowY + drawHeight, chartRight, 1);
      }
    }
    drawColumnsHoverCell(ctx, state.columnsHoverCell);

    const startLabel = interfaceOnly ? interfaceColumns[0] : Math.round(range.start);
    const endLabel = interfaceOnly
      ? interfaceColumns[interfaceColumns.length - 1]
      : Math.max(0, Math.round(range.end - 1));
    const hiddenColumnCount = Math.max(
      0,
      Math.ceil(range.end) - Math.floor(range.start) - interfaceColumns.length
    );
    const modeLabel = interfaceOnly
      ? `interface-only axis hides ${hiddenColumnCount} empty columns`
      : "full MSA axis";
    const colorMeaning = gapShading
      ? "gap fraction darkens each bin"
      : "color luminance shows interacting fraction";
    setColumnsInfo(
      `${sourceTitle} barcodes: ${modeLabel}; ${colorMeaning} (${visibleClusters.length}/${state.columnsChart.clusters.length} ${sourcePlural} visible, columns ${startLabel}-${endLabel}).`
    );
  }

  function embeddingClusterByRowKey() {
    const source = state.embeddingClustering?.points || emptyClusteringPoints;
    if (
      embeddingAnnotationCache.clusteringPoints === source &&
      embeddingAnnotationCache.clusterByRowKey
    ) {
      return embeddingAnnotationCache.clusterByRowKey;
    }
    const clusterByRowKey = new Map(
      source.map((point) => [
        interactionRowKey(point.row_key, point.partner_domain),
        Number(point.cluster_label),
      ])
    );
    embeddingAnnotationCache.clusteringPoints = source;
    embeddingAnnotationCache.clusterByRowKey = clusterByRowKey;
    embeddingAnnotationCache.embeddingPoints = null;
    embeddingAnnotationCache.annotatedPoints = [];
    return clusterByRowKey;
  }

  function embeddingPointMembers(point) {
    const members = Array.isArray(point?.members) ? point.members : [];
    if (members.length > 0) {
      return members
        .map((member) => metricMemberFromPayload(member))
        .filter((member) => member.row_key && member.partner_domain);
    }
    const rowKey = String(point?.row_key || "");
    const partnerDomain = String(point?.partner_domain || "");
    return rowKey && partnerDomain
      ? [metricMemberFromPayload(point)]
      : [];
  }

  function embeddingPointMemberCount(point, members = null) {
    const explicitCount = Number(point?.member_count);
    if (Number.isFinite(explicitCount) && explicitCount > 0) {
      return explicitCount;
    }
    return Math.max(1, (members || embeddingPointMembers(point)).length);
  }

  function clusterLabelForEmbeddingPoint(memberKeys, clusterByRowKey) {
    const counts = new Map();
    for (const key of memberKeys) {
      if (!clusterByRowKey.has(key)) {
        continue;
      }
      const clusterLabel = Number(clusterByRowKey.get(key));
      counts.set(clusterLabel, (counts.get(clusterLabel) || 0) + 1);
    }
    if (counts.size === 0) {
      return null;
    }
    return [...counts.entries()]
      .sort((left, right) => right[1] - left[1] || Number(left[0]) - Number(right[0]))[0][0];
  }

  function annotatedEmbeddingPoints() {
    const embeddingPoints = state.embedding?.points || emptyEmbeddingPoints;
    const clusteringPoints = state.embeddingClustering?.points || emptyClusteringPoints;
    if (
      embeddingAnnotationCache.embeddingPoints === embeddingPoints &&
      embeddingAnnotationCache.clusteringPoints === clusteringPoints
    ) {
      return embeddingAnnotationCache.annotatedPoints;
    }
    const clusterByRowKey = embeddingClusterByRowKey();
    const annotatedPoints = embeddingPoints.map((point) => {
      const members = embeddingPointMembers(point);
      const memberKeys = members.map((member) => interactionRowKey(member.row_key, member.partner_domain));
      const representativeKey = interactionRowKey(point.row_key, point.partner_domain);
      const normalizedMemberKeys = memberKeys.includes(representativeKey)
        ? memberKeys
        : [representativeKey].concat(memberKeys);
      const domainSize = embeddingPointDomainSize(point, members);
      const pfamRowCoverage = embeddingPointPfamRowCoverage(point, members);
      return {
        ...point,
        members,
        memberCount: embeddingPointMemberCount(point, members),
        memberKeys: normalizedMemberKeys,
        interactionRowKey: representativeKey,
        domainSize,
        pfamRowCoverage,
        clusterLabel: clusterLabelForEmbeddingPoint(normalizedMemberKeys, clusterByRowKey),
      };
    });
    embeddingAnnotationCache.embeddingPoints = embeddingPoints;
    embeddingAnnotationCache.clusteringPoints = clusteringPoints;
    embeddingAnnotationCache.clusterByRowKey = clusterByRowKey;
    embeddingAnnotationCache.annotatedPoints = annotatedPoints;
    return annotatedPoints;
  }

  function applyEmbeddingPointJitter(projectedPoints) {
    const buckets = new Map();
    for (const point of projectedPoints) {
      const bucketKey = `${point.screenX.toFixed(3)}|${point.screenY.toFixed(3)}`;
      if (!buckets.has(bucketKey)) {
        buckets.set(bucketKey, []);
      }
      buckets.get(bucketKey).push(point);
    }
    for (const bucket of buckets.values()) {
      if (bucket.length < 2) {
        continue;
      }
      const sortedBucket = [...bucket].sort((left, right) =>
        String(left.group_id || left.interactionRowKey).localeCompare(
          String(right.group_id || right.interactionRowKey)
        )
      );
      const jitterRadius = Math.min(10, 2.8 + Math.log1p(sortedBucket.length) * 1.8);
      for (let index = 0; index < sortedBucket.length; index += 1) {
        const angle = (-Math.PI / 2) + (index * Math.PI * 2) / sortedBucket.length;
        sortedBucket[index].screenX += Math.cos(angle) * jitterRadius;
        sortedBucket[index].screenY += Math.sin(angle) * jitterRadius;
      }
    }
  }

  function embeddingPointSprite(color, radius, alpha) {
    const normalizedRadius = Math.max(1, Math.round(Number(radius || 1) * 2) / 2);
    const normalizedAlpha = Math.max(0.05, Math.min(1, Math.round(Number(alpha || 1) * 20) / 20));
    const key = `${color}|${normalizedRadius}|${normalizedAlpha}`;
    const cached = embeddingPointSpriteCache.get(key);
    if (cached) {
      return cached;
    }
    if (embeddingPointSpriteCache.size > 800) {
      embeddingPointSpriteCache.clear();
    }
    const padding = 3;
    const cssSize = Math.ceil((normalizedRadius + padding) * 2);
    const scale = 2;
    const canvas = document.createElement("canvas");
    canvas.width = cssSize * scale;
    canvas.height = cssSize * scale;
    const spriteCtx = canvas.getContext("2d");
    if (spriteCtx) {
      spriteCtx.setTransform(scale, 0, 0, scale, 0, 0);
      spriteCtx.clearRect(0, 0, cssSize, cssSize);
      spriteCtx.globalAlpha = normalizedAlpha;
      spriteCtx.fillStyle = color;
      spriteCtx.beginPath();
      spriteCtx.arc(cssSize / 2, cssSize / 2, normalizedRadius, 0, Math.PI * 2);
      spriteCtx.fill();
      spriteCtx.globalAlpha = 1;
    }
    const sprite = { canvas, size: cssSize };
    embeddingPointSpriteCache.set(key, sprite);
    return sprite;
  }

  function drawEmbeddingPointSprite(ctx, point, color, alpha) {
    const sprite = embeddingPointSprite(color, point.radius, alpha);
    const left = point.screenX - sprite.size / 2;
    const top = point.screenY - sprite.size / 2;
    ctx.drawImage(sprite.canvas, left, top, sprite.size, sprite.size);
  }

  function selectedEmbeddingMemberKey() {
    const selection = state.embeddingMemberSelection;
    const member = selection?.members?.[selection.index];
    if (!member) {
      return "";
    }
    return interactionRowKey(member.row_key, member.partner_domain);
  }

  function syncMemberControl(element, countElement, label, visible) {
    if (!element) {
      return;
    }
    element.classList.toggle("hidden", !visible);
    element.setAttribute("aria-hidden", visible ? "false" : "true");
    if (visible && countElement) {
      countElement.textContent = label;
    }
  }

  function syncEmbeddingMemberControls(projectedPoints = state.embeddingProjectedPoints || []) {
    const selection = state.embeddingMemberSelection;
    const members = Array.isArray(selection?.members) ? selection.members : [];
    const visible = members.length > 1 && Number.isInteger(selection?.index);
    if (!visible) {
      syncMemberControl(elements.embeddingMemberControls, elements.embeddingMemberCount, "", false);
      syncMemberControl(elements.structureMemberControls, elements.structureMemberCount, "", false);
      return;
    }

    const normalizedIndex = Math.max(0, Math.min(members.length - 1, Number(selection.index)));
    const label = `${normalizedIndex + 1} / ${members.length}`;
    const selectedKey = selectedEmbeddingMemberKey();
    const point = projectedPoints.find(
      (candidate) =>
        String(candidate.group_id || candidate.interactionRowKey) === String(selection.pointKey || "") ||
        (Array.isArray(candidate.memberKeys) && candidate.memberKeys.includes(selectedKey))
    );
    if (point && elements.embeddingMemberControls && elements.embeddingRoot) {
      const width = elements.embeddingRoot.clientWidth;
      const height = elements.embeddingRoot.clientHeight;
      const left = Math.max(58, Math.min(width - 58, point.screenX));
      const top = Math.max(48, Math.min(height - 18, point.screenY - point.radius - 12));
      elements.embeddingMemberControls.style.left = `${left}px`;
      elements.embeddingMemberControls.style.top = `${top}px`;
      syncMemberControl(elements.embeddingMemberControls, elements.embeddingMemberCount, label, true);
    } else {
      syncMemberControl(elements.embeddingMemberControls, elements.embeddingMemberCount, label, false);
    }
    syncMemberControl(elements.structureMemberControls, elements.structureMemberCount, label, true);
  }

  function renderEmbeddingPlot() {
    if (embeddingRenderFrameId) {
      window.cancelAnimationFrame(embeddingRenderFrameId);
      embeddingRenderFrameId = 0;
    }
    const ctx = elements.embeddingCanvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    const width = elements.embeddingRoot.clientWidth;
    const height = elements.embeddingRoot.clientHeight;
    if (!ctx || width <= 0 || height <= 0) {
      return;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#fffdf8";
    ctx.fillRect(0, 0, width, height);
    const centerX = width / 2;
    const centerY = height / 2;
    const scale = Math.min(width, height) * 0.34 * state.embeddingView.zoom;
    const embeddingPoints = state.embedding?.points || [];
    const colorMode = embeddingLegendMode();
    if (state.embedding?.error) {
      state.embeddingProjectedPoints = [];
      syncEmbeddingMemberControls([]);
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(state.embedding.error, centerX, centerY);
      setEmbeddingInfo(state.embedding.error);
      return;
    }
    if (embeddingPoints.length === 0) {
      state.embeddingProjectedPoints = [];
      syncEmbeddingMemberControls([]);
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      if (state.embeddingLoading) {
        ctx.fillText("Preparing embedding...", centerX, centerY);
        setEmbeddingInfo(
          `3D ${pointMethodLabel()} points on ${embeddingDistanceLabel()} input. Loading in the background.`
        );
      } else {
        ctx.fillText("Load an interface selection to view embeddings.", centerX, centerY);
        setEmbeddingInfo(
          `3D ${pointMethodLabel()} points on ${embeddingDistanceLabel()} input. Drag to rotate.`
        );
      }
      return;
    }
    if (colorMode === "cluster" && state.embeddingClustering?.error) {
      state.embeddingProjectedPoints = [];
      syncEmbeddingMemberControls([]);
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(state.embeddingClustering.error, centerX, centerY);
      setEmbeddingInfo(state.embeddingClustering.error);
      return;
    }
    if (colorMode === "cluster" && (state.embeddingClustering?.points || []).length === 0) {
      state.embeddingProjectedPoints = [];
      syncEmbeddingMemberControls([]);
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(
        state.embeddingClusteringLoading ? "Preparing clustering..." : "Load clustering to color by cluster.",
        centerX,
        centerY
      );
      setEmbeddingInfo(
        state.embeddingClusteringLoading
          ? `Computing ${clusteringMethodLabel(state.embeddingClusteringSettings.method)} clustering from the ${embeddingDistanceLabel(
              state.embeddingClusteringSettings.distance
            )} interface distance matrix.`
          : "Switch to cluster coloring after clustering has loaded."
      );
      return;
    }
    const annotatedPoints = annotatedEmbeddingPoints();
    const sizeBrackets = embeddingSizeBrackets(annotatedPoints);
    const coverageBrackets = embeddingCoverageBrackets(annotatedPoints);
    const clusterKeys = allEmbeddingClusterLabels();
    const partnerKeys = state.interface?.partnerDomains || [];
    const visibleClusters = visibleEmbeddingClusters();
    const visiblePartners = visibleEmbeddingPartners();
    const visibleSizeBracketKeys = new Set(visibleEmbeddingSizeBracketKeys(sizeBrackets));
    const visibleCoverageBracketKeys = new Set(visibleEmbeddingCoverageBracketKeys(coverageBrackets));
    const partnerFilterActive = partnerKeys.length > 0 && visiblePartners.length < partnerKeys.length;
    const clusterFilterActive =
      clusterKeys.length > 0 && !state.embeddingClustering?.error && visibleClusters.length < clusterKeys.length;
    const sizeFilterActive = sizeBrackets.length > 0 && visibleSizeBracketKeys.size < sizeBrackets.length;
    const coverageFilterActive =
      coverageBrackets.length > 0 && visibleCoverageBracketKeys.size < coverageBrackets.length;
    const filteredPoints = annotatedPoints.filter((point) => {
      if (partnerFilterActive && !state.embeddingVisiblePartners.has(point.partner_domain)) {
        return false;
      }
      if (clusterFilterActive && !state.embeddingVisibleClusters.has(String(point.clusterLabel))) {
        return false;
      }
      if (sizeFilterActive) {
        const bracketKey = sizeBracketForPoint(point, sizeBrackets)?.key;
        if (!bracketKey || !visibleSizeBracketKeys.has(bracketKey)) {
          return false;
        }
      }
      if (coverageFilterActive) {
        const bracketKey = coverageBracketForPoint(point, coverageBrackets)?.key;
        if (!bracketKey || !visibleCoverageBracketKeys.has(bracketKey)) {
          return false;
        }
      }
      return true;
    });
    if (filteredPoints.length === 0) {
      state.embeddingProjectedPoints = [];
      syncEmbeddingMemberControls([]);
      if (state.embeddingHoverRowKey !== null) {
        state.embeddingHoverRowKey = null;
      }
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      const emptyFilterMessage =
        partnerFilterActive && visiblePartners.length === 0
          ? "Select at least one partner in the Domains legend."
          : clusterFilterActive && visibleClusters.length === 0
            ? "Select at least one cluster in the Clusters legend."
            : sizeFilterActive && visibleSizeBracketKeys.size === 0
              ? "Select at least one size bracket in the Size legend."
              : coverageFilterActive && visibleCoverageBracketKeys.size === 0
                ? "Select at least one coverage bracket in the Coverage legend."
                : "No embedding points match the active legend filters.";
      ctx.fillText(emptyFilterMessage, centerX, centerY);
      setEmbeddingInfo(
        emptyFilterMessage === "No embedding points match the active legend filters."
          ? "The active domain, cluster, size, and coverage filters hide all points. Adjust legend filters to show points again."
          : emptyFilterMessage
      );
      return;
    }
    const projectedPoints = filteredPoints
      .map((point) => {
        const rotated = rotateEmbeddingPoint(point);
        const depthRatio = (rotated.z + 1) / 2;
        const memberRadius = Math.min(
          8,
          Math.log1p(Math.max(0, Number(point.memberCount || 1) - 1)) * 1.6
        );
        return {
          ...point,
          screenX: centerX + rotated.x * scale,
          screenY: centerY - rotated.y * scale,
          depth: rotated.z,
          radius: 4.2 + depthRatio * 2.2 + memberRadius,
          alpha: 0.58 + depthRatio * 0.34,
        };
      });
    applyEmbeddingPointJitter(projectedPoints);
    projectedPoints.sort((left, right) => left.depth - right.depth);
    state.embeddingProjectedPoints = projectedPoints;
    syncEmbeddingMemberControls(projectedPoints);
    ctx.textAlign = "center";
    for (const point of projectedPoints) {
      const color = embeddingPointColor(point, colorMode, sizeBrackets, coverageBrackets);
      const isSelected = point.memberKeys.includes(state.selectedRowKey);
      const isRepresentative = point.memberKeys.includes(state.representativeRowKey);
      const isHovered = point.memberKeys.includes(state.embeddingHoverRowKey);
      drawEmbeddingPointSprite(ctx, point, color, isHovered ? 1.0 : point.alpha);
      if (isSelected || isRepresentative || isHovered) {
        ctx.beginPath();
        ctx.lineWidth = isHovered ? 2.4 : 1.8;
        ctx.strokeStyle = isRepresentative ? "#d49a38" : "#2e261d";
        ctx.arc(point.screenX, point.screenY, point.radius + 2.4, 0, Math.PI * 2);
        ctx.stroke();
      }
    }
    const hoveredPoint =
      projectedPoints.find((point) => point.memberKeys.includes(state.embeddingHoverRowKey)) || null;
    if (hoveredPoint) {
      const compressionText =
        Number(hoveredPoint.memberCount || 1) > 1
          ? ` | compressed interfaces: ${hoveredPoint.memberCount}`
          : "";
      const sizeText = Number.isFinite(Number(hoveredPoint.domainSize)) && Number(hoveredPoint.domainSize) > 0
        ? ` | Domain A size: ${hoveredPoint.domainSize} residues`
        : "";
      const coverageText = Number.isFinite(Number(hoveredPoint.pfamRowCoverage)) && Number(hoveredPoint.pfamRowCoverage) >= 0
        ? ` | Domain A coverage: ${hoveredPoint.pfamRowCoverage}%`
        : "";
      const bracketText =
        colorMode === "size"
          ? ` | ${sizeBracketLabel(sizeBracketForPoint(hoveredPoint, sizeBrackets))}`
          : colorMode === "coverage"
            ? ` | ${coverageBracketLabel(coverageBracketForPoint(hoveredPoint, coverageBrackets))}`
            : "";
      setEmbeddingInfo(
        `${hoveredPoint.row_key} | ${hoveredPoint.partner_domain} | ${embeddingClusterLabel(
          hoveredPoint.clusterLabel
        )}${sizeText}${coverageText}${bracketText} | interface columns: ${hoveredPoint.interface_size}${compressionText}`
      );
    } else {
      const distanceLabel = embeddingDistanceLabel(
        state.embedding?.distance || state.embeddingSettings.distance
      );
      const clusteringDistanceLabel = embeddingDistanceLabel(
        state.embeddingClustering?.distance || state.embeddingClusteringSettings.distance
      );
      const clusteringMethod = clusteringMethodLabel(
        state.embeddingClustering?.clustering || state.embeddingClusteringSettings.method
      );
      const clusteringSummary =
        colorMode === "cluster" && state.embeddingClustering
          ? ` ${clusteringMethod} on ${clusteringDistanceLabel} distance: ${state.embeddingClustering.cluster_count} clusters, ${state.embeddingClustering.noise_count} noise points.`
          : "";
      const sizeSummary =
        colorMode === "size" && sizeBrackets.length > 0
          ? ` Domain A size coloring uses ${sizeBrackets.length} rainbow brackets.`
          : "";
      const coverageSummary =
        colorMode === "coverage" && coverageBrackets.length > 0
          ? ` Domain A coverage coloring uses ${coverageBrackets.length} rainbow brackets.`
          : "";
      const methodLabel = pointMethodLabel(state.embedding?.method || state.embeddingSettings.method);
      const representedCount = filteredPoints.reduce(
        (total, point) => total + Number(point.memberCount || 1),
        0
      );
      setEmbeddingInfo(
        `3D ${methodLabel} points on ${distanceLabel} input. ${filteredPoints.length} visible points representing ${representedCount} interface rows. Drag to rotate.${clusteringSummary}${sizeSummary}${coverageSummary}`
      );
    }
  }

  async function ensureEmbeddingDataLoaded() {
    if (!interfaceSelect.value) {
      state.embedding = null;
      state.embeddingLoading = false;
      state.embeddingLoadingKey = null;
      state.embeddingPromise = null;
      state.embeddingVisibleSizeBrackets = new Set();
      state.embeddingVisibleCoverageBrackets = new Set();
      syncEmbeddingLoadingUi();
      renderEmbeddingLegend();
      renderEmbeddingPlot();
      return;
    }
    const settingsKey = embeddingSettingsKey();
    const requestKey = currentEmbeddingRequestKey();
    if (
      state.embedding?.file === interfaceSelect.value &&
      state.embedding?.settingsKey === settingsKey &&
      !state.embedding?.error
    ) {
      state.embeddingLoading = false;
      state.embeddingLoadingKey = null;
      state.embeddingPromise = null;
      syncEmbeddingLoadingUi();
      renderEmbeddingPlot();
      return;
    }
    if (
      state.embeddingLoading &&
      state.embeddingLoadingKey === requestKey &&
      state.embeddingPromise
    ) {
      syncEmbeddingLoadingUi();
      renderEmbeddingPlot();
      return state.embeddingPromise;
    }
    const requestId = ++state.embeddingRequestId;
    state.embeddingLoading = true;
    state.embeddingLoadingKey = requestKey;
    syncEmbeddingLoadingUi();
    renderEmbeddingPlot();
    setEmbeddingInfo(
      `Loading 3D ${pointMethodLabel()} points (${embeddingDistanceLabel()} input)...`
    );
    state.embeddingPromise = (async () => {
      try {
        const payload = await fetchJson(currentEmbeddingQuery());
        if (requestId !== state.embeddingRequestId) {
          return;
        }
        const resolvedPerplexity = Number.isFinite(Number(payload.perplexity))
          ? Number(payload.perplexity)
          : state.embeddingSettings.perplexity;
        state.embeddingSettings = {
          ...state.embeddingSettings,
          method: payload.method || state.embeddingSettings.method,
          distance: payload.distance || state.embeddingSettings.distance,
          perplexity: resolvedPerplexity,
        };
        state.embeddingSettingsDraft = {
          ...state.embeddingSettings,
        };
        state.embedding = {
          ...payload,
          settingsKey,
        };
        resetEmbeddingMetricSelections();
        syncEmbeddingSettingsUi();
      } catch (error) {
        if (requestId !== state.embeddingRequestId) {
          return;
        }
        state.embedding = {
          error: error.message,
          points: [],
          settingsKey,
        };
      } finally {
        if (requestId === state.embeddingRequestId) {
          state.embeddingLoading = false;
          state.embeddingLoadingKey = null;
          state.embeddingPromise = null;
          syncEmbeddingLoadingUi();
          renderEmbeddingLegend();
          renderEmbeddingPlot();
        }
      }
    })();
    return state.embeddingPromise;
  }

  async function ensureHierarchyStatusLoaded(options = {}) {
    if (!interfaceSelect.value || state.embeddingClusteringSettingsDraft.method !== "hierarchical") {
      state.hierarchyStatus = null;
      state.hierarchyStatusLoadingKey = null;
      state.hierarchyStatusPromise = null;
      syncHierarchyWarningUi();
      return;
    }
    const requestKey = currentHierarchyStatusRequestKey();
    if (state.hierarchyStatus?.requestKey !== requestKey) {
      state.hierarchyStatus = null;
      syncHierarchyWarningUi();
    }
    if (
      !options.force &&
      state.hierarchyStatus?.requestKey === requestKey &&
      !state.hierarchyStatus?.error
    ) {
      syncHierarchyWarningUi();
      return;
    }
    if (
      state.hierarchyStatusLoadingKey === requestKey &&
      state.hierarchyStatusPromise
    ) {
      syncHierarchyWarningUi();
      return state.hierarchyStatusPromise;
    }
    const requestId = ++state.hierarchyStatusRequestId;
    state.hierarchyStatusLoadingKey = requestKey;
    state.hierarchyStatusPromise = (async () => {
      try {
        const payload = await fetchJson(currentHierarchyStatusQuery());
        if (requestId !== state.hierarchyStatusRequestId) {
          return;
        }
        state.hierarchyStatus = {
          ...payload,
          requestKey,
        };
      } catch (error) {
        if (requestId !== state.hierarchyStatusRequestId) {
          return;
        }
        state.hierarchyStatus = {
          error: error.message,
          requestKey,
          local_calculation_required: false,
        };
      } finally {
        if (requestId === state.hierarchyStatusRequestId) {
          state.hierarchyStatusLoadingKey = null;
          state.hierarchyStatusPromise = null;
          syncHierarchyWarningUi();
        }
      }
    })();
    return state.hierarchyStatusPromise;
  }

  function applyHierarchyStatusFromClusteringPayload(payload) {
    if (state.embeddingClusteringSettings.method !== "hierarchical" || !payload) {
      return;
    }
    const requestKey = currentHierarchyStatusRequestKey();
    state.hierarchyStatusRequestId += 1;
    state.hierarchyStatusLoadingKey = null;
    state.hierarchyStatusPromise = null;
    state.hierarchyStatus = {
      method: "hierarchical",
      distance: payload.distance || state.embeddingClusteringSettings.distance,
      linkage: state.embeddingClusteringSettings.linkage,
      source: payload.hierarchy_source || "clustering",
      local_calculation_required: false,
      interface_count: payload.sample_count,
      leaf_count: payload.hierarchy_leaf_count,
      requestKey,
    };
    syncHierarchyWarningUi();
  }

  async function ensureEmbeddingClusteringLoaded() {
    if (!interfaceSelect.value) {
      state.embeddingClustering = null;
      state.embeddingClusteringLoading = false;
      state.embeddingClusteringLoadingKey = null;
      state.embeddingClusteringPromise = null;
      state.columnsChart = null;
      state.columnsChartKey = null;
      state.columnsChartLoading = false;
      state.columnsChartLoadingKey = null;
      state.columnsChartErrorKey = null;
      state.columnsChartPromise = null;
      state.columnsView = { start: 0, end: null };
      state.columnsInterfaceView = { start: 0, end: null };
      state.columnsVisibleClusters = new Set();
      state.columnsClusterOrder = [];
      state.columnsDrag = null;
      state.columnsHoverCell = null;
      state.columnsInteractionLayout = null;
      state.representativeClusterSummaries = null;
      state.representativeVisibleClusters = new Set();
      state.representativeHoveredClusterLabel = null;
      syncRepresentativeScopeControls();
      syncEmbeddingLoadingUi();
      renderEmbeddingLegend();
      renderEmbeddingPlot();
      renderColumnsClusterLegend();
      renderColumnsChart();
      renderRepresentativeClusterLegend();
      return;
    }
    const settingsKey = embeddingClusteringSettingsKey();
    const requestKey = currentEmbeddingClusteringRequestKey();
    if (
      state.embeddingClustering?.file === interfaceSelect.value &&
      state.embeddingClustering?.settingsKey === settingsKey
    ) {
      state.embeddingClusteringLoading = false;
      state.embeddingClusteringLoadingKey = null;
      state.embeddingClusteringPromise = null;
      syncEmbeddingLoadingUi();
      renderEmbeddingLegend();
      renderEmbeddingPlot();
      renderColumnsClusterLegend();
      renderColumnsChart();
      syncRepresentativeScopeControls();
      return;
    }
    if (
      state.embeddingClusteringLoading &&
      state.embeddingClusteringLoadingKey === requestKey &&
      state.embeddingClusteringPromise
    ) {
      syncEmbeddingLoadingUi();
      renderEmbeddingLegend();
      renderEmbeddingPlot();
      syncRepresentativeScopeControls();
      return state.embeddingClusteringPromise;
    }
    const requestId = ++state.embeddingClusteringRequestId;
    state.embeddingClusteringLoading = true;
    state.embeddingClusteringLoadingKey = requestKey;
    state.representativeClusterSummaries = null;
    state.representativeVisibleClusters = new Set();
    state.representativeHoveredClusterLabel = null;
    syncRepresentativeScopeControls();
    syncEmbeddingLoadingUi();
    renderEmbeddingLegend();
    renderEmbeddingPlot();
    renderRepresentativeClusterLegend();
    setEmbeddingInfo(hierarchyLoadingInfoMessage());
    state.embeddingClusteringPromise = (async () => {
      try {
        const payload = await fetchJson(currentEmbeddingClusteringQuery());
        if (requestId !== state.embeddingClusteringRequestId) {
          return;
        }
        state.embeddingClustering = {
          ...payload,
          settingsKey,
        };
        state.embeddingClusteringSettings = {
          ...state.embeddingClusteringSettings,
          distance: payload.distance || state.embeddingClusteringSettings.distance,
        };
        applyHierarchyStatusFromClusteringPayload(payload);
        if (!state.embeddingSettingsOpen) {
          state.embeddingClusteringSettingsDraft = {
            ...state.embeddingClusteringSettings,
          };
        }
        state.columnsChart = null;
        state.columnsChartKey = null;
        state.columnsChartLoading = false;
        state.columnsChartLoadingKey = null;
        state.columnsChartErrorKey = null;
        state.columnsChartPromise = null;
        state.columnsView = { start: 0, end: null };
        state.columnsInterfaceView = { start: 0, end: null };
        state.columnsDrag = null;
        state.columnsHoverCell = null;
        state.columnsInteractionLayout = null;
        resetEmbeddingClusterSelection();
        resetColumnsClusterSelection();
        state.representativeClusterSummaries = null;
        resetRepresentativeClusterSelection();
        state.representativeHoveredClusterLabel = null;
      } catch (error) {
        if (requestId !== state.embeddingClusteringRequestId) {
          return;
        }
        state.embeddingClustering = {
          file: interfaceSelect.value,
          error: error.message,
          points: [],
          settingsKey,
        };
        state.columnsChart = null;
        state.columnsChartKey = null;
        state.columnsChartLoading = false;
        state.columnsChartLoadingKey = null;
        state.columnsChartErrorKey = null;
        state.columnsChartPromise = null;
        state.columnsView = { start: 0, end: null };
        state.columnsInterfaceView = { start: 0, end: null };
        state.columnsVisibleClusters = new Set();
        state.columnsClusterOrder = [];
        state.columnsDrag = null;
        state.columnsHoverCell = null;
        state.columnsInteractionLayout = null;
        state.representativeClusterSummaries = null;
        state.representativeVisibleClusters = new Set();
        state.representativeHoveredClusterLabel = null;
      } finally {
        if (requestId === state.embeddingClusteringRequestId) {
          state.embeddingClusteringLoading = false;
          state.embeddingClusteringLoadingKey = null;
          state.embeddingClusteringPromise = null;
          syncEmbeddingLoadingUi();
          renderEmbeddingLegend();
          renderEmbeddingPlot();
          renderColumnsClusterLegend();
          renderColumnsChart();
          syncRepresentativeScopeControls();
          renderRepresentativeClusterLegend();
          if (state.representativeStructure && representativeLens() === "cluster") {
            void renderRepresentativeStructure();
          }
          if (
            state.embeddingSettingsOpen &&
            state.hierarchyStatus?.requestKey !== currentHierarchyStatusRequestKey()
          ) {
            void ensureHierarchyStatusLoaded({ force: true });
          }
        }
      }
    })();
    return state.embeddingClusteringPromise;
  }

  function embeddingPointAt(clientX, clientY) {
    const rect = elements.embeddingCanvas.getBoundingClientRect();
    const canvasX = clientX - rect.left;
    const canvasY = clientY - rect.top;
    for (let index = state.embeddingProjectedPoints.length - 1; index >= 0; index -= 1) {
      const point = state.embeddingProjectedPoints[index];
      const dx = canvasX - point.screenX;
      const dy = canvasY - point.screenY;
      if (Math.hypot(dx, dy) <= point.radius + 4) {
        return point;
      }
    }
    return null;
  }

  return {
    allColumnsClusterLabels,
    allEmbeddingClusterLabels,
    allEmbeddingCoverageBracketKeys,
    allEmbeddingSizeBracketKeys,
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
    handleColumnsDoubleClick,
    handleColumnsPointerLeave,
    handleColumnsPointerDown,
    handleColumnsPointerMove,
    handleColumnsPointerUp,
    handleColumnsScroll,
    handleColumnsWheel,
    parseEmbeddingClusteringSettingsDraft,
    parseEmbeddingSettingsDraft,
    readEmbeddingClusteringDraftInputs,
    renderColumnsChart,
    renderColumnsClusterLegend,
    renderEmbeddingLegend,
    renderEmbeddingPlot,
    requestEmbeddingRender,
    resetColumnsClusterSelection,
    resetEmbeddingClusterSelection,
    resetEmbeddingMetricSelections,
    resetEmbeddingPartnerSelection,
    resetRepresentativeClusterSelection,
    resizeColumnsCanvas,
    resizeEmbeddingCanvas,
    syncEmbeddingLoadingUi,
    syncEmbeddingSettingsUi,
    syncHierarchyWarningUi,
    syncDistanceThresholdValueUi,
    syncPersistenceMinLifetimeValueUi,
    syncPersistenceStabilityWeightValueUi,
    syncHierarchicalTargetMemoryFromDraft,
    syncHierarchicalTargetUi,
    normalizeHierarchicalDraft,
    allRepresentativeClusterLabels,
    visibleColumnsClusters,
    visibleRepresentativeClusters,
    clusteringMethodLabel,
    setEmbeddingInfo,
    setColumnsInfo,
    syncEmbeddingMemberControls,
  };
}
