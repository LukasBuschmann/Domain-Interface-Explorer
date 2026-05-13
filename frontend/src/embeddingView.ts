// @ts-nocheck
import {
  CLUSTER_COLOR_PALETTE,
  DEFAULT_CLUSTERING_SETTINGS,
  DEFAULT_EMBEDDING_SETTINGS,
} from "./constants.js";
import { fetchJson } from "./api.js";
import { interactionRowKey } from "./interfaceModel.js";
import {
  appendSelectionSettingsToParams,
  selectionSettingsKey,
} from "./selectionSettings.js";

export function createEmbeddingViewController({
  state,
  elements,
  interfaceSelect,
  partnerColor,
  renderRepresentativeClusterLegend,
  renderRepresentativeStructure,
  syncRepresentativeScopeControls = () => {},
  representativeLens,
}) {
  let columnsRenderFrameId = 0;
  let embeddingRenderFrameId = 0;
  const emptyEmbeddingPoints = [];
  const emptyClusteringPoints = [];
  const embeddingAnnotationCache = {
    embeddingPoints: null,
    clusteringPoints: null,
    clusterByRowKey: null,
    annotatedPoints: [],
  };
  const embeddingPointSpriteCache = new Map();

  function embeddingSettingsKey(settings = state.embeddingSettings) {
    return JSON.stringify(settings);
  }

  function embeddingClusteringSettingsKey(settings = state.embeddingClusteringSettings) {
    return JSON.stringify(settings);
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
    ].join("|");
  }

  function syncEmbeddingLoadingUi() {
    const showEmbeddingLoading = state.embeddingLoading;
    const showClusteringLoading = state.embeddingClusteringLoading;
    const isVisible = showEmbeddingLoading || showClusteringLoading;
    elements.embeddingLoading.classList.toggle("hidden", !isVisible);
    elements.embeddingLoadingLabel.textContent = showClusteringLoading
      ? "Loading clustering..."
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
      state.hierarchyStatus?.requestKey === currentRequestKey &&
      Boolean(state.hierarchyStatus?.local_calculation_required);
    const message = showWarning ? hierarchyWarningMessage() : "";
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

  function readEmbeddingClusteringDraftInputs() {
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
    return clusterKeys.filter((clusterKey) => state.representativeVisibleClusters.has(clusterKey));
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

  function columnsClusterShortLabel(clusterLabel) {
    if (clusterLabel === null || clusterLabel === undefined || Number.isNaN(Number(clusterLabel))) {
      return "?";
    }
    return Number(clusterLabel) < 0 ? "N" : `C${Number(clusterLabel) + 1}`;
  }

  function embeddingClusterColor(clusterLabel) {
    const numericLabel = Number(clusterLabel);
    if (numericLabel < 0) {
      return "#8a847a";
    }
    return CLUSTER_COLOR_PALETTE[numericLabel % CLUSTER_COLOR_PALETTE.length];
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

  function columnsBarcodeColor(clusterLabel, fraction) {
    const clamped = Math.max(0, Math.min(1, Number(fraction) || 0));
    const base = colorToRgb(embeddingClusterColor(clusterLabel));
    const strength = Math.sqrt(clamped);
    const low = base.map((channel) => Math.round(channel * 0.42));
    const high = base.map((channel) => Math.round(channel + (255 - channel) * 0.34));
    const channels = low.map((channel, index) =>
      Math.round(channel + (high[index] - channel) * strength)
    );
    return `rgb(${channels[0]}, ${channels[1]}, ${channels[2]})`;
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
    if (partners.length === 0) {
      elements.embeddingPartnerLegend.innerHTML = "";
      return;
    }
    const modeControls = `
      <div class="embedding-legend-header">
        <div class="embedding-legend-mode" role="tablist" aria-label="Embedding color mode">
          <button type="button" class="embedding-legend-mode-button ${colorMode === "domain" ? "active" : ""}" data-legend-mode="domain" aria-pressed="${colorMode === "domain"}">Domains</button>
          <button type="button" class="embedding-legend-mode-button ${colorMode === "cluster" ? "active" : ""}" data-legend-mode="cluster" aria-pressed="${colorMode === "cluster"}">Clusters</button>
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

  function columnsChartCacheKey() {
    const serverChart = state.embeddingClustering?.columns_chart;
    return [
      interfaceSelect.value || "",
      state.embeddingClustering?.settingsKey || "",
      "server",
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
    for (const clusterLabel of clusters) {
      const values = rawRelativeByCluster[clusterLabel];
      relativeByCluster[clusterLabel] = Array.isArray(values) ? values : [];
    }
    return {
      file: interfaceSelect.value,
      alignmentLength,
      clusters,
      clusterSizes,
      relativeByCluster,
      maxStackValue: Number(chart.maxStackValue || 0),
      source: "server",
    };
  }

  function rebuildColumnsChartIfNeeded() {
    const nextKey = columnsChartCacheKey();
    if (state.columnsChartKey === nextKey) {
      return;
    }

    const serverChart = normalizedServerColumnsChart();
    if (!serverChart) {
      state.columnsChart = null;
      state.columnsChartKey = nextKey;
      state.columnsView = { start: 0, end: null };
      state.columnsVisibleClusters = new Set();
      state.columnsClusterOrder = [];
      state.columnsDrag = null;
      state.columnsInteractionLayout = null;
      return;
    }

    state.columnsChart = serverChart;
    state.columnsChartKey = nextKey;
    state.columnsView = {
      start: 0,
      end: Math.max(1, Number(serverChart.alignmentLength || 1)),
    };
    const clusterKeys = serverChart.clusters.map((clusterLabel) => String(clusterLabel));
    const clusterSet = new Set(clusterKeys);
    const ordered = Array.isArray(state.columnsClusterOrder)
      ? state.columnsClusterOrder.filter((clusterLabel) => clusterSet.has(String(clusterLabel))).map(String)
      : [];
    const orderedSet = new Set(ordered);
    state.columnsClusterOrder = [
      ...ordered,
      ...clusterKeys.filter((clusterLabel) => !orderedSet.has(clusterLabel)),
    ];
    const stillVisible = serverChart.clusters.filter((clusterLabel) =>
      state.columnsVisibleClusters.has(String(clusterLabel))
    );
    state.columnsVisibleClusters = new Set(
      stillVisible.length > 0 ? stillVisible : serverChart.clusters
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

  function columnsRangeHasInterface(range, visibleClusters, alignmentLength) {
    return visibleInterfaceColumns(visibleClusters, range, alignmentLength).length > 0;
  }

  function nearestInterfaceColumn(targetColumn, visibleClusters, alignmentLength) {
    let bestColumn = null;
    let bestDistance = Number.POSITIVE_INFINITY;
    const target = Number(targetColumn) || 0;
    for (let columnIndex = 0; columnIndex < alignmentLength; columnIndex += 1) {
      if (!columnHasVisibleInteraction(columnIndex, visibleClusters)) {
        continue;
      }
      const distance = Math.abs(columnIndex - target);
      if (distance < bestDistance) {
        bestColumn = columnIndex;
        bestDistance = distance;
      }
    }
    return bestColumn;
  }

  function rangeCenteredOnColumn(columnIndex, span, alignmentLength) {
    const nextSpan = Math.max(1, Math.min(alignmentLength, Number(span) || 1));
    let start = Number(columnIndex) - nextSpan / 2;
    start = Math.max(0, Math.min(alignmentLength - nextSpan, start));
    return normalizedColumnsRange(start, start + nextSpan, alignmentLength);
  }

  function setColumnsViewRange(start, end, alignmentLength, visibleClusters = null) {
    const range = normalizedColumnsRange(start, end, alignmentLength);
    if (
      state.columnsInterfaceOnly &&
      visibleClusters?.length &&
      !columnsRangeHasInterface(range, visibleClusters, alignmentLength)
    ) {
      return false;
    }
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

  function syncColumnsScrollSpace(alignmentLength, range, contentHeight) {
    const scrollElement = elements.columnsScroll;
    const spacer = elements.columnsSpacer;
    if (!scrollElement || !spacer) {
      return;
    }
    const viewportWidth = Math.max(1, scrollElement.clientWidth);
    const viewportHeight = Math.max(1, scrollElement.clientHeight);
    const virtualWidth = columnsVirtualWidth(viewportWidth, alignmentLength, range.span);
    const virtualHeight = Math.max(viewportHeight, Math.ceil(contentHeight));
    spacer.style.width = `${Math.ceil(virtualWidth)}px`;
    spacer.style.height = `${virtualHeight}px`;
    const targetScrollLeft = columnsScrollLeftForRange(
      range,
      viewportWidth,
      virtualWidth,
      alignmentLength
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

  function panColumnsByPixels(pixelDelta, chartWidth, alignmentLength, visibleClusters = null) {
    const range = columnsVisibleRange(alignmentLength);
    const deltaColumns = (Number(pixelDelta) / Math.max(1, chartWidth)) * range.span;
    let nextStart = range.start + deltaColumns;
    nextStart = Math.max(0, Math.min(alignmentLength - range.span, nextStart));
    return setColumnsViewRange(nextStart, nextStart + range.span, alignmentLength, visibleClusters);
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

  function handleColumnsPointerDown(event) {
    if (event.button !== 0 || !state.columnsChart?.alignmentLength) {
      return;
    }
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

  function handleColumnsWheel(event) {
    if (!state.columnsChart?.alignmentLength) {
      return;
    }
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
    const range = columnsVisibleRange(alignmentLength);
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
    const minSpan = Math.min(alignmentLength, 8);
    const nextSpan = Math.max(minSpan, Math.min(alignmentLength, range.span * zoomFactor));
    const anchorColumn = range.start + pointerRatio * range.span;
    let nextStart = anchorColumn - pointerRatio * nextSpan;
    nextStart = Math.max(0, Math.min(alignmentLength - nextSpan, nextStart));
    if (setColumnsViewRange(nextStart, nextStart + nextSpan, alignmentLength, visibleClusters)) {
      requestColumnsRenderNextFrame();
    }
  }

  function handleColumnsScroll() {
    const scrollElement = elements.columnsScroll;
    if (!scrollElement || !state.columnsChart?.alignmentLength) {
      requestColumnsRenderNextFrame();
      return;
    }
    const alignmentLength = Math.max(1, Number(state.columnsChart.alignmentLength || 1));
    const range = columnsVisibleRange(alignmentLength);
    const viewportWidth = Math.max(1, scrollElement.clientWidth);
    const virtualWidth = columnsVirtualWidth(viewportWidth, alignmentLength, range.span);
    const nextStart = columnsRangeStartForScroll(
      scrollElement.scrollLeft,
      viewportWidth,
      virtualWidth,
      alignmentLength,
      range.span
    );
    const visibleClusters = visibleColumnsClusters();
    if (!setColumnsViewRange(nextStart, nextStart + range.span, alignmentLength, visibleClusters)) {
      const nearestColumn = nearestInterfaceColumn(
        nextStart + range.span / 2,
        visibleClusters,
        alignmentLength
      );
      if (nearestColumn !== null) {
        const nextRange = rangeCenteredOnColumn(nearestColumn, range.span, alignmentLength);
        state.columnsView = {
          start: nextRange.start,
          end: nextRange.end,
        };
        const targetScrollLeft = columnsScrollLeftForRange(
          nextRange,
          viewportWidth,
          virtualWidth,
          alignmentLength
        );
        if (Math.abs(scrollElement.scrollLeft - targetScrollLeft) > 1) {
          scrollElement.scrollLeft = targetScrollLeft;
        }
      }
    }
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

    rebuildColumnsChartIfNeeded();
    renderColumnsClusterLegend();
    elements.columnsLoading.classList.toggle(
      "hidden",
      !(state.embeddingClusteringLoading && !(state.columnsChart?.clusters || []).length)
    );
    elements.columnsLoadingLabel.textContent = "Loading clustering data...";

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
      setColumnsInfo("Cluster-column interaction barcode.");
      return;
    }
    if (state.embeddingClustering?.error) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(state.embeddingClustering.error, centerX, centerY);
      setColumnsInfo(state.embeddingClustering.error);
      return;
    }
    if (
      state.embeddingClustering &&
      !state.embeddingClusteringLoading &&
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
        state.embeddingClusteringLoading
          ? "Preparing clustering..."
          : "Load clustering to inspect cluster-column interactions.",
        centerX,
        centerY
      );
      setColumnsInfo(
        "Shows for each MSA column the fraction of each cluster that interacts at that position."
      );
      return;
    }

    const visibleClusters = visibleColumnsClusters();
    if (visibleClusters.length === 0) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Select at least one cluster in the legend.", centerX, centerY);
      setColumnsInfo("Cluster filter hides all columns.");
      return;
    }

    if (elements.columnsInterfaceOnlyToggle) {
      elements.columnsInterfaceOnlyToggle.checked = Boolean(state.columnsInterfaceOnly);
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
    if (chartWidth < 20 || chartHeight < 20) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Columns view is too small.", centerX, centerY);
      setColumnsInfo("Expand the panel to render the cluster-column barcode.");
      return;
    }

    const alignmentLength = Math.max(1, Number(state.columnsChart.alignmentLength || 1));
    const range = columnsVisibleRange(alignmentLength);
    syncColumnsScrollSpace(alignmentLength, range, contentHeight);
    if (
      Math.round(scrollElement.clientWidth || width) !== width ||
      Math.round(scrollElement.clientHeight || height) !== height
    ) {
      resizeColumnsCanvas();
      requestColumnsRenderNextFrame();
      return;
    }
    const interfaceOnly = Boolean(state.columnsInterfaceOnly);
    const interfaceColumns = interfaceOnly
      ? visibleInterfaceColumns(visibleClusters, range, alignmentLength)
      : [];
    const visibleColumnCount = interfaceOnly
      ? interfaceColumns.length
      : Math.max(1, Math.ceil(range.end) - Math.floor(range.start));
    if (interfaceOnly && visibleColumnCount === 0) {
      const nearestColumn = nearestInterfaceColumn(
        (range.start + range.end) / 2,
        visibleClusters,
        alignmentLength
      );
      if (nearestColumn !== null) {
        const nextRange = rangeCenteredOnColumn(nearestColumn, range.span, alignmentLength);
        state.columnsView = {
          start: nextRange.start,
          end: nextRange.end,
        };
        requestColumnsRenderNextFrame();
        return;
      }
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("No interface columns in the current range.", centerX, centerY);
      setColumnsInfo("Interface-only mode hides columns without interactions.");
      return;
    }
    const binCount = Math.max(1, Math.min(visibleColumnCount, Math.floor(chartWidth)));
    const binWidth = chartWidth / binCount;
    const binnedValues = new Map(visibleClusters.map((clusterLabel) => [clusterLabel, new Float64Array(binCount)]));
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
          let sum = 0;
          for (let ordinal = startOrdinal; ordinal < endOrdinal; ordinal += 1) {
            sum += Number(values[interfaceColumns[ordinal]] || 0);
          }
          const averageValue = sum / Math.max(1, endOrdinal - startOrdinal);
          binnedValues.get(clusterLabel)[binIndex] = averageValue;
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
          let sum = 0;
          for (let columnIndex = startColumn; columnIndex < endColumn; columnIndex += 1) {
            sum += Number(values[columnIndex] || 0);
          }
          const averageValue = sum / Math.max(1, endColumn - startColumn);
          binnedValues.get(clusterLabel)[binIndex] = averageValue;
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
      ctx.fillText("No interface residues found for selected clusters.", centerX, centerY);
      setColumnsInfo("Selected clusters have no interacting columns.");
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
      ctx.fillStyle = "#eadfcf";
      ctx.fillRect(chartLeft, rowY, chartWidth, drawHeight);
      if (rowHeight >= 8) {
        ctx.fillStyle = "#b0a491";
        ctx.font = '10px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "left";
        ctx.textBaseline = "middle";
        ctx.fillText("::", 7, y + rowHeight / 2);
        ctx.fillText("::", 13, y + rowHeight / 2);
        ctx.fillStyle = embeddingClusterColor(clusterLabel);
        ctx.fillRect(chartLeft - 34, y + Math.max(1, (rowHeight - 9) / 2), 9, Math.min(9, rowHeight - 2));
        ctx.fillStyle = "#6f6658";
        ctx.font = '10px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        ctx.fillText(columnsClusterShortLabel(clusterLabel), chartLeft - 7, y + rowHeight / 2);
      } else {
        ctx.fillStyle = embeddingClusterColor(clusterLabel);
        ctx.fillRect(chartLeft - 10, rowY, 5, drawHeight);
      }
      const values = binnedValues.get(clusterLabel);
      for (let binIndex = 0; binIndex < binCount; binIndex += 1) {
        const value = values[binIndex];
        if (value <= 0) {
          continue;
        }
        const x0 = chartLeft + binIndex * binWidth;
        const x1 = chartLeft + (binIndex + 1) * binWidth;
        const drawWidth = Math.max(1, Math.ceil(x1 - x0));
        const x = Math.floor(x0);
        ctx.fillStyle = columnsBarcodeColor(clusterLabel, value);
        ctx.fillRect(x, rowY, drawWidth, drawHeight);
      }
      if (rowGap > 0) {
        ctx.fillStyle = "rgba(114, 98, 76, 0.13)";
        ctx.fillRect(0, rowY + drawHeight, chartRight, 1);
      }
    }

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
    setColumnsInfo(
      `Cluster barcodes: ${modeLabel}; color luminance shows interacting fraction (${visibleClusters.length}/${state.columnsChart.clusters.length} clusters visible, columns ${startLabel}-${endLabel}).`
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
      return {
        ...point,
        members,
        memberCount: embeddingPointMemberCount(point, members),
        memberKeys: normalizedMemberKeys,
        interactionRowKey: representativeKey,
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
    const visibleClusters = visibleEmbeddingClusters();
    const visiblePartners = visibleEmbeddingPartners();
    const filteredPoints =
      colorMode === "cluster"
        ? annotatedPoints.filter((point) => visibleClusters.includes(String(point.clusterLabel)))
        : annotatedPoints.filter((point) => visiblePartners.includes(point.partner_domain));
    if (filteredPoints.length === 0) {
      state.embeddingProjectedPoints = [];
      syncEmbeddingMemberControls([]);
      if (state.embeddingHoverRowKey !== null) {
        state.embeddingHoverRowKey = null;
      }
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(
        colorMode === "cluster"
          ? "Select at least one cluster in the legend."
          : "Select at least one partner in the legend.",
        centerX,
        centerY
      );
      setEmbeddingInfo(
        colorMode === "cluster"
          ? "Clustering filter hides all clusters. Click legend items to show them again."
          : "Embedding filter hides all partners. Click legend items to show them again."
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
      const color =
        colorMode === "cluster"
          ? embeddingClusterColor(point.clusterLabel)
          : partnerColor(point.partner_domain);
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
      setEmbeddingInfo(
        `${hoveredPoint.row_key} | ${hoveredPoint.partner_domain} | ${embeddingClusterLabel(
          hoveredPoint.clusterLabel
        )} | interface columns: ${hoveredPoint.interface_size}${compressionText}`
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
      const methodLabel = pointMethodLabel(state.embedding?.method || state.embeddingSettings.method);
      const representedCount = filteredPoints.reduce(
        (total, point) => total + Number(point.memberCount || 1),
        0
      );
      setEmbeddingInfo(
        `3D ${methodLabel} points on ${distanceLabel} input. ${filteredPoints.length} visible points representing ${representedCount} interface rows. Drag to rotate.${clusteringSummary}`
      );
    }
  }

  async function ensureEmbeddingDataLoaded() {
    if (!interfaceSelect.value) {
      state.embedding = null;
      state.embeddingLoading = false;
      state.embeddingLoadingKey = null;
      state.embeddingPromise = null;
      syncEmbeddingLoadingUi();
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
      state.columnsView = { start: 0, end: null };
      state.columnsVisibleClusters = new Set();
      state.columnsClusterOrder = [];
      state.columnsDrag = null;
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
    setEmbeddingInfo(
      `Loading ${clusteringMethodLabel(state.embeddingClusteringSettings.method)} clustering (${embeddingDistanceLabel(state.embeddingClusteringSettings.distance)} distance)...`
    );
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
        state.columnsView = { start: 0, end: null };
        state.columnsDrag = null;
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
        state.columnsView = { start: 0, end: null };
        state.columnsVisibleClusters = new Set();
        state.columnsClusterOrder = [];
        state.columnsDrag = null;
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
