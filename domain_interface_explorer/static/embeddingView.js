import { CLUSTER_COLOR_PALETTE, DEFAULT_CLUSTERING_SETTINGS } from "./constants.js";
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
  representativeLens,
}) {
  let distanceRenderFrameId = 0;
  let columnsRenderFrameId = 0;

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
    const distanceThresholdValue = String(settings?.distanceThreshold ?? "").trim();
    const nClustersValue = String(settings?.nClusters ?? "").trim();
    return distanceThresholdValue !== "" && nClustersValue === "" ? "distance_threshold" : "n_clusters";
  }

  function currentEmbeddingQuery() {
    const params = new URLSearchParams({
      file: interfaceSelect.value,
      distance: String(state.embeddingSettings.distance),
      learning_rate: String(state.embeddingSettings.learningRate),
      max_iter: String(state.embeddingSettings.maxIter),
      early_exaggeration: String(state.embeddingSettings.earlyExaggeration),
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

  function currentDistanceMatrixQuery() {
    const params = new URLSearchParams({
      file: interfaceSelect.value,
      distance: String(state.embeddingSettings.distance),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    return `/api/distance-matrix?${params.toString()}`;
  }

  function currentDistanceMatrixRequestKey() {
    return `${interfaceSelect.value}|${selectionSettingsKey(state.selectionSettings)}|${state.embeddingSettings.distance}`;
  }

  function currentEmbeddingClusteringQuery() {
    const params = new URLSearchParams({
      file: interfaceSelect.value,
      method: String(state.embeddingClusteringSettings.method),
      distance: String(state.embeddingClusteringSettings.distance),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (state.embeddingClusteringSettings.method === "hierarchical") {
      const hierarchicalTarget = currentHierarchicalTarget(state.embeddingClusteringSettings);
      params.set("linkage", String(state.embeddingClusteringSettings.linkage));
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
        params.set(
          "distance_threshold",
          String(state.embeddingClusteringSettings.distanceThreshold)
        );
        params.set(
          "hierarchical_min_cluster_size",
          String(state.embeddingClusteringSettings.hierarchicalMinClusterSize)
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
      const hierarchicalTarget = currentHierarchicalTarget(state.embeddingClusteringSettings);
      params.set("linkage", String(state.embeddingClusteringSettings.linkage));
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
        params.set(
          "distance_threshold",
          String(state.embeddingClusteringSettings.distanceThreshold)
        );
        params.set(
          "hierarchical_min_cluster_size",
          String(state.embeddingClusteringSettings.hierarchicalMinClusterSize)
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
    return `/api/cluster-compare?${params.toString()}`;
  }

  function currentEmbeddingClusteringRequestKey() {
    return `${interfaceSelect.value}|${selectionSettingsKey(state.selectionSettings)}|${embeddingClusteringSettingsKey()}`;
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
      hierarchicalMinClusterSize:
        elements.embeddingClusterHierarchicalMinSizeInput.value.trim(),
    };
  }

  function syncHierarchicalTargetMemoryFromDraft() {
    const nClustersValue = elements.embeddingClusterNClustersInput.value.trim();
    const distanceThresholdValue = elements.embeddingClusterDistanceThresholdInput.value.trim();
    if (nClustersValue !== "") {
      state.embeddingHierarchicalTargetMemory.nClusters = nClustersValue;
    }
    if (distanceThresholdValue !== "") {
      state.embeddingHierarchicalTargetMemory.distanceThreshold = distanceThresholdValue;
    }
  }

  function hierarchicalTargetFallbackValue(target) {
    if (target === "distance_threshold") {
      return (
        state.embeddingHierarchicalTargetMemory.distanceThreshold ||
        String(DEFAULT_CLUSTERING_SETTINGS.distanceThreshold)
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
    elements.embeddingSettingsToggle.setAttribute(
      "aria-expanded",
      String(state.embeddingSettingsOpen)
    );
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
    elements.embeddingEarlyExaggerationInput.value = String(
      state.embeddingSettingsDraft.earlyExaggeration
    );
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
      state.embeddingClusteringSettingsDraft.distanceThreshold
    );
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
  }

  function parseEmbeddingSettingsDraft() {
    const distance = elements.embeddingDistanceInput.value.trim().toLowerCase();
    const perplexityRaw = elements.embeddingPerplexityInput.value.trim();
    const learningRateRaw = elements.embeddingLearningRateInput.value.trim().toLowerCase();
    const maxIterRaw = elements.embeddingMaxIterInput.value.trim();
    const earlyExaggerationRaw = elements.embeddingEarlyExaggerationInput.value.trim();
    if (!["jaccard", "dice", "overlap"].includes(distance)) {
      throw new Error("Distance must be Jaccard, Dice, or Overlap.");
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
    const earlyExaggeration = Number.parseFloat(earlyExaggerationRaw);
    if (!Number.isFinite(earlyExaggeration) || earlyExaggeration <= 0) {
      throw new Error("Early exaggeration must be positive.");
    }
    return {
      distance,
      perplexity,
      learningRate,
      maxIter,
      earlyExaggeration,
    };
  }

  function parseEmbeddingClusteringSettingsDraft() {
    const method = state.embeddingClusteringSettingsDraft.method;
    const hierarchicalTarget = currentHierarchicalTarget();
    const distance = elements.embeddingClusterDistanceInput.value.trim().toLowerCase();
    if (!["jaccard", "dice", "overlap"].includes(distance)) {
      throw new Error("Clustering distance must be Jaccard, Dice, or Overlap.");
    }
    const linkage = elements.embeddingClusterLinkageInput.value.trim().toLowerCase();
    if (!["single", "complete", "average"].includes(linkage)) {
      throw new Error("Hierarchical linkage must be single, complete, or average.");
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
      } else {
        if (distanceThreshold === "") {
          throw new Error("Hierarchical clustering needs a cutoff distance.");
        }
        distanceThreshold = Number.parseFloat(distanceThreshold);
        if (!Number.isFinite(distanceThreshold) || distanceThreshold < 0) {
          throw new Error("Cutoff distance must be a non-negative number.");
        }
        nClusters = "";
      }
    }
    return {
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
      hierarchicalMinClusterSize,
    };
  }

  function embeddingDistanceLabel(distance = state.embeddingSettings.distance) {
    if (distance === "dice") {
      return "Dice";
    }
    if (distance === "overlap") {
      return "Overlap";
    }
    return "Jaccard";
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

  function setDistanceInfo(message) {
    elements.distanceInfo.textContent = message;
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
    return [...state.columnsChart.clusters];
  }

  function resetColumnsClusterSelection() {
    state.columnsVisibleClusters = new Set(allColumnsClusterLabels());
  }

  function allRepresentativeClusterLabels() {
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
    if (state.columnsVisibleClusters.size === 0) {
      state.columnsVisibleClusters = new Set(clusterKeys);
      return clusterKeys;
    }
    const visible = clusterKeys.filter((clusterKey) => state.columnsVisibleClusters.has(clusterKey));
    if (visible.length === 0) {
      state.columnsVisibleClusters = new Set(clusterKeys);
      return clusterKeys;
    }
    return visible;
  }

  function embeddingClusterLabel(clusterLabel) {
    if (clusterLabel === null || clusterLabel === undefined || Number.isNaN(Number(clusterLabel))) {
      return "Unclustered";
    }
    return Number(clusterLabel) < 0 ? "Noise" : `Cluster ${Number(clusterLabel) + 1}`;
  }

  function embeddingClusterColor(clusterLabel) {
    const numericLabel = Number(clusterLabel);
    if (numericLabel < 0) {
      return "#8a847a";
    }
    return CLUSTER_COLOR_PALETTE[numericLabel % CLUSTER_COLOR_PALETTE.length];
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

  function resizeDistanceCanvas() {
    const width = Math.max(1, Math.round(elements.distanceRoot.clientWidth));
    const height = Math.max(1, Math.round(elements.distanceRoot.clientHeight));
    const dpr = window.devicePixelRatio || 1;
    elements.distanceCanvas.width = Math.round(width * dpr);
    elements.distanceCanvas.height = Math.round(height * dpr);
    elements.distanceCanvas.style.width = `${width}px`;
    elements.distanceCanvas.style.height = `${height}px`;
  }

  function requestDistanceRenderNextFrame() {
    if (distanceRenderFrameId) {
      return;
    }
    distanceRenderFrameId = window.requestAnimationFrame(() => {
      distanceRenderFrameId = 0;
      if (state.msaPanelView !== "distances") {
        return;
      }
      resizeDistanceCanvas();
      renderDistanceMatrixPlot();
    });
  }

  function renderDistanceMatrixPlot() {
    const ctx = elements.distanceCanvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    const width = Math.max(1, Math.round(elements.distanceRoot.clientWidth));
    const height = Math.max(1, Math.round(elements.distanceRoot.clientHeight));
    if (!ctx || width <= 0 || height <= 0) {
      return;
    }
    const expectedCanvasWidth = Math.round(width * dpr);
    const expectedCanvasHeight = Math.round(height * dpr);
    if (
      elements.distanceCanvas.width !== expectedCanvasWidth ||
      elements.distanceCanvas.height !== expectedCanvasHeight
    ) {
      resizeDistanceCanvas();
      requestDistanceRenderNextFrame();
      return;
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#fffdf8";
    ctx.fillRect(0, 0, width, height);

    elements.distanceLoading.classList.toggle("hidden", !state.distanceMatrixLoading);
    elements.distanceLoadingLabel.textContent = "Loading distance matrix...";

    if (state.distanceMatrix?.error) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(state.distanceMatrix.error, width / 2, height / 2);
      setDistanceInfo(state.distanceMatrix.error);
      return;
    }
    const payload = state.distanceMatrix;
    if (!payload?.matrix?.length) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText(
        state.distanceMatrixLoading
          ? "Preparing distance matrix..."
          : "Open this tab after selecting an interface file.",
        width / 2,
        height / 2
      );
      setDistanceInfo(
        `Distance matrix uses ${embeddingDistanceLabel()} interface distance on row-level interface sets.`
      );
      return;
    }

    const matrix = payload.matrix;
    const n = matrix.length;
    let minValue = 1;
    let maxValue = 0;
    for (let rowIndex = 0; rowIndex < n; rowIndex += 1) {
      const row = matrix[rowIndex] || [];
      for (let columnIndex = 0; columnIndex < n; columnIndex += 1) {
        const value = Math.max(0, Math.min(1, Number(row[columnIndex]) || 0));
        if (value < minValue) {
          minValue = value;
        }
        if (value > maxValue) {
          maxValue = value;
        }
      }
    }
    const valueRange = Math.max(1e-9, maxValue - minValue);

    const padding = 14;
    const legendBarWidth = 12;
    const legendGap = 10;
    const legendLabelSpace = 34;
    const matrixRegionWidth = Math.max(
      0,
      width - padding * 2 - legendGap - legendBarWidth - legendLabelSpace
    );
    const matrixRegionHeight = Math.max(0, height - padding * 2);
    const size = Math.floor(Math.min(matrixRegionWidth, matrixRegionHeight));
    if (size < 8) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Distance view is too small.", width / 2, height / 2);
      setDistanceInfo("Expand the panel to render the distance matrix.");
      return;
    }
    const originX =
      padding + Math.floor((matrixRegionWidth - size) / 2);
    const originY =
      padding + Math.floor((matrixRegionHeight - size) / 2);

    const rasterSize = Math.max(1, Math.min(n, Math.max(64, Math.min(640, size))));
    const rasterCanvas = document.createElement("canvas");
    rasterCanvas.width = rasterSize;
    rasterCanvas.height = rasterSize;
    const rasterCtx = rasterCanvas.getContext("2d");
    if (!rasterCtx) {
      return;
    }
    const imageData = rasterCtx.createImageData(rasterSize, rasterSize);
    for (let y = 0; y < rasterSize; y += 1) {
      const rowIndex = Math.min(
        n - 1,
        Math.floor(((y + 0.5) * n) / rasterSize)
      );
      for (let x = 0; x < rasterSize; x += 1) {
        const columnIndex = Math.min(
          n - 1,
          Math.floor(((x + 0.5) * n) / rasterSize)
        );
        const value = Math.max(0, Math.min(1, Number(matrix[rowIndex][columnIndex]) || 0));
        const normalizedValue = (value - minValue) / valueRange;
        const red = Math.round(245 - normalizedValue * 120);
        const green = Math.round(245 - normalizedValue * 210);
        const blue = Math.round(245 - normalizedValue * 220);
        const pixelIndex = (y * rasterSize + x) * 4;
        imageData.data[pixelIndex] = red;
        imageData.data[pixelIndex + 1] = green;
        imageData.data[pixelIndex + 2] = blue;
        imageData.data[pixelIndex + 3] = 255;
      }
    }
    rasterCtx.putImageData(imageData, 0, 0);
    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(rasterCanvas, originX, originY, size, size);
    ctx.imageSmoothingEnabled = true;

    const legendX = padding + matrixRegionWidth + legendGap;
    const legendY = originY;
    const gradient = ctx.createLinearGradient(
      legendX,
      legendY + size,
      legendX,
      legendY
    );
    gradient.addColorStop(0, "rgb(245, 245, 245)");
    gradient.addColorStop(1, "rgb(125, 35, 25)");
    ctx.fillStyle = gradient;
    ctx.fillRect(legendX, legendY, legendBarWidth, size);
    ctx.strokeStyle = "rgba(46, 38, 29, 0.28)";
    ctx.strokeRect(legendX + 0.5, legendY + 0.5, legendBarWidth - 1, size - 1);
    ctx.font = '11px "IBM Plex Sans", sans-serif';
    ctx.fillStyle = "#6f6658";
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    const legendMax = maxValue.toFixed(2);
    const legendMid = ((minValue + maxValue) / 2).toFixed(2);
    const legendMin = minValue.toFixed(2);
    ctx.fillText(legendMax, legendX + legendBarWidth + 6, legendY + 8);
    ctx.fillText(legendMid, legendX + legendBarWidth + 6, legendY + size / 2);
    ctx.fillText(legendMin, legendX + legendBarWidth + 6, legendY + size - 8);
    ctx.save();
    ctx.translate(legendX + legendBarWidth + 24, legendY + size / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = "center";
    ctx.fillText("Distance", 0, 0);
    ctx.restore();
    ctx.textBaseline = "alphabetic";

    const suffix = payload.truncated
      ? ` Showing ${payload.row_count}/${payload.original_row_count} rows.`
      : "";
    const sortSuffix =
      payload.sort === "dominant_partner_domain"
        ? " Sorted by dominant interacting domain."
        : "";
    setDistanceInfo(
      `${embeddingDistanceLabel(payload.distance)} distance matrix on row-level interface unions.${sortSuffix}${suffix}`
    );
  }

  function columnsChartCacheKey() {
    return [
      interfaceSelect.value || "",
      state.embeddingClustering?.settingsKey || "",
      Number(state.embeddingClustering?.points?.length || 0),
      Number(state.msa?.alignment_length || 0),
    ].join("|");
  }

  function rebuildColumnsChartIfNeeded() {
    if (
      !state.msa ||
      !state.interface ||
      !(state.interface.overlayByRow instanceof Map) ||
      !(state.embeddingClustering?.points || []).length
    ) {
      state.columnsChart = null;
      state.columnsChartKey = null;
      return;
    }

    const nextKey = columnsChartCacheKey();
    if (state.columnsChart && state.columnsChartKey === nextKey) {
      return;
    }

    const alignmentLength = Math.max(1, Number(state.msa.alignment_length || 0));
    const clusterSizes = new Map();
    const clusterPoints = [];
    for (const point of state.embeddingClustering?.points || []) {
      const rowKey = String(point.row_key || "");
      const partnerDomain = String(point.partner_domain || "");
      const clusterLabel = String(point.cluster_label);
      if (!rowKey || !partnerDomain) {
        continue;
      }
      clusterPoints.push({
        rowKey,
        partnerDomain,
        clusterLabel,
      });
      clusterSizes.set(clusterLabel, (clusterSizes.get(clusterLabel) || 0) + 1);
    }
    const clusterKeys = [...clusterSizes.keys()].sort((left, right) => Number(left) - Number(right));
    const countsByCluster = new Map(
      clusterKeys.map((clusterLabel) => [clusterLabel, new Uint32Array(alignmentLength)])
    );

    for (const point of clusterPoints) {
      const rowState = state.interface.overlayByRow.get(point.rowKey);
      const counts = countsByCluster.get(point.clusterLabel);
      if (!counts) {
        continue;
      }
      const interfaceColumns = rowState?.byPartner.get(point.partnerDomain)?.interface;
      if (!(interfaceColumns instanceof Set) || interfaceColumns.size === 0) {
        continue;
      }
      for (const column of interfaceColumns) {
        const columnIndex = Number(column);
        if (
          Number.isInteger(columnIndex) &&
          columnIndex >= 0 &&
          columnIndex < alignmentLength
        ) {
          counts[columnIndex] += 1;
        }
      }
    }

    const relativeByCluster = {};
    let maxStackValue = 0;
    const stackTotals = new Float64Array(alignmentLength);
    for (const clusterLabel of clusterKeys) {
      const size = Math.max(1, Number(clusterSizes.get(clusterLabel) || 0));
      const counts = countsByCluster.get(clusterLabel) || new Uint32Array(alignmentLength);
      const relative = new Float32Array(alignmentLength);
      for (let columnIndex = 0; columnIndex < alignmentLength; columnIndex += 1) {
        const value = counts[columnIndex] / size;
        relative[columnIndex] = value;
        stackTotals[columnIndex] += value;
      }
      relativeByCluster[clusterLabel] = Array.from(relative);
    }
    for (let columnIndex = 0; columnIndex < alignmentLength; columnIndex += 1) {
      if (stackTotals[columnIndex] > maxStackValue) {
        maxStackValue = stackTotals[columnIndex];
      }
    }

    state.columnsChart = {
      file: interfaceSelect.value,
      alignmentLength,
      clusters: clusterKeys,
      clusterSizes: Object.fromEntries(clusterSizes),
      relativeByCluster,
      maxStackValue,
    };
    state.columnsChartKey = nextKey;

    const stillVisible = clusterKeys.filter((clusterLabel) =>
      state.columnsVisibleClusters.has(clusterLabel)
    );
    state.columnsVisibleClusters = new Set(
      stillVisible.length > 0 ? stillVisible : clusterKeys
    );
  }

  function renderColumnsClusterLegend() {
    if (!elements.columnsClusterLegend) {
      return;
    }
    if (state.embeddingClusteringLoading && !(state.columnsChart?.clusters || []).length) {
      elements.columnsClusterLegend.classList.remove("hidden");
      elements.columnsClusterLegend.innerHTML =
        '<div class="embedding-legend-header"><strong>Clusters</strong></div><p class="embedding-legend-empty">Loading clustering...</p>';
      return;
    }
    const clusterKeys = allColumnsClusterLabels();
    if (clusterKeys.length === 0) {
      elements.columnsClusterLegend.classList.add("hidden");
      elements.columnsClusterLegend.innerHTML = "";
      return;
    }
    const visibleClusters = new Set(visibleColumnsClusters());
    const legendEntries = clusterKeys
      .map((clusterKey) => {
        const isActive = visibleClusters.has(clusterKey);
        const clusterSize = Number(state.columnsChart?.clusterSizes?.[clusterKey] || 0);
        return `
          <button class="embedding-partner-chip ${isActive ? "active" : "inactive"}" type="button" data-columns-cluster-label="${clusterKey}" aria-pressed="${isActive}" title="${embeddingClusterLabel(clusterKey)}">
            <span class="representative-partner-filter-swatch" style="background: ${embeddingClusterColor(clusterKey)};"></span>
            <span class="embedding-partner-chip-label">${embeddingClusterLabel(clusterKey)} (${clusterSize})</span>
          </button>
        `;
      })
      .join("");
    elements.columnsClusterLegend.innerHTML = `
      <div class="embedding-legend-header"><strong>Cluster Filter</strong></div>
      <div class="embedding-legend-list">${legendEntries}</div>
    `;
    elements.columnsClusterLegend.classList.remove("hidden");
  }

  function resizeColumnsCanvas() {
    const width = Math.max(1, Math.round(elements.columnsRoot.clientWidth));
    const height = Math.max(1, Math.round(elements.columnsRoot.clientHeight));
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

  function renderColumnsChart() {
    const ctx = elements.columnsCanvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;
    const width = Math.max(1, Math.round(elements.columnsRoot.clientWidth));
    const height = Math.max(1, Math.round(elements.columnsRoot.clientHeight));
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

    const centerX = width / 2;
    const centerY = height / 2;

    if (!state.interface || !state.msa) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Load an interface selection to view columns.", centerX, centerY);
      setColumnsInfo("Stacked per-column cluster interaction profile.");
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

    const legendWidth = elements.columnsClusterLegend.classList.contains("hidden")
      ? 0
      : Math.min(Math.max(0, elements.columnsClusterLegend.offsetWidth), Math.floor(width * 0.42));
    const chartPaddingLeft = 48;
    const chartPaddingTop = 18;
    const chartPaddingBottom = 36;
    const chartPaddingRight = 16 + legendWidth;
    const chartLeft = chartPaddingLeft;
    const chartTop = chartPaddingTop;
    const chartRight = width - chartPaddingRight;
    const chartBottom = height - chartPaddingBottom;
    const chartWidth = Math.max(0, chartRight - chartLeft);
    const chartHeight = Math.max(0, chartBottom - chartTop);
    if (chartWidth < 20 || chartHeight < 20) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("Columns view is too small.", centerX, centerY);
      setColumnsInfo("Expand the panel to render the stacked column chart.");
      return;
    }

    const alignmentLength = Math.max(1, Number(state.columnsChart.alignmentLength || 1));
    const binCount = Math.max(1, Math.min(alignmentLength, Math.floor(chartWidth)));
    const binWidth = chartWidth / binCount;
    const binnedValues = new Map(visibleClusters.map((clusterLabel) => [clusterLabel, new Float64Array(binCount)]));
    const totals = new Float64Array(binCount);
    let maxStack = 0;
    for (let binIndex = 0; binIndex < binCount; binIndex += 1) {
      const startColumn = Math.floor((binIndex * alignmentLength) / binCount);
      const endColumn = Math.max(
        startColumn + 1,
        Math.floor(((binIndex + 1) * alignmentLength) / binCount)
      );
      let stack = 0;
      for (const clusterLabel of visibleClusters) {
        const values = state.columnsChart.relativeByCluster?.[clusterLabel] || [];
        let sum = 0;
        for (let columnIndex = startColumn; columnIndex < endColumn; columnIndex += 1) {
          sum += Number(values[columnIndex] || 0);
        }
        const averageValue = sum / Math.max(1, endColumn - startColumn);
        binnedValues.get(clusterLabel)[binIndex] = averageValue;
        stack += averageValue;
      }
      totals[binIndex] = stack;
      if (stack > maxStack) {
        maxStack = stack;
      }
    }

    if (maxStack <= 0) {
      ctx.fillStyle = "#6f6658";
      ctx.font = '13px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "center";
      ctx.fillText("No interface residues found for selected clusters.", centerX, centerY);
      setColumnsInfo("Selected clusters have no interacting columns.");
      return;
    }

    ctx.strokeStyle = "rgba(62, 51, 39, 0.18)";
    ctx.lineWidth = 1;
    for (let tickIndex = 0; tickIndex <= 4; tickIndex += 1) {
      const ratio = tickIndex / 4;
      const y = chartBottom - ratio * chartHeight;
      ctx.beginPath();
      ctx.moveTo(chartLeft, y + 0.5);
      ctx.lineTo(chartRight, y + 0.5);
      ctx.stroke();
      const label = `${Math.round(ratio * maxStack * 100)}%`;
      ctx.fillStyle = "#6f6658";
      ctx.font = '11px "IBM Plex Sans", sans-serif';
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      ctx.fillText(label, chartLeft - 8, y);
    }

    for (let binIndex = 0; binIndex < binCount; binIndex += 1) {
      const x0 = chartLeft + binIndex * binWidth;
      const x1 = chartLeft + (binIndex + 1) * binWidth;
      const drawWidth = Math.max(1, Math.ceil(x1 - x0));
      let yCursor = chartBottom;
      for (const clusterLabel of visibleClusters) {
        const value = binnedValues.get(clusterLabel)[binIndex];
        if (value <= 0) {
          continue;
        }
        const heightValue = (value / maxStack) * chartHeight;
        yCursor -= heightValue;
        const drawY = Math.round(yCursor);
        const drawHeight = Math.max(1, Math.ceil(heightValue));
        ctx.fillStyle = embeddingClusterColor(clusterLabel);
        ctx.fillRect(Math.floor(x0), drawY, drawWidth, drawHeight);
      }
    }

    ctx.strokeStyle = "rgba(46, 38, 29, 0.45)";
    ctx.lineWidth = 1.2;
    ctx.beginPath();
    ctx.moveTo(chartLeft, chartTop);
    ctx.lineTo(chartLeft, chartBottom);
    ctx.lineTo(chartRight, chartBottom);
    ctx.stroke();

    const startLabel = 0;
    const midLabel = Math.round((alignmentLength - 1) / 2);
    const endLabel = Math.max(0, alignmentLength - 1);
    ctx.fillStyle = "#6f6658";
    ctx.font = '11px "IBM Plex Sans", sans-serif';
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    ctx.fillText(String(startLabel), chartLeft, chartBottom + 7);
    ctx.fillText(String(midLabel), chartLeft + chartWidth / 2, chartBottom + 7);
    ctx.fillText(String(endLabel), chartRight, chartBottom + 7);

    setColumnsInfo(
      `Stacked bars: per MSA column, each segment shows the fraction of a cluster interacting at that column (${visibleClusters.length}/${state.columnsChart.clusters.length} clusters visible).`
    );
  }

  function embeddingClusterByRowKey() {
    return new Map(
      (state.embeddingClustering?.points || []).map((point) => [
        interactionRowKey(point.row_key, point.partner_domain),
        Number(point.cluster_label),
      ])
    );
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
    const clusterByRowKey = embeddingClusterByRowKey();
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
          `3D t-SNE on ${embeddingDistanceLabel()} interface distance. Loading in the background.`
        );
      } else {
        ctx.fillText("Load an interface selection to view embeddings.", centerX, centerY);
        setEmbeddingInfo(
          `3D t-SNE on ${embeddingDistanceLabel()} interface distance. Drag to rotate.`
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
      ctx.beginPath();
      ctx.fillStyle = color;
      ctx.globalAlpha = isHovered ? 1.0 : point.alpha;
      ctx.arc(point.screenX, point.screenY, point.radius, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalAlpha = 1.0;
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
      const representedCount = filteredPoints.reduce(
        (total, point) => total + Number(point.memberCount || 1),
        0
      );
      setEmbeddingInfo(
        `3D t-SNE on ${distanceLabel} interface distance. ${filteredPoints.length} visible points representing ${representedCount} interface rows. Drag to rotate.${clusteringSummary}`
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
    setEmbeddingInfo(`Loading 3D t-SNE embedding (${embeddingDistanceLabel()} distance)...`);
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

  async function ensureEmbeddingClusteringLoaded() {
    if (!interfaceSelect.value) {
      state.embeddingClustering = null;
      state.embeddingClusteringLoading = false;
      state.embeddingClusteringLoadingKey = null;
      state.embeddingClusteringPromise = null;
      state.columnsChart = null;
      state.columnsChartKey = null;
      state.columnsVisibleClusters = new Set();
      syncEmbeddingLoadingUi();
      renderEmbeddingLegend();
      renderEmbeddingPlot();
      renderColumnsClusterLegend();
      renderColumnsChart();
      return;
    }
    const settingsKey = embeddingClusteringSettingsKey();
    const requestKey = currentEmbeddingClusteringRequestKey();
    if (
      state.embeddingClustering?.file === interfaceSelect.value &&
      state.embeddingClustering?.settingsKey === settingsKey &&
      !state.embeddingClustering?.error
    ) {
      state.embeddingClusteringLoading = false;
      state.embeddingClusteringLoadingKey = null;
      state.embeddingClusteringPromise = null;
      syncEmbeddingLoadingUi();
      renderEmbeddingLegend();
      renderEmbeddingPlot();
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
      return state.embeddingClusteringPromise;
    }
    const requestId = ++state.embeddingClusteringRequestId;
    state.embeddingClusteringLoading = true;
    state.embeddingClusteringLoadingKey = requestKey;
    syncEmbeddingLoadingUi();
    renderEmbeddingLegend();
    renderEmbeddingPlot();
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
        state.embeddingClusteringSettingsDraft = {
          ...state.embeddingClusteringSettings,
        };
        state.columnsChart = null;
        state.columnsChartKey = null;
        resetEmbeddingClusterSelection();
        resetColumnsClusterSelection();
        resetRepresentativeClusterSelection();
        state.representativeHoveredClusterLabel = null;
      } catch (error) {
        if (requestId !== state.embeddingClusteringRequestId) {
          return;
        }
        state.embeddingClustering = {
          error: error.message,
          points: [],
          settingsKey,
        };
        state.columnsChart = null;
        state.columnsChartKey = null;
        state.columnsVisibleClusters = new Set();
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
          renderRepresentativeClusterLegend();
          if (state.representativeStructure && representativeLens() === "cluster") {
            void renderRepresentativeStructure();
          }
        }
      }
    })();
    return state.embeddingClusteringPromise;
  }

  async function ensureDistanceMatrixLoaded() {
    const renderDistanceView = () => {
      resizeDistanceCanvas();
      renderDistanceMatrixPlot();
    };
    if (!interfaceSelect.value) {
      state.distanceMatrix = null;
      state.distanceMatrixLoading = false;
      state.distanceMatrixLoadingKey = null;
      state.distanceMatrixPromise = null;
      renderDistanceView();
      return;
    }
    const requestKey = currentDistanceMatrixRequestKey();
    if (
      state.distanceMatrix?.file === interfaceSelect.value &&
      state.distanceMatrix?.distance === state.embeddingSettings.distance &&
      !state.distanceMatrix?.error
    ) {
      state.distanceMatrixLoading = false;
      state.distanceMatrixLoadingKey = null;
      state.distanceMatrixPromise = null;
      renderDistanceView();
      return;
    }
    if (
      state.distanceMatrixLoading &&
      state.distanceMatrixLoadingKey === requestKey &&
      state.distanceMatrixPromise
    ) {
      renderDistanceView();
      return state.distanceMatrixPromise;
    }
    const requestId = ++state.distanceMatrixRequestId;
    state.distanceMatrixLoading = true;
    state.distanceMatrixLoadingKey = requestKey;
    renderDistanceView();
    state.distanceMatrixPromise = (async () => {
      try {
        const payload = await fetchJson(currentDistanceMatrixQuery());
        if (requestId !== state.distanceMatrixRequestId) {
          return;
        }
        state.distanceMatrix = payload;
      } catch (error) {
        if (requestId !== state.distanceMatrixRequestId) {
          return;
        }
        state.distanceMatrix = {
          error: error.message,
          matrix: [],
          file: interfaceSelect.value,
          distance: state.embeddingSettings.distance,
        };
      } finally {
        if (requestId === state.distanceMatrixRequestId) {
          state.distanceMatrixLoading = false;
          state.distanceMatrixLoadingKey = null;
          state.distanceMatrixPromise = null;
          renderDistanceView();
        }
      }
    })();
    return state.distanceMatrixPromise;
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
    currentDistanceMatrixQuery,
    currentDistanceMatrixRequestKey,
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
    ensureDistanceMatrixLoaded,
    parseEmbeddingClusteringSettingsDraft,
    parseEmbeddingSettingsDraft,
    readEmbeddingClusteringDraftInputs,
    renderColumnsChart,
    renderColumnsClusterLegend,
    renderEmbeddingLegend,
    renderEmbeddingPlot,
    renderDistanceMatrixPlot,
    resetColumnsClusterSelection,
    resetEmbeddingClusterSelection,
    resetEmbeddingPartnerSelection,
    resetRepresentativeClusterSelection,
    resizeColumnsCanvas,
    resizeEmbeddingCanvas,
    resizeDistanceCanvas,
    syncEmbeddingLoadingUi,
    syncEmbeddingSettingsUi,
    syncHierarchicalTargetMemoryFromDraft,
    syncHierarchicalTargetUi,
    normalizeHierarchicalDraft,
    allRepresentativeClusterLabels,
    visibleColumnsClusters,
    visibleRepresentativeClusters,
    clusteringMethodLabel,
    setEmbeddingInfo,
    setColumnsInfo,
    setDistanceInfo,
    syncEmbeddingMemberControls,
  };
}
