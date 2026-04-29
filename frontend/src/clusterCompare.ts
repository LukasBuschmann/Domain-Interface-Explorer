// @ts-nocheck
import { fetchJson, fetchText } from "./api.js";
import { interactionRowKey } from "./interfaceModel.js";
import { appendSelectionSettingsToParams } from "./selectionSettings.js";
import { createDomainMolstarViewer } from "./molstarView.js";

export function createClusterCompareController({
  state,
  elements,
  interfaceSelect,
  currentClusterCompareQuery,
  getRowByKey,
  embeddingClusterLabel,
  embeddingDistanceLabel,
  nextBrowserPaint,
  openStructureForEntry,
  openClusterResidueMembers,
  representativeClusterCompareSummaries = () => [],
  representativeClusterCompareUrl = () => "",
  normalizeRepresentativeRow = (row) => row,
  representativeClusterSummaryFromPayload = (_payload, _clusterLabel, fallbackSummary) => fallbackSummary,
  representativeClusterCompareTileStyles = () => ({
    residueStyles: [],
    clusterLensData: null,
  }),
}) {
  const CLUSTER_COMPARE_CACHE_LIMIT = 8;

  function clampPercent(value) {
    return Math.max(0, Math.min(100, Number(value) || 0));
  }

  function roundedPercent(value) {
    const clamped = clampPercent(value);
    const rounded = Math.round(clamped);
    if (clamped > 0 && rounded === 0) {
      return 1;
    }
    return rounded;
  }

  function coverageColor(percent) {
    const clamped = clampPercent(percent);
    const lightness = 86 - clamped * 0.42;
    return `hsl(22 76% ${lightness}%)`;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function fullScreenIconSvg() {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path d="M8 4H4v4"></path>
        <path d="M16 4h4v4"></path>
        <path d="M20 16v4h-4"></path>
        <path d="M8 20H4v-4"></path>
      </svg>
    `;
  }

  function representativeMethodLabel(method) {
    return method === "residue" ? "Residue" : "Balanced";
  }

  function normalizedRepresentativeMethod(method = state.representativeClusterCompareMethod) {
    return String(method || "") === "residue" ? "residue" : "balanced";
  }

  function setClusterCompareRepresentativeMethodMenuOpen(open) {
    if (!elements.clusterCompareRepresentativeMethodMenu || !elements.clusterCompareRepresentativeMethodButton) {
      return;
    }
    elements.clusterCompareRepresentativeMethodMenu.classList.toggle("hidden", !open);
    elements.clusterCompareRepresentativeMethodButton.setAttribute(
      "aria-expanded",
      open ? "true" : "false"
    );
  }

  function syncClusterCompareHeaderControls() {
    const isRepresentativeClusterMode = state.clusterCompareMode === "representative-clusters";
    elements.clusterCompareRepresentativeMethodControl?.classList.toggle(
      "hidden",
      !isRepresentativeClusterMode
    );
    elements.clusterCompareRerollButton?.classList.toggle(
      "hidden",
      isRepresentativeClusterMode
    );
    if (!isRepresentativeClusterMode) {
      setClusterCompareRepresentativeMethodMenuOpen(false);
      return;
    }
    const method = normalizedRepresentativeMethod();
    if (elements.clusterCompareRepresentativeMethodLabel) {
      elements.clusterCompareRepresentativeMethodLabel.textContent = representativeMethodLabel(method);
    }
    if (elements.clusterCompareRepresentativeMethodMenu) {
      [
        ...elements.clusterCompareRepresentativeMethodMenu.querySelectorAll(
          "[data-cluster-compare-representative-method]"
        ),
      ].forEach((button) => {
        const isActive = button.dataset.clusterCompareRepresentativeMethod === method;
        button.classList.toggle("active", isActive);
        button.setAttribute("aria-checked", isActive ? "true" : "false");
      });
    }
  }

  function clusterCompareCacheKey(clusterLabel) {
    return currentClusterCompareQuery(clusterLabel);
  }

  function representativeClusterCompareCacheKey(method, clusters) {
    return [
      "representative-clusters",
      normalizedRepresentativeMethod(method),
      ...clusters.map((cluster) => representativeClusterCompareUrl(cluster.clusterLabel, method)),
    ].join("|");
  }

  function clusterCompareFetchUrl(cacheKey, reroll) {
    if (!reroll) {
      return cacheKey;
    }
    const separator = cacheKey.includes("?") ? "&" : "?";
    return `${cacheKey}${separator}_reroll=${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }

  function clusterCompareRecordHasModelData(record) {
    return (record?.results || []).every(
      (result) =>
        result?.error ||
        (
          result?.payload &&
          typeof result.modelText === "string" &&
          result.modelText.length > 0
        )
    );
  }

  function readClusterCompareCache(cacheKey) {
    const cache = state.clusterCompareCache;
    if (!cache || typeof cache.get !== "function") {
      return null;
    }
    const cached = cache.get(cacheKey);
    if (!cached) {
      return null;
    }
    if (!clusterCompareRecordHasModelData(cached)) {
      cache.delete(cacheKey);
      return null;
    }
    if (typeof cache.delete === "function" && typeof cache.set === "function") {
      cache.delete(cacheKey);
      cache.set(cacheKey, cached);
    }
    return cached;
  }

  function writeClusterCompareCache(cacheKey, value) {
    if (!state.clusterCompareCache || typeof state.clusterCompareCache.set !== "function") {
      state.clusterCompareCache = new Map();
    }
    state.clusterCompareCache.delete(cacheKey);
    state.clusterCompareCache.set(cacheKey, value);
    while (state.clusterCompareCache.size > CLUSTER_COMPARE_CACHE_LIMIT) {
      const oldestKey = state.clusterCompareCache.keys().next().value;
      state.clusterCompareCache.delete(oldestKey);
    }
  }

  function renderedClusterCompareRecord(record) {
    return {
      ...record,
      results: currentRenderedClusterCompareResults(),
    };
  }

  function setClusterCompareGridLoading(visible) {
    if (!elements.clusterCompareGrid) {
      return;
    }
    elements.clusterCompareGrid.classList.toggle(
      "is-loading",
      Boolean(visible && elements.clusterCompareGrid.children.length > 0)
    );
  }

  function cleanupClusterCompareTileState(options = {}) {
    const resetSharedView = options.resetSharedView !== false;
    for (const tile of state.clusterCompareTiles) {
      tile.cleanupSync?.();
      tile.viewer?.destroy?.();
    }
    state.clusterCompareTiles = [];
    if (resetSharedView) {
      state.clusterCompareSharedView = null;
      state.clusterCompareAlignmentAnchorRowKey = null;
    }
    state.clusterComparePendingSyncIndex = null;
    state.clusterCompareSyncing = false;
    if (state.clusterCompareSyncFrame) {
      window.cancelAnimationFrame(state.clusterCompareSyncFrame);
      state.clusterCompareSyncFrame = 0;
    }
  }

  function clearClusterCompareTiles(options = {}) {
    cleanupClusterCompareTileState(options);
    setClusterCompareGridLoading(false);
    if (elements.clusterCompareGrid) {
      elements.clusterCompareGrid.innerHTML = "";
    }
  }

  function setClusterCompareLoading(
    visible,
    label = "Loading aligned structures...",
    progress = null,
  ) {
    if (elements.clusterCompareLoading) {
      elements.clusterCompareLoading.classList.toggle("hidden", !visible);
    }
    setClusterCompareGridLoading(visible && state.clusterCompareTiles.length > 0);
    if (elements.clusterCompareLoadingLabel) {
      elements.clusterCompareLoadingLabel.textContent = label;
    }
    if (elements.clusterCompareLoadingBar) {
      const clampedProgress =
        progress === null || progress === undefined
          ? 1
          : Math.max(1, Math.min(100, Number(progress) || 1));
      elements.clusterCompareLoadingBar.style.width = `${clampedProgress}%`;
    }
  }

  function applyClusterCompareGridLayout(tileCount) {
    if (!elements.clusterCompareGrid) {
      return;
    }
    const columns =
      tileCount >= 5 ? 3 : tileCount === 4 ? 2 : tileCount >= 3 ? 3 : Math.max(1, tileCount);
    const rows = Math.max(1, Math.ceil(tileCount / columns));
    elements.clusterCompareGrid.style.gridTemplateColumns = `repeat(${columns}, minmax(0, 1fr))`;
    elements.clusterCompareGrid.style.gridTemplateRows = `repeat(${rows}, minmax(0, 1fr))`;
  }

  function renderClusterCompareFatalError(message) {
    if (!elements.clusterCompareGrid) {
      return;
    }
    elements.clusterCompareGrid.innerHTML = "";
    const panel = document.createElement("article");
    panel.className = "cluster-compare-error-panel";
    const heading = document.createElement("strong");
    heading.textContent = "Unable to load structure comparison";
    const body = document.createElement("p");
    body.textContent = message || "Unknown cluster comparison error.";
    panel.append(heading, body);
    elements.clusterCompareGrid.append(panel);
  }

  function renderClusterCompareTileError(tileIndex) {
    const tile = state.clusterCompareTiles[tileIndex];
    if (!tile?.viewerRoot) {
      return;
    }
    tile.viewer?.destroy?.();
    tile.viewer = null;
    tile.viewerRoot.replaceChildren();
    tile.viewerRoot.classList.add("cluster-compare-tile-viewer-error");
    const errorBox = document.createElement("div");
    errorBox.className = "cluster-compare-tile-error";
    const heading = document.createElement("strong");
    heading.textContent = "Structure unavailable";
    const body = document.createElement("p");
    body.textContent = tile.error || "Unknown structure loading error.";
    errorBox.append(heading, body);
    tile.viewerRoot.append(errorBox);
  }

  function openClusterCompareModal() {
    elements.clusterCompareModal?.classList.remove("hidden");
    elements.clusterCompareModal?.setAttribute("aria-hidden", "false");
    syncClusterCompareHeaderControls();
  }

  function closeClusterCompareModal() {
    state.clusterCompareRequestId += 1;
    state.clusterCompareMode = "cluster";
    state.clusterCompareClusterLabel = null;
    state.clusterCompareAlignmentAnchorRowKey = null;
    clearClusterCompareTiles();
    if (elements.clusterCompareModalTitle) {
      elements.clusterCompareModalTitle.textContent = "Cluster Structure Comparison";
    }
    if (elements.clusterCompareModalSubtitle) {
      elements.clusterCompareModalSubtitle.textContent = "No cluster selected.";
    }
    setClusterCompareLoading(false);
    syncClusterCompareHeaderControls();
    elements.clusterCompareModal?.classList.add("hidden");
    elements.clusterCompareModal?.setAttribute("aria-hidden", "true");
  }

  function scheduleClusterCompareViewSync(sourceIndex) {
    state.clusterComparePendingSyncIndex = sourceIndex;
    if (state.clusterCompareSyncFrame) {
      return;
    }
    state.clusterCompareSyncFrame = window.requestAnimationFrame(() => {
      state.clusterCompareSyncFrame = 0;
      const pendingIndex = state.clusterComparePendingSyncIndex;
      state.clusterComparePendingSyncIndex = null;
      if (pendingIndex === null || pendingIndex === undefined) {
        return;
      }
      syncClusterCompareViews(pendingIndex);
    });
  }

  function syncClusterCompareViews(sourceIndex) {
    if (state.clusterCompareSyncing) {
      return;
    }
    const sourceTile = state.clusterCompareTiles[sourceIndex];
    if (!sourceTile?.viewer || typeof sourceTile.viewer.getView !== "function") {
      return;
    }

    const sharedView = sourceTile.viewer.getView();
    state.clusterCompareSharedView = sharedView;
    state.clusterCompareSyncing = true;
    try {
      state.clusterCompareTiles.forEach((tile, tileIndex) => {
        if (tileIndex === sourceIndex || !tile.viewer || typeof tile.viewer.setView !== "function") {
          return;
        }
        tile.viewer.setView(sharedView);
        tile.viewer.render();
      });
    } finally {
      state.clusterCompareSyncing = false;
    }
  }

  function bindClusterCompareViewerSync(tileIndex, viewerRoot) {
    let dragging = false;
    const onPointerDown = () => {
      dragging = true;
    };
    const onPointerMove = (event) => {
      if (dragging || event.buttons > 0) {
        scheduleClusterCompareViewSync(tileIndex);
      }
    };
    const onPointerUp = () => {
      if (!dragging) {
        return;
      }
      dragging = false;
      scheduleClusterCompareViewSync(tileIndex);
    };
    const onWheel = () => {
      window.requestAnimationFrame(() => scheduleClusterCompareViewSync(tileIndex));
    };

    viewerRoot.addEventListener("pointerdown", onPointerDown);
    viewerRoot.addEventListener("pointermove", onPointerMove);
    viewerRoot.addEventListener("wheel", onWheel, { passive: true });
    window.addEventListener("pointerup", onPointerUp);

    return () => {
      viewerRoot.removeEventListener("pointerdown", onPointerDown);
      viewerRoot.removeEventListener("pointermove", onPointerMove);
      viewerRoot.removeEventListener("wheel", onWheel);
      window.removeEventListener("pointerup", onPointerUp);
    };
  }

  function copyView(view) {
    if (!view) {
      return null;
    }
    if (typeof structuredClone === "function") {
      try {
        return structuredClone(view);
      } catch (_error) {
        // Fall through to a plain object/array copy.
      }
    }
    if (Array.isArray(view)) {
      return view.map((item) => {
        if (Array.isArray(item)) {
          return item.slice();
        }
        if (item && typeof item === "object") {
          return { ...item };
        }
        return item;
      });
    }
    if (ArrayBuffer.isView(view) && typeof view.slice === "function") {
      return view.slice();
    }
    if (view && typeof view === "object") {
      return { ...view };
    }
    return view;
  }

  function currentClusterCompareView() {
    if (state.clusterCompareSharedView) {
      return copyView(state.clusterCompareSharedView);
    }
    const tileWithView = state.clusterCompareTiles.find(
      (tile) => tile?.viewer && typeof tile.viewer.getView === "function"
    );
    return tileWithView?.viewer ? copyView(tileWithView.viewer.getView()) : null;
  }

  function cloneCameraView(view) {
    if (!view) {
      return null;
    }
    if (typeof structuredClone === "function") {
      try {
        return structuredClone(view);
      } catch (_error) {
        // Fall through to JSON cloning.
      }
    }
    try {
      return JSON.parse(JSON.stringify(view));
    } catch (_error) {
      return copyView(view);
    }
  }

  function nudgeCameraView(view) {
    const nudged = cloneCameraView(view);
    const position = nudged?.position;
    if (!position || position.length < 3) {
      return null;
    }
    const target = nudged?.target || [0, 0, 0];
    const dx = Number(position[0]) - Number(target[0] || 0);
    const dy = Number(position[1]) - Number(target[1] || 0);
    const dz = Number(position[2]) - Number(target[2] || 0);
    const distance = Math.hypot(dx, dy, dz);
    const amount = Math.max(0.01, distance * 0.0025);
    position[0] = Number(position[0]) + amount;
    position[1] = Number(position[1]) + amount * 0.35;
    return nudged;
  }

  function applyClusterCompareView(view) {
    if (!view) {
      return;
    }
    for (const tile of state.clusterCompareTiles) {
      if (!tile?.viewer || tile.error || typeof tile.viewer.setView !== "function") {
        continue;
      }
      tile.viewer.setView(view);
      tile.viewer.resize();
      tile.viewer.render();
    }
  }

  function nudgeClusterCompareCameras() {
    const originalView = currentClusterCompareView();
    const nudgedView = nudgeCameraView(originalView);
    if (!originalView || !nudgedView) {
      resizeClusterCompareViewers();
      return;
    }
    applyClusterCompareView(nudgedView);
    window.requestAnimationFrame(() => {
      applyClusterCompareView(originalView);
    });
  }

  async function openClusterCompareTileStructure(tileIndex) {
    const tile = state.clusterCompareTiles[Number(tileIndex)];
    const entry = tile?.entry;
    if (!entry || typeof openStructureForEntry !== "function") {
      return;
    }
    const initialView =
      tile.viewer && typeof tile.viewer.getView === "function"
        ? copyView(tile.viewer.getView())
        : currentClusterCompareView();
    await openStructureForEntry(entry, {
      payload: tile.payload,
      modelText: tile.modelText,
      previewUrl: tile.previewUrl || "",
      initialView,
    });
  }

  elements.clusterCompareGrid?.addEventListener("click", (event) => {
    const button = event.target.closest("[data-cluster-compare-open]");
    if (!button || !elements.clusterCompareGrid.contains(button)) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    void openClusterCompareTileStructure(button.dataset.clusterCompareOpen);
  });

  elements.clusterCompareGrid?.addEventListener("dblclick", (event) => {
    if (
      state.clusterCompareMode !== "representative-clusters" ||
      typeof openClusterResidueMembers !== "function" ||
      event.target.closest("[data-cluster-compare-open]")
    ) {
      return;
    }
    const tileElement = event.target.closest("[data-cluster-compare-index]");
    if (!tileElement || !elements.clusterCompareGrid.contains(tileElement)) {
      return;
    }
    const tileIndex = Number(tileElement.dataset.clusterCompareIndex);
    const tile = state.clusterCompareTiles[tileIndex];
    const residueId = Number(tile?.hoverResidue?.residueId);
    const residueCluster = tile?.clusterLensData?.clusterByResidueId?.get?.(residueId);
    const columnIndex = Number(residueCluster?.columnIndex);
    if (!tile || !Number.isFinite(residueId) || !Number.isInteger(columnIndex)) {
      if (elements.clusterCompareModalSubtitle) {
        elements.clusterCompareModalSubtitle.textContent =
          "Double-click a colored cluster residue to open matching interfaces.";
      }
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    void openClusterResidueMembers({
      tile,
      entry: tile.entry,
      clusterLabel: residueCluster.clusterLabel ?? tile.entry?.clusterLabel,
      columnIndex,
      residueId,
      residueName: tile.hoverResidue?.residueName || "",
    }).catch((error) => {
      if (elements.clusterCompareModalSubtitle) {
        elements.clusterCompareModalSubtitle.textContent =
          error.message || "Unable to open matching cluster interfaces.";
      }
    });
  });

  elements.clusterCompareRecenterButton?.addEventListener("click", () => {
    centerClusterCompareDomains();
  });

  function clusterCompareDomainSelection(payload) {
    return {
      resi:
        (Array.isArray(payload?.fragment_residue_ids) && payload.fragment_residue_ids.length > 0)
          ? payload.fragment_residue_ids
          : Array.from(
              { length: payload.fragment_end - payload.fragment_start + 1 },
              (_value, index) => payload.fragment_start + index
            ),
    };
  }

  function centerClusterCompareDomains(options = {}) {
    const anchorTile = state.clusterCompareTiles.find(
      (tile) => tile?.viewer && tile?.payload && !tile.error
    );
    if (!anchorTile?.viewer) {
      return;
    }
    const domainSelection = clusterCompareDomainSelection(anchorTile.payload);
    if (typeof anchorTile.viewer.focusResiduesStable === "function") {
      anchorTile.viewer.focusResiduesStable(domainSelection.resi, 8);
    } else if (typeof anchorTile.viewer.focusResidues === "function") {
      anchorTile.viewer.focusResidues(domainSelection.resi, 8);
    }
    anchorTile.viewer.resize();
    anchorTile.viewer.render();
    window.requestAnimationFrame(() => {
      if (typeof anchorTile.viewer.getView === "function") {
        state.clusterCompareSharedView = copyView(anchorTile.viewer.getView());
      }
      applyClusterCompareView(state.clusterCompareSharedView);
      if (options.nudge !== false) {
        window.requestAnimationFrame(nudgeClusterCompareCameras);
      }
    });
  }

  function retryModelUrl(modelUrl) {
    const separator = String(modelUrl || "").includes("?") ? "&" : "?";
    return `${modelUrl}${separator}_retry=${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }

  function shouldRetryClusterCompareModelLoad(error, tile) {
    const message = String(error?.message || error || "");
    return Boolean(
      tile?.payload?.model_url &&
      !tile.modelRetryAttempted &&
      (
        message.includes("Mol* could not parse") ||
        message.includes("s is undefined") ||
        message.includes("can't access property") ||
        message.includes("Cannot read properties")
      )
    );
  }

  async function loadClusterCompareTileViewer(viewer, tile, modelText) {
    await viewer.loadStructure({
      modelText,
      payload: tile.payload,
      format: tile.payload.model_format || "pdb",
      label: tile.entry?.title || tile.entry?.rowKey || "Cluster comparison structure",
      mode: tile.residueStyles?.length ? "representative" : "compare",
      columnView: false,
      contactsVisible: false,
      residueLookup: new Map(),
      residueStyles: tile.residueStyles || [],
      clusterLensData: tile.clusterLensData || null,
      representativeLens: tile.residueStyles?.length ? "cluster" : "",
      displaySettings: state.structureDisplaySettings,
      onHover: (hover) => {
        tile.hoverResidue = hover;
      },
      onHoverEnd: () => {
        tile.hoverResidue = null;
      },
    });
  }

  function ensureClusterCompareTileViewer(tileIndex) {
    const tile = state.clusterCompareTiles[tileIndex];
    if (!tile || tile.error || !tile.viewerRoot) {
      return null;
    }
    tile.viewerRoot.classList.remove("cluster-compare-tile-viewer-error");
    if (!tile.viewer) {
      tile.viewer = createDomainMolstarViewer(tile.viewerRoot, {
        kind: "cluster-compare",
      });
      tile.cleanupSync = bindClusterCompareViewerSync(tileIndex, tile.viewerRoot);
    }
    return tile.viewer;
  }

  function initializeClusterCompareTileViewers() {
    return Promise.allSettled(
      state.clusterCompareTiles.map(async (_tile, tileIndex) => {
        const viewer = ensureClusterCompareTileViewer(tileIndex);
        if (!viewer || typeof viewer.ensureViewer !== "function") {
          return;
        }
        await viewer.ensureViewer(state.structureDisplaySettings);
        viewer.resize();
        viewer.render();
      })
    );
  }

  function initializeClusterCompareSharedView(options = {}) {
    if (state.clusterCompareSharedView) {
      applyClusterCompareView(state.clusterCompareSharedView);
      if (options.nudge) {
        window.requestAnimationFrame(nudgeClusterCompareCameras);
      }
      return;
    }
    centerClusterCompareDomains({ nudge: Boolean(options.nudge) });
  }

  async function renderClusterCompareTile(tileIndex) {
    const tile = state.clusterCompareTiles[tileIndex];
    if (!tile || tile.error || !tile.viewerRoot) {
      return;
    }
    const viewer = ensureClusterCompareTileViewer(tileIndex);
    const { payload, modelText } = tile;
    if (!payload || typeof modelText !== "string" || modelText.length === 0) {
      throw new Error("Structure preview is missing model data.");
    }
    try {
      await loadClusterCompareTileViewer(viewer, tile, modelText);
    } catch (error) {
      if (!shouldRetryClusterCompareModelLoad(error, tile)) {
        throw error;
      }
      tile.modelRetryAttempted = true;
      const refreshedModelText = await fetchText(retryModelUrl(payload.model_url));
      if (typeof refreshedModelText !== "string" || refreshedModelText.length === 0) {
        throw error;
      }
      tile.modelText = refreshedModelText;
      await loadClusterCompareTileViewer(viewer, tile, refreshedModelText);
    }
    viewer.resize();

    const domainSelection = clusterCompareDomainSelection(payload);
    if (state.clusterCompareSharedView && typeof viewer.setView === "function") {
      viewer.setView(state.clusterCompareSharedView);
    } else {
      if (typeof viewer.focusResiduesStable === "function") {
        viewer.focusResiduesStable(domainSelection.resi, 8);
      } else {
        viewer.focusResidues(domainSelection.resi, 8);
      }
    }
    viewer.render();
  }

  async function applyClusterCompareTileResult(tileIndex, result) {
    const tile = state.clusterCompareTiles[tileIndex];
    if (!tile) {
      return;
    }
    Object.assign(tile, result);
    if (tile.error) {
      renderClusterCompareTileError(tileIndex);
      return;
    }
    try {
      await renderClusterCompareTile(tileIndex);
    } catch (error) {
      tile.error = error.message || "Unknown structure render error.";
      renderClusterCompareTileError(tileIndex);
    }
  }

  function resizeClusterCompareViewers() {
    for (const tile of state.clusterCompareTiles) {
      if (!tile.viewer) {
        continue;
      }
      tile.viewer.resize();
      tile.viewer.render();
    }
  }

  async function fetchClusterCompareStructure(entry, alignToRowKey = "") {
    const params = new URLSearchParams({
      interface_file: interfaceSelect.value,
      row_key: String(entry.rowKey),
      partner: String(entry.partnerDomain),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    if (alignToRowKey) {
      params.set("align_to_row_key", alignToRowKey);
    }
    const previewUrl = `/api/structure-preview?${params.toString()}`;
    const payload = await fetchJson(previewUrl);
    if (!payload?.model_url) {
      throw new Error("Structure preview did not include a model URL.");
    }
    const modelText = await fetchText(payload.model_url);
    if (typeof modelText !== "string" || modelText.length === 0) {
      throw new Error("Structure model download was empty.");
    }
    return {
      entry,
      payload,
      modelText,
      previewUrl,
    };
  }

  async function fetchClusterCompareData(clusterLabel, fetchUrl = currentClusterCompareQuery(clusterLabel)) {
    const payload = await fetchJson(fetchUrl);
    return {
      ...payload,
      selectedEntries: (payload.selected_entries || []).map((entry, index) => ({
        key: `${entry.row_key}|${entry.partner_domain}|${index}`,
        rowKey: String(entry.row_key),
        partnerDomain: String(entry.partner_domain),
        clusterLabel: Number(payload.cluster_label),
        selectionRank: Number(entry.selection_rank ?? index),
        coverageCount: Number(entry.coverage_count || 0),
        coveragePercent: Number(entry.coverage_percent || 0),
        coverageFraction: Number(entry.coverage_fraction || 0),
        row: getRowByKey(interactionRowKey(entry.row_key, entry.partner_domain)),
      })),
    };
  }

  function clusterCompareSubtitle(record) {
    if (record?.subtitle) {
      return record.subtitle;
    }
    return (
      `Showing ${record.selectedEntries.length} of ${record.clusterEntryCount} cluster entries, ` +
      `selected by greedy max-min ${embeddingDistanceLabel(record.distanceMetric)} distance ` +
      `from a random cluster start, ordered by nearest-neighbor coverage on the ` +
      `${record.remainingEntryCount} remaining entries.`
    );
  }

  function renderClusterCompareGridShell(record) {
    const selectedEntries = record.selectedEntries || [];
    const remainingEntryCount = Number(record.remainingEntryCount || 0);
    cleanupClusterCompareTileState({ resetSharedView: false });
    setClusterCompareGridLoading(false);
    applyClusterCompareGridLayout(selectedEntries.length);
    elements.clusterCompareGrid.innerHTML = selectedEntries
      .map((entry, index) => {
        const rowLabel = escapeHtml(entry.title || entry.rowKey);
        const openLabel = escapeHtml(entry.openLabel || entry.rowKey);
        const partnerLabel = escapeHtml(entry.partnerDomain);
        const subtitle = escapeHtml(entry.subtitle || `Partner ${entry.partnerDomain}`);
        const swatch =
          entry.color
            ? `<span class="cluster-compare-tile-swatch" style="background: ${entry.color};"></span>`
            : "";
        const coverageTitle = escapeHtml(
          entry.coverageTitle ||
            `${entry.coverageCount}/${remainingEntryCount} nearest cluster members`
        );
        const coverageFillColor = entry.color || coverageColor(entry.coveragePercent);
        const coverageLabel = escapeHtml(
          entry.coverageLabel || `${roundedPercent(entry.coveragePercent)}%`
        );
        return `
          <article class="cluster-compare-tile" data-cluster-compare-index="${index}">
            <div class="cluster-compare-tile-viewer" data-cluster-compare-viewer="${index}"></div>
            <div class="cluster-compare-tile-meta">
              <div class="cluster-compare-tile-title-row">
                <span class="cluster-compare-tile-title">${swatch}${rowLabel}</span>
                <button
                  class="cluster-compare-open-structure"
                  type="button"
                  data-cluster-compare-open="${index}"
                  title="Open in structure viewer"
                  aria-label="Open ${openLabel} in structure viewer"
                >
                  ${fullScreenIconSvg()}
                </button>
                <span
                  class="cluster-compare-coverage"
                  title="${coverageTitle}"
                >
                  <span class="cluster-compare-coverage-track">
                    <span
                      class="cluster-compare-coverage-fill"
                      style="width: ${clampPercent(entry.coveragePercent)}%; background: ${coverageFillColor};"
                    ></span>
                  </span>
                  <span class="cluster-compare-coverage-value">${coverageLabel}</span>
                </span>
              </div>
              <span class="cluster-compare-tile-subtitle">${subtitle}</span>
            </div>
          </article>
        `;
      })
      .join("");
  }

  function setClusterCompareTilesFromResults(results) {
    state.clusterCompareTiles = results.map((result, index) => ({
      ...result,
      viewer: null,
      cleanupSync: null,
      viewerRoot: elements.clusterCompareGrid.querySelector(
        `[data-cluster-compare-viewer="${index}"]`
      ),
    }));
  }

  function prepareClusterCompareTiles(record) {
    renderClusterCompareGridShell(record);
    setClusterCompareTilesFromResults(
      (record.selectedEntries || []).map((entry) => ({ entry }))
    );
    void initializeClusterCompareTileViewers();
  }

  function currentRenderedClusterCompareResults() {
    return state.clusterCompareTiles.map((tile) => ({
      entry: tile.entry,
      row: tile.row,
      representativePayload: tile.representativePayload,
      payload: tile.payload,
      modelText: tile.modelText,
      previewUrl: tile.previewUrl,
      residueStyles: tile.residueStyles || [],
      clusterLensData: tile.clusterLensData || null,
      error: tile.error,
      hoverResidue: null,
    }));
  }

  async function renderClusterCompareResults(record) {
    state.clusterCompareMode =
      record?.kind === "representative-clusters" ? "representative-clusters" : "cluster";
    syncClusterCompareHeaderControls();
    if (elements.clusterCompareModalTitle) {
      elements.clusterCompareModalTitle.textContent = record.title;
    }
    if (elements.clusterCompareModalSubtitle) {
      elements.clusterCompareModalSubtitle.textContent = clusterCompareSubtitle(record);
    }
    state.clusterCompareAlignmentAnchorRowKey = record.alignmentAnchorRowKey || "";
    renderClusterCompareGridShell(record);
    setClusterCompareTilesFromResults(record.results || []);
    await initializeClusterCompareTileViewers();

    for (let index = 0; index < state.clusterCompareTiles.length; index += 1) {
      const tile = state.clusterCompareTiles[index];
      if (tile.error) {
        renderClusterCompareTileError(index);
        continue;
      }
      try {
        await renderClusterCompareTile(index);
      } catch (error) {
        tile.error = error.message || "Unknown structure render error.";
        renderClusterCompareTileError(index);
      }
    }
    initializeClusterCompareSharedView({ nudge: true });
  }

  async function openClusterCompareForLabel(clusterLabel, options = {}) {
    if (!interfaceSelect.value || !state.embeddingClustering?.points?.length) {
      return;
    }
    const reroll = Boolean(options.reroll);
    const targetClusterLabel = Number(clusterLabel);
    const previousClusterLabel = state.clusterCompareClusterLabel;
    const previousMode = state.clusterCompareMode;
    state.clusterCompareMode = "cluster";
    const requestId = state.clusterCompareRequestId + 1;
    state.clusterCompareRequestId = requestId;
    state.clusterCompareClusterLabel = targetClusterLabel;
    syncClusterCompareHeaderControls();
    const preservedView = currentClusterCompareView();
    const keepExistingTilesDuringLoad = Boolean(
      previousMode === "cluster" &&
      state.clusterCompareTiles.length > 0 &&
      elements.clusterCompareGrid?.children.length
    );
    const previousAlignmentAnchorRowKey =
      keepExistingTilesDuringLoad && previousClusterLabel === targetClusterLabel
        ? state.clusterCompareAlignmentAnchorRowKey || ""
        : "";
    if (preservedView) {
      state.clusterCompareSharedView = preservedView;
    } else if (!keepExistingTilesDuringLoad) {
      state.clusterCompareSharedView = null;
    }
    if (!keepExistingTilesDuringLoad) {
      clearClusterCompareTiles({ resetSharedView: !preservedView });
    }
    const clusterTitle = `${embeddingClusterLabel(clusterLabel)} Structure Comparison`;
    const cacheKey = clusterCompareCacheKey(clusterLabel);
    if (elements.clusterCompareModalTitle) {
      elements.clusterCompareModalTitle.textContent = clusterTitle;
    }
    if (elements.clusterCompareModalSubtitle) {
      elements.clusterCompareModalSubtitle.textContent = `Loading ${embeddingClusterLabel(clusterLabel)}...`;
    }
    openClusterCompareModal();
    const cachedRecord = reroll ? null : readClusterCompareCache(cacheKey);
    setClusterCompareLoading(
      true,
      cachedRecord ? "Restoring cached comparison..." : "Selecting diverse interfaces...",
      cachedRecord ? 80 : 12
    );
    try {
      await nextBrowserPaint();
      if (cachedRecord) {
        if (requestId !== state.clusterCompareRequestId) {
          return;
        }
        await renderClusterCompareResults(cachedRecord);
        writeClusterCompareCache(cacheKey, renderedClusterCompareRecord(cachedRecord));
        return;
      }

      const clusterCompareData = await fetchClusterCompareData(
        clusterLabel,
        clusterCompareFetchUrl(cacheKey, reroll)
      );
      if (requestId !== state.clusterCompareRequestId) {
        return;
      }
      const clusterEntryCount = Number(clusterCompareData.entry_count || 0);
      const remainingEntryCount = Number(clusterCompareData.remaining_entry_count || 0);
      const selectedEntries = [...(clusterCompareData.selectedEntries || [])].sort(
        (left, right) =>
          right.coveragePercent - left.coveragePercent ||
          left.selectionRank - right.selectionRank ||
          left.rowKey.localeCompare(right.rowKey) ||
          left.partnerDomain.localeCompare(right.partnerDomain)
      );
      const distanceMetric =
        clusterCompareData.distance || state.embeddingClusteringSettings.distance || "overlap";
      if (selectedEntries.length === 0) {
        throw new Error(`${embeddingClusterLabel(clusterLabel)} has no entries to compare.`);
      }
      const record = {
        title: clusterTitle,
        clusterLabel: targetClusterLabel,
        clusterEntryCount,
        remainingEntryCount,
        selectedEntries,
        distanceMetric,
        alignmentAnchorRowKey: previousAlignmentAnchorRowKey || selectedEntries[0]?.rowKey || "",
        results: [],
      };
      if (elements.clusterCompareModalSubtitle) {
        elements.clusterCompareModalSubtitle.textContent = clusterCompareSubtitle(record);
      }
      const anchorRowKey = record.alignmentAnchorRowKey || selectedEntries[0]?.rowKey || "";
      setClusterCompareLoading(
        true,
        `Loading aligned structures (0/${selectedEntries.length})...`,
        1
      );
      if (!keepExistingTilesDuringLoad) {
        prepareClusterCompareTiles(record);
      }

      let completedStructures = 0;
      const results = await Promise.all(
        selectedEntries.map(async (entry, index) => {
          let result;
          try {
            result = await fetchClusterCompareStructure(
              entry,
              entry.rowKey === anchorRowKey ? "" : anchorRowKey
            );
          } catch (error) {
            result = {
              entry,
              error: error.message,
            };
          } finally {
            completedStructures += 1;
            if (requestId === state.clusterCompareRequestId) {
              setClusterCompareLoading(
                true,
                `Loading aligned structures (${completedStructures}/${selectedEntries.length})...`,
                Math.round((completedStructures / selectedEntries.length) * 100)
              );
            }
          }
          if (!keepExistingTilesDuringLoad && requestId === state.clusterCompareRequestId) {
            await applyClusterCompareTileResult(index, result);
          }
          return result;
        })
      );

      if (requestId !== state.clusterCompareRequestId) {
        return;
      }

      record.results = results.map((result) => ({
        entry: result.entry,
        payload: result.payload,
        modelText: result.modelText,
        previewUrl: result.previewUrl,
        error: result.error,
      }));
      if (keepExistingTilesDuringLoad) {
        await renderClusterCompareResults(record);
      } else {
        record.results = currentRenderedClusterCompareResults();
        state.clusterCompareAlignmentAnchorRowKey = record.alignmentAnchorRowKey || "";
        initializeClusterCompareSharedView({ nudge: true });
      }
      writeClusterCompareCache(cacheKey, renderedClusterCompareRecord(record));
    } catch (error) {
      if (requestId === state.clusterCompareRequestId) {
        if (elements.clusterCompareModalSubtitle) {
          elements.clusterCompareModalSubtitle.textContent = error.message || "Unable to load cluster structure comparison.";
        }
        if (keepExistingTilesDuringLoad) {
          setClusterCompareGridLoading(false);
        } else {
          clearClusterCompareTiles({ resetSharedView: !preservedView });
          renderClusterCompareFatalError(error.message || "Unable to load cluster structure comparison.");
        }
      }
      throw error;
    } finally {
      if (requestId === state.clusterCompareRequestId) {
        setClusterCompareLoading(false);
      }
    }
  }

  async function fetchRepresentativeClusterSelection(cluster, method) {
    const payload = await fetchJson(
      representativeClusterCompareUrl(cluster.clusterLabel, method)
    );
    const row = normalizeRepresentativeRow(payload.row || null, payload.alignment_length);
    if (!row?.row_key && !row?.interface_row_key) {
      throw new Error(`${cluster.label || embeddingClusterLabel(cluster.clusterLabel)} has no representative row.`);
    }
    const clusterSummary = representativeClusterSummaryFromPayload(
      payload,
      cluster.clusterLabel,
      cluster
    );
    const memberCount = Number(cluster.compareMemberCount ?? cluster.memberCount ?? 0);
    return {
      key: `representative-cluster:${cluster.clusterLabel}`,
      rowKey: String(row.interface_row_key || row.row_key),
      openLabel: row.display_row_key || row.row_key || row.interface_row_key || "",
      partnerDomain: String(row.partner_domain || ""),
      clusterLabel: Number(cluster.clusterLabel),
      title: cluster.label || embeddingClusterLabel(cluster.clusterLabel),
      subtitle: `${memberCount} members · ${row.display_row_key || row.row_key || row.interface_row_key || ""}`,
      color: cluster.color,
      coverageCount: memberCount,
      coveragePercent: Number(cluster.coveragePercent || 0),
      coverageFraction: Number(cluster.coverageFraction || 0),
      coverageLabel: `${roundedPercent(cluster.coveragePercent || 0)}%`,
      coverageTitle: `${memberCount}/${cluster.totalMemberCount || memberCount} clustered entries`,
      selectionRank: Number(cluster.selectionRank || 0),
      row,
      clusterSummary,
      representativePayload: payload,
    };
  }

  async function openRepresentativeClusterCompare(options = {}) {
    if (!interfaceSelect.value || typeof representativeClusterCompareSummaries !== "function") {
      return;
    }
    const method = normalizedRepresentativeMethod(options.method);
    const previousMode = state.clusterCompareMode;
    state.representativeClusterCompareMethod = method;
    state.clusterCompareMode = "representative-clusters";
    state.clusterCompareClusterLabel = null;
    syncClusterCompareHeaderControls();

    const clusters = representativeClusterCompareSummaries()
      .slice(0, 9)
      .map((cluster, index) => ({
        ...cluster,
        selectionRank: index,
      }));
    if (clusters.length === 0) {
      state.clusterCompareMode = previousMode;
      syncClusterCompareHeaderControls();
      return;
    }

    const requestId = state.clusterCompareRequestId + 1;
    state.clusterCompareRequestId = requestId;
    const preservedView = currentClusterCompareView();
    const keepExistingTilesDuringLoad = Boolean(
      previousMode === "representative-clusters" &&
      state.clusterCompareTiles.length > 0 &&
      elements.clusterCompareGrid?.children.length &&
      state.clusterCompareMode === "representative-clusters"
    );
    if (preservedView) {
      state.clusterCompareSharedView = preservedView;
    } else if (!keepExistingTilesDuringLoad) {
      state.clusterCompareSharedView = null;
    }
    if (!keepExistingTilesDuringLoad) {
      clearClusterCompareTiles({ resetSharedView: !preservedView });
    }

    const title = "Cluster Overview";
    const cacheKey = representativeClusterCompareCacheKey(method, clusters);
    if (elements.clusterCompareModalTitle) {
      elements.clusterCompareModalTitle.textContent = title;
    }
    if (elements.clusterCompareModalSubtitle) {
      elements.clusterCompareModalSubtitle.textContent =
        `Loading overview representatives selected by ${representativeMethodLabel(method)}...`;
    }
    openClusterCompareModal();
    const cachedRecord = options.force ? null : readClusterCompareCache(cacheKey);
    setClusterCompareLoading(
      true,
      cachedRecord ? "Restoring cached Cluster Overview..." : "Selecting cluster overview representatives...",
      cachedRecord ? 80 : 10
    );

    try {
      await nextBrowserPaint();
      if (cachedRecord) {
        if (requestId !== state.clusterCompareRequestId) {
          return;
        }
        await renderClusterCompareResults(cachedRecord);
        writeClusterCompareCache(cacheKey, renderedClusterCompareRecord(cachedRecord));
        return;
      }

      let completedSelections = 0;
      const selectedEntries = await Promise.all(
        clusters.map(async (cluster) => {
          try {
            return await fetchRepresentativeClusterSelection(cluster, method);
          } finally {
            completedSelections += 1;
            if (requestId === state.clusterCompareRequestId) {
              setClusterCompareLoading(
                true,
                `Selecting overview representatives (${completedSelections}/${clusters.length})...`,
                Math.round((completedSelections / clusters.length) * 35)
              );
            }
          }
        })
      );
      if (requestId !== state.clusterCompareRequestId) {
        return;
      }
      const record = {
        kind: "representative-clusters",
        title,
        subtitle:
          `Showing the ${selectedEntries.length} largest clusters. Representatives selected by ${representativeMethodLabel(method)}.`,
        representativeMethod: method,
        selectedEntries,
        clusterEntryCount: clusters.reduce(
          (sum, cluster) => sum + Number(cluster.compareMemberCount ?? cluster.memberCount ?? 0),
          0
        ),
        remainingEntryCount: clusters.reduce(
          (sum, cluster) => sum + Number(cluster.compareMemberCount ?? cluster.memberCount ?? 0),
          0
        ),
        distanceMetric: state.embeddingClusteringSettings.distance || "overlap",
        alignmentAnchorRowKey: selectedEntries[0]?.rowKey || "",
        results: [],
      };
      if (elements.clusterCompareModalSubtitle) {
        elements.clusterCompareModalSubtitle.textContent = clusterCompareSubtitle(record);
      }
      if (!keepExistingTilesDuringLoad) {
        prepareClusterCompareTiles(record);
      }

      const anchorRowKey = record.alignmentAnchorRowKey || selectedEntries[0]?.rowKey || "";
      let completedStructures = 0;
      const results = await Promise.all(
        selectedEntries.map(async (entry, index) => {
          let result;
          try {
            const structureResult = await fetchClusterCompareStructure(
              entry,
              entry.rowKey === anchorRowKey ? "" : anchorRowKey
            );
            const stylePayload = representativeClusterCompareTileStyles(
              entry.row,
              entry.clusterSummary
            );
            result = {
              ...structureResult,
              row: entry.row,
              representativePayload: entry.representativePayload,
              residueStyles: stylePayload.residueStyles || [],
              clusterLensData: stylePayload.clusterLensData || null,
            };
          } catch (error) {
            result = {
              entry,
              row: entry.row,
              representativePayload: entry.representativePayload,
              error: error.message,
            };
          } finally {
            completedStructures += 1;
            if (requestId === state.clusterCompareRequestId) {
              setClusterCompareLoading(
                true,
                `Loading aligned structures (${completedStructures}/${selectedEntries.length})...`,
                35 + Math.round((completedStructures / selectedEntries.length) * 65)
              );
            }
          }
          if (!keepExistingTilesDuringLoad && requestId === state.clusterCompareRequestId) {
            await applyClusterCompareTileResult(index, result);
          }
          return result;
        })
      );
      if (requestId !== state.clusterCompareRequestId) {
        return;
      }
      record.results = results.map((result) => ({
        entry: result.entry,
        row: result.row,
        representativePayload: result.representativePayload,
        payload: result.payload,
        modelText: result.modelText,
        previewUrl: result.previewUrl,
        residueStyles: result.residueStyles || [],
        clusterLensData: result.clusterLensData || null,
        error: result.error,
      }));
      if (keepExistingTilesDuringLoad) {
        await renderClusterCompareResults(record);
      } else {
        record.results = currentRenderedClusterCompareResults();
        state.clusterCompareAlignmentAnchorRowKey = record.alignmentAnchorRowKey || "";
        initializeClusterCompareSharedView({ nudge: true });
      }
      writeClusterCompareCache(cacheKey, renderedClusterCompareRecord(record));
    } catch (error) {
      if (requestId === state.clusterCompareRequestId) {
        if (elements.clusterCompareModalSubtitle) {
          elements.clusterCompareModalSubtitle.textContent =
            error.message || "Unable to load Cluster Overview.";
        }
        if (keepExistingTilesDuringLoad) {
          setClusterCompareGridLoading(false);
        } else {
          clearClusterCompareTiles({ resetSharedView: !preservedView });
          renderClusterCompareFatalError(error.message || "Unable to load Cluster Overview.");
        }
      }
      throw error;
    } finally {
      if (requestId === state.clusterCompareRequestId) {
        setClusterCompareLoading(false);
      }
    }
  }

  elements.clusterCompareRepresentativeMethodButton?.addEventListener("click", () => {
    setClusterCompareRepresentativeMethodMenuOpen(
      elements.clusterCompareRepresentativeMethodMenu?.classList.contains("hidden")
    );
  });

  elements.clusterCompareRepresentativeMethodMenu?.addEventListener("click", (event) => {
    if (event.target.closest(".representative-method-item-help")) {
      return;
    }
    const button = event.target.closest("[data-cluster-compare-representative-method]");
    if (!button) {
      return;
    }
    setClusterCompareRepresentativeMethodMenuOpen(false);
    const method = normalizedRepresentativeMethod(button.dataset.clusterCompareRepresentativeMethod);
    if (state.representativeClusterCompareMethod === method) {
      syncClusterCompareHeaderControls();
      return;
    }
    state.representativeClusterCompareMethod = method;
    syncClusterCompareHeaderControls();
    if (state.clusterCompareMode === "representative-clusters") {
      void openRepresentativeClusterCompare({ method, force: true });
    }
  });

  document.addEventListener("click", (event) => {
    if (!event.target.closest("#cluster-compare-representative-method-control")) {
      setClusterCompareRepresentativeMethodMenuOpen(false);
    }
  });

  return {
    closeClusterCompareModal,
    openClusterCompareForLabel,
    openRepresentativeClusterCompare,
    resizeClusterCompareViewers,
  };
}
