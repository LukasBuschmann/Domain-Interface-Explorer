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

  function clusterCompareCacheKey(clusterLabel) {
    return currentClusterCompareQuery(clusterLabel);
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
      results: state.clusterCompareTiles.map((tile) => ({
        entry: tile.entry,
        payload: tile.payload,
        modelText: tile.modelText,
        previewUrl: tile.previewUrl,
        error: tile.error,
      })),
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
  }

  function closeClusterCompareModal() {
    state.clusterCompareRequestId += 1;
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
      label: tile.entry?.rowKey || "Cluster comparison structure",
      mode: "compare",
      columnView: false,
      contactsVisible: false,
      residueLookup: new Map(),
      residueStyles: [],
      displaySettings: state.structureDisplaySettings,
    });
  }

  function initializeClusterCompareSharedView() {
    if (state.clusterCompareSharedView) {
      for (const tile of state.clusterCompareTiles) {
        if (!tile?.viewer || typeof tile.viewer.setView !== "function") {
          continue;
        }
        tile.viewer.setView(state.clusterCompareSharedView);
        tile.viewer.render();
      }
      return;
    }
    const anchorTile = state.clusterCompareTiles.find(
      (tile) => tile?.viewer && tile?.payload && !tile.error
    );
    if (!anchorTile?.viewer) {
      return;
    }
    const viewer = anchorTile.viewer;
    const domainSelection = clusterCompareDomainSelection(anchorTile.payload);
    if (typeof viewer.center === "function") {
      viewer.center(domainSelection);
    }
    if (typeof viewer.zoomTo === "function") {
      viewer.zoomTo(domainSelection, 8);
    }
    viewer.render();
    if (typeof viewer.getView === "function") {
      state.clusterCompareSharedView = viewer.getView();
    }
    if (!state.clusterCompareSharedView) {
      return;
    }
    for (const tile of state.clusterCompareTiles) {
      if (!tile?.viewer || tile === anchorTile || typeof tile.viewer.setView !== "function") {
        continue;
      }
      tile.viewer.setView(state.clusterCompareSharedView);
      tile.viewer.render();
    }
  }

  async function renderClusterCompareTile(tileIndex) {
    const tile = state.clusterCompareTiles[tileIndex];
    if (!tile || tile.error || !tile.viewerRoot) {
      return;
    }
    tile.viewerRoot.classList.remove("cluster-compare-tile-viewer-error");

    if (!tile.viewer) {
      tile.viewer = createDomainMolstarViewer(tile.viewerRoot, {
        kind: "cluster-compare",
      });
      tile.cleanupSync = bindClusterCompareViewerSync(tileIndex, tile.viewerRoot);
    }

    const viewer = tile.viewer;
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
      viewer.focusResidues(domainSelection.resi, 8);
    }
    viewer.render();
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
        const rowLabel = escapeHtml(entry.rowKey);
        const partnerLabel = escapeHtml(entry.partnerDomain);
        return `
          <article class="cluster-compare-tile" data-cluster-compare-index="${index}">
            <div class="cluster-compare-tile-viewer" data-cluster-compare-viewer="${index}"></div>
            <div class="cluster-compare-tile-meta">
              <div class="cluster-compare-tile-title-row">
                <span class="cluster-compare-tile-title">${rowLabel}</span>
                <button
                  class="cluster-compare-open-structure"
                  type="button"
                  data-cluster-compare-open="${index}"
                  title="Open in structure viewer"
                  aria-label="Open ${rowLabel} in structure viewer"
                >
                  ${fullScreenIconSvg()}
                </button>
                <span
                  class="cluster-compare-coverage"
                  title="${entry.coverageCount}/${remainingEntryCount} nearest cluster members"
                >
                  <span class="cluster-compare-coverage-track">
                    <span
                      class="cluster-compare-coverage-fill"
                      style="width: ${clampPercent(entry.coveragePercent)}%; background: ${coverageColor(entry.coveragePercent)};"
                    ></span>
                  </span>
                  <span class="cluster-compare-coverage-value">${roundedPercent(entry.coveragePercent)}%</span>
                </span>
              </div>
              <span class="cluster-compare-tile-subtitle">Partner ${partnerLabel}</span>
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

  async function renderClusterCompareResults(record) {
    if (elements.clusterCompareModalTitle) {
      elements.clusterCompareModalTitle.textContent = record.title;
    }
    if (elements.clusterCompareModalSubtitle) {
      elements.clusterCompareModalSubtitle.textContent = clusterCompareSubtitle(record);
    }
    state.clusterCompareAlignmentAnchorRowKey = record.alignmentAnchorRowKey || "";
    renderClusterCompareGridShell(record);
    setClusterCompareTilesFromResults(record.results || []);

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
    initializeClusterCompareSharedView();
  }

  async function openClusterCompareForLabel(clusterLabel, options = {}) {
    if (!interfaceSelect.value || !state.embeddingClustering?.points?.length) {
      return;
    }
    const reroll = Boolean(options.reroll);
    const targetClusterLabel = Number(clusterLabel);
    const previousClusterLabel = state.clusterCompareClusterLabel;
    const requestId = state.clusterCompareRequestId + 1;
    state.clusterCompareRequestId = requestId;
    state.clusterCompareClusterLabel = targetClusterLabel;
    const preservedView = currentClusterCompareView();
    const keepExistingTilesDuringLoad = Boolean(
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
        renderClusterCompareGridShell(record);
      }

      let completedStructures = 0;
      const results = await Promise.all(
        selectedEntries.map(async (entry) => {
          try {
            return await fetchClusterCompareStructure(
              entry,
              entry.rowKey === anchorRowKey ? "" : anchorRowKey
            );
          } catch (error) {
            return {
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
      await renderClusterCompareResults(record);
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

  return {
    closeClusterCompareModal,
    openClusterCompareForLabel,
    resizeClusterCompareViewers,
  };
}
