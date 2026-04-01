import { fetchJson, fetchText } from "./api.js";
import { interactionRowKey } from "./interfaceModel.js";
import { appendSelectionSettingsToParams } from "./selectionSettings.js";

export function createClusterCompareController({
  state,
  elements,
  interfaceSelect,
  currentClusterCompareQuery,
  getRowByKey,
  embeddingClusterLabel,
  embeddingDistanceLabel,
  nextBrowserPaint,
  applyStructureStyles,
}) {
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

  function clearClusterCompareTiles() {
    for (const tile of state.clusterCompareTiles) {
      tile.cleanupSync?.();
      tile.viewer?.clear?.();
    }
    state.clusterCompareTiles = [];
    state.clusterCompareSharedView = null;
    state.clusterComparePendingSyncIndex = null;
    state.clusterCompareSyncing = false;
    if (state.clusterCompareSyncFrame) {
      window.cancelAnimationFrame(state.clusterCompareSyncFrame);
      state.clusterCompareSyncFrame = 0;
    }
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

  function openClusterCompareModal() {
    elements.clusterCompareModal?.classList.remove("hidden");
    elements.clusterCompareModal?.setAttribute("aria-hidden", "false");
  }

  function closeClusterCompareModal() {
    state.clusterCompareRequestId += 1;
    state.clusterCompareClusterLabel = null;
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

  function initializeClusterCompareSharedView() {
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

  function renderClusterCompareTile(tileIndex) {
    const tile = state.clusterCompareTiles[tileIndex];
    if (!tile || tile.error || !tile.viewerRoot || !window.$3Dmol) {
      return;
    }

    if (!tile.viewer) {
      tile.viewer = window.$3Dmol.createViewer(tile.viewerRoot, {
        backgroundColor: "white",
      });
      tile.cleanupSync = bindClusterCompareViewerSync(tileIndex, tile.viewerRoot);
    }

    const viewer = tile.viewer;
    const { payload, modelText } = tile;
    viewer.clear();
    viewer.addModel(modelText, payload.model_format || "pdb");
    applyStructureStyles(viewer, payload, {
      columnView: false,
      residueLookup: new Map(),
    });
    viewer.resize();

    const domainSelection = clusterCompareDomainSelection(payload);
    if (state.clusterCompareSharedView && typeof viewer.setView === "function") {
      viewer.setView(state.clusterCompareSharedView);
    } else {
      if (typeof viewer.center === "function") {
        viewer.center(domainSelection);
      }
      viewer.zoomTo(domainSelection, 8);
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
    const payload = await fetchJson(`/api/structure-preview?${params.toString()}`);
    const modelText = await fetchText(payload.model_url);
    return {
      entry,
      payload,
      modelText,
    };
  }

  async function fetchClusterCompareData(clusterLabel) {
    const payload = await fetchJson(currentClusterCompareQuery(clusterLabel));
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

  async function openClusterCompareForLabel(clusterLabel) {
    if (!interfaceSelect.value || !state.embeddingClustering?.points?.length) {
      return;
    }
    const requestId = state.clusterCompareRequestId + 1;
    state.clusterCompareRequestId = requestId;
    state.clusterCompareClusterLabel = Number(clusterLabel);
    state.clusterCompareSharedView = null;
    clearClusterCompareTiles();
    openClusterCompareModal();

    if (elements.clusterCompareModalTitle) {
      elements.clusterCompareModalTitle.textContent =
        `${embeddingClusterLabel(clusterLabel)} Structure Comparison`;
    }
    setClusterCompareLoading(true, "Selecting diverse interfaces...", 12);
    try {
      await nextBrowserPaint();
      const clusterCompareData = await fetchClusterCompareData(clusterLabel);
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
        return;
      }
      if (elements.clusterCompareModalSubtitle) {
        elements.clusterCompareModalSubtitle.textContent =
          `Showing ${selectedEntries.length} of ${clusterEntryCount} cluster entries, selected by greedy max-min ${embeddingDistanceLabel(distanceMetric)} distance from a random cluster start, ordered by nearest-neighbor coverage on the ${remainingEntryCount} remaining entries.`;
      }
      const anchorRowKey = selectedEntries[0]?.rowKey || "";
      setClusterCompareLoading(
        true,
        `Loading aligned structures (0/${selectedEntries.length})...`,
        1
      );
      applyClusterCompareGridLayout(selectedEntries.length);

      elements.clusterCompareGrid.innerHTML = selectedEntries
        .map(
          (entry, index) => `
            <article class="cluster-compare-tile" data-cluster-compare-index="${index}">
              <div class="cluster-compare-tile-viewer" data-cluster-compare-viewer="${index}"></div>
              <div class="cluster-compare-tile-meta">
                <div class="cluster-compare-tile-title-row">
                  <span class="cluster-compare-tile-title">${entry.rowKey}</span>
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
                <span class="cluster-compare-tile-subtitle">Partner ${entry.partnerDomain}</span>
              </div>
            </article>
          `
        )
        .join("");

      let completedStructures = 0;
      const results = await Promise.all(
        selectedEntries.map(async (entry, index) => {
          try {
            return await fetchClusterCompareStructure(entry, index === 0 ? "" : anchorRowKey);
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

      state.clusterCompareTiles = results.map((result, index) => ({
        ...result,
        viewer: null,
        cleanupSync: null,
        viewerRoot: elements.clusterCompareGrid.querySelector(
          `[data-cluster-compare-viewer="${index}"]`
        ),
      }));

      for (let index = 0; index < state.clusterCompareTiles.length; index += 1) {
        const tile = state.clusterCompareTiles[index];
        if (tile.error) {
          continue;
        }
        renderClusterCompareTile(index);
      }
      initializeClusterCompareSharedView();
    } catch (error) {
      if (requestId === state.clusterCompareRequestId) {
        closeClusterCompareModal();
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
