// @ts-nocheck
import { fetchJson, fetchText } from "./api.js";
import { interfaceFilePfamId } from "./interfaceModel.js";
import { appendSelectionSettingsToParams } from "./selectionSettings.js";
import { createDomainMolstarViewer } from "./molstarView.js";

export function createStructureViewController({
  state,
  elements,
  THREE_TO_ONE,
  interfaceSelect,
  setLoading,
  hideLoading,
  buildStructureResidueLookup,
  columnResidueStyles,
  structureMarkerResidueStyles = () => [],
  msaColumnMaxIndex,
  topResiduesForColumn,
  columnStateDistribution,
  syncColumnLegends,
  getSelectedRow,
  getStructurePreloadRows,
  clearEmbeddingMemberSelection,
}) {
  const STRUCTURE_PREVIEW_CACHE_LIMIT = 40;
  const STRUCTURE_MODEL_TEXT_CACHE_LIMIT = 24;
  const STRUCTURE_PRELOAD_CONCURRENCY = 1;
  const structurePreviewInFlight = new Map();
  const structureModelTextInFlight = new Map();

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

  function renderStructureHeader(row, payload) {
    const title = elements.structureModalTitle;
    const uniprotId = String(payload?.uniprot_id || row?.protein_id || "").trim();
    const mainPfamId = currentPfamId();
    const partnerId = partnerPfamId(row, payload);
    title.replaceChildren();

    if (!uniprotId && !mainPfamId && !partnerId) {
      title.textContent = "Structure";
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
    title.appendChild(content);
  }

  function structureRowKey(row) {
    return String(row?.interface_row_key || row?.row_key || "");
  }

  function structurePartnerForRow(row) {
    return row?.partner_domain || "__all__";
  }

  function structureRowLabel(row) {
    return row?.display_row_key || row?.row_key || "";
  }

  function structureModalIsOpen() {
    return !elements.structureModal.classList.contains("hidden");
  }

  function readCacheValue(cache, key) {
    if (!cache || !key || typeof cache.get !== "function") {
      return null;
    }
    const value = cache.get(key);
    if (value !== undefined && typeof cache.delete === "function" && typeof cache.set === "function") {
      cache.delete(key);
      cache.set(key, value);
    }
    return value ?? null;
  }

  function writeCacheValue(cache, key, value, limit) {
    if (!cache || !key || typeof cache.set !== "function") {
      return;
    }
    if (typeof cache.delete === "function") {
      cache.delete(key);
    }
    cache.set(key, value);
    while (Number.isFinite(limit) && cache.size > limit) {
      const oldestKey = cache.keys().next().value;
      cache.delete(oldestKey);
    }
  }

  function alignmentReferenceRowKeyFor(row) {
    const requestRowKey = structureRowKey(row);
    return state.structureAnchorRowKey && state.structureAnchorRowKey !== requestRowKey
      ? state.structureAnchorRowKey
      : "";
  }

  function structurePreviewUrlForRow(row) {
    const params = new URLSearchParams({
      interface_file: interfaceSelect.value,
      row_key: structureRowKey(row),
      uniprot_id: String(row.protein_id || ""),
      fragment_key: String(row.fragment_key || ""),
      partner: String(structurePartnerForRow(row)),
    });
    appendSelectionSettingsToParams(params, state.selectionSettings);
    const alignmentReferenceRowKey = alignmentReferenceRowKeyFor(row);
    if (alignmentReferenceRowKey) {
      params.set("align_to_row_key", alignmentReferenceRowKey);
    }
    return `/api/structure-preview?${params.toString()}`;
  }

  function structureModelKey(row, payload) {
    return [
      row?.row_key || structureRowKey(row),
      payload?.model_url || "",
      payload?.alignment_reference_row_key || "",
      payload?.alignment_method || "",
    ].join("|");
  }

  function copyStructureView(view) {
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

  async function loadStructurePreviewPayload(previewUrl, options = {}) {
    const cachedStructure = readCacheValue(state.structurePreviewCache, previewUrl);
    if (cachedStructure?.payload) {
      return cachedStructure.payload;
    }
    const inFlight = structurePreviewInFlight.get(previewUrl);
    if (inFlight) {
      return inFlight;
    }
    const request = fetchJson(previewUrl, options)
      .then((payload) => {
        writeCacheValue(
          state.structurePreviewCache,
          previewUrl,
          { payload },
          STRUCTURE_PREVIEW_CACHE_LIMIT
        );
        return payload;
      })
      .finally(() => {
        structurePreviewInFlight.delete(previewUrl);
      });
    structurePreviewInFlight.set(previewUrl, request);
    return request;
  }

  async function loadStructureModelText(modelUrl, options = {}) {
    const cachedModelText = readCacheValue(state.structureModelTextCache, modelUrl);
    if (typeof cachedModelText === "string") {
      return cachedModelText;
    }
    const inFlight = structureModelTextInFlight.get(modelUrl);
    if (inFlight) {
      return inFlight;
    }
    const request = fetchText(modelUrl, options)
      .then((modelText) => {
        writeCacheValue(
          state.structureModelTextCache,
          modelUrl,
          modelText,
          STRUCTURE_MODEL_TEXT_CACHE_LIMIT
        );
        return modelText;
      })
      .finally(() => {
        structureModelTextInFlight.delete(modelUrl);
      });
    structureModelTextInFlight.set(modelUrl, request);
    return request;
  }

  function cacheLoadedStructure(previewUrl, payload, modelText) {
    if (previewUrl && payload) {
      writeCacheValue(
        state.structurePreviewCache,
        previewUrl,
        { payload },
        STRUCTURE_PREVIEW_CACHE_LIMIT
      );
    }
    if (payload?.model_url && typeof modelText === "string") {
      writeCacheValue(
        state.structureModelTextCache,
        payload.model_url,
        modelText,
        STRUCTURE_MODEL_TEXT_CACHE_LIMIT
      );
    }
  }

  function modelFileLabel(payload, row) {
    const modelUrl = String(payload?.model_url || "").trim();
    if (modelUrl) {
      const filename = modelUrl.split("/").pop();
      if (filename) {
        return decodeURIComponent(filename);
      }
    }
    const modelSource = String(payload?.model_source || "").trim();
    if (modelSource) {
      return modelSource;
    }
    const proteinId = String(row?.protein_id || "").trim();
    const fragmentKey = String(row?.fragment_key || "").trim();
    return [proteinId, fragmentKey].filter(Boolean).join(" ") || structureRowLabel(row) || "structure";
  }

  function setStructureLoadingUi(isLoading, label = "", detail = "") {
    const displayLabel = detail ? `${label}: ${detail}` : label;
    elements.structureLoadingBadge?.classList.toggle("hidden", !isLoading);
    elements.structureLoadingBadge?.setAttribute("aria-hidden", isLoading ? "false" : "true");
    elements.structureLoadingOverlay?.classList.toggle("hidden", !isLoading);
    elements.structureLoadingOverlay?.setAttribute("aria-hidden", isLoading ? "false" : "true");
    if (isLoading) {
      if (elements.structureLoadingBadgeLabel) {
        elements.structureLoadingBadgeLabel.textContent = label || "Loading structure";
      }
      if (elements.structureLoadingOverlayLabel) {
        elements.structureLoadingOverlayLabel.textContent = displayLabel || "Loading structure";
      }
    }
  }

  function renderStructureLoadingState(row, label, detail = "") {
    renderStructureHeader(row, {
      uniprot_id: row?.protein_id || "",
      partner: structurePartnerForRow(row),
      matched_partners: structurePartnerForRow(row) === "__all__" ? [] : [structurePartnerForRow(row)],
    });
    const rowLabel = structureRowLabel(row);
    elements.structureModalSubtitle.textContent = detail
      ? `Loading ${detail}`
      : `Loading ${rowLabel || "structure"}`;
    elements.structureStatus.textContent = detail ? `${label}: ${detail}` : label;
    elements.structureModalStatus.textContent = label;
    setStructureLoadingUi(true, label, detail || rowLabel);
  }

  function stopStructurePreloading() {
    state.structurePreloadGeneration += 1;
  }

  function stopForegroundStructureLoad() {
    state.structureRequestId += 1;
  }

  function structurePreloadRowsAfter(activeRow) {
    const preloadRows = getStructurePreloadRows?.();
    const rows = Array.isArray(preloadRows) ? preloadRows : [];
    if (rows.length <= 1) {
      return [];
    }
    const activeKey = String(activeRow?.row_key || "");
    const activeIndex = rows.findIndex((row) => String(row?.row_key || "") === activeKey);
    const ordered =
      activeIndex >= 0
        ? rows.slice(activeIndex + 1).concat(rows.slice(0, activeIndex))
        : rows;
    const seen = new Set();
    return ordered.filter((row) => {
      const rowKey = String(row?.row_key || "");
      if (!rowKey || rowKey === activeKey || seen.has(rowKey)) {
        return false;
      }
      seen.add(rowKey);
      return true;
    });
  }

  async function preloadStructureRow(row, generation) {
    const previewUrl = structurePreviewUrlForRow(row);
    const payload = await loadStructurePreviewPayload(previewUrl);
    if (generation !== state.structurePreloadGeneration || !structureModalIsOpen()) {
      return;
    }
    await loadStructureModelText(payload.model_url);
  }

  async function preloadStructureRows(rows, generation) {
    let nextIndex = 0;
    const worker = async () => {
      while (generation === state.structurePreloadGeneration && structureModalIsOpen()) {
        const row = rows[nextIndex];
        nextIndex += 1;
        if (!row) {
          return;
        }
        try {
          await preloadStructureRow(row, generation);
        } catch (error) {
          if (generation !== state.structurePreloadGeneration || !structureModalIsOpen()) {
            return;
          }
        }
      }
    };
    await Promise.all(
      Array.from(
        { length: Math.min(STRUCTURE_PRELOAD_CONCURRENCY, rows.length) },
        () => worker()
      )
    );
  }

  function startStructurePreloading(activeRow) {
    stopStructurePreloading();
    if (!structureModalIsOpen()) {
      return;
    }
    const rows = structurePreloadRowsAfter(activeRow);
    if (rows.length === 0) {
      return;
    }
    const generation = state.structurePreloadGeneration;
    void preloadStructureRows(rows, generation)
      .catch((error) => {
        if (generation === state.structurePreloadGeneration && structureModalIsOpen()) {
          console.debug("[structure-preload] stopped", error);
        }
      });
  }

  function resetStructurePanel(message = "Click a row name or use the button to open the structure.") {
    setStructureLoadingUi(false);
    elements.structureModalTitle.textContent = "Structure";
    elements.structureStatus.textContent = message;
    elements.structureModalSubtitle.textContent = message;
    elements.structureModalStatus.textContent =
      "Whole protein: gray transparent. Main domain: gray. Main surface/interface: orange and red. Partner domain: muted blue with stronger blue interaction layers.";
    state.structureResidueLookup = null;
    state.structureData = null;
    state.structureRenderedModelKey = null;
    elements.structureHoverCard.classList.add("hidden");
    setStructureHoverDetails(null);
    setStructureHoverHistogram(null);
    setStructureHoverDistribution(null);
    syncColumnLegends();
  }

  function handleStructureLoadFailure(error) {
    setStructureLoadingUi(false);
    elements.loadingPanel.classList.remove("hidden");
    elements.loadingLabel.textContent = "Structure load failed";
    elements.loadingDetail.textContent = error.message;
    elements.progressBar.style.width = "100%";
    elements.structureModalTitle.textContent = "Structure";
    elements.structureStatus.textContent = error.message;
    elements.structureModalSubtitle.textContent = error.message;
  }

  function setStructureHoverDetails(payload) {
    const values = payload || {
      residueId: "-",
      aminoAcid: "-",
      conservedness: "-",
      columnIndex: null,
    };
    const items = [
      values.residueId,
      values.aminoAcid,
      values.conservedness,
      values.columnIndex === null || values.columnIndex === undefined ? "-" : values.columnIndex,
    ];
    [...elements.structureHoverDetails.querySelectorAll("dd")].forEach((el, index) => {
      el.textContent = String(items[index]);
    });
  }

  function setStructureHoverHistogram(entries) {
    elements.structureHoverHistogram.innerHTML = "";
    if (!entries?.length) {
      elements.structureHoverHistogram.innerHTML =
        '<p class="structure-hover-empty">No residue frequencies for this column.</p>';
      return;
    }

    const maxPercent = Math.max(...entries.map((entry) => entry.percent), 1);
    for (const entry of entries) {
      const row = document.createElement("div");
      row.className = `structure-hist-row${entry.isSelected ? " selected" : ""}`;
      const label = document.createElement("span");
      label.className = "structure-hist-label";
      label.textContent = entry.residue;
      const bar = document.createElement("div");
      bar.className = "structure-hist-bar";
      const fill = document.createElement("div");
      fill.className = "structure-hist-fill";
      fill.style.width = `${Math.max(4, (entry.percent / maxPercent) * 100)}%`;
      const value = document.createElement("span");
      value.className = "structure-hist-value";
      value.textContent = `${entry.percent}%`;
      bar.append(fill);
      row.append(label, bar, value);
      elements.structureHoverHistogram.append(row);
    }
  }

  function setStructureHoverDistribution(entries) {
    elements.structureHoverDistributionChart.innerHTML = "";
    elements.structureHoverDistributionLegend.innerHTML = "";
    if (!entries?.length) {
      elements.structureHoverDistributionChart.style.background = "none";
      elements.structureHoverDistributionLegend.innerHTML =
        '<p class="structure-hover-empty">No interaction-state data for this column.</p>';
      return;
    }

    const total = entries.reduce((sum, entry) => sum + entry.count, 0);
    const slices = [];
    let offset = 0;
    for (const entry of entries) {
      const share = total > 0 ? (entry.count / total) * 100 : 0;
      slices.push(`${entry.color} ${offset}% ${offset + share}%`);
      offset += share;

      const item = document.createElement("div");
      item.className = "structure-distribution-row";
      item.innerHTML = `
        <span class="structure-distribution-swatch" style="background:${entry.color}"></span>
        <span class="structure-distribution-label">${entry.label}</span>
        <span class="structure-distribution-value">${entry.percent}%</span>
      `;
      elements.structureHoverDistributionLegend.append(item);
    }
    elements.structureHoverDistributionChart.style.background = `conic-gradient(${slices.join(", ")})`;
  }

  function openStructureModal() {
    elements.structureModal.classList.remove("hidden");
    elements.structureModal.setAttribute("aria-hidden", "false");
  }

  function closeStructureModal() {
    stopStructurePreloading();
    stopStructurePreloading();
    stopForegroundStructureLoad();
    elements.structureModal.classList.add("hidden");
    elements.structureModal.setAttribute("aria-hidden", "true");
    setStructureLoadingUi(false);
    clearEmbeddingMemberSelection?.();
  }

  function getStructureViewer() {
    if (!state.structureViewer) {
      state.structureViewer = createDomainMolstarViewer(elements.structureViewerRoot, {
        kind: "structure",
      });
    }
    return state.structureViewer;
  }

  function recenterStructureDomain() {
    const structure = state.structureData;
    if (!structure?.payload || !state.structureViewer) {
      return;
    }
    const residues = mainFragmentResidues(structure.payload);
    if (typeof state.structureViewer.focusResiduesStable === "function") {
      state.structureViewer.focusResiduesStable(residues, 8);
    } else {
      state.structureViewer.resize?.();
      state.structureViewer.focusResidues?.(residues, 8);
      state.structureViewer.render?.();
    }
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

  function residueContactPairs(structurePayload) {
    const contacts = Array.isArray(structurePayload?.residue_contacts)
      ? structurePayload.residue_contacts
      : [];
    const pairs = [];
    const seen = new Set();
    for (const contact of contacts) {
      if (!Array.isArray(contact) || contact.length < 2) {
        continue;
      }
      const mainResidueId = Number.parseInt(contact[0], 10);
      const partnerResidueId = Number.parseInt(contact[1], 10);
      if (!Number.isFinite(mainResidueId) || !Number.isFinite(partnerResidueId)) {
        continue;
      }
      const key = `${mainResidueId}:${partnerResidueId}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      pairs.push([mainResidueId, partnerResidueId]);
    }
    return pairs;
  }

  function formatStructureHover(hover) {
    const residueId = Number(hover?.residueId);
    const mapped = state.structureResidueLookup?.get(residueId);
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
      residueLetter: oneLetter,
    };
  }

  function handleStructureHover(hoverPayload) {
    const residueId = Number(hoverPayload?.residueId);
    if (!Number.isFinite(residueId) || !state.structureResidueLookup?.has(residueId)) {
      clearStructureHover();
      return;
    }
    const hover = formatStructureHover(hoverPayload);
    elements.structureHoverCard.classList.remove("hidden");
    setStructureHoverDetails(hover);
    setStructureHoverHistogram(topResiduesForColumn(hover.columnIndex, hover.residueLetter));
    setStructureHoverDistribution(columnStateDistribution(hover.columnIndex));
  }

  function clearStructureHover() {
    elements.structureHoverCard.classList.add("hidden");
    setStructureHoverDetails(null);
    setStructureHoverHistogram(null);
    setStructureHoverDistribution(null);
  }

  async function renderInteractiveStructure() {
    const structure = state.structureData;
    if (!structure) {
      return;
    }

    const { row, payload, modelText } = structure;
    const viewer = getStructureViewer();
    const currentModelKey = structure.modelKey || structureModelKey(row, payload);
    const shouldPreserveView =
      Boolean(state.structureRenderedModelKey) &&
      typeof viewer.getView === "function" &&
      typeof viewer.setView === "function" &&
      currentModelKey === state.structureRenderedModelKey;
    const initialView = copyStructureView(structure.initialView);
    const previousView = initialView || (shouldPreserveView ? copyStructureView(viewer.getView()) : null);
    state.structureResidueLookup = buildStructureResidueLookup(row);
    const residueStyles = columnResidueStyles(state.structureResidueLookup);
    const markerResidueStyles = structureMarkerResidueStyles(state.structureResidueLookup);
    await viewer.loadStructure({
      modelText,
      payload,
      format: payload.model_format || "pdb",
      label: structureRowLabel(row) || "Structure",
      mode: "structure",
      columnView: state.structureColumnView,
      contactsVisible: state.structureContactsVisible,
      residueLookup: state.structureResidueLookup,
      residueStyles,
      markerResidueStyles,
      displaySettings: state.structureDisplaySettings,
      onHover: handleStructureHover,
      onHoverEnd: clearStructureHover,
    });
    viewer.resize();
    const domainSelection = { resi: mainFragmentResidues(payload) };
    if (previousView) {
      viewer.setView(copyStructureView(previousView));
    } else {
      if (typeof viewer.focusResiduesStable === "function") {
        viewer.focusResiduesStable(domainSelection.resi, 8);
      } else {
        viewer.focusResidues(domainSelection.resi, 8);
      }
    }
    viewer.render();
    if (initialView && typeof viewer.setView === "function") {
      const applyInitialView = () => {
        if (state.structureData?.modelKey !== currentModelKey) {
          return;
        }
        viewer.resize();
        viewer.setView(copyStructureView(initialView));
        viewer.render();
      };
      window.requestAnimationFrame(() => {
        applyInitialView();
        window.requestAnimationFrame(applyInitialView);
      });
    }
    state.structureRenderedRowKey = row.row_key;
    state.structureRenderedModelKey = currentModelKey;
    if (state.structureData === structure) {
      state.structureData.initialView = null;
      state.structureData.modelKey = currentModelKey;
    }
    if (!state.structureAnchorRowKey) {
      state.structureAnchorRowKey = payload.alignment_reference_row_key || structureRowKey(row);
    }

    const alignmentNote = payload.alignment_reference_row_key
      ? ` | Aligned to reference structure ${payload.alignment_reference_row_key} (${payload.alignment_method || "alignment"}).`
      : payload.alignment_error
        ? ` | Alignment fallback: ${payload.alignment_error}`
        : "";
    const lensNote = state.structureColumnView
      ? ` | Main domain hues follow MSA columns 0-${msaColumnMaxIndex()}.`
      : "";
    const markerNote = markerResidueStyles.length > 0
      ? " | Selected column residue is marked green."
      : "";
    const markerStatusNote = markerResidueStyles.length > 0
      ? " Selected column residue is marked green."
      : "";
    elements.structureStatus.textContent =
      `Interactive structure ready for ${structureRowLabel(row)}. Partners: ${payload.matched_partners.join(", ") || "none"}${lensNote}${markerNote}${alignmentNote}`;
    renderStructureHeader(row, payload);
    const partnerRanges = payload.partner_fragment_ranges?.join(", ") || "none";
    elements.structureModalSubtitle.textContent =
      `fragment ${payload.fragment_key} | ` +
      `partner range: ${partnerRanges}` +
      `${payload.matched_partners.join(", ") ? ` | partners: ${payload.matched_partners.join(", ")}` : ""}` +
      `${alignmentNote}`;
    elements.structureModalStatus.textContent = state.structureColumnView
      ? `Whole protein: gray transparent. Main domain: rainbow by MSA column 0-${msaColumnMaxIndex()}. Partner domain keeps the blue context layers.${markerStatusNote}`
      : `Main interface: ${payload.interface_residue_ids.length} | ` +
        `Main surface: ${payload.surface_residue_ids.length} | ` +
        `Partner interface: ${payload.partner_interface_residue_ids.length} | ` +
        `Partner surface: ${payload.partner_surface_residue_ids.length} | ` +
        `Contacts: ${residueContactPairs(payload).length} | ` +
        `AlphaFold: ${payload.model_source || "unknown"}${markerNote}`;
    syncColumnLegends();
  }

  async function renderLoadedStructure(row, payload, modelText, options = {}) {
    if (!row || !payload || typeof modelText !== "string") {
      return;
    }
    stopForegroundStructureLoad();
    stopStructurePreloading();
    cacheLoadedStructure(options.previewUrl || "", payload, modelText);
    state.structureData = {
      row,
      payload,
      modelText,
      initialView: options.initialView || null,
      modelKey: options.modelKey || structureModelKey(row, payload),
    };
    openStructureModal();
    setStructureLoadingUi(true, "Rendering structure", modelFileLabel(payload, row));
    await renderInteractiveStructure();
    setStructureLoadingUi(false);
    startStructurePreloading(row);
    setLoading(100, "Structure ready", structureRowLabel(row));
    window.setTimeout(hideLoading, 250);
  }

  async function loadInteractiveStructure() {
    const row = getSelectedRow();
    if (!row || !interfaceSelect.value) {
      return;
    }

    stopForegroundStructureLoad();
    const requestId = state.structureRequestId + 1;
    state.structureRequestId = requestId;
    const previewUrl = structurePreviewUrlForRow(row);
    const preliminaryLabel = modelFileLabel(null, row);
    setLoading(10, "Loading structure", `Preparing ${preliminaryLabel}`);
    renderStructureLoadingState(row, "Fetching structure from AlphaFold", preliminaryLabel);
    openStructureModal();
    await new Promise((resolve) => window.requestAnimationFrame(resolve));

    const payload = await loadStructurePreviewPayload(previewUrl);
    if (requestId !== state.structureRequestId || !structureModalIsOpen()) {
      return;
    }

    const modelLabel = modelFileLabel(payload, row);
    renderStructureHeader(row, payload);
    elements.structureModalSubtitle.textContent = `Loading ${modelLabel}`;
    const hasCachedModelText = typeof readCacheValue(state.structureModelTextCache, payload.model_url) === "string";
    let modelText = "";
    if (hasCachedModelText) {
      elements.structureModalStatus.textContent = "Using cached structure model";
      setStructureLoadingUi(true, "Using cached structure model", modelLabel);
      setLoading(72, "Loading structure", `Using cached model for ${modelLabel}`);
    } else {
      elements.structureModalStatus.textContent = "Downloading structure model";
      setStructureLoadingUi(true, "Downloading structure model", modelLabel);
      setLoading(50, "Loading structure", `Fetching model for ${modelLabel}`);
    }
    modelText = await loadStructureModelText(payload.model_url);
    if (requestId !== state.structureRequestId || !structureModalIsOpen()) {
      return;
    }

    const renderLabel = modelFileLabel(payload, row);
    elements.structureModalStatus.textContent = "Rendering structure";
    setStructureLoadingUi(true, "Rendering structure", renderLabel);
    setLoading(80, "Rendering structure", `Applying cartoon styles for ${structureRowLabel(row)}`);
    state.structureData = {
      row,
      payload,
      modelText,
      modelKey: structureModelKey(row, payload),
    };
    await renderInteractiveStructure();
    setStructureLoadingUi(false);
    startStructurePreloading(row);
    setLoading(100, "Structure ready", structureRowLabel(row));
    window.setTimeout(hideLoading, 250);
  }

  return {
    closeStructureModal,
    getStructureViewer,
    handleStructureLoadFailure,
    loadInteractiveStructure,
    openStructureModal,
    recenterStructureDomain,
    renderLoadedStructure,
    renderInteractiveStructure,
    resetStructurePanel,
  };
}
