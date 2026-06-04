import { fetchJson, fetchText } from "./api.js";
import { interactionRowKey, interfaceFilePfamId, parseInteractionRowKey } from "./interfaceModel.js";
import { appendSelectionSettingsToParams } from "./selectionSettings.js";
import { createDomainMolstarViewer } from "./molstarView.js";
export function createStructureViewController({ state, elements, THREE_TO_ONE, interfaceSelect, setLoading, hideLoading, buildStructureResidueLookup, columnResidueStyles, structureMarkerResidueStyles = () => [], msaColumnMaxIndex, topResiduesForColumn, columnStateDistribution, syncColumnLegends, getSelectedRow, getStructurePreloadRows, clearEmbeddingMemberSelection, partnerColor = () => "#817a71", onResidueClick = null, structureDisplaySettingsForView = () => state.structureDisplaySettings, }) {
    const STRUCTURE_PREVIEW_CACHE_LIMIT = 40;
    const STRUCTURE_MODEL_TEXT_CACHE_LIMIT = 24;
    const STRUCTURE_HMM_SCORE_CACHE_LIMIT = 24;
    const STRUCTURE_PRELOAD_CONCURRENCY = 1;
    const STRUCTURE_PRELOAD_LIMIT = 2;
    const STRUCTURE_PRELOAD_DELAY_MS = 750;
    const structurePreviewInFlight = new Map();
    const structureModelTextInFlight = new Map();
    const structureHmmScoresInFlight = new Map();
    let structurePreloadAbortController = null;
    let structurePreloadTimer = 0;
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
            content.appendChild(createExternalLink(uniprotId, uniprotEntryUrl(uniprotId), "structure-header-link structure-header-link-protein"));
        }
        if (mainPfamId) {
            if (content.childNodes.length > 0) {
                content.appendChild(document.createTextNode(" | "));
            }
            content.appendChild(createExternalLink(mainPfamId, pfamEntryUrl(mainPfamId), "structure-header-link structure-header-link-main-pfam"));
        }
        if (partnerId) {
            if (content.childNodes.length > 0) {
                content.appendChild(document.createTextNode(" | "));
            }
            content.appendChild(createExternalLink(partnerId, pfamEntryUrl(partnerId), "structure-header-link structure-header-link-partner-pfam"));
        }
        title.appendChild(content);
    }
    function structureRowKey(row) {
        return String(row?.interface_row_key || row?.row_key || "");
    }
    function structureInteractionRowKey(row) {
        const fullRowKey = String(row?.row_key || "");
        if (fullRowKey.includes("@@")) {
            return fullRowKey;
        }
        return interactionRowKey(structureRowKey(row), structurePartnerForRow(row));
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
    function structureHmmScoresUrlForRow(row) {
        const params = new URLSearchParams({
            interface_file: interfaceSelect.value,
            row_key: structureRowKey(row),
            partner: String(structurePartnerForRow(row)),
        });
        appendSelectionSettingsToParams(params, state.selectionSettings);
        return `/api/hmm-bit-scores?${params.toString()}`;
    }
    function structureModelKey(row, payload) {
        return [
            row?.row_key || structureRowKey(row),
            payload?.model_url || "",
            payload?.alignment_reference_row_key || "",
            payload?.alignment_method || "",
        ].join("|");
    }
    function structureModelIdentityKey(payload) {
        return [
            payload?.model_url || "",
            payload?.alignment_reference_row_key || "",
            payload?.alignment_method || "",
        ].join("|");
    }
    function syntheticStructureRowFromInteractionKey(rowKey) {
        const parsed = parseInteractionRowKey(rowKey);
        const interfaceRowKey = parsed.interfaceRowKey || String(rowKey || "");
        const partnerDomain = parsed.partnerDomain || "";
        if (!interfaceRowKey || !partnerDomain || !parsed.proteinId || !parsed.fragmentKey) {
            return null;
        }
        return {
            interface_row_key: interfaceRowKey,
            partner_domain: partnerDomain,
            row_key: interactionRowKey(interfaceRowKey, partnerDomain),
            display_row_key: `${parsed.proteinId} | ${partnerDomain}`,
            protein_id: parsed.proteinId,
            fragment_key: parsed.fragmentKey,
            alignment_fragment_key: parsed.fragmentKey,
            partner_fragment_key: parsed.partnerFragmentKey || "",
            interacting_fragment_key: parsed.partnerFragmentKey || "",
            aligned_sequence: "",
            residueIds: [],
            has_alignment: false,
            synthetic: true,
        };
    }
    function structurePartnerOptionLabel(row) {
        const fullRowKey = structureInteractionRowKey(row);
        const parsed = parseInteractionRowKey(fullRowKey);
        const partnerDomain = String(row?.partner_domain || parsed.partnerDomain || "");
        const partnerFragmentKey = String(row?.interacting_fragment_key || row?.partner_fragment_key || parsed.partnerFragmentKey || "");
        return [partnerDomain, partnerFragmentKey].filter(Boolean).join(" | ") || fullRowKey;
    }
    function structurePartnerDomain(row) {
        const parsed = parseInteractionRowKey(structureInteractionRowKey(row));
        const domain = String(row?.partner_domain || parsed.partnerDomain || "");
        return domain && domain !== "__all__" ? domain : "";
    }
    function applyStructurePartnerControlColor(row) {
        const control = elements.structurePartnerPicker;
        const select = elements.structurePartnerSelect;
        if (!control || !select) {
            return;
        }
        const domain = structurePartnerDomain(row);
        const color = domain ? partnerColor(domain) : "";
        if (color) {
            control.style.setProperty("--structure-partner-color", color);
            select.style.color = color;
        }
        else {
            control.style.removeProperty("--structure-partner-color");
            select.style.removeProperty("color");
        }
    }
    function structurePartnerCandidateRows(row) {
        const activeParsed = parseInteractionRowKey(structureInteractionRowKey(row));
        if (!activeParsed.proteinId || !activeParsed.fragmentKey) {
            return row ? [row] : [];
        }
        const byKey = new Map();
        const addCandidate = (candidate) => {
            if (!candidate) {
                return;
            }
            const fullRowKey = structureInteractionRowKey(candidate);
            const parsed = parseInteractionRowKey(fullRowKey);
            if (!fullRowKey ||
                !parsed.partnerDomain ||
                parsed.proteinId !== activeParsed.proteinId ||
                parsed.fragmentKey !== activeParsed.fragmentKey) {
                return;
            }
            if (!byKey.has(fullRowKey)) {
                byKey.set(fullRowKey, candidate);
            }
        };
        addCandidate(row);
        addCandidate(state.selectedRowSnapshot);
        addCandidate(state.structureData?.row);
        for (const candidate of state.msa?.rows || []) {
            addCandidate(candidate);
        }
        const overlayByRow = state.interface?.overlayByRow;
        if (overlayByRow && typeof overlayByRow.entries === "function") {
            for (const [interfaceRowKey, rowState] of overlayByRow.entries()) {
                const parsed = parseInteractionRowKey(interfaceRowKey);
                if (parsed.proteinId !== activeParsed.proteinId || parsed.fragmentKey !== activeParsed.fragmentKey) {
                    continue;
                }
                for (const partnerDomain of rowState?.byPartner?.keys?.() || []) {
                    addCandidate(syntheticStructureRowFromInteractionKey(interactionRowKey(interfaceRowKey, partnerDomain)));
                }
            }
        }
        return [...byKey.values()].sort((left, right) => structurePartnerOptionLabel(left).localeCompare(structurePartnerOptionLabel(right)));
    }
    function renderStructurePartnerControl(row) {
        const control = elements.structurePartnerPicker;
        const select = elements.structurePartnerSelect;
        if (!control || !select) {
            return;
        }
        const partnerRows = structurePartnerCandidateRows(row);
        state.structurePartnerRows = partnerRows;
        if (partnerRows.length <= 1) {
            control.classList.add("hidden");
            control.setAttribute("aria-hidden", "true");
            control.style.removeProperty("--structure-partner-color");
            select.style.removeProperty("color");
            select.replaceChildren();
            return;
        }
        select.replaceChildren();
        for (const partnerRow of partnerRows) {
            const option = document.createElement("option");
            option.value = structureInteractionRowKey(partnerRow);
            option.textContent = structurePartnerOptionLabel(partnerRow);
            const domain = structurePartnerDomain(partnerRow);
            if (domain) {
                option.style.color = partnerColor(domain);
            }
            select.appendChild(option);
        }
        const activeRowKey = structureInteractionRowKey(row);
        select.value = activeRowKey;
        applyStructurePartnerControlColor(row);
        control.classList.remove("hidden");
        control.setAttribute("aria-hidden", "false");
    }
    function copyStructureView(view) {
        if (!view) {
            return null;
        }
        if (typeof structuredClone === "function") {
            try {
                return structuredClone(view);
            }
            catch (_error) {
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
        const useSharedInFlight = !options?.signal;
        const inFlight = useSharedInFlight ? structurePreviewInFlight.get(previewUrl) : null;
        if (inFlight) {
            return inFlight;
        }
        const request = fetchJson(previewUrl, options)
            .then((payload) => {
            writeCacheValue(state.structurePreviewCache, previewUrl, { payload }, STRUCTURE_PREVIEW_CACHE_LIMIT);
            return payload;
        })
            .finally(() => {
            if (useSharedInFlight) {
                structurePreviewInFlight.delete(previewUrl);
            }
        });
        if (useSharedInFlight) {
            structurePreviewInFlight.set(previewUrl, request);
        }
        return request;
    }
    async function loadStructureModelText(modelUrl, options = {}) {
        const cachedModelText = readCacheValue(state.structureModelTextCache, modelUrl);
        if (typeof cachedModelText === "string") {
            return cachedModelText;
        }
        const useSharedInFlight = !options?.signal;
        const inFlight = useSharedInFlight ? structureModelTextInFlight.get(modelUrl) : null;
        if (inFlight) {
            return inFlight;
        }
        const request = fetchText(modelUrl, options)
            .then((modelText) => {
            writeCacheValue(state.structureModelTextCache, modelUrl, modelText, STRUCTURE_MODEL_TEXT_CACHE_LIMIT);
            return modelText;
        })
            .finally(() => {
            if (useSharedInFlight) {
                structureModelTextInFlight.delete(modelUrl);
            }
        });
        if (useSharedInFlight) {
            structureModelTextInFlight.set(modelUrl, request);
        }
        return request;
    }
    function cacheLoadedStructure(previewUrl, payload, modelText) {
        if (previewUrl && payload) {
            writeCacheValue(state.structurePreviewCache, previewUrl, { payload }, STRUCTURE_PREVIEW_CACHE_LIMIT);
        }
        if (payload?.model_url && typeof modelText === "string") {
            writeCacheValue(state.structureModelTextCache, payload.model_url, modelText, STRUCTURE_MODEL_TEXT_CACHE_LIMIT);
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
    function syncStructureHmmScoresButton(isLoading = false) {
        const button = elements.structureHmmScoresButton;
        if (!button) {
            return;
        }
        button.disabled = Boolean(isLoading) || !state.structureData;
        button.classList.toggle("loading", Boolean(isLoading));
        button.classList.toggle("active", !elements.structureHmmScoresPanel?.classList.contains("hidden"));
    }
    function closeStructureHmmScoresPanel() {
        elements.structureHmmScoresPanel?.classList.add("hidden");
        syncStructureHmmScoresButton(false);
    }
    function resetStructureHmmScoresPanel(options = {}) {
        state.structureHmmScoresRequestId += 1;
        state.structureHmmScoresPayload = null;
        elements.structureHmmScoresOutput?.replaceChildren();
        if (elements.structureHmmScoresStatus) {
            elements.structureHmmScoresStatus.textContent = options.message || "";
        }
        if (options.hide !== false) {
            elements.structureHmmScoresPanel?.classList.add("hidden");
        }
        syncStructureHmmScoresButton(false);
    }
    function setStructureHmmScoresStatus(message) {
        if (elements.structureHmmScoresStatus) {
            elements.structureHmmScoresStatus.textContent = message;
        }
    }
    function formatScoreValue(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "-";
        }
        return numeric >= 100 ? numeric.toFixed(1) : numeric.toFixed(2);
    }
    function formatEValue(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "-";
        }
        if (numeric === 0) {
            return "0";
        }
        if (numeric < 0.001 || numeric >= 1000) {
            return numeric.toExponential(1);
        }
        return numeric.toPrecision(3);
    }
    function hmmHeaderLabel(hmm) {
        const pfamId = String(hmm?.pfam_id || "");
        const name = String(hmm?.name || "");
        return name && name !== pfamId ? `${pfamId} ${name}` : pfamId;
    }
    function thresholdForPlot(hmm) {
        const thresholds = hmm?.thresholds || {};
        for (const [key, label] of [["ga", "GA"], ["tc", "TC"], ["nc", "NC"]]) {
            const threshold = thresholds[key];
            const sequence = Number(threshold?.sequence);
            if (Number.isFinite(sequence)) {
                return { key, label, value: sequence };
            }
        }
        return null;
    }
    function domainPlotClass(domain, index) {
        const key = String(domain?.key || "");
        if (key === "main") {
            return "main";
        }
        if (key === "partner") {
            return "partner";
        }
        return index % 2 === 0 ? "main" : "partner";
    }
    function domainPlotLabel(domain) {
        const label = String(domain?.label || domain?.key || "Domain");
        const pfamId = String(domain?.pfam_id || "");
        return pfamId ? `${label} (${pfamId})` : label;
    }
    function formatDeltaValue(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "-";
        }
        const formatted = formatScoreValue(Math.abs(numeric));
        return numeric >= 0 ? `+${formatted}` : `-${formatted}`;
    }
    function renderStructureHmmScorePlots(domains, hmms, scores) {
        const plotsRoot = document.createElement("div");
        plotsRoot.className = "structure-hmm-score-plots";
        for (const hmm of hmms) {
            const pfamId = String(hmm?.pfam_id || "");
            const threshold = thresholdForPlot(hmm);
            const plotScores = domains.map((domain, index) => {
                const score = scores?.[domain.key]?.[pfamId];
                const fullScore = Number(score?.full_score);
                const delta = threshold && Number.isFinite(fullScore) ? fullScore - threshold.value : NaN;
                return { domain, index, score, fullScore, delta };
            });
            const finiteDeltas = plotScores
                .map((item) => Math.abs(item.delta))
                .filter((value) => Number.isFinite(value));
            const maxAbsDelta = Math.max(1, ...finiteDeltas);
            const plot = document.createElement("section");
            plot.className = "structure-hmm-score-plot";
            const header = document.createElement("div");
            header.className = "structure-hmm-score-plot-header";
            const title = document.createElement("strong");
            title.textContent = hmmHeaderLabel(hmm);
            const baseline = document.createElement("span");
            baseline.textContent = threshold
                ? `${threshold.label} seq ${formatScoreValue(threshold.value)} = 0`
                : "threshold unavailable";
            header.append(title, baseline);
            plot.appendChild(header);
            const body = document.createElement("div");
            body.className = "structure-hmm-score-plot-body";
            for (const item of plotScores) {
                const row = document.createElement("div");
                row.className = `structure-hmm-score-bar-row ${domainPlotClass(item.domain, item.index)}`;
                const label = document.createElement("span");
                label.className = "structure-hmm-score-bar-label";
                label.textContent = domainPlotLabel(item.domain);
                const track = document.createElement("div");
                track.className = "structure-hmm-score-bar-track";
                const zero = document.createElement("span");
                zero.className = "structure-hmm-score-zero-line";
                track.appendChild(zero);
                if (threshold && Number.isFinite(item.delta)) {
                    const width = Math.min(50, Math.max(2, (Math.abs(item.delta) / maxAbsDelta) * 50));
                    const bar = document.createElement("span");
                    bar.className = `structure-hmm-score-bar ${item.delta < 0 ? "negative" : "positive"}`;
                    bar.style.width = `${width}%`;
                    bar.style.left = item.delta < 0 ? `${50 - width}%` : "50%";
                    track.appendChild(bar);
                }
                const value = document.createElement("span");
                value.className = "structure-hmm-score-bar-value";
                value.textContent = threshold && Number.isFinite(item.delta)
                    ? `${formatDeltaValue(item.delta)} (${formatScoreValue(item.fullScore)})`
                    : "No score";
                row.append(label, track, value);
                body.appendChild(row);
            }
            plot.appendChild(body);
            plotsRoot.appendChild(plot);
        }
        return plotsRoot;
    }
    function clampCoveragePercent(value) {
        const numeric = Number(value);
        return Number.isFinite(numeric) ? Math.max(0, Math.min(100, numeric)) : NaN;
    }
    function percentFromItem(item, percentKeys, fractionKeys, { clamp = true } = {}) {
        for (const key of percentKeys) {
            const percent = Number(item?.[key]);
            if (Number.isFinite(percent)) {
                return clamp ? clampCoveragePercent(percent) : percent;
            }
        }
        for (const key of fractionKeys) {
            const fraction = Number(item?.[key]);
            if (Number.isFinite(fraction)) {
                const percent = fraction * 100;
                return clamp ? clampCoveragePercent(percent) : percent;
            }
        }
        return NaN;
    }
    function coveragePercentForItem(item) {
        const explicit = percentFromItem(item, ["coverage_percent"], ["coverage_fraction", "hmm_coverage"]);
        if (Number.isFinite(explicit)) {
            return explicit;
        }
        const hmmFrom = Number(item?.hmm_from);
        const hmmTo = Number(item?.hmm_to);
        const hmmLength = Number(item?.hmm_length);
        if (Number.isFinite(hmmFrom) && Number.isFinite(hmmTo) && Number.isFinite(hmmLength) && hmmLength > 0) {
            return clampCoveragePercent(((hmmTo - hmmFrom + 1) / hmmLength) * 100);
        }
        return 0;
    }
    function matchedCoveragePercentForItem(item) {
        return percentFromItem(item, ["matched_coverage_percent", "matched_hmm_coverage_percent"], ["matched_coverage_fraction", "matched_hmm_coverage"]);
    }
    function combinedCoveragePercentForItem(item) {
        return percentFromItem(item, ["combined_coverage_percent", "combined_hmm_coverage_percent"], ["combined_coverage_fraction", "combined_hmm_coverage"]);
    }
    function combinedMatchedCoveragePercentForItem(item) {
        return percentFromItem(item, ["combined_matched_coverage_percent", "combined_matched_hmm_coverage_percent"], ["combined_matched_coverage_fraction", "combined_matched_hmm_coverage"]);
    }
    function coverageGainPercentForItem(item) {
        return percentFromItem(item, ["coverage_gain_percent", "hmm_coverage_gain_percent"], ["coverage_gain_fraction", "hmm_coverage_gain"], { clamp: false });
    }
    function matchedCoverageGainPercentForItem(item) {
        return percentFromItem(item, ["matched_coverage_gain_percent", "matched_hmm_coverage_gain_percent"], ["matched_coverage_gain_fraction", "matched_hmm_coverage_gain"], { clamp: false });
    }
    function formatCoveragePercent(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "0.0%";
        }
        return `${numeric.toFixed(1)}%`;
    }
    function formatCoverageGainPercent(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "";
        }
        const magnitude = `${Math.abs(numeric).toFixed(1)}pp`;
        return numeric >= 0 ? `+${magnitude}` : `-${magnitude}`;
    }
    function scoreGainForItem(item) {
        const explicit = Number(item?.domain_score_gain ?? item?.score_gain ?? item?.bit_score_gain);
        if (Number.isFinite(explicit)) {
            return explicit;
        }
        const domainScore = Number(item?.domain_score);
        const combinedDomainScore = Number(item?.combined_domain_score);
        return Number.isFinite(domainScore) && Number.isFinite(combinedDomainScore)
            ? combinedDomainScore - domainScore
            : NaN;
    }
    function scoreGainValueLabel(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "-";
        }
        return `${formatDeltaValue(numeric)} bits`;
    }
    function hmmForPfamId(hmms, pfamId) {
        return hmms.find((hmm) => String(hmm?.pfam_id || "") === String(pfamId || "")) || null;
    }
    function coverageItemsForPayload(payload, domains, hmms, scores) {
        const coverage = Array.isArray(payload?.coverage) ? payload.coverage : [];
        if (coverage.length > 0) {
            return coverage;
        }
        return domains.map((domain) => {
            const pfamId = String(domain?.pfam_id || "");
            const score = scores?.[domain.key]?.[pfamId] || {};
            const hmm = hmmForPfamId(hmms, pfamId);
            return {
                key: domain.key,
                label: domain.label || domain.key,
                domain_pfam_id: pfamId,
                hmm_pfam_id: pfamId,
                hmm_from: score.hmm_from,
                hmm_to: score.hmm_to,
                hmm_length: score.hmm_length ?? hmm?.length,
                hmm_covered: score.hmm_covered,
                coverage_fraction: score.hmm_coverage,
                coverage_percent: score.hmm_coverage_percent,
                matched_hmm_covered: score.matched_hmm_covered,
                matched_coverage_fraction: score.matched_hmm_coverage,
                matched_coverage_percent: score.matched_hmm_coverage_percent,
                deleted_hmm_columns: score.deleted_hmm_columns,
                combined_hmm_from: score.combined_hmm_from,
                combined_hmm_to: score.combined_hmm_to,
                combined_hmm_length: score.combined_hmm_length,
                combined_hmm_covered: score.combined_hmm_covered,
                combined_coverage_fraction: score.combined_hmm_coverage,
                combined_coverage_percent: score.combined_hmm_coverage_percent,
                combined_matched_hmm_covered: score.combined_matched_hmm_covered,
                combined_matched_coverage_fraction: score.combined_matched_hmm_coverage,
                combined_matched_coverage_percent: score.combined_matched_hmm_coverage_percent,
                combined_deleted_hmm_columns: score.combined_deleted_hmm_columns,
                domain_score: score.domain_score,
                combined_domain_score: score.combined_domain_score,
                domain_score_gain: score.domain_score_gain,
                combined_full_score: score.combined_full_score,
                score_gain: score.score_gain,
                bit_score_gain: score.bit_score_gain,
                full_score_gain: score.full_score_gain,
                coverage_gain_fraction: score.coverage_gain_fraction ?? score.hmm_coverage_gain,
                coverage_gain_percent: score.coverage_gain_percent ?? score.hmm_coverage_gain_percent,
                matched_coverage_gain_fraction: score.matched_coverage_gain_fraction ?? score.matched_hmm_coverage_gain,
                matched_coverage_gain_percent: score.matched_coverage_gain_percent ?? score.matched_hmm_coverage_gain_percent,
                reported: score.reported,
                full_score: score.full_score,
            };
        });
    }
    function coverageCoordinateLabel(item) {
        const pfamId = String(item?.hmm_pfam_id || item?.domain_pfam_id || "");
        const hmmFrom = Number(item?.hmm_from);
        const hmmTo = Number(item?.hmm_to);
        const hmmLength = Number(item?.hmm_length);
        if (Number.isFinite(hmmFrom) && Number.isFinite(hmmTo) && Number.isFinite(hmmLength) && hmmLength > 0) {
            return `${pfamId} ${hmmFrom}-${hmmTo} / ${hmmLength}`;
        }
        return pfamId ? `${pfamId} no reported span` : "No reported span";
    }
    function coverageValueLabel(spanPercent, matchedPercent) {
        return Number.isFinite(matchedPercent)
            ? `${formatCoveragePercent(matchedPercent)} / ${formatCoveragePercent(spanPercent)}`
            : formatCoveragePercent(spanPercent);
    }
    function coverageTrackTitle(item, spanPercent, matchedPercent, combinedSpanPercent, combinedMatchedPercent) {
        const hmmLength = Number(item?.hmm_length);
        const spanCovered = Number(item?.hmm_covered);
        const matchedCovered = Number(item?.matched_hmm_covered);
        const deletedColumns = Number(item?.deleted_hmm_columns);
        const combinedCovered = Number(item?.combined_hmm_covered);
        const combinedMatchedCovered = Number(item?.combined_matched_hmm_covered);
        const combinedDeletedColumns = Number(item?.combined_deleted_hmm_columns);
        const parts = [];
        if (Number.isFinite(matchedPercent)) {
            let matchedLabel = `matched ${formatCoveragePercent(matchedPercent)}`;
            if (Number.isFinite(matchedCovered) && Number.isFinite(hmmLength) && hmmLength > 0) {
                matchedLabel += ` (${matchedCovered}/${hmmLength})`;
            }
            parts.push(matchedLabel);
        }
        let spanLabel = `span ${formatCoveragePercent(spanPercent)}`;
        if (Number.isFinite(spanCovered) && Number.isFinite(hmmLength) && hmmLength > 0) {
            spanLabel += ` (${spanCovered}/${hmmLength})`;
        }
        parts.push(spanLabel);
        if (Number.isFinite(combinedMatchedPercent)) {
            let combinedMatchedLabel = `combined matched ${formatCoveragePercent(combinedMatchedPercent)}`;
            if (Number.isFinite(combinedMatchedCovered) && Number.isFinite(hmmLength) && hmmLength > 0) {
                combinedMatchedLabel += ` (${combinedMatchedCovered}/${hmmLength})`;
            }
            parts.push(combinedMatchedLabel);
        }
        if (Number.isFinite(combinedSpanPercent)) {
            let combinedLabel = `combined span ${formatCoveragePercent(combinedSpanPercent)}`;
            if (Number.isFinite(combinedCovered) && Number.isFinite(hmmLength) && hmmLength > 0) {
                combinedLabel += ` (${combinedCovered}/${hmmLength})`;
            }
            parts.push(combinedLabel);
        }
        if (Number.isFinite(deletedColumns) && deletedColumns > 0) {
            parts.push(`${deletedColumns} deleted model columns`);
        }
        if (Number.isFinite(combinedDeletedColumns) && combinedDeletedColumns > 0) {
            parts.push(`${combinedDeletedColumns} combined deleted model columns`);
        }
        return parts.join(" | ");
    }
    function appendZeroCenteredGainBar(track, value, maxAbsValue, className) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return;
        }
        const scale = Math.max(1, Number(maxAbsValue) || 1);
        const width = Math.min(50, Math.max(2, (Math.abs(numeric) / scale) * 50));
        const bar = document.createElement("span");
        bar.className = `structure-hmm-gain-bar ${className} ${numeric < 0 ? "negative" : "positive"}`;
        bar.style.width = `${width}%`;
        bar.style.left = numeric < 0 ? `${50 - width}%` : "50%";
        track.appendChild(bar);
    }
    function createGainPlot(titleText, baselineText) {
        const plot = document.createElement("section");
        plot.className = "structure-hmm-gain-plot";
        const header = document.createElement("div");
        header.className = "structure-hmm-gain-plot-header";
        const title = document.createElement("strong");
        title.textContent = titleText;
        const baseline = document.createElement("span");
        baseline.textContent = baselineText;
        header.append(title, baseline);
        const body = document.createElement("div");
        body.className = "structure-hmm-gain-plot-body";
        plot.append(header, body);
        return { plot, body };
    }
    function createGainRow(item, index, valueText, titleText) {
        const row = document.createElement("div");
        row.className = `structure-hmm-gain-row ${domainPlotClass(item, index)}`;
        const label = document.createElement("span");
        label.className = "structure-hmm-gain-label";
        label.textContent = `${item?.label || item?.key || "Domain"}${item?.domain_pfam_id ? ` (${item.domain_pfam_id})` : ""}`;
        const track = document.createElement("div");
        track.className = "structure-hmm-gain-track";
        track.title = titleText;
        const zero = document.createElement("span");
        zero.className = "structure-hmm-gain-zero-line";
        track.appendChild(zero);
        const value = document.createElement("span");
        value.className = "structure-hmm-gain-value";
        value.textContent = valueText;
        row.append(label, track, value);
        return { row, track };
    }
    function renderStructureHmmGainBars(payload, domains, hmms, scores) {
        const coverageItems = coverageItemsForPayload(payload, domains, hmms, scores);
        const scoreGainItems = coverageItems
            .map((item, index) => ({ item, index, scoreGain: scoreGainForItem(item) }))
            .filter((entry) => Number.isFinite(entry.scoreGain));
        const coverageGainItems = coverageItems
            .map((item, index) => ({
            item,
            index,
            spanGain: coverageGainPercentForItem(item),
            matchedGain: matchedCoverageGainPercentForItem(item),
        }))
            .filter((entry) => Number.isFinite(entry.spanGain) || Number.isFinite(entry.matchedGain));
        if (scoreGainItems.length === 0 && coverageGainItems.length === 0) {
            return null;
        }
        const root = document.createElement("div");
        root.className = "structure-hmm-gain-plots";
        if (scoreGainItems.length > 0) {
            const maxAbsScoreGain = Math.max(1, ...scoreGainItems.map((entry) => Math.abs(entry.scoreGain)));
            const { plot, body } = createGainPlot("Score Gain", "selected domain combined - fragment bits = 0");
            for (const entry of scoreGainItems) {
                const domainScore = Number(entry.item?.domain_score);
                const combinedDomainScore = Number(entry.item?.combined_domain_score);
                const combinedFullScore = Number(entry.item?.combined_full_score);
                const fullScore = Number(entry.item?.full_score);
                const fullScoreGain = Number(entry.item?.full_score_gain);
                const titleParts = [`domain score gain ${scoreGainValueLabel(entry.scoreGain)}`];
                if (Number.isFinite(combinedDomainScore) && Number.isFinite(domainScore)) {
                    titleParts.push(`combined domain ${formatScoreValue(combinedDomainScore)}`);
                    titleParts.push(`fragment domain ${formatScoreValue(domainScore)}`);
                }
                if (Number.isFinite(fullScoreGain)) {
                    titleParts.push(`full score gain ${scoreGainValueLabel(fullScoreGain)}`);
                }
                else if (Number.isFinite(combinedFullScore) && Number.isFinite(fullScore)) {
                    titleParts.push(`combined full ${formatScoreValue(combinedFullScore)}`);
                    titleParts.push(`fragment full ${formatScoreValue(fullScore)}`);
                }
                const { row, track } = createGainRow(entry.item, entry.index, scoreGainValueLabel(entry.scoreGain), titleParts.join(" | "));
                appendZeroCenteredGainBar(track, entry.scoreGain, maxAbsScoreGain, "structure-hmm-gain-bar-score");
                body.appendChild(row);
            }
            root.appendChild(plot);
        }
        if (coverageGainItems.length > 0) {
            const coverageValues = coverageGainItems
                .flatMap((entry) => [entry.matchedGain, entry.spanGain])
                .filter((value) => Number.isFinite(value))
                .map((value) => Math.abs(value));
            const maxAbsCoverageGain = Math.max(1, ...coverageValues);
            const { plot, body } = createGainPlot("Coverage Gain", "combined - fragment pp = 0");
            for (const entry of coverageGainItems) {
                const valueText = Number.isFinite(entry.matchedGain) && Number.isFinite(entry.spanGain)
                    ? `${formatCoverageGainPercent(entry.matchedGain)} / ${formatCoverageGainPercent(entry.spanGain)}`
                    : Number.isFinite(entry.matchedGain)
                        ? formatCoverageGainPercent(entry.matchedGain)
                        : formatCoverageGainPercent(entry.spanGain);
                const titleParts = [];
                if (Number.isFinite(entry.matchedGain)) {
                    titleParts.push(`matched gain ${formatCoverageGainPercent(entry.matchedGain)}`);
                }
                if (Number.isFinite(entry.spanGain)) {
                    titleParts.push(`span gain ${formatCoverageGainPercent(entry.spanGain)}`);
                }
                const { row, track } = createGainRow(entry.item, entry.index, valueText, titleParts.join(" | "));
                appendZeroCenteredGainBar(track, entry.spanGain, maxAbsCoverageGain, "structure-hmm-gain-bar-span");
                appendZeroCenteredGainBar(track, entry.matchedGain, maxAbsCoverageGain, "structure-hmm-gain-bar-matched");
                body.appendChild(row);
            }
            root.appendChild(plot);
        }
        return root;
    }
    function renderStructureHmmCoverageBars(payload, domains, hmms, scores) {
        const coverageItems = coverageItemsForPayload(payload, domains, hmms, scores);
        const root = document.createElement("div");
        root.className = "structure-hmm-coverage-bars";
        for (const [index, item] of coverageItems.entries()) {
            const percent = coveragePercentForItem(item);
            const matchedPercent = matchedCoveragePercentForItem(item);
            const combinedPercent = combinedCoveragePercentForItem(item);
            const combinedMatchedPercent = combinedMatchedCoveragePercentForItem(item);
            const row = document.createElement("div");
            row.className = `structure-hmm-coverage-row ${domainPlotClass(item, index)}`;
            const label = document.createElement("span");
            label.className = "structure-hmm-coverage-label";
            const title = document.createElement("strong");
            const domainPfamId = String(item?.domain_pfam_id || "");
            title.textContent = `${item?.label || item?.key || "Domain"}${domainPfamId ? ` (${domainPfamId})` : ""}`;
            const detail = document.createElement("span");
            detail.textContent = coverageCoordinateLabel(item);
            label.append(title, detail);
            const track = document.createElement("div");
            track.className = "structure-hmm-coverage-track";
            track.title = coverageTrackTitle(item, percent, matchedPercent, combinedPercent, combinedMatchedPercent);
            const fill = document.createElement("span");
            fill.className = "structure-hmm-coverage-fill structure-hmm-coverage-fill-span";
            fill.style.width = `${percent}%`;
            track.appendChild(fill);
            if (Number.isFinite(matchedPercent)) {
                const matchedFill = document.createElement("span");
                matchedFill.className = "structure-hmm-coverage-fill structure-hmm-coverage-fill-matched";
                matchedFill.style.width = `${matchedPercent}%`;
                track.appendChild(matchedFill);
            }
            const value = document.createElement("span");
            value.className = "structure-hmm-coverage-value";
            const coverageValue = document.createElement("span");
            coverageValue.className = "structure-hmm-coverage-value-cell";
            coverageValue.title = "Matched coverage / span coverage";
            coverageValue.textContent = coverageValueLabel(percent, matchedPercent);
            value.appendChild(coverageValue);
            row.append(label, track, value);
            root.appendChild(row);
        }
        return root;
    }
    function renderStructureHmmScores(payload) {
        const output = elements.structureHmmScoresOutput;
        if (!output) {
            return;
        }
        output.replaceChildren();
        const domains = Array.isArray(payload?.domains) ? payload.domains : [];
        const hmms = Array.isArray(payload?.hmms) ? payload.hmms : [];
        const scores = payload?.scores || {};
        if (domains.length === 0 || hmms.length === 0) {
            setStructureHmmScoresStatus("No HMM score data returned.");
            return;
        }
        output.appendChild(renderStructureHmmScorePlots(domains, hmms, scores));
        const gainBars = renderStructureHmmGainBars(payload, domains, hmms, scores);
        if (gainBars) {
            output.appendChild(gainBars);
        }
        output.appendChild(renderStructureHmmCoverageBars(payload, domains, hmms, scores));
        setStructureHmmScoresStatus("");
    }
    async function loadStructureHmmScores() {
        const structure = state.structureData;
        if (!structure?.row || !interfaceSelect.value) {
            resetStructureHmmScoresPanel({ hide: false, message: "Open a concrete structure before calculating HMM scores." });
            elements.structureHmmScoresPanel?.classList.remove("hidden");
            syncStructureHmmScoresButton(false);
            return;
        }
        const url = structureHmmScoresUrlForRow(structure.row);
        const requestId = state.structureHmmScoresRequestId + 1;
        state.structureHmmScoresRequestId = requestId;
        elements.structureHmmScoresPanel?.classList.remove("hidden");
        elements.structureHmmScoresOutput?.replaceChildren();
        setStructureHmmScoresStatus("Calculating HMM bit scores...");
        syncStructureHmmScoresButton(true);
        try {
            const cachedScores = readCacheValue(state.structureHmmScoresCache, url);
            if (cachedScores) {
                if (requestId === state.structureHmmScoresRequestId) {
                    state.structureHmmScoresPayload = cachedScores;
                    renderStructureHmmScores(cachedScores);
                }
                return;
            }
            let request = structureHmmScoresInFlight.get(url);
            if (!request) {
                request = fetchJson(url)
                    .then((payload) => {
                    writeCacheValue(state.structureHmmScoresCache, url, payload, STRUCTURE_HMM_SCORE_CACHE_LIMIT);
                    return payload;
                })
                    .finally(() => {
                    structureHmmScoresInFlight.delete(url);
                });
                structureHmmScoresInFlight.set(url, request);
            }
            const payload = await request;
            if (requestId !== state.structureHmmScoresRequestId) {
                return;
            }
            state.structureHmmScoresPayload = payload;
            renderStructureHmmScores(payload);
        }
        catch (error) {
            if (requestId !== state.structureHmmScoresRequestId) {
                return;
            }
            elements.structureHmmScoresOutput?.replaceChildren();
            setStructureHmmScoresStatus(error.message || "HMM bit-score calculation failed.");
            if (elements.structureModalStatus) {
                elements.structureModalStatus.textContent = error.message || "HMM bit-score calculation failed.";
            }
        }
        finally {
            if (requestId === state.structureHmmScoresRequestId) {
                syncStructureHmmScoresButton(false);
            }
        }
    }
    function renderStructureLoadingState(row, label, detail = "") {
        resetStructureHmmScoresPanel();
        renderStructurePartnerControl(null);
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
        if (structurePreloadTimer) {
            window.clearTimeout(structurePreloadTimer);
            structurePreloadTimer = 0;
        }
        structurePreloadAbortController?.abort?.();
        structurePreloadAbortController = null;
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
        const ordered = [];
        if (activeIndex >= 0) {
            for (const offset of [1, -1, 2, -2]) {
                const index = (activeIndex + offset + rows.length) % rows.length;
                ordered.push(rows[index]);
            }
        }
        else {
            ordered.push(...rows);
        }
        const seen = new Set();
        return ordered.filter((row) => {
            const rowKey = String(row?.row_key || "");
            if (!rowKey || rowKey === activeKey || seen.has(rowKey)) {
                return false;
            }
            seen.add(rowKey);
            return true;
        }).slice(0, STRUCTURE_PRELOAD_LIMIT);
    }
    async function preloadStructureRow(row, generation, signal) {
        const previewUrl = structurePreviewUrlForRow(row);
        const payload = await loadStructurePreviewPayload(previewUrl, { signal });
        if (signal?.aborted || generation !== state.structurePreloadGeneration || !structureModalIsOpen()) {
            return;
        }
        await loadStructureModelText(payload.model_url, { signal });
    }
    async function preloadStructureRows(rows, generation, signal) {
        let nextIndex = 0;
        const worker = async () => {
            while (!signal?.aborted && generation === state.structurePreloadGeneration && structureModalIsOpen()) {
                const row = rows[nextIndex];
                nextIndex += 1;
                if (!row) {
                    return;
                }
                try {
                    await preloadStructureRow(row, generation, signal);
                }
                catch (error) {
                    if (signal?.aborted || generation !== state.structurePreloadGeneration || !structureModalIsOpen()) {
                        return;
                    }
                }
            }
        };
        await Promise.all(Array.from({ length: Math.min(STRUCTURE_PRELOAD_CONCURRENCY, rows.length) }, () => worker()));
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
        const controller = new AbortController();
        structurePreloadAbortController = controller;
        structurePreloadTimer = window.setTimeout(() => {
            structurePreloadTimer = 0;
            if (controller.signal.aborted || generation !== state.structurePreloadGeneration || !structureModalIsOpen()) {
                return;
            }
            void preloadStructureRows(rows, generation, controller.signal)
                .catch((error) => {
                if (!controller.signal.aborted && generation === state.structurePreloadGeneration && structureModalIsOpen()) {
                    console.debug("[structure-preload] stopped", error);
                }
            })
                .finally(() => {
                if (structurePreloadAbortController === controller) {
                    structurePreloadAbortController = null;
                }
            });
        }, STRUCTURE_PRELOAD_DELAY_MS);
    }
    function resetStructurePanel(message = "Click a row name or use the button to open the structure.") {
        setStructureLoadingUi(false);
        elements.structureModalTitle.textContent = "Structure";
        elements.structureStatus.textContent = message;
        elements.structureModalSubtitle.textContent = message;
        elements.structureModalStatus.textContent =
            "Open display settings with D to adjust regions, clipping, DOF, effects, and geometry.";
        state.structureResidueLookup = null;
        state.structureData = null;
        state.structureRenderedModelKey = null;
        state.structureRenderedModelIdentityKey = null;
        state.structurePartnerRows = [];
        resetStructureHmmScoresPanel();
        renderStructurePartnerControl(null);
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
        closeStructureHmmScoresPanel();
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
        }
        else {
            state.structureViewer.resize?.();
            state.structureViewer.focusResidues?.(residues, 8);
            state.structureViewer.render?.();
        }
    }
    function mainFragmentResidues(structurePayload) {
        if (Array.isArray(structurePayload?.fragment_residue_ids) && structurePayload.fragment_residue_ids.length > 0) {
            return structurePayload.fragment_residue_ids;
        }
        return Array.from({ length: structurePayload.fragment_end - structurePayload.fragment_start + 1 }, (_value, index) => structurePayload.fragment_start + index);
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
            conservedness: mapped?.conservedness === "-" || mapped?.conservedness === undefined
                ? "-"
                : `${mapped.conservedness}%`,
            columnIndex: mapped?.columnIndex ?? null,
            residueLetter: oneLetter,
        };
    }
    function handleStructureHover(hoverPayload) {
        const residueIds = [hoverPayload?.residueId, ...(Array.isArray(hoverPayload?.residueIds) ? hoverPayload.residueIds : [])]
            .map((residueId) => Number(residueId))
            .filter((residueId) => Number.isFinite(residueId));
        const residueId = residueIds.find((candidate) => state.structureResidueLookup?.has(candidate));
        if (!Number.isFinite(residueId)) {
            clearStructureHover();
            return;
        }
        const hover = formatStructureHover({ ...hoverPayload, residueId });
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
    async function renderInteractiveStructure(options = {}) {
        const structure = state.structureData;
        if (!structure) {
            return;
        }
        const { row, payload, modelText } = structure;
        const viewer = getStructureViewer();
        const currentModelKey = structure.modelKey || structureModelKey(row, payload);
        const currentModelIdentityKey = structure.modelIdentityKey || structureModelIdentityKey(payload);
        const shouldReuseStructure = Boolean(options.reuseModel) &&
            Boolean(state.structureRenderedModelIdentityKey) &&
            currentModelIdentityKey === state.structureRenderedModelIdentityKey &&
            typeof viewer.updateStructureRepresentations === "function";
        const shouldPreserveView = Boolean(state.structureRenderedModelIdentityKey) &&
            typeof viewer.getView === "function" &&
            typeof viewer.setView === "function" &&
            currentModelIdentityKey === state.structureRenderedModelIdentityKey;
        const initialView = copyStructureView(structure.initialView);
        const previousView = initialView || (shouldPreserveView ? copyStructureView(viewer.getView()) : null);
        let renderedNotified = false;
        const notifyRendered = () => {
            if (renderedNotified) {
                return;
            }
            renderedNotified = true;
            options.onRendered?.();
        };
        state.structureResidueLookup = buildStructureResidueLookup(row);
        const residueStyles = columnResidueStyles(state.structureResidueLookup);
        const markerResidueStyles = structureMarkerResidueStyles(state.structureResidueLookup);
        const displaySettings = structureDisplaySettingsForView("structure");
        const representationOptions = {
            modelText,
            payload,
            format: payload.model_format || "pdb",
            label: structureRowLabel(row) || "Structure",
            mode: "structure",
            columnView: state.structureColumnView,
            contactsVisible: state.structureContactsVisible && displaySettings?.contactLinesVisible !== false,
            residueLookup: state.structureResidueLookup,
            residueStyles,
            markerResidueStyles,
            displaySettings,
            onRendered: notifyRendered,
            onHover: handleStructureHover,
            onHoverEnd: clearStructureHover,
            onResidueClick,
        };
        if (shouldReuseStructure) {
            try {
                await viewer.updateStructureRepresentations(representationOptions);
            }
            catch (_error) {
                await viewer.loadStructure(representationOptions);
            }
        }
        else {
            await viewer.loadStructure(representationOptions);
        }
        viewer.resize();
        const domainSelection = { resi: mainFragmentResidues(payload) };
        if (previousView) {
            viewer.setView(copyStructureView(previousView));
        }
        else {
            if (typeof viewer.focusResiduesStable === "function") {
                viewer.focusResiduesStable(domainSelection.resi, 8);
            }
            else {
                viewer.focusResidues(domainSelection.resi, 8);
            }
        }
        viewer.render();
        notifyRendered();
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
        state.structureRenderedModelIdentityKey = currentModelIdentityKey;
        if (state.structureData === structure) {
            state.structureData.initialView = null;
            state.structureData.modelKey = currentModelKey;
            state.structureData.modelIdentityKey = currentModelIdentityKey;
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
        renderStructurePartnerControl(row);
        const partnerRanges = payload.partner_fragment_ranges?.join(", ") || "none";
        elements.structureModalSubtitle.textContent =
            `fragment ${payload.fragment_key} | ` +
                `partner range: ${partnerRanges}` +
                `${payload.matched_partners.join(", ") ? ` | partners: ${payload.matched_partners.join(", ")}` : ""}` +
                `${alignmentNote}`;
        elements.structureModalStatus.textContent = state.structureColumnView
            ? `Main domain uses MSA column colors 0-${msaColumnMaxIndex()}. Region styles, alpha, and contact appearance can be adjusted from Display.${markerStatusNote}`
            : `Main interface: ${payload.interface_residue_ids.length} | ` +
                `Main surface: ${payload.surface_residue_ids.length} | ` +
                `Partner interface: ${payload.partner_interface_residue_ids.length} | ` +
                `Partner surface: ${payload.partner_surface_residue_ids.length} | ` +
                `Contacts: ${residueContactPairs(payload).length} | ` +
                `AlphaFold: ${payload.model_source || "unknown"}${markerNote}`;
        syncStructureHmmScoresButton(false);
        syncColumnLegends();
    }
    async function renderLoadedStructure(row, payload, modelText, options = {}) {
        if (!row || !payload || typeof modelText !== "string") {
            return;
        }
        stopForegroundStructureLoad();
        stopStructurePreloading();
        const requestId = state.structureRequestId;
        cacheLoadedStructure(options.previewUrl || "", payload, modelText);
        state.structureData = {
            row,
            payload,
            modelText,
            initialView: options.initialView || null,
            modelKey: options.modelKey || structureModelKey(row, payload),
            modelIdentityKey: options.modelIdentityKey || structureModelIdentityKey(payload),
        };
        openStructureModal();
        setStructureLoadingUi(true, "Rendering structure", modelFileLabel(payload, row));
        await renderInteractiveStructure({
            onRendered: () => {
                if (requestId === state.structureRequestId) {
                    setStructureLoadingUi(false);
                }
            },
        });
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
        stopStructurePreloading();
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
        }
        else {
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
            modelIdentityKey: structureModelIdentityKey(payload),
        };
        await renderInteractiveStructure({
            onRendered: () => {
                if (requestId === state.structureRequestId) {
                    setStructureLoadingUi(false);
                }
            },
        });
        setStructureLoadingUi(false);
        startStructurePreloading(row);
        setLoading(100, "Structure ready", structureRowLabel(row));
        window.setTimeout(hideLoading, 250);
    }
    async function selectStructurePartner(rowKey) {
        const currentStructure = state.structureData;
        if (!currentStructure || !rowKey || !interfaceSelect.value) {
            return;
        }
        const currentRowKey = structureInteractionRowKey(currentStructure.row);
        if (rowKey === currentRowKey) {
            return;
        }
        const partnerRows = structurePartnerCandidateRows(currentStructure.row);
        const targetRow = partnerRows.find((row) => structureInteractionRowKey(row) === rowKey) ||
            syntheticStructureRowFromInteractionKey(rowKey);
        if (!targetRow) {
            throw new Error("Selected structure partner is no longer available.");
        }
        if (elements.structurePartnerSelect) {
            elements.structurePartnerSelect.value = rowKey;
        }
        applyStructurePartnerControlColor(targetRow);
        resetStructureHmmScoresPanel();
        stopStructurePreloading();
        const requestId = state.structureRequestId + 1;
        state.structureRequestId = requestId;
        const previewUrl = structurePreviewUrlForRow(targetRow);
        const viewer = getStructureViewer();
        const previousView = typeof viewer.getView === "function" ? copyStructureView(viewer.getView()) : null;
        elements.structureModalStatus.textContent = "Updating interface colors";
        elements.structureStatus.textContent = `Updating interface colors for ${structurePartnerOptionLabel(targetRow)}.`;
        setStructureLoadingUi(true, "Updating interface", structurePartnerOptionLabel(targetRow));
        const payload = await loadStructurePreviewPayload(previewUrl);
        if (requestId !== state.structureRequestId || !structureModalIsOpen()) {
            return;
        }
        const currentIdentityKey = state.structureRenderedModelIdentityKey || structureModelIdentityKey(currentStructure.payload);
        const nextIdentityKey = structureModelIdentityKey(payload);
        const reuseModel = Boolean(currentIdentityKey && nextIdentityKey && currentIdentityKey === nextIdentityKey);
        let modelText = currentStructure.modelText;
        if (!reuseModel || typeof modelText !== "string") {
            modelText = await loadStructureModelText(payload.model_url);
            if (requestId !== state.structureRequestId || !structureModalIsOpen()) {
                return;
            }
        }
        cacheLoadedStructure(previewUrl, payload, modelText);
        state.selectedRowKey = targetRow.row_key;
        state.selectedRowSnapshot = targetRow;
        state.structureData = {
            row: targetRow,
            payload,
            modelText,
            initialView: previousView,
            modelKey: structureModelKey(targetRow, payload),
            modelIdentityKey: nextIdentityKey,
        };
        await renderInteractiveStructure({
            reuseModel,
            onRendered: () => {
                if (requestId === state.structureRequestId) {
                    setStructureLoadingUi(false);
                }
            },
        });
        if (requestId === state.structureRequestId) {
            setStructureLoadingUi(false);
        }
    }
    return {
        closeStructureModal,
        getStructureViewer,
        handleStructureLoadFailure,
        loadInteractiveStructure,
        loadStructureHmmScores,
        openStructureModal,
        recenterStructureDomain,
        closeStructureHmmScoresPanel,
        renderLoadedStructure,
        renderInteractiveStructure,
        resetStructurePanel,
        selectStructurePartner,
    };
}
