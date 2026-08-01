import { DEFAULT_CLUSTERING_SETTINGS, DEFAULT_EMBEDDING_SETTINGS, DEFAULT_SELECTION_SETTINGS, DEFAULT_STRUCTURE_DISPLAY_SETTINGS, DEFAULT_STRUCTURE_DISPLAY_VIEW_PRESETS, } from "./constants.js";
import { normalizeSelectionSettings } from "./selectionSettings.js";
const UI_PREFERENCES_KEY = "die.uiPreferences.v1";
const UI_PREFERENCES_VERSION = 1;
const PANEL_VIEWS = ["info", "msa", "embeddings", "columns", "dendrogram"];
const EMBEDDING_SETTINGS_SECTIONS = ["points", "clustering"];
const STRUCTURE_DISPLAY_BUILT_IN_PRESETS = ["soft", "crisp", "illustrative", "performance"];
const STRUCTURE_USER_PRESET_PREFIX = "user:";
function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function browserStorage() {
    try {
        return window.localStorage;
    }
    catch {
        return null;
    }
}
function enumValue(value, allowedValues, fallback) {
    return allowedValues.includes(value) ? value : fallback;
}
function finiteNumber(value, fallback, { min = -Infinity, max = Infinity } = {}) {
    const numberValue = Number(value);
    if (!Number.isFinite(numberValue) || numberValue < min || numberValue > max) {
        return fallback;
    }
    return numberValue;
}
function positiveInteger(value, fallback) {
    const numberValue = Number.parseInt(value, 10);
    if (!Number.isFinite(numberValue) || numberValue <= 0) {
        return fallback;
    }
    return numberValue;
}
function optionalPositiveInteger(value, fallback = "") {
    if (String(value ?? "").trim() === "") {
        return fallback;
    }
    return positiveInteger(value, fallback);
}
function optionalPercentInteger(value, fallback = "") {
    if (String(value ?? "").trim() === "") {
        return fallback;
    }
    const numberValue = Number.parseInt(value, 10);
    if (!Number.isFinite(numberValue) || numberValue < 0 || numberValue > 100) {
        return fallback;
    }
    return numberValue;
}
function optionalNonNegativeNumber(value, fallback = "") {
    if (String(value ?? "").trim() === "") {
        return fallback;
    }
    return finiteNumber(value, fallback, { min: 0 });
}
function optionalUnitNumber(value, fallback = "") {
    if (String(value ?? "").trim() === "") {
        return fallback;
    }
    return finiteNumber(value, fallback, { min: 0, max: 1 });
}
function percentNumber(value, fallback = 0) {
    return finiteNumber(value, fallback, { min: 0, max: 100 });
}
function autoOrPositiveNumber(value, fallback = "auto") {
    if (String(value ?? "").trim().toLowerCase() === "auto" || String(value ?? "").trim() === "") {
        return "auto";
    }
    return finiteNumber(value, fallback, { min: Number.MIN_VALUE });
}
function normalizeEmbeddingSettings(rawSettings = {}, fallback = DEFAULT_EMBEDDING_SETTINGS) {
    const source = isRecord(rawSettings) ? rawSettings : {};
    return {
        method: enumValue(source.method, ["pca", "tsne"], fallback.method),
        distance: enumValue(source.distance, ["binary", "jaccard", "dice", "overlap"], fallback.distance),
        perplexity: autoOrPositiveNumber(source.perplexity, fallback.perplexity),
        learningRate: autoOrPositiveNumber(source.learningRate, fallback.learningRate),
        maxIter: positiveInteger(source.maxIter, fallback.maxIter),
        earlyExaggerationIter: positiveInteger(source.earlyExaggerationIter, fallback.earlyExaggerationIter),
        earlyExaggeration: finiteNumber(source.earlyExaggeration, fallback.earlyExaggeration, {
            min: Number.MIN_VALUE,
        }),
        neighbors: enumValue(source.neighbors, ["approx", "auto", "exact"], fallback.neighbors),
        theta: finiteNumber(source.theta, fallback.theta, { min: 0, max: 1 }),
    };
}
function normalizeClusteringSettings(rawSettings = {}, fallback = DEFAULT_CLUSTERING_SETTINGS) {
    const source = isRecord(rawSettings) ? rawSettings : {};
    const method = enumValue(source.method, ["hierarchical", "hdbscan"], fallback.method);
    const allowedDistances = method === "hdbscan"
        ? ["binary", "jaccard", "dice", "overlap"]
        : ["jaccard", "dice", "overlap"];
    const fallbackDistance = allowedDistances.includes(fallback.distance)
        ? fallback.distance
        : DEFAULT_CLUSTERING_SETTINGS.distance;
    const hierarchicalTarget = enumValue(source.hierarchicalTarget, ["distance_threshold", "n_clusters", "persistence"], fallback.hierarchicalTarget);
    const nClustersFallback = positiveInteger(fallback.nClusters, 8);
    return {
        method,
        distance: enumValue(source.distance, allowedDistances, fallbackDistance),
        minClusterSize: positiveInteger(source.minClusterSize, fallback.minClusterSize),
        minSamples: optionalPositiveInteger(source.minSamples, fallback.minSamples),
        hdbscanEffectiveMinSamples: optionalPositiveInteger(source.hdbscanEffectiveMinSamples, fallback.hdbscanEffectiveMinSamples || ""),
        clusterSelectionEpsilon: finiteNumber(source.clusterSelectionEpsilon, fallback.clusterSelectionEpsilon, { min: 0 }),
        hdbscanSampleScope: enumValue(source.hdbscanSampleScope, ["expanded", "unique"], fallback.hdbscanSampleScope || DEFAULT_CLUSTERING_SETTINGS.hdbscanSampleScope),
        linkage: enumValue(source.linkage, ["single", "complete", "average", "average_deduplicated", "weighted"], fallback.linkage),
        hierarchicalTarget,
        nClusters: hierarchicalTarget === "n_clusters"
            ? optionalPositiveInteger(source.nClusters, nClustersFallback)
            : "",
        distanceThreshold: hierarchicalTarget === "distance_threshold"
            ? optionalNonNegativeNumber(source.distanceThreshold, fallback.distanceThreshold)
            : "",
        persistenceMinLifetime: hierarchicalTarget === "persistence"
            ? optionalNonNegativeNumber(source.persistenceMinLifetime, fallback.persistenceMinLifetime)
            : "",
        persistenceLifetimeWeight: hierarchicalTarget === "persistence"
            ? optionalUnitNumber(source.persistenceLifetimeWeight, fallback.persistenceLifetimeWeight)
            : "",
        persistenceScoreMode: hierarchicalTarget === "persistence"
            ? enumValue(source.persistenceScoreMode, ["rectangle", "integral"], fallback.persistenceScoreMode || DEFAULT_CLUSTERING_SETTINGS.persistenceScoreMode)
            : "",
        hierarchicalMinClusterSize: positiveInteger(source.hierarchicalMinClusterSize, fallback.hierarchicalMinClusterSize),
        domainSizeMin: optionalPositiveInteger(source.domainSizeMin, fallback.domainSizeMin || ""),
        domainSizeMax: optionalPositiveInteger(source.domainSizeMax, fallback.domainSizeMax || ""),
        pfamRowCoverageMin: optionalPercentInteger(source.pfamRowCoverageMin, fallback.pfamRowCoverageMin || ""),
        pfamRowCoverageMax: optionalPercentInteger(source.pfamRowCoverageMax, fallback.pfamRowCoverageMax || ""),
    };
}
function normalizeDomainSizeRange(rawRange = {}) {
    const source = isRecord(rawRange) ? rawRange : {};
    let domainSizeMin = optionalPositiveInteger(source.domainSizeMin);
    let domainSizeMax = optionalPositiveInteger(source.domainSizeMax);
    if (domainSizeMin !== "" &&
        domainSizeMax !== "" &&
        Number(domainSizeMin) > Number(domainSizeMax)) {
        [domainSizeMin, domainSizeMax] = [domainSizeMax, domainSizeMin];
    }
    return {
        domainSizeMin: domainSizeMin === "" ? "" : String(domainSizeMin),
        domainSizeMax: domainSizeMax === "" ? "" : String(domainSizeMax),
    };
}
function domainSizeRangeIsFull(range) {
    return (String(range?.domainSizeMin ?? "").trim() === "" &&
        String(range?.domainSizeMax ?? "").trim() === "");
}
function normalizeDomainSizeRangeMap(rawMap = {}, { keepFull = false } = {}) {
    const source = isRecord(rawMap) ? rawMap : {};
    const normalized = {};
    for (const [pfamId, rawRange] of Object.entries(source)) {
        const key = String(pfamId || "").trim();
        if (!key) {
            continue;
        }
        const range = normalizeDomainSizeRange(rawRange);
        if (!keepFull && domainSizeRangeIsFull(range)) {
            continue;
        }
        normalized[key] = range;
    }
    return normalized;
}
function normalizePfamCoverageRange(rawRange = {}) {
    const source = isRecord(rawRange) ? rawRange : {};
    let pfamRowCoverageMin = optionalPercentInteger(source.pfamRowCoverageMin);
    let pfamRowCoverageMax = optionalPercentInteger(source.pfamRowCoverageMax);
    if (pfamRowCoverageMin !== "" &&
        pfamRowCoverageMax !== "" &&
        Number(pfamRowCoverageMin) > Number(pfamRowCoverageMax)) {
        [pfamRowCoverageMin, pfamRowCoverageMax] = [pfamRowCoverageMax, pfamRowCoverageMin];
    }
    return {
        pfamRowCoverageMin: pfamRowCoverageMin === "" ? "" : String(pfamRowCoverageMin),
        pfamRowCoverageMax: pfamRowCoverageMax === "" ? "" : String(pfamRowCoverageMax),
    };
}
function pfamCoverageRangeIsFull(range) {
    const minValue = String(range?.pfamRowCoverageMin ?? "").trim();
    const maxValue = String(range?.pfamRowCoverageMax ?? "").trim();
    return ((minValue === "" || minValue === "0") &&
        (maxValue === "" || maxValue === "100"));
}
function normalizePfamCoverageRangeMap(rawMap = {}, { keepFull = false } = {}) {
    const source = isRecord(rawMap) ? rawMap : {};
    const normalized = {};
    for (const [pfamId, rawRange] of Object.entries(source)) {
        const key = String(pfamId || "").trim();
        if (!key) {
            continue;
        }
        const range = normalizePfamCoverageRange(rawRange);
        if (!keepFull && pfamCoverageRangeIsFull(range)) {
            continue;
        }
        normalized[key] = range;
    }
    return normalized;
}
function clusteringSettingsWithoutRangeFilters(settings = {}) {
    const { domainSizeMin: _domainSizeMin, domainSizeMax: _domainSizeMax, pfamRowCoverageMin: _pfamRowCoverageMin, pfamRowCoverageMax: _pfamRowCoverageMax, ...rest } = settings || {};
    return rest;
}
function normalizeHierarchicalTargetMemory(rawMemory = {}) {
    const source = isRecord(rawMemory) ? rawMemory : {};
    const nClusters = optionalPositiveInteger(source.nClusters);
    const distanceThreshold = optionalNonNegativeNumber(source.distanceThreshold);
    const persistenceMinLifetime = optionalNonNegativeNumber(source.persistenceMinLifetime);
    const persistenceLifetimeWeight = optionalUnitNumber(source.persistenceLifetimeWeight);
    const persistenceScoreMode = enumValue(source.persistenceScoreMode, ["rectangle", "integral"], DEFAULT_CLUSTERING_SETTINGS.persistenceScoreMode);
    return {
        nClusters: nClusters === "" ? String(DEFAULT_CLUSTERING_SETTINGS.nClusters) : String(nClusters),
        distanceThreshold: distanceThreshold === ""
            ? String(DEFAULT_CLUSTERING_SETTINGS.distanceThreshold)
            : String(distanceThreshold),
        persistenceMinLifetime: persistenceMinLifetime === ""
            ? String(DEFAULT_CLUSTERING_SETTINGS.persistenceMinLifetime)
            : String(persistenceMinLifetime),
        persistenceLifetimeWeight: persistenceLifetimeWeight === ""
            ? String(DEFAULT_CLUSTERING_SETTINGS.persistenceLifetimeWeight)
            : String(persistenceLifetimeWeight),
        persistenceScoreMode,
    };
}
function normalizeStructureDisplaySettings(rawSettings = {}) {
    const source = isRecord(rawSettings) ? rawSettings : {};
    const normalized = { ...DEFAULT_STRUCTURE_DISPLAY_SETTINGS };
    for (const [key, fallback] of Object.entries(DEFAULT_STRUCTURE_DISPLAY_SETTINGS)) {
        const value = source[key];
        if (typeof fallback === "boolean" && typeof value === "boolean") {
            normalized[key] = value;
        }
        else if (typeof fallback === "number" && Number.isFinite(Number(value))) {
            normalized[key] = Number(value);
        }
        else if (typeof fallback === "string" && typeof value === "string") {
            normalized[key] = value;
        }
    }
    if (source.restProteinAlpha === undefined && Number.isFinite(Number(source.contextAlpha))) {
        normalized.restProteinAlpha = Number(source.contextAlpha);
    }
    normalized.representativeClusterSupportMin = percentNumber(source.representativeClusterSupportMin, DEFAULT_STRUCTURE_DISPLAY_SETTINGS.representativeClusterSupportMin);
    normalized.clusterOverviewClusterSupportMin = percentNumber(source.clusterOverviewClusterSupportMin, DEFAULT_STRUCTURE_DISPLAY_SETTINGS.clusterOverviewClusterSupportMin);
    return normalized;
}
function structureDisplayPresetId(value, fallbackName = "preset") {
    const raw = String(value || "").trim();
    const source = raw || String(fallbackName || "preset");
    const slug = source
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .slice(0, 40);
    return slug || "preset";
}
function normalizeStructureDisplayViewPresets(rawPresets = {}) {
    const source = isRecord(rawPresets) ? rawPresets : {};
    const normalized = { ...DEFAULT_STRUCTURE_DISPLAY_VIEW_PRESETS };
    for (const key of Object.keys(DEFAULT_STRUCTURE_DISPLAY_VIEW_PRESETS)) {
        const value = String(source[key] || "");
        if (value === "current" ||
            STRUCTURE_DISPLAY_BUILT_IN_PRESETS.includes(value) ||
            value.startsWith(STRUCTURE_USER_PRESET_PREFIX)) {
            normalized[key] = value;
        }
    }
    return normalized;
}
function normalizeStructureDisplayCustomPresets(rawPresets = []) {
    const source = Array.isArray(rawPresets) ? rawPresets : [];
    const presets = [];
    const seen = new Set();
    for (const rawPreset of source) {
        if (!isRecord(rawPreset)) {
            continue;
        }
        const name = String(rawPreset.name || "").trim().slice(0, 48);
        if (!name) {
            continue;
        }
        let id = structureDisplayPresetId(rawPreset.id, name);
        if (seen.has(id)) {
            let suffix = 2;
            while (seen.has(`${id}-${suffix}`)) {
                suffix += 1;
            }
            id = `${id}-${suffix}`;
        }
        seen.add(id);
        presets.push({
            id,
            name,
            settings: {
                ...normalizeStructureDisplaySettings(rawPreset.settings),
                preset: "custom",
            },
        });
    }
    return presets.slice(-24);
}
function loadUiPreferences() {
    const storage = browserStorage();
    if (!storage) {
        return null;
    }
    try {
        const raw = storage.getItem(UI_PREFERENCES_KEY);
        if (!raw) {
            return null;
        }
        const parsed = JSON.parse(raw);
        return isRecord(parsed) && parsed.version === UI_PREFERENCES_VERSION ? parsed : null;
    }
    catch {
        return null;
    }
}
export function applyUiPreferences(state) {
    const preferences = loadUiPreferences();
    if (!preferences) {
        return;
    }
    state.selectionSettings = normalizeSelectionSettings(preferences.selectionSettings || DEFAULT_SELECTION_SETTINGS);
    state.selectionSettingsDraft = { ...state.selectionSettings };
    state.embeddingSettings = normalizeEmbeddingSettings(preferences.embeddingSettings);
    state.embeddingSettingsDraft = normalizeEmbeddingSettings(preferences.embeddingSettingsDraft || state.embeddingSettings, state.embeddingSettings);
    state.embeddingClusteringSettings = {
        ...normalizeClusteringSettings(preferences.embeddingClusteringSettings),
        domainSizeMin: "",
        domainSizeMax: "",
        pfamRowCoverageMin: "",
        pfamRowCoverageMax: "",
    };
    state.embeddingClusteringSettingsDraft = {
        ...normalizeClusteringSettings(preferences.embeddingClusteringSettingsDraft || state.embeddingClusteringSettings, state.embeddingClusteringSettings),
        domainSizeMin: "",
        domainSizeMax: "",
        pfamRowCoverageMin: "",
        pfamRowCoverageMax: "",
    };
    state.embeddingDomainSizeRangesByPfamId = normalizeDomainSizeRangeMap(preferences.embeddingDomainSizeRangesByPfamId);
    state.embeddingDomainSizeRangeDraftsByPfamId = normalizeDomainSizeRangeMap(preferences.embeddingDomainSizeRangeDraftsByPfamId, { keepFull: true });
    state.embeddingPfamCoverageRangesByPfamId = normalizePfamCoverageRangeMap(preferences.embeddingPfamCoverageRangesByPfamId);
    state.embeddingPfamCoverageRangeDraftsByPfamId = normalizePfamCoverageRangeMap(preferences.embeddingPfamCoverageRangeDraftsByPfamId, { keepFull: true });
    state.embeddingHierarchicalTargetMemory = normalizeHierarchicalTargetMemory(preferences.embeddingHierarchicalTargetMemory);
    state.embeddingColorMode = enumValue(preferences.embeddingColorMode, ["cluster", "domain", "size", "coverage"], state.embeddingColorMode);
    const uiState = isRecord(preferences.uiState) ? preferences.uiState : {};
    state.msaPanelView = enumValue(uiState.msaPanelView ?? preferences.msaPanelView, PANEL_VIEWS, state.msaPanelView);
    state.selectionSettingsOpen =
        typeof uiState.selectionSettingsOpen === "boolean"
            ? uiState.selectionSettingsOpen
            : state.selectionSettingsOpen;
    state.embeddingSettingsOpen =
        typeof uiState.embeddingSettingsOpen === "boolean"
            ? uiState.embeddingSettingsOpen
            : state.embeddingSettingsOpen;
    state.embeddingSettingsSection = enumValue(uiState.embeddingSettingsSection, EMBEDDING_SETTINGS_SECTIONS, state.embeddingSettingsSection);
    state.dendrogramSettingsOpen =
        typeof uiState.dendrogramSettingsOpen === "boolean"
            ? uiState.dendrogramSettingsOpen
            : state.dendrogramSettingsOpen;
    state.structureDisplaySettingsOpen =
        typeof uiState.structureDisplaySettingsOpen === "boolean"
            ? uiState.structureDisplaySettingsOpen
            : state.structureDisplaySettingsOpen;
    state.columnsSettingsOpen =
        typeof uiState.columnsSettingsOpen === "boolean"
            ? uiState.columnsSettingsOpen
            : state.columnsSettingsOpen;
    state.columnsInterfaceOnly =
        typeof preferences.columnsInterfaceOnly === "boolean"
            ? preferences.columnsInterfaceOnly
            : state.columnsInterfaceOnly;
    state.columnsEmptyBinsWhite =
        typeof preferences.columnsEmptyBinsWhite === "boolean"
            ? preferences.columnsEmptyBinsWhite
            : state.columnsEmptyBinsWhite;
    state.columnsGapShading =
        typeof preferences.columnsGapShading === "boolean"
            ? preferences.columnsGapShading
            : state.columnsGapShading;
    state.columnsSource = enumValue(preferences.columnsSource, ["clusters", "domains"], state.columnsSource);
    const dendrogram = isRecord(preferences.dendrogram) ? preferences.dendrogram : {};
    state.dendrogramStyle = enumValue(dendrogram.style, ["radial", "linear"], state.dendrogramStyle);
    state.dendrogramDepth = positiveInteger(dendrogram.depth, state.dendrogramDepth);
    state.dendrogramRadiusMode = enumValue(dendrogram.radiusMode, ["depth", "distance"], state.dendrogramRadiusMode);
    state.dendrogramTrueDistanceEdges =
        typeof dendrogram.trueDistanceEdges === "boolean"
            ? dendrogram.trueDistanceEdges
            : state.dendrogramTrueDistanceEdges;
    state.dendrogramColorMode = enumValue(dendrogram.colorMode, ["cluster", "domain"], state.dendrogramColorMode);
    state.representativeLens = enumValue(preferences.representativeLens, ["interface", "conservedness", "column", "partners", "cluster"], state.representativeLens);
    state.representativeMethod = enumValue(preferences.representativeMethod, ["balanced", "residue"], state.representativeMethod);
    state.clusterCompareDomainSizeFilter = normalizeDomainSizeRange(preferences.clusterCompareDomainSizeFilter);
    state.clusterCompareDomainSizeFilterDraft = normalizeDomainSizeRange(preferences.clusterCompareDomainSizeFilterDraft || state.clusterCompareDomainSizeFilter);
    state.structureContactsVisible =
        typeof preferences.structureContactsVisible === "boolean"
            ? preferences.structureContactsVisible
            : state.structureContactsVisible;
    state.structureColumnView =
        typeof preferences.structureColumnView === "boolean"
            ? preferences.structureColumnView
            : state.structureColumnView;
    state.structureDisplayCustomPresets = normalizeStructureDisplayCustomPresets(preferences.structureDisplayCustomPresets);
    state.structureDisplaySettings = normalizeStructureDisplaySettings(preferences.structureDisplaySettings);
    state.structureDisplayViewPresets = normalizeStructureDisplayViewPresets(preferences.structureDisplayViewPresets);
    const displaySettingsSource = isRecord(preferences.structureDisplaySettings)
        ? preferences.structureDisplaySettings
        : {};
    if (typeof displaySettingsSource.contactLinesVisible === "boolean") {
        state.structureContactsVisible = state.structureDisplaySettings.contactLinesVisible;
    }
    else {
        state.structureDisplaySettings.contactLinesVisible = state.structureContactsVisible;
    }
}
function preferencesPayload(state) {
    return {
        version: UI_PREFERENCES_VERSION,
        selectionSettings: state.selectionSettings,
        embeddingSettings: state.embeddingSettings,
        embeddingSettingsDraft: state.embeddingSettingsDraft,
        embeddingClusteringSettings: clusteringSettingsWithoutRangeFilters(state.embeddingClusteringSettings),
        embeddingClusteringSettingsDraft: clusteringSettingsWithoutRangeFilters(state.embeddingClusteringSettingsDraft),
        embeddingDomainSizeRangesByPfamId: state.embeddingDomainSizeRangesByPfamId || {},
        embeddingDomainSizeRangeDraftsByPfamId: state.embeddingDomainSizeRangeDraftsByPfamId || {},
        embeddingPfamCoverageRangesByPfamId: state.embeddingPfamCoverageRangesByPfamId || {},
        embeddingPfamCoverageRangeDraftsByPfamId: state.embeddingPfamCoverageRangeDraftsByPfamId || {},
        embeddingHierarchicalTargetMemory: state.embeddingHierarchicalTargetMemory,
        embeddingColorMode: state.embeddingColorMode,
        uiState: {
            msaPanelView: state.msaPanelView,
            selectionSettingsOpen: state.selectionSettingsOpen,
            embeddingSettingsOpen: state.embeddingSettingsOpen,
            embeddingSettingsSection: state.embeddingSettingsSection,
            dendrogramSettingsOpen: state.dendrogramSettingsOpen,
            structureDisplaySettingsOpen: state.structureDisplaySettingsOpen,
            columnsSettingsOpen: state.columnsSettingsOpen,
        },
        columnsInterfaceOnly: state.columnsInterfaceOnly,
        columnsEmptyBinsWhite: state.columnsEmptyBinsWhite,
        columnsGapShading: state.columnsGapShading,
        columnsSource: state.columnsSource,
        dendrogram: {
            style: state.dendrogramStyle,
            depth: state.dendrogramDepth,
            radiusMode: state.dendrogramRadiusMode,
            trueDistanceEdges: state.dendrogramTrueDistanceEdges,
            colorMode: state.dendrogramColorMode,
        },
        representativeLens: state.representativeLens,
        representativeMethod: state.representativeMethod,
        clusterCompareDomainSizeFilter: state.clusterCompareDomainSizeFilter || {},
        clusterCompareDomainSizeFilterDraft: state.clusterCompareDomainSizeFilterDraft || {},
        structureContactsVisible: state.structureContactsVisible,
        structureColumnView: state.structureColumnView,
        structureDisplayCustomPresets: state.structureDisplayCustomPresets || [],
        structureDisplayViewPresets: state.structureDisplayViewPresets || DEFAULT_STRUCTURE_DISPLAY_VIEW_PRESETS,
        structureDisplaySettings: state.structureDisplaySettings,
    };
}
export function saveUiPreferences(state) {
    const storage = browserStorage();
    if (!storage) {
        return;
    }
    try {
        storage.setItem(UI_PREFERENCES_KEY, JSON.stringify(preferencesPayload(state)));
    }
    catch {
    }
}
