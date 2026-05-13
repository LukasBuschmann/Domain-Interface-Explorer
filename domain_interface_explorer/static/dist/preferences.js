import { DEFAULT_CLUSTERING_SETTINGS, DEFAULT_EMBEDDING_SETTINGS, DEFAULT_SELECTION_SETTINGS, DEFAULT_STRUCTURE_DISPLAY_SETTINGS, } from "./constants.js";
import { normalizeSelectionSettings } from "./selectionSettings.js";
const UI_PREFERENCES_KEY = "die.uiPreferences.v1";
const UI_PREFERENCES_VERSION = 1;
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
    const hierarchicalTarget = enumValue(source.hierarchicalTarget, ["distance_threshold", "n_clusters", "persistence"], fallback.hierarchicalTarget);
    const nClustersFallback = positiveInteger(fallback.nClusters, 8);
    return {
        method: enumValue(source.method, ["hierarchical", "hdbscan"], fallback.method),
        distance: enumValue(source.distance, ["jaccard", "dice", "overlap"], fallback.distance),
        minClusterSize: positiveInteger(source.minClusterSize, fallback.minClusterSize),
        minSamples: optionalPositiveInteger(source.minSamples, fallback.minSamples),
        clusterSelectionEpsilon: finiteNumber(source.clusterSelectionEpsilon, fallback.clusterSelectionEpsilon, { min: 0 }),
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
        hierarchicalMinClusterSize: positiveInteger(source.hierarchicalMinClusterSize, fallback.hierarchicalMinClusterSize),
    };
}
function normalizeHierarchicalTargetMemory(rawMemory = {}) {
    const source = isRecord(rawMemory) ? rawMemory : {};
    const nClusters = optionalPositiveInteger(source.nClusters);
    const distanceThreshold = optionalNonNegativeNumber(source.distanceThreshold);
    const persistenceMinLifetime = optionalNonNegativeNumber(source.persistenceMinLifetime);
    const persistenceLifetimeWeight = optionalUnitNumber(source.persistenceLifetimeWeight);
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
    return normalized;
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
    state.embeddingClusteringSettings = normalizeClusteringSettings(preferences.embeddingClusteringSettings);
    state.embeddingClusteringSettingsDraft = normalizeClusteringSettings(preferences.embeddingClusteringSettingsDraft || state.embeddingClusteringSettings, state.embeddingClusteringSettings);
    state.embeddingHierarchicalTargetMemory = normalizeHierarchicalTargetMemory(preferences.embeddingHierarchicalTargetMemory);
    state.embeddingColorMode = enumValue(preferences.embeddingColorMode, ["cluster", "domain"], state.embeddingColorMode);
    state.columnsInterfaceOnly =
        typeof preferences.columnsInterfaceOnly === "boolean"
            ? preferences.columnsInterfaceOnly
            : state.columnsInterfaceOnly;
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
    state.structureContactsVisible =
        typeof preferences.structureContactsVisible === "boolean"
            ? preferences.structureContactsVisible
            : state.structureContactsVisible;
    state.structureColumnView =
        typeof preferences.structureColumnView === "boolean"
            ? preferences.structureColumnView
            : state.structureColumnView;
    state.structureDisplaySettings = normalizeStructureDisplaySettings(preferences.structureDisplaySettings);
}
function preferencesPayload(state) {
    return {
        version: UI_PREFERENCES_VERSION,
        selectionSettings: state.selectionSettings,
        embeddingSettings: state.embeddingSettings,
        embeddingSettingsDraft: state.embeddingSettingsDraft,
        embeddingClusteringSettings: state.embeddingClusteringSettings,
        embeddingClusteringSettingsDraft: state.embeddingClusteringSettingsDraft,
        embeddingHierarchicalTargetMemory: state.embeddingHierarchicalTargetMemory,
        embeddingColorMode: state.embeddingColorMode,
        columnsInterfaceOnly: state.columnsInterfaceOnly,
        dendrogram: {
            style: state.dendrogramStyle,
            depth: state.dendrogramDepth,
            radiusMode: state.dendrogramRadiusMode,
            trueDistanceEdges: state.dendrogramTrueDistanceEdges,
            colorMode: state.dendrogramColorMode,
        },
        representativeLens: state.representativeLens,
        representativeMethod: state.representativeMethod,
        structureContactsVisible: state.structureContactsVisible,
        structureColumnView: state.structureColumnView,
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
