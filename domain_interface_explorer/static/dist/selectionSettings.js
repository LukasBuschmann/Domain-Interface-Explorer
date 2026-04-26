import { DEFAULT_SELECTION_SETTINGS } from "./constants.js";
export function parseSelectionSettingsDraft(rawSettings = {}) {
    const rawValue = String(rawSettings.minInterfaceSize ?? "").trim();
    if (rawValue === "") {
        return { ...DEFAULT_SELECTION_SETTINGS };
    }
    const value = Number(rawValue);
    if (!Number.isInteger(value) || value < 0) {
        throw new Error("Minimal interface size must be a non-negative integer.");
    }
    return {
        minInterfaceSize: value,
    };
}
export function normalizeSelectionSettings(settings = {}) {
    try {
        return parseSelectionSettingsDraft(settings);
    }
    catch {
        return { ...DEFAULT_SELECTION_SETTINGS };
    }
}
export function selectionSettingsKey(settings = DEFAULT_SELECTION_SETTINGS) {
    return JSON.stringify(normalizeSelectionSettings(settings));
}
export function appendSelectionSettingsToParams(params, settings = DEFAULT_SELECTION_SETTINGS) {
    const normalized = normalizeSelectionSettings(settings);
    params.set("min_interface_size", String(normalized.minInterfaceSize));
    return params;
}
