import { DEFAULT_SELECTION_SETTINGS } from "./constants.js";

export type SelectionSettings = {
  minInterfaceSize: number;
};

type SelectionSettingsDraft = Partial<Record<keyof SelectionSettings, unknown>>;

export function parseSelectionSettingsDraft(rawSettings: SelectionSettingsDraft = {}): SelectionSettings {
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

export function normalizeSelectionSettings(settings: SelectionSettingsDraft = {}): SelectionSettings {
  try {
    return parseSelectionSettingsDraft(settings);
  } catch {
    return { ...DEFAULT_SELECTION_SETTINGS };
  }
}

export function selectionSettingsKey(settings: SelectionSettingsDraft = DEFAULT_SELECTION_SETTINGS) {
  return JSON.stringify(normalizeSelectionSettings(settings));
}

export function appendSelectionSettingsToParams(
  params: URLSearchParams,
  settings: SelectionSettingsDraft = DEFAULT_SELECTION_SETTINGS,
) {
  const normalized = normalizeSelectionSettings(settings);
  params.set("min_interface_size", String(normalized.minInterfaceSize));
  return params;
}
