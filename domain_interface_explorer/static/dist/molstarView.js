import { Color, MS, ParamDefinition, PluginConfig, PluginCommands, StructureElement, StructureProperties, Viewer, } from "../vendor/molstar-bridge.js";
const WHOLE_PROTEIN_COLOR = "#c7c3bc";
const MAIN_DOMAIN_COLOR = "#8f8a82";
const MAIN_SURFACE_COLOR = "#d7a84c";
const MAIN_INTERFACE_COLOR = "#bc402d";
const PARTNER_DOMAIN_COLOR = "#b8c9dc";
const PARTNER_SURFACE_COLOR = "#5b9fe3";
const PARTNER_INTERFACE_COLOR = "#0b3f78";
const RESIDUE_CONTACT_COLOR = "#4f4f4f";
const REGION_COLOR_THEME_NAME = "domain-interface-region";
function clamp(value, min, max) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return min;
    }
    return Math.max(min, Math.min(max, numeric));
}
function enumSetting(value, allowedValues, fallback) {
    return allowedValues.includes(value) ? value : fallback;
}
function integerClamp(value, min, max) {
    return Math.round(clamp(value, min, max));
}
function coordinateVector(value) {
    if (!value) {
        return null;
    }
    const source = typeof value.length === "number"
        ? value
        : [value.x, value.y, value.z];
    const x = Number(source[0]);
    const y = Number(source[1]);
    const z = Number(source[2]);
    return Number.isFinite(x) && Number.isFinite(y) && Number.isFinite(z) ? [x, y, z] : null;
}
function addVectors(left, right) {
    return [left[0] + right[0], left[1] + right[1], left[2] + right[2]];
}
function subtractVectors(left, right) {
    return [left[0] - right[0], left[1] - right[1], left[2] - right[2]];
}
function displayCameraClipping(settings = {}) {
    return {
        radius: clamp(settings.clipFocusRadius ?? 0, 0, 99),
        far: Boolean(settings.clipFar ?? false),
        minNear: clamp(settings.clipMinNear ?? 0.01, 0.01, 100),
    };
}
function shouldForceFullCameraClipping(settings = {}) {
    return (clamp(settings.clipFocusRadius ?? 0, 0, 99) <= 0 &&
        !Boolean(settings.clipFar ?? false) &&
        clamp(settings.clipMinNear ?? 0.01, 0.01, 100) <= 0.011);
}
function applyCameraForceFullClipping(canvas, settings = {}) {
    const camera = canvas?.camera;
    if (!camera) {
        return false;
    }
    const nextForceFull = shouldForceFullCameraClipping(settings);
    const changed = camera.forceFull !== nextForceFull;
    camera.forceFull = nextForceFull;
    return changed;
}
function surfaceColorSmoothing(settings = {}) {
    const mode = enumSetting(settings.surfaceColorSmoothing, ["auto", "on", "off"], "auto");
    if (mode !== "on") {
        return { name: mode, params: {} };
    }
    return {
        name: "on",
        params: {
            resolutionFactor: clamp(settings.surfaceColorSmoothingResolutionFactor ?? 2, 0.5, 6),
            sampleStride: integerClamp(settings.surfaceColorSmoothingSampleStride ?? 3, 1, 12),
        },
    };
}
function stableJson(value) {
    try {
        return JSON.stringify(value);
    }
    catch (_error) {
        return "";
    }
}
function colorFromHex(value, fallback = "#ffffff") {
    const fallbackHex = String(fallback).replace(/^#/, "");
    const fallbackValue = Number.parseInt(fallbackHex, 16);
    const color = String(value || fallback).trim();
    const hexMatch = color.match(/^#?([0-9a-f]{3}|[0-9a-f]{6})$/i);
    if (hexMatch) {
        const hex = hexMatch[1].length === 3
            ? hexMatch[1].split("").map((part) => `${part}${part}`).join("")
            : hexMatch[1];
        return Color(Number.parseInt(hex, 16));
    }
    const rgbMatch = color.match(/^rgba?\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})(?:\s*,\s*(?:0|1|0?\.\d+))?\s*\)$/i);
    if (rgbMatch) {
        const red = clamp(Number.parseInt(rgbMatch[1], 10), 0, 255);
        const green = clamp(Number.parseInt(rgbMatch[2], 10), 0, 255);
        const blue = clamp(Number.parseInt(rgbMatch[3], 10), 0, 255);
        return Color((red << 16) + (green << 8) + blue);
    }
    return Color(Number.isFinite(fallbackValue) ? fallbackValue : 0xffffff);
}
function normalizeFormat(format) {
    const normalized = String(format || "pdb").trim().toLowerCase();
    if (normalized === "cif" || normalized === "mmcif") {
        return "mmcif";
    }
    if (normalized === "pdbx") {
        return "mmcif";
    }
    return normalized || "pdb";
}
function inferStructureFormat(modelText, requestedFormat = "pdb") {
    const trimmed = String(modelText || "").trimStart();
    if (trimmed.startsWith("data_") || trimmed.includes("_atom_site.")) {
        return "mmcif";
    }
    if (/^(ATOM|HETATM|MODEL|HEADER|REMARK)\b/m.test(trimmed)) {
        return "pdb";
    }
    return normalizeFormat(requestedFormat);
}
function validateStructureModelText(modelText) {
    const text = String(modelText || "");
    const trimmed = text.trimStart();
    if (!trimmed) {
        throw new Error("Structure model download was empty.");
    }
    if (trimmed.startsWith("{")) {
        try {
            const payload = JSON.parse(trimmed);
            throw new Error(payload?.error || "Structure endpoint returned JSON instead of a model file.");
        }
        catch (error) {
            if (error instanceof SyntaxError) {
                throw new Error("Structure endpoint returned JSON instead of a model file.");
            }
            throw error;
        }
    }
    if (!trimmed.startsWith("data_") && !/^(ATOM|HETATM|MODEL|HEADER|REMARK)\b/m.test(trimmed)) {
        throw new Error("Downloaded structure model is not a recognized PDB or mmCIF file.");
    }
}
function normalizeStructureModelText(modelText, requestedFormat = "pdb") {
    let text = String(modelText || "")
        .replace(/^\uFEFF/, "")
        .replace(/\0/g, "")
        .replace(/\r\n?/g, "\n");
    text = `${text.trimEnd()}\n`;
    if (inferStructureFormat(text, requestedFormat) === "pdb" && !/\nEND\s*$/m.test(text)) {
        text += "END\n";
    }
    return text;
}
function friendlyMolstarParseError(error) {
    const message = error?.message ? String(error.message) : String(error);
    if (message.includes("s is undefined") ||
        message.includes("Cannot read properties of undefined") ||
        message.includes("can't access property")) {
        return "the downloaded model could not be read as a PDB/mmCIF structure";
    }
    return message;
}
function numberList(values) {
    const seen = new Set();
    const output = [];
    let candidates;
    if (values === null || values === undefined) {
        candidates = [];
    }
    else if (typeof values === "string") {
        candidates = [values];
    }
    else if (typeof values[Symbol.iterator] === "function") {
        candidates = values;
    }
    else {
        candidates = [values];
    }
    for (const value of candidates) {
        const residueId = Number.parseInt(value, 10);
        if (!Number.isFinite(residueId) || seen.has(residueId)) {
            continue;
        }
        seen.add(residueId);
        output.push(residueId);
    }
    return output;
}
function residueIdPropertyTest(ids, property) {
    if (ids.length === 1) {
        return MS.core.rel.eq([property, ids[0]]);
    }
    return MS.core.set.has([MS.core.type.set(ids), property]);
}
function unionResidues(...groups) {
    const residues = new Set();
    for (const group of groups) {
        for (const residueId of numberList(group)) {
            residues.add(residueId);
        }
    }
    return [...residues].sort((left, right) => left - right);
}
function differenceResidues(source, ...excludedGroups) {
    const excluded = new Set(unionResidues(...excludedGroups));
    return numberList(source).filter((residueId) => !excluded.has(residueId));
}
function residueStyleIds(residueStyles) {
    return Array.isArray(residueStyles)
        ? residueStyles.map((style) => style?.residueId)
        : [];
}
function mainFragmentResidues(payload) {
    if (Array.isArray(payload?.fragment_residue_ids) && payload.fragment_residue_ids.length > 0) {
        return numberList(payload.fragment_residue_ids);
    }
    const start = Number(payload?.fragment_start);
    const end = Number(payload?.fragment_end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
        return [];
    }
    return Array.from({ length: end - start + 1 }, (_value, index) => start + index);
}
function residueContactPairs(payload) {
    const contacts = Array.isArray(payload?.residue_contacts) ? payload.residue_contacts : [];
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
function residueExpression(residueIds) {
    const ids = numberList(residueIds);
    if (ids.length === 0) {
        return null;
    }
    return MS.struct.generator.atomGroups({
        "residue-test": MS.core.logic.or([
            residueIdPropertyTest(ids, MS.struct.atomProperty.macromolecular.auth_seq_id()),
            residueIdPropertyTest(ids, MS.struct.atomProperty.macromolecular.label_seq_id()),
        ]),
        "group-by": MS.struct.atomProperty.macromolecular.residueKey(),
    });
}
function typeParamsFor(settings, options = {}) {
    const alpha = clamp(options.alpha ?? 1, 0.05, 1);
    const material = {
        metalness: clamp(settings?.metalness ?? 0, 0, 1),
        roughness: clamp(settings?.roughness ?? 0.72, 0, 1),
        bumpiness: clamp(settings?.bumpiness ?? 0.08, 0, 1),
    };
    return {
        alpha,
        quality: settings?.quality || "auto",
        tryUseGpu: true,
        material,
        xrayShaded: alpha < 1 ? "inverted" : false,
        ...options.extra,
    };
}
const REGION_REPRESENTATION_TYPES = {
    cartoon: "cartoon",
    surface: "molecular-surface",
    stick: "ball-and-stick",
    spacefill: "spacefill",
    line: "line",
};
function normalizeRegionStyle(value, fallback = "cartoon") {
    const style = String(value || fallback).trim().toLowerCase();
    if (style === "hidden" || REGION_REPRESENTATION_TYPES[style]) {
        return style;
    }
    return fallback;
}
function alphaFromCssColor(value, fallback = 1) {
    const match = String(value || "").trim().match(/^rgba\(\s*\d{1,3}\s*,\s*\d{1,3}\s*,\s*\d{1,3}\s*,\s*(0|1|0?\.\d+)\s*\)$/i);
    if (!match) {
        return fallback;
    }
    const alpha = Number(match[1]);
    return Number.isFinite(alpha) ? clamp(alpha, 0.02, 1) : fallback;
}
function regionRenderSpec(settings, regionKey, fallbackColor, options = {}) {
    const styleFallback = options.styleFallback || "cartoon";
    if (!regionKey) {
        return {
            style: normalizeRegionStyle(options.style || styleFallback, styleFallback),
            color: options.colorOverride || fallbackColor,
            alpha: clamp(options.alpha ?? alphaFromCssColor(options.colorOverride || fallbackColor, 1), 0.02, 1),
        };
    }
    const color = options.colorOverride || settings?.[`${regionKey}Color`] || fallbackColor;
    const defaultAlpha = regionKey === "restProtein"
        ? Number(settings?.contextAlpha ?? 0.24)
        : 1;
    const alphaFallback = Number.isFinite(Number(options.alphaFallback))
        ? Number(options.alphaFallback)
        : defaultAlpha;
    const configuredAlpha = settings?.[`${regionKey}Alpha`];
    return {
        style: normalizeRegionStyle(settings?.[`${regionKey}Style`], styleFallback),
        color,
        alpha: clamp(configuredAlpha ?? alphaFromCssColor(color, alphaFallback), 0.02, 1),
    };
}
function representationTypeForStyle(style) {
    return REGION_REPRESENTATION_TYPES[normalizeRegionStyle(style)] || "cartoon";
}
function setNumericParamIfCustom(extra, paramKey, settings, settingKey, defaultValue, min, max, options = {}) {
    const raw = settings?.[settingKey];
    const clamped = options.integer
        ? integerClamp(raw ?? defaultValue, min, max)
        : clamp(raw ?? defaultValue, min, max);
    if (Math.abs(clamped - defaultValue) > 1e-9) {
        extra[paramKey] = clamped;
    }
}
function setBooleanParamIfCustom(extra, paramKey, settings, settingKey, defaultValue) {
    const value = Boolean(settings?.[settingKey] ?? defaultValue);
    if (value !== defaultValue) {
        extra[paramKey] = value;
    }
}
function setEnumParamIfCustom(extra, paramKey, settings, settingKey, allowedValues, defaultValue) {
    const value = enumSetting(settings?.[settingKey], allowedValues, defaultValue);
    if (value !== defaultValue) {
        extra[paramKey] = value;
    }
}
function typeParamsForRegionStyle(style, settings, options = {}) {
    const normalized = normalizeRegionStyle(style);
    const extra = { ...(options.extra || {}) };
    if (normalized === "cartoon") {
        setNumericParamIfCustom(extra, "sizeFactor", settings, "cartoonSizeFactor", 0.2, 0, 10);
        setNumericParamIfCustom(extra, "aspectRatio", settings, "cartoonAspectRatio", 5, 0.1, 10);
        setNumericParamIfCustom(extra, "arrowFactor", settings, "cartoonArrowFactor", 1.5, 0, 3);
        setBooleanParamIfCustom(extra, "tubularHelices", settings, "cartoonTubularHelices", false);
        setBooleanParamIfCustom(extra, "roundCap", settings, "cartoonRoundCaps", false);
        setEnumParamIfCustom(extra, "helixProfile", settings, "cartoonHelixProfile", ["elliptical", "rounded", "square"], "elliptical");
        setEnumParamIfCustom(extra, "nucleicProfile", settings, "cartoonNucleicProfile", ["elliptical", "rounded", "square"], "square");
        setNumericParamIfCustom(extra, "linearSegments", settings, "cartoonLinearSegments", 8, 1, 48, { integer: true });
        setNumericParamIfCustom(extra, "radialSegments", settings, "cartoonRadialSegments", 16, 2, 56, { integer: true });
        setNumericParamIfCustom(extra, "detail", settings, "cartoonDetail", 0, 0, 3, { integer: true });
    }
    else if (normalized === "stick") {
        extra.sizeFactor = options.sizeFactor ?? extra.sizeFactor ?? 0.28;
        extra.sizeAspectRatio = options.sizeAspectRatio ?? extra.sizeAspectRatio ?? 0.5;
    }
    else if (normalized === "spacefill") {
        extra.sizeFactor = options.sizeFactor ?? extra.sizeFactor ?? 0.58;
    }
    else if (normalized === "line") {
        extra.sizeFactor = options.sizeFactor ?? extra.sizeFactor ?? 0.35;
    }
    else if (normalized === "surface") {
        setNumericParamIfCustom(extra, "resolution", settings, "surfaceResolution", 0.5, 0.01, 20);
        setNumericParamIfCustom(extra, "probeRadius", settings, "surfaceProbeRadius", 1.4, 0, 10);
        setNumericParamIfCustom(extra, "probePositions", settings, "surfaceProbePositions", 36, 12, 90, { integer: true });
        const smoothingMode = enumSetting(settings?.surfaceColorSmoothing, ["auto", "on", "off"], "auto");
        if (smoothingMode !== "auto" ||
            Math.abs(clamp(settings?.surfaceColorSmoothingResolutionFactor ?? 2, 0.5, 6) - 2) > 1e-9 ||
            integerClamp(settings?.surfaceColorSmoothingSampleStride ?? 3, 1, 12) !== 3) {
            extra.smoothColors = surfaceColorSmoothing(settings);
        }
    }
    return typeParamsFor(settings, {
        ...options,
        extra,
    });
}
function regionRepresentationKey(spec, settings) {
    return stableJson({
        type: representationTypeForStyle(spec.style),
        typeParams: typeParamsForRegionStyle(spec.style, settings, { alpha: spec.alpha }),
    });
}
function parsePdbCaCoordinates(modelText) {
    const coordinates = new Map();
    const lines = String(modelText || "").split(/\r?\n/);
    for (const line of lines) {
        if (!line.startsWith("ATOM") && !line.startsWith("HETATM")) {
            continue;
        }
        const atomName = line.slice(12, 16).trim();
        if (atomName !== "CA") {
            continue;
        }
        const residueId = Number.parseInt(line.slice(22, 26), 10);
        const x = Number.parseFloat(line.slice(30, 38));
        const y = Number.parseFloat(line.slice(38, 46));
        const z = Number.parseFloat(line.slice(46, 54));
        if (!Number.isFinite(residueId) ||
            !Number.isFinite(x) ||
            !Number.isFinite(y) ||
            !Number.isFinite(z) ||
            coordinates.has(residueId)) {
            continue;
        }
        coordinates.set(residueId, { x, y, z });
    }
    return coordinates;
}
function pdbResidueIds(modelText) {
    return [...parsePdbCaCoordinates(modelText).keys()].sort((left, right) => left - right);
}
function formatPdbNumber(value) {
    const numeric = Number(value);
    return (Number.isFinite(numeric) ? numeric : 0).toFixed(3).padStart(8).slice(-8);
}
function contactLinePdb(modelText, payload) {
    const coordinates = parsePdbCaCoordinates(modelText);
    if (coordinates.size === 0) {
        return "";
    }
    const atomLines = [];
    const connectLines = [];
    let atomSerial = 1;
    let residueSerial = 1;
    for (const [mainResidueId, partnerResidueId] of residueContactPairs(payload)) {
        const start = coordinates.get(mainResidueId);
        const end = coordinates.get(partnerResidueId);
        if (!start || !end) {
            continue;
        }
        const firstSerial = atomSerial;
        const secondSerial = atomSerial + 1;
        const residueId = ((residueSerial - 1) % 9000) + 1;
        atomLines.push(`HETATM${String(firstSerial).padStart(5)} HE   DCL Z${String(residueId).padStart(4)}    ` +
            `${formatPdbNumber(start.x)}${formatPdbNumber(start.y)}${formatPdbNumber(start.z)}  1.00  0.00          He`);
        atomLines.push(`HETATM${String(secondSerial).padStart(5)} HE   DCL Z${String(residueId).padStart(4)}    ` +
            `${formatPdbNumber(end.x)}${formatPdbNumber(end.y)}${formatPdbNumber(end.z)}  1.00  0.00          He`);
        connectLines.push(`CONECT${String(firstSerial).padStart(5)}${String(secondSerial).padStart(5)}`);
        atomSerial += 2;
        residueSerial += 1;
    }
    return atomLines.length ? `${atomLines.join("\n")}\n${connectLines.join("\n")}\nEND\n` : "";
}
function residueNameForLocation(location) {
    const residueName = String(StructureProperties.atom.label_comp_id(location) || "").toUpperCase();
    return residueName || String(StructureProperties.atom.auth_comp_id?.(location) || "").toUpperCase();
}
function uniqueNumbers(values) {
    const seen = new Set();
    const output = [];
    for (const value of values) {
        const numberValue = Number(value);
        if (!Number.isFinite(numberValue) || seen.has(numberValue)) {
            continue;
        }
        seen.add(numberValue);
        output.push(numberValue);
    }
    return output;
}
function residueIdsForUnitElement(unit, element) {
    const ids = [];
    try {
        const residueIndex = unit?.residueIndex?.[element]
            ?? unit?.model?.atomicHierarchy?.residueAtomSegments?.index?.[element];
        const residues = unit?.model?.atomicHierarchy?.residues;
        if (Number.isFinite(Number(residueIndex)) && residues) {
            ids.push(residues.auth_seq_id?.value?.(residueIndex));
            ids.push(residues.label_seq_id?.value?.(residueIndex));
        }
    }
    catch (_error) {
    }
    return uniqueNumbers(ids);
}
function residueIdsForLocation(location) {
    const ids = [];
    if (StructureElement.Location.is(location)) {
        try {
            ids.push(StructureProperties.residue.auth_seq_id(location));
            ids.push(StructureProperties.residue.label_seq_id(location));
        }
        catch (_error) {
        }
        ids.push(...residueIdsForUnitElement(location.unit, location.element));
    }
    else if (location?.kind === "bond-location") {
        const aElement = location.aUnit?.elements?.[location.aIndex];
        const bElement = location.bUnit?.elements?.[location.bIndex];
        ids.push(...residueIdsForUnitElement(location.aUnit, aElement));
        ids.push(...residueIdsForUnitElement(location.bUnit, bElement));
    }
    return uniqueNumbers(ids);
}
function residueIdForLocation(location) {
    const ids = residueIdsForLocation(location);
    return ids.length > 0 ? ids[0] : null;
}
function colorForResidueLocation(location, colorsByResidue, defaultColor) {
    for (const residueId of residueIdsForLocation(location)) {
        if (colorsByResidue.has(residueId)) {
            return colorsByResidue.get(residueId);
        }
    }
    return defaultColor;
}
const DomainInterfaceRegionColorThemeParams = {
    defaultColor: ParamDefinition.Value("#cccccc", { isHidden: true }),
    colors: ParamDefinition.Value([], { isHidden: true }),
};
function DomainInterfaceRegionColorTheme(_ctx, props = {}) {
    const defaultColor = colorFromHex(props.defaultColor || "#cccccc", "#cccccc");
    const colorsByResidue = new Map();
    for (const entry of Array.isArray(props.colors) ? props.colors : []) {
        const color = colorFromHex(entry?.color || props.defaultColor || "#cccccc", "#cccccc");
        for (const residueId of numberList(entry?.residueIds)) {
            colorsByResidue.set(residueId, color);
        }
    }
    return {
        factory: DomainInterfaceRegionColorTheme,
        granularity: "group",
        preferSmoothing: true,
        color: (location) => colorForResidueLocation(location, colorsByResidue, defaultColor),
        props,
        description: "Domain interface region colors",
    };
}
const DomainInterfaceRegionColorThemeProvider = {
    name: REGION_COLOR_THEME_NAME,
    label: "Domain interface regions",
    category: "Misc",
    factory: DomainInterfaceRegionColorTheme,
    getParams: () => DomainInterfaceRegionColorThemeParams,
    defaultValues: ParamDefinition.getDefaultValues(DomainInterfaceRegionColorThemeParams),
    isApplicable: () => true,
};
function displaySettingsWithPreset(settings = {}) {
    const preset = settings.preset || "soft";
    const base = { ...settings };
    if (preset === "performance") {
        return {
            ...base,
            ambientOcclusion: false,
            depthOfField: false,
            shadows: false,
            outline: false,
            fog: false,
            bloom: false,
            quality: "medium",
            antialiasing: "smaa",
            antialiasSampleLevel: 1,
            pixelScale: 0.75,
        };
    }
    if (preset === "illustrative") {
        return {
            ...base,
            ambientOcclusion: true,
            outline: true,
            depthOfField: false,
            shadows: false,
            quality: "auto",
            antialiasing: "smaa",
            antialiasSampleLevel: Math.max(Number(base.antialiasSampleLevel || 3), 3),
            contextAlpha: Math.max(Number(base.contextAlpha || 0.24), 0.3),
        };
    }
    if (preset === "crisp") {
        return {
            ...base,
            ambientOcclusion: true,
            outline: false,
            depthOfField: false,
            shadows: true,
            quality: "higher",
            antialiasing: "smaa",
            antialiasSampleLevel: Math.max(Number(base.antialiasSampleLevel || 4), 4),
            pixelScale: 1,
        };
    }
    return base;
}
function antialiasingName(value) {
    const name = String(value || "smaa").toLowerCase();
    return name === "off" || name === "fxaa" || name === "smaa" ? name : "smaa";
}
function antialiasingParams(name, settings = {}) {
    if (name === "fxaa") {
        const edgeThresholdMin = clamp(settings.fxaaEdgeThresholdMin ?? 0.0312, 0.0312, 0.0833);
        const edgeThresholdMax = Math.max(edgeThresholdMin, clamp(settings.fxaaEdgeThresholdMax ?? 0.063, 0.063, 0.333));
        return {
            edgeThresholdMin,
            edgeThresholdMax,
            iterations: integerClamp(settings.fxaaIterations ?? 12, 0, 16),
            subpixelQuality: clamp(settings.fxaaSubpixelQuality ?? 0.3, 0, 1),
        };
    }
    if (name === "smaa") {
        return {
            edgeThreshold: clamp(settings.smaaEdgeThreshold ?? 0.1, 0.05, 0.15),
            maxSearchSteps: integerClamp(settings.smaaMaxSearchSteps ?? 16, 0, 32),
        };
    }
    return {};
}
function multisampleLevel(settings) {
    if (settings?.antialiasing === "off") {
        return 0;
    }
    if (settings?.quality === "higher") {
        return Math.round(clamp(settings?.antialiasSampleLevel ?? 4, 0, 5));
    }
    if (settings?.quality === "medium") {
        return Math.round(clamp(settings?.antialiasSampleLevel ?? 2, 0, 5));
    }
    return Math.round(clamp(settings?.antialiasSampleLevel ?? 3, 0, 5));
}
function cameraVector(value) {
    if (!value || typeof value.length !== "number") {
        return value;
    }
    return [Number(value[0] || 0), Number(value[1] || 0), Number(value[2] || 0)];
}
function cameraPoseSnapshot(snapshot) {
    if (!snapshot) {
        return null;
    }
    const pose = {};
    if (typeof snapshot.mode !== "undefined") {
        pose.mode = snapshot.mode;
    }
    if (typeof snapshot.fov !== "undefined") {
        pose.fov = snapshot.fov;
    }
    if (typeof snapshot.position !== "undefined") {
        pose.position = cameraVector(snapshot.position);
    }
    if (typeof snapshot.up !== "undefined") {
        pose.up = cameraVector(snapshot.up);
    }
    if (typeof snapshot.target !== "undefined") {
        pose.target = cameraVector(snapshot.target);
    }
    return Object.keys(pose).length > 0 ? pose : null;
}
export function createDomainMolstarViewer(root, options = {}) {
    return new DomainMolstarViewer(root, options);
}
class DomainMolstarViewer {
    constructor(root, options = {}) {
        this.root = root;
        this.options = options;
        this.mount = null;
        this.viewer = null;
        this.plugin = null;
        this.structure = null;
        this.structureRef = null;
        this.representationRefs = [];
        this.currentModelText = "";
        this.currentResidueCoordinates = null;
        this.displaySettingsKey = "";
        this.hoverSubscription = null;
        this.clickSubscription = null;
        this.pointer = null;
        this.loadGeneration = 0;
        this.displaySettings = {};
        this.hoverMarkOriginals = null;
        this.hoverMarkingDisabled = false;
        this.explicitHighlightActive = false;
        this.regionColorThemeRegistered = false;
        this.readyPromise = null;
        this.root?.addEventListener("pointermove", (event) => {
            this.pointer = { x: event.clientX, y: event.clientY };
        });
        this.root?.addEventListener("pointerleave", () => {
            this.pointer = null;
        });
    }
    ensureMount() {
        if (this.mount?.isConnected) {
            return this.mount;
        }
        const existing = this.root?.querySelector?.(":scope > .molstar-viewer-mount");
        if (existing) {
            this.mount = existing;
            return existing;
        }
        const mount = document.createElement("div");
        mount.className = "molstar-viewer-mount";
        this.root?.prepend(mount);
        this.mount = mount;
        return mount;
    }
    registerRegionColorTheme() {
        const registry = this.plugin?.representation?.structure?.themes?.colorThemeRegistry;
        if (!registry || this.regionColorThemeRegistered) {
            return;
        }
        try {
            if (!registry.has?.(DomainInterfaceRegionColorThemeProvider)) {
                registry.add(DomainInterfaceRegionColorThemeProvider);
            }
            this.regionColorThemeRegistered = true;
        }
        catch (_error) {
            this.regionColorThemeRegistered = true;
        }
    }
    setHoverMarkingEnabled(enabled) {
        const highlights = this.plugin?.managers?.interactivity?.lociHighlights;
        if (!highlights) {
            return;
        }
        try {
            if (!this.hoverMarkOriginals) {
                this.hoverMarkOriginals = {
                    highlightOnly: typeof highlights.highlightOnly === "function"
                        ? highlights.highlightOnly.bind(highlights)
                        : null,
                    highlightOnlyExtend: typeof highlights.highlightOnlyExtend === "function"
                        ? highlights.highlightOnlyExtend.bind(highlights)
                        : null,
                };
            }
            if (enabled) {
                if (this.hoverMarkingDisabled && this.hoverMarkOriginals) {
                    if (this.hoverMarkOriginals.highlightOnly) {
                        highlights.highlightOnly = this.hoverMarkOriginals.highlightOnly;
                    }
                    if (this.hoverMarkOriginals.highlightOnlyExtend) {
                        highlights.highlightOnlyExtend = this.hoverMarkOriginals.highlightOnlyExtend;
                    }
                }
                this.hoverMarkingDisabled = false;
                return;
            }
            if (!this.hoverMarkingDisabled && this.hoverMarkOriginals) {
                if (!this.explicitHighlightActive) {
                    highlights.clearHighlights?.(true);
                }
                if (this.hoverMarkOriginals.highlightOnly) {
                    highlights.highlightOnly = (current, applyGranularity = true) => {
                        if (applyGranularity === false) {
                            this.hoverMarkOriginals.highlightOnly(current, applyGranularity);
                        }
                    };
                }
                if (this.hoverMarkOriginals.highlightOnlyExtend) {
                    highlights.highlightOnlyExtend = (current, applyGranularity = true) => {
                        if (applyGranularity === false) {
                            this.hoverMarkOriginals.highlightOnlyExtend(current, applyGranularity);
                        }
                    };
                }
            }
            this.hoverMarkingDisabled = true;
        }
        catch (_error) {
        }
    }
    async ensureViewer(settings = {}) {
        if (this.viewer) {
            this.applyDisplaySettings(settings);
            return this.viewer;
        }
        if (this.readyPromise) {
            await this.readyPromise;
            this.applyDisplaySettings(settings);
            return this.viewer;
        }
        this.readyPromise = Viewer.create(this.ensureMount(), {
            layoutIsExpanded: false,
            layoutShowControls: false,
            layoutShowSequence: false,
            layoutShowLog: false,
            layoutShowLeftPanel: false,
            layoutShowRemoteState: false,
            viewportShowControls: false,
            viewportShowSettings: false,
            viewportShowSelectionMode: false,
            viewportShowAnimation: false,
            viewportShowTrajectoryControls: false,
            viewportShowExpand: false,
            viewportShowToggleFullscreen: false,
            viewportShowReset: false,
            viewportShowScreenshotControls: false,
            viewportBackgroundColor: "#fdfcf8",
            viewportFocusBehavior: "disabled",
            illumination: false,
            config: [
                [PluginConfig.Viewport.ShowIllumination, false],
                [PluginConfig.Viewport.ShowXR, "never"],
            ],
            pickScale: 0.35,
            pixelScale: 1,
        }).then((viewer) => {
            this.viewer = viewer;
            this.plugin = viewer.plugin;
            this.registerRegionColorTheme();
            this.plugin.managers.interactivity.setProps({ granularity: "residue" });
            this.applyDisplaySettings(settings);
            return viewer;
        });
        try {
            return await this.readyPromise;
        }
        finally {
            this.readyPromise = null;
        }
    }
    async loadStructure({ modelText, payload, format = "pdb", label = "Structure", mode = "structure", columnView = false, contactsVisible = false, residueLookup = new Map(), residueStyles = [], markerResidueStyles = [], clusterLensData = null, representativeLens = "", onHover = null, onHoverEnd = null, onResidueClick = null, onRendered = null, displaySettings = {}, cameraView = null, }) {
        const generation = this.loadGeneration + 1;
        this.loadGeneration = generation;
        const settings = displaySettingsWithPreset(displaySettings);
        await this.ensureViewer(settings);
        if (generation !== this.loadGeneration) {
            return;
        }
        if (typeof modelText !== "string" || modelText.length === 0) {
            throw new Error("Structure model text is missing.");
        }
        const normalizedModelText = normalizeStructureModelText(modelText, format);
        validateStructureModelText(normalizedModelText);
        this.detachHover();
        this.detachResidueClick();
        this.representationRefs = [];
        this.currentModelText = normalizedModelText;
        this.currentResidueCoordinates = null;
        await this.plugin.clear();
        if (generation !== this.loadGeneration) {
            return;
        }
        this.applyDisplaySettings(settings);
        try {
            const data = await this.plugin.builders.data.rawData({ data: normalizedModelText, label });
            const trajectory = await this.plugin.builders.structure.parseTrajectory(data, inferStructureFormat(normalizedModelText, format));
            const model = await this.plugin.builders.structure.createModel(trajectory);
            this.structureRef = await this.plugin.builders.structure.createStructure(model, {
                name: "model",
                params: {},
            });
        }
        catch (error) {
            throw new Error(`Mol* could not parse this structure model: ${friendlyMolstarParseError(error)}.`);
        }
        this.structure = this.structureRef?.cell?.obj?.data || null;
        await this.addRepresentations({
            payload,
            mode,
            columnView,
            contactsVisible,
            residueLookup,
            residueStyles,
            markerResidueStyles,
            clusterLensData,
            representativeLens,
            settings,
            modelText: normalizedModelText,
        });
        this.attachHover(onHover, onHoverEnd);
        this.attachResidueClick(onResidueClick);
        this.resize();
        if (cameraView) {
            this.setView(cameraView, { poseOnly: true });
        }
        this.render();
        onRendered?.();
    }
    representationRefFor(selector) {
        return typeof selector === "string"
            ? selector
            : selector?.ref || selector?.cell?.transform?.ref || selector?.transform?.ref || "";
    }
    trackRepresentationRef(selector) {
        const ref = this.representationRefFor(selector);
        if (ref) {
            this.representationRefs.push(ref);
        }
    }
    async clearRepresentations() {
        if (!this.plugin || !this.plugin.state?.data) {
            this.representationRefs = [];
            return;
        }
        const refs = [...this.representationRefs].reverse();
        this.representationRefs = [];
        for (const ref of refs) {
            try {
                await PluginCommands.State.RemoveObject(this.plugin, {
                    state: this.plugin.state.data,
                    ref,
                    removeParentGhosts: true,
                });
            }
            catch (_error) {
            }
        }
    }
    async updateStructureRepresentations({ payload, mode = "structure", columnView = false, contactsVisible = false, residueLookup = new Map(), residueStyles = [], markerResidueStyles = [], clusterLensData = null, representativeLens = "", onHover = null, onHoverEnd = null, onResidueClick = null, onRendered = null, displaySettings = {}, }) {
        const generation = this.loadGeneration + 1;
        this.loadGeneration = generation;
        const settings = displaySettingsWithPreset(displaySettings);
        await this.ensureViewer(settings);
        if (generation !== this.loadGeneration) {
            return;
        }
        if (!this.structureRef || !this.structure || !this.currentModelText) {
            throw new Error("Structure model is not loaded yet.");
        }
        this.applyDisplaySettings(settings);
        this.detachHover();
        this.detachResidueClick();
        await this.clearRepresentations();
        if (generation !== this.loadGeneration) {
            return;
        }
        await this.addRepresentations({
            payload,
            mode,
            columnView,
            contactsVisible,
            residueLookup,
            residueStyles,
            markerResidueStyles,
            clusterLensData,
            representativeLens,
            settings,
            modelText: this.currentModelText,
        });
        this.attachHover(onHover, onHoverEnd);
        this.resize();
        this.render();
        onRendered?.();
    }
    async addRepresentations(options) {
        const { mode, payload, settings, modelText, residueStyles, markerResidueStyles } = options;
        const fragmentResidues = mainFragmentResidues(payload);
        const partnerResidues = payload?.partner_fragment_residue_ids || [];
        const contextExclusions = mode === "representative"
            ? [
                fragmentResidues,
                residueStyleIds(residueStyles),
                residueStyleIds(markerResidueStyles),
            ]
            : [
                fragmentResidues,
                partnerResidues,
                residueStyleIds(residueStyles),
                residueStyleIds(markerResidueStyles),
            ];
        const contextResidues = differenceResidues(pdbResidueIds(modelText), ...contextExclusions);
        await this.addContextRepresentation(settings, contextResidues, {
            fallbackAll: mode !== "representative",
        });
        if (mode === "representative") {
            await this.addRepresentativeRepresentations(options);
            return;
        }
        await this.addStructureRepresentations(options);
    }
    async addContextRepresentation(settings, residueIds, options = {}) {
        const ids = numberList(residueIds);
        if (ids.length > 0) {
            await this.addResidueRepresentation(ids, WHOLE_PROTEIN_COLOR, settings, "protein-context", "Protein context", {
                regionKey: "restProtein",
                alphaFallback: settings.contextAlpha ?? 0.24,
            });
            return;
        }
        if (options.fallbackAll === false) {
            return;
        }
        await this.addStaticRepresentation("all", WHOLE_PROTEIN_COLOR, settings, "Protein context", {
            regionKey: "restProtein",
            alphaFallback: settings.contextAlpha ?? 0.24,
        });
    }
    async addStaticRepresentation(componentType, fallbackColor, settings, label, options = {}) {
        const spec = regionRenderSpec(settings, options.regionKey, fallbackColor, options);
        if (spec.style === "hidden") {
            return;
        }
        try {
            const component = await this.plugin.builders.structure.tryCreateComponentStatic(this.structureRef, componentType, { label });
            if (!component) {
                return;
            }
            this.trackRepresentationRef(component);
            await this.plugin.builders.structure.representation.addRepresentation(component, {
                type: representationTypeForStyle(spec.style),
                typeParams: typeParamsForRegionStyle(spec.style, settings, { alpha: spec.alpha }),
                color: "uniform",
                colorParams: { value: colorFromHex(spec.color, fallbackColor) },
            });
        }
        catch (_error) {
        }
    }
    async addStructureRepresentations(options) {
        const { payload, columnView, contactsVisible, residueStyles, markerResidueStyles = [], settings, modelText, mode, } = options;
        const fragmentResidues = mainFragmentResidues(payload);
        const interfaceResidues = payload?.interface_residue_ids || [];
        const surfaceOnlyResidues = differenceResidues(payload?.surface_residue_ids, interfaceResidues);
        const domainOnlyResidues = differenceResidues(fragmentResidues, payload?.surface_residue_ids, interfaceResidues);
        const partnerInterfaceResidues = payload?.partner_interface_residue_ids || [];
        const partnerSurfaceOnlyResidues = differenceResidues(payload?.partner_surface_residue_ids, partnerInterfaceResidues);
        const partnerDomainOnlyResidues = differenceResidues(payload?.partner_fragment_residue_ids, payload?.partner_surface_residue_ids, partnerInterfaceResidues);
        if (columnView) {
            await this.addResiduesByColor(stylesToColorMap(residueStyles), settings, "column", { regionKey: "mainDomain" });
        }
        else if (settings.combineSameStyleRegions) {
            await this.addMergedRegionRepresentations([
                {
                    residueIds: domainOnlyResidues,
                    fallbackColor: MAIN_DOMAIN_COLOR,
                    regionKey: "mainDomain",
                    key: "main-domain",
                    label: "Main domain",
                },
                {
                    residueIds: surfaceOnlyResidues,
                    fallbackColor: MAIN_SURFACE_COLOR,
                    regionKey: "mainSurface",
                    key: "main-surface",
                    label: "Main surface",
                },
                {
                    residueIds: interfaceResidues,
                    fallbackColor: MAIN_INTERFACE_COLOR,
                    regionKey: "mainInterface",
                    key: "main-interface",
                    label: "Main interface",
                },
            ], settings, "main");
        }
        else {
            await this.addResidueRepresentation(domainOnlyResidues, MAIN_DOMAIN_COLOR, settings, "main-domain", "Main domain", {
                regionKey: "mainDomain",
            });
            await this.addResidueRepresentation(surfaceOnlyResidues, MAIN_SURFACE_COLOR, settings, "main-surface", "Main surface", {
                regionKey: "mainSurface",
            });
            await this.addResidueRepresentation(interfaceResidues, MAIN_INTERFACE_COLOR, settings, "main-interface", "Main interface", { regionKey: "mainInterface" });
        }
        await this.addMarkerResidues(stylesToColorMap(markerResidueStyles), settings);
        if (settings.combineSameStyleRegions) {
            await this.addMergedRegionRepresentations([
                {
                    residueIds: partnerDomainOnlyResidues,
                    fallbackColor: PARTNER_DOMAIN_COLOR,
                    regionKey: "partnerDomain",
                    key: "partner-domain",
                    label: "Partner domain",
                },
                {
                    residueIds: partnerSurfaceOnlyResidues,
                    fallbackColor: PARTNER_SURFACE_COLOR,
                    regionKey: "partnerSurface",
                    key: "partner-surface",
                    label: "Partner surface",
                },
                {
                    residueIds: partnerInterfaceResidues,
                    fallbackColor: PARTNER_INTERFACE_COLOR,
                    regionKey: "partnerInterface",
                    key: "partner-interface",
                    label: "Partner interface",
                },
            ], settings, "partner");
        }
        else {
            await this.addResidueRepresentation(partnerDomainOnlyResidues, PARTNER_DOMAIN_COLOR, settings, "partner-domain", "Partner domain", { regionKey: "partnerDomain" });
            await this.addResidueRepresentation(partnerSurfaceOnlyResidues, PARTNER_SURFACE_COLOR, settings, "partner-surface", "Partner surface", { regionKey: "partnerSurface" });
            await this.addResidueRepresentation(partnerInterfaceResidues, PARTNER_INTERFACE_COLOR, settings, "partner-interface", "Partner interface", { regionKey: "partnerInterface" });
        }
        if (mode !== "compare" && contactsVisible) {
            await this.addResidueContactRepresentation(modelText, payload, settings);
        }
    }
    async addRepresentativeRepresentations(options) {
        const { payload, residueStyles, clusterLensData, representativeLens, settings } = options;
        const styledResidues = residueStyleIds(residueStyles);
        await this.addResidueRepresentation(differenceResidues(mainFragmentResidues(payload), styledResidues), MAIN_DOMAIN_COLOR, settings, "representative-domain", "Representative domain", { regionKey: "mainDomain" });
        await this.addResiduesByColor(stylesToColorMap(residueStyles), settings, "representative", {
            regionKey: "mainDomain",
        });
    }
    async addResiduesByColor(residueColorMap, settings, keyPrefix, options = {}) {
        for (const [color, residueIds] of residueColorMap.entries()) {
            await this.addResidueRepresentation(residueIds, color, settings, `${keyPrefix}-${String(color).replace(/[^a-z0-9]/gi, "")}`, "Residue group", {
                ...options,
                colorOverride: color,
            });
        }
    }
    async addMergedRegionRepresentations(regionGroups, settings, keyPrefix) {
        const buckets = new Map();
        for (const group of regionGroups) {
            const ids = numberList(group.residueIds);
            if (ids.length === 0) {
                continue;
            }
            const spec = regionRenderSpec(settings, group.regionKey, group.fallbackColor, group);
            if (spec.style === "hidden") {
                continue;
            }
            const bucketKey = regionRepresentationKey(spec, settings);
            const bucket = buckets.get(bucketKey) || [];
            bucket.push({ ...group, residueIds: ids, spec });
            buckets.set(bucketKey, bucket);
        }
        let bucketIndex = 0;
        for (const bucket of buckets.values()) {
            bucketIndex += 1;
            if (bucket.length === 1) {
                const [group] = bucket;
                await this.addResidueRepresentation(group.residueIds, group.fallbackColor, settings, group.key, group.label, { regionKey: group.regionKey });
                continue;
            }
            await this.addColoredResidueRepresentation(bucket, settings, `${keyPrefix}-merged-${bucketIndex}`, bucket.map((group) => group.label).join(" + "));
        }
    }
    async addColoredResidueRepresentation(regionGroups, settings, key, label) {
        const ids = unionResidues(...regionGroups.map((group) => group.residueIds));
        if (ids.length === 0) {
            return;
        }
        const expression = residueExpression(ids);
        if (!expression) {
            return;
        }
        const first = regionGroups[0];
        try {
            const component = await this.plugin.builders.structure.tryCreateComponentFromExpression(this.structureRef, expression, key, { label });
            if (!component) {
                return;
            }
            this.trackRepresentationRef(component);
            await this.plugin.builders.structure.representation.addRepresentation(component, {
                type: representationTypeForStyle(first.spec.style),
                typeParams: typeParamsForRegionStyle(first.spec.style, settings, { alpha: first.spec.alpha }),
                color: REGION_COLOR_THEME_NAME,
                colorParams: {
                    defaultColor: first.spec.color,
                    colors: regionGroups.map((group) => ({
                        residueIds: group.residueIds,
                        color: group.spec.color,
                    })),
                },
            });
        }
        catch (_error) {
        }
    }
    async addMarkerResidues(residueColorMap, settings) {
        for (const [color, residueIds] of residueColorMap.entries()) {
            const keyColor = String(color).replace(/[^a-z0-9]/gi, "");
            await this.addResidueSpacefill(residueIds, color, settings, `structure-marker-spacefill-${keyColor}`, "Selected column residue", { alpha: 0.65, sizeFactor: 0.64 });
        }
    }
    async addResidueRepresentation(residueIds, fallbackColor, settings, key, label, options = {}) {
        const ids = numberList(residueIds);
        if (ids.length === 0) {
            return;
        }
        const spec = regionRenderSpec(settings, options.regionKey, fallbackColor, options);
        if (spec.style === "hidden") {
            return;
        }
        const expression = residueExpression(ids);
        if (!expression) {
            return;
        }
        try {
            const component = await this.plugin.builders.structure.tryCreateComponentFromExpression(this.structureRef, expression, key, { label });
            if (!component) {
                return;
            }
            this.trackRepresentationRef(component);
            await this.plugin.builders.structure.representation.addRepresentation(component, {
                type: representationTypeForStyle(spec.style),
                typeParams: typeParamsForRegionStyle(spec.style, settings, { alpha: spec.alpha }),
                color: "uniform",
                colorParams: { value: colorFromHex(spec.color, fallbackColor) },
            });
        }
        catch (_error) {
        }
    }
    async addResidueSpacefill(residueIds, color, settings, key, label, options = {}) {
        const ids = numberList(residueIds);
        if (ids.length === 0) {
            return;
        }
        const expression = residueExpression(ids);
        if (!expression) {
            return;
        }
        try {
            const component = await this.plugin.builders.structure.tryCreateComponentFromExpression(this.structureRef, expression, key, { label });
            if (!component) {
                return;
            }
            this.trackRepresentationRef(component);
            await this.plugin.builders.structure.representation.addRepresentation(component, {
                type: "spacefill",
                typeParams: typeParamsFor(settings, {
                    alpha: options.alpha ?? 1,
                    extra: {
                        sizeFactor: options.sizeFactor ?? 0.58,
                    },
                }),
                color: "uniform",
                colorParams: { value: colorFromHex(color) },
            });
        }
        catch (_error) {
        }
    }
    async addResidueContactRepresentation(modelText, payload, settings) {
        try {
            const pdb = contactLinePdb(modelText, payload);
            if (!pdb) {
                return;
            }
            const data = await this.plugin.builders.data.rawData({ data: pdb, label: "Residue contacts" });
            this.trackRepresentationRef(data);
            const trajectory = await this.plugin.builders.structure.parseTrajectory(data, "pdb");
            const model = await this.plugin.builders.structure.createModel(trajectory);
            const structure = await this.plugin.builders.structure.createStructure(model, {
                name: "model",
                params: {},
            });
            const component = await this.plugin.builders.structure.tryCreateComponentStatic(structure, "all", { label: "Residue contacts" });
            if (!component) {
                return;
            }
            await this.plugin.builders.structure.representation.addRepresentation(component, {
                type: "line",
                typeParams: typeParamsFor(settings, {
                    alpha: clamp(settings.contactOpacity ?? 0.6, 0.05, 1),
                    extra: {
                        visuals: ["intra-bond"],
                        sizeFactor: clamp((settings.contactRadius ?? 0.06) * 18, 0.7, 3),
                        linkScale: 1,
                        linkSpacing: 0.1,
                        dashCount: 0,
                        multipleBonds: "off",
                        ignoreHydrogens: false,
                    },
                }),
                color: "uniform",
                colorParams: { value: colorFromHex(settings.contactColor || RESIDUE_CONTACT_COLOR, RESIDUE_CONTACT_COLOR) },
            });
        }
        catch (_error) {
        }
    }
    attachHover(onHover, onHoverEnd) {
        if (!this.plugin || typeof onHover !== "function") {
            return;
        }
        this.hoverSubscription = this.plugin.behaviors.interaction.hover.subscribe(({ current }) => {
            const loci = current?.loci;
            if (!StructureElement.Loci.is(loci) || StructureElement.Loci.isEmpty(loci)) {
                onHoverEnd?.();
                return;
            }
            if (!this.isPrimaryStructureLoci(loci)) {
                onHoverEnd?.();
                return;
            }
            const firstResidue = StructureElement.Loci.firstResidue(loci);
            const location = StructureElement.Loci.getFirstLocation(firstResidue);
            if (!location) {
                onHoverEnd?.();
                return;
            }
            const residueIds = residueIdsForLocation(location);
            const residueId = residueIds[0] ?? null;
            if (residueId === null) {
                onHoverEnd?.();
                return;
            }
            onHover({
                residueId,
                residueIds,
                residueName: residueNameForLocation(location),
                pointer: this.pointer,
            });
        });
    }
    isPrimaryStructureLoci(loci) {
        if (!this.structure || !loci?.structure) {
            return false;
        }
        return loci.structure === this.structure || loci.structure.root === this.structure.root;
    }
    detachHover() {
        this.hoverSubscription?.unsubscribe?.();
        this.hoverSubscription = null;
    }
    residueCoordinate(residueId) {
        if (!this.currentResidueCoordinates && this.currentModelText) {
            this.currentResidueCoordinates = parsePdbCaCoordinates(this.currentModelText);
        }
        return this.currentResidueCoordinates?.get(residueId) || null;
    }
    residuePickFromInteraction(event) {
        const loci = event?.current?.loci;
        if (!StructureElement.Loci.is(loci) || StructureElement.Loci.isEmpty(loci)) {
            return null;
        }
        if (!this.isPrimaryStructureLoci(loci)) {
            return null;
        }
        const firstResidue = StructureElement.Loci.firstResidue(loci);
        const location = StructureElement.Loci.getFirstLocation(firstResidue);
        if (!location) {
            return null;
        }
        const residueIds = residueIdsForLocation(location);
        const residueId = residueIds[0] ?? null;
        if (residueId === null) {
            return null;
        }
        const position = coordinateVector(event?.position);
        const fallback = position ? null : this.residueCoordinate(residueId);
        return {
            residueId,
            residueIds,
            residueName: residueNameForLocation(location),
            position: position || coordinateVector(fallback),
            pointer: this.pointer,
        };
    }
    attachResidueClick(onResidueClick) {
        this.detachResidueClick();
        if (!this.plugin || typeof onResidueClick !== "function") {
            return;
        }
        this.clickSubscription = this.plugin.behaviors.interaction.click.subscribe((event) => {
            const pick = this.residuePickFromInteraction(event);
            if (pick) {
                onResidueClick(pick);
            }
        });
    }
    detachResidueClick() {
        this.clickSubscription?.unsubscribe?.();
        this.clickSubscription = null;
    }
    getResidueLoci(residueIds) {
        if (!this.structure) {
            return null;
        }
        const expression = residueExpression(residueIds);
        if (!expression) {
            return null;
        }
        try {
            return StructureElement.Loci.fromExpression(this.structure, expression);
        }
        catch (_error) {
            return null;
        }
    }
    applyCameraClippingSettings(settings = this.displaySettings) {
        if (!this.plugin?.canvas3d) {
            return;
        }
        const resolved = displaySettingsWithPreset(settings || {});
        try {
            const forceFullChanged = applyCameraForceFullClipping(this.plugin.canvas3d, resolved);
            this.plugin.canvas3d.setProps({ cameraClipping: displayCameraClipping(resolved) });
            if (forceFullChanged) {
                this.render();
            }
        }
        catch (_error) {
        }
    }
    setCameraTarget(position) {
        const nextTarget = coordinateVector(position);
        const canvas = this.plugin?.canvas3d;
        const snapshot = canvas?.camera?.getSnapshot?.();
        const currentTarget = coordinateVector(snapshot?.target);
        const currentPosition = coordinateVector(snapshot?.position);
        if (!nextTarget || !canvas || !snapshot || !currentTarget || !currentPosition) {
            return false;
        }
        const delta = subtractVectors(nextTarget, currentTarget);
        canvas.requestCameraReset?.({
            snapshot: {
                ...snapshot,
                target: nextTarget,
                position: addVectors(currentPosition, delta),
            },
            durationMs: 0,
        });
        this.applyCameraClippingSettings();
        this.render();
        return true;
    }
    focusResidues(residueIds, extraRadius = 6) {
        const loci = this.getResidueLoci(residueIds);
        if (loci && !StructureElement.Loci.isEmpty(loci)) {
            this.plugin?.managers?.camera?.focusLoci(loci, { durationMs: 0, extraRadius });
            this.applyCameraClippingSettings();
            return;
        }
        if (this.plugin) {
            PluginCommands.Camera.Reset(this.plugin, { durationMs: 0 });
            this.applyCameraClippingSettings();
        }
    }
    focusResiduesStable(residueIds, extraRadius = 6) {
        const focus = () => {
            this.resize();
            this.focusResidues(residueIds, extraRadius);
            this.render();
        };
        focus();
        window.requestAnimationFrame(() => {
            focus();
            window.requestAnimationFrame(focus);
        });
    }
    highlightResidues(residueIds) {
        const loci = this.getResidueLoci(residueIds);
        const highlights = this.plugin?.managers?.interactivity?.lociHighlights;
        if (!highlights) {
            return;
        }
        if (loci && !StructureElement.Loci.isEmpty(loci)) {
            this.explicitHighlightActive = true;
            highlights.highlightOnly({ loci }, false);
            this.render();
            return;
        }
        this.clearHighlight();
    }
    clearHighlight() {
        this.explicitHighlightActive = false;
        this.plugin?.managers?.interactivity?.lociHighlights?.clearHighlights?.();
        this.render();
    }
    getView() {
        return this.plugin?.canvas3d?.camera?.getSnapshot?.() || null;
    }
    setView(view, options = {}) {
        if (!view || !this.plugin) {
            return;
        }
        const snapshot = options.poseOnly ? cameraPoseSnapshot(view) : view;
        if (!snapshot) {
            return;
        }
        PluginCommands.Camera.SetSnapshot(this.plugin, { snapshot, durationMs: 0 });
        this.applyCameraClippingSettings();
    }
    resize() {
        this.plugin?.handleResize?.();
        this.plugin?.canvas3d?.requestResize?.();
    }
    render() {
        this.plugin?.canvas3d?.commit?.(true);
    }
    clear() {
        this.loadGeneration += 1;
        this.detachHover();
        this.detachResidueClick();
        if (this.plugin) {
            void this.plugin.clear();
        }
        this.structure = null;
        this.structureRef = null;
        this.representationRefs = [];
        this.currentModelText = "";
        this.currentResidueCoordinates = null;
    }
    destroy() {
        this.loadGeneration += 1;
        this.detachHover();
        this.detachResidueClick();
        this.structure = null;
        this.structureRef = null;
        this.representationRefs = [];
        this.currentModelText = "";
        this.currentResidueCoordinates = null;
        this.displaySettingsKey = "";
        this.hoverMarkOriginals = null;
        this.hoverMarkingDisabled = false;
        this.explicitHighlightActive = false;
        this.regionColorThemeRegistered = false;
        this.viewer?.dispose?.();
        if (!this.viewer?.dispose && this.plugin) {
            this.plugin.dispose?.();
        }
        this.viewer = null;
        this.plugin = null;
    }
    applyDisplaySettings(settings = {}) {
        if (!this.plugin?.canvas3d) {
            return false;
        }
        const resolved = displaySettingsWithPreset(settings);
        const canvas = this.plugin.canvas3d;
        const background = colorFromHex(resolved.background || "#fdfcf8");
        const occlusionStrength = clamp(resolved.occlusionStrength ?? 0.8, 0, 3);
        const antialiasing = antialiasingName(resolved.antialiasing);
        const sampleLevel = multisampleLevel(resolved);
        const illuminationRendersMin = integerClamp(resolved.illuminationRendersPerFrameMin ?? 1, 1, 64);
        const illuminationRendersMax = Math.max(illuminationRendersMin, integerClamp(resolved.illuminationRendersPerFrameMax ?? 16, 1, 64));
        const illuminationDenoiseMin = clamp(resolved.illuminationDenoiseMin ?? 0.15, 0, 4);
        const illuminationDenoiseMax = Math.max(illuminationDenoiseMin, clamp(resolved.illuminationDenoiseMax ?? 1, 0, 4));
        const forceFullChanged = applyCameraForceFullClipping(canvas, resolved);
        const props = {
            camera: {
                mode: resolved.cameraMode || "perspective",
                helper: { axes: { name: "off", params: {} } },
                fov: clamp(resolved.fieldOfView ?? 45, 20, 90),
            },
            cameraClipping: displayCameraClipping(resolved),
            cameraFog: resolved.fog
                ? { name: "on", params: { intensity: clamp(resolved.fogIntensity ?? 18, 1, 100) } }
                : { name: "off", params: {} },
            renderer: {
                backgroundColor: background,
                ambientIntensity: clamp(resolved.ambientIntensity ?? 0.48, 0, 2),
                exposure: clamp(resolved.exposure ?? 1.0, 0.2, 2.5),
                highlightColor: colorFromHex(resolved.highlightColor || "#f3c14f"),
                highlightStrength: clamp(resolved.highlightStrength ?? 0.42, 0, 1),
                light: [
                    {
                        inclination: 145,
                        azimuth: 320,
                        color: Color(0xffffff),
                        intensity: clamp(resolved.lightIntensity ?? 0.82, 0, 3),
                    },
                ],
            },
            illumination: {
                enabled: Boolean(resolved.illumination ?? false),
                maxIterations: integerClamp(resolved.illuminationMaxIterations ?? 8, 0, 16),
                rendersPerFrame: [illuminationRendersMin, illuminationRendersMax],
                targetFps: clamp(resolved.illuminationTargetFps ?? 30, 0, 120),
                steps: integerClamp(resolved.illuminationSteps ?? 32, 1, 1024),
                refineSteps: integerClamp(resolved.illuminationRefineSteps ?? 4, 0, 8),
                bounces: integerClamp(resolved.illuminationBounces ?? 4, 1, 32),
                denoise: Boolean(resolved.illuminationDenoise ?? true),
                denoiseThreshold: [illuminationDenoiseMin, illuminationDenoiseMax],
                ignoreOutline: Boolean(resolved.illuminationIgnoreOutline ?? true),
            },
            multiSample: {
                mode: sampleLevel > 0 ? "on" : "off",
                sampleLevel,
                reduceFlicker: true,
                reuseOcclusion: false,
            },
            postprocessing: {
                enabled: true,
                occlusion: resolved.ambientOcclusion
                    ? {
                        name: "on",
                        params: {
                            multiScale: { name: "off", params: {} },
                            radius: clamp(resolved.aoRadius ?? 5, 0, 20),
                            bias: occlusionStrength,
                            blurKernelSize: integerClamp(resolved.aoBlurKernelSize ?? 15, 1, 25),
                            blurDepthBias: 0.5,
                            samples: integerClamp(resolved.aoSamples ?? 32, 1, 256),
                            resolutionScale: clamp(resolved.aoResolutionScale ?? (resolved.quality === "low" ? 0.6 : 1), 0.1, 1),
                            color: Color(0x000000),
                            transparentThreshold: 0.4,
                        },
                    }
                    : { name: "off", params: {} },
                shadow: resolved.shadows
                    ? {
                        name: "on",
                        params: {
                            steps: integerClamp(resolved.shadowSteps ?? 2, 1, 64),
                            maxDistance: clamp(resolved.shadowDistance ?? 4, 0, 256),
                            tolerance: clamp(resolved.shadowTolerance ?? 1, 0, 10),
                        },
                    }
                    : { name: "off", params: {} },
                outline: resolved.outline
                    ? {
                        name: "on",
                        params: {
                            scale: clamp(resolved.outlineScale ?? 1, 1, 5),
                            threshold: clamp(resolved.outlineThreshold ?? 0.33, 0.01, 1),
                            color: colorFromHex(resolved.outlineColor || "#1e1b17", "#1e1b17"),
                            includeTransparent: Boolean(resolved.outlineTransparent ?? true),
                        },
                    }
                    : { name: "off", params: {} },
                dof: resolved.depthOfField
                    ? {
                        name: "on",
                        params: {
                            blurSize: clamp(resolved.dofBlur ?? 8, 1, 24),
                            blurSpread: clamp(resolved.dofBlurSpread ?? 1, 0, 10),
                            inFocus: clamp(resolved.dofFocusOffset ?? 0, -5000, 5000),
                            PPM: clamp(resolved.dofFocusRange ?? 28, 1, 160),
                            center: enumSetting(resolved.dofFocusTarget, ["camera-target", "scene-center"], "camera-target"),
                            mode: enumSetting(resolved.dofFocusMode, ["sphere", "plane"], "sphere"),
                        },
                    }
                    : { name: "off", params: {} },
                antialiasing: { name: antialiasing, params: antialiasingParams(antialiasing, resolved) },
                sharpening: resolved.sharpen
                    ? {
                        name: "on",
                        params: {
                            sharpness: clamp(resolved.sharpenStrength ?? 0.35, 0, 1),
                            denoise: Boolean(resolved.sharpenDenoise ?? true),
                        },
                    }
                    : { name: "off", params: {} },
                bloom: resolved.bloom
                    ? {
                        name: "on",
                        params: {
                            strength: clamp(resolved.bloomStrength ?? 0.65, 0, 3),
                            radius: clamp(resolved.bloomRadius ?? 0.18, 0, 1),
                            threshold: clamp(resolved.bloomThreshold ?? 0.82, 0, 1),
                            mode: enumSetting(resolved.bloomMode, ["luminosity", "emissive"], "luminosity"),
                        },
                    }
                    : { name: "off", params: {} },
            },
        };
        const propsKey = stableJson(props);
        this.displaySettings = resolved;
        this.setHoverMarkingEnabled(!Boolean(resolved.illumination ?? false));
        if (propsKey && propsKey === this.displaySettingsKey) {
            if (forceFullChanged) {
                this.render();
            }
            return forceFullChanged;
        }
        try {
            canvas.setProps(props);
            this.displaySettingsKey = propsKey;
            this.render();
            return true;
        }
        catch (error) {
            console.debug("[molstar] display settings update failed", error);
            return false;
        }
    }
}
function stylesToColorMap(residueStyles) {
    const colors = new Map();
    for (const style of Array.isArray(residueStyles) ? residueStyles : []) {
        const id = Number.parseInt(style?.residueId, 10);
        if (!Number.isFinite(id) || !style?.color) {
            continue;
        }
        const bucket = colors.get(style.color) || [];
        bucket.push(id);
        colors.set(style.color, bucket);
    }
    return colors;
}
export const MOLSTAR_STRUCTURE_COLORS = {
    WHOLE_PROTEIN_COLOR,
    MAIN_DOMAIN_COLOR,
    MAIN_SURFACE_COLOR,
    MAIN_INTERFACE_COLOR,
    PARTNER_DOMAIN_COLOR,
    PARTNER_SURFACE_COLOR,
    PARTNER_INTERFACE_COLOR,
    RESIDUE_CONTACT_COLOR,
};
