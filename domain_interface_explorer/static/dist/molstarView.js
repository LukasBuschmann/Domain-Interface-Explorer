import { Color, MS, PluginConfig, PluginCommands, StructureElement, StructureProperties, Viewer, } from "../vendor/molstar-bridge.js";
const WHOLE_PROTEIN_COLOR = "#c7c3bc";
const MAIN_DOMAIN_COLOR = "#8f8a82";
const MAIN_SURFACE_COLOR = "#d7a84c";
const MAIN_INTERFACE_COLOR = "#bc402d";
const PARTNER_DOMAIN_COLOR = "#b8c9dc";
const PARTNER_SURFACE_COLOR = "#5b9fe3";
const PARTNER_INTERFACE_COLOR = "#0b3f78";
const RESIDUE_CONTACT_COLOR = "#4f4f4f";
function clamp(value, min, max) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return min;
    }
    return Math.max(min, Math.min(max, numeric));
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
function filterResidueStyles(residueStyles, excludedResidues) {
    const excluded = new Set(numberList(excludedResidues));
    return Array.isArray(residueStyles)
        ? residueStyles.filter((style) => {
            const residueId = Number.parseInt(style?.residueId, 10);
            return !Number.isFinite(residueId) || !excluded.has(residueId);
        })
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
function residueIdForLocation(location) {
    const authSeqId = Number(StructureProperties.residue.auth_seq_id(location));
    if (Number.isFinite(authSeqId)) {
        return authSeqId;
    }
    const labelSeqId = Number(StructureProperties.residue.label_seq_id(location));
    return Number.isFinite(labelSeqId) ? labelSeqId : null;
}
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
        this.hoverSubscription = null;
        this.pointer = null;
        this.loadGeneration = 0;
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
    async loadStructure({ modelText, payload, format = "pdb", label = "Structure", mode = "structure", columnView = false, contactsVisible = false, residueLookup = new Map(), residueStyles = [], markerResidueStyles = [], clusterLensData = null, representativeLens = "", onHover = null, onHoverEnd = null, displaySettings = {}, cameraView = null, }) {
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
        this.resize();
        if (cameraView) {
            this.setView(cameraView, { poseOnly: true });
        }
        this.render();
    }
    async addRepresentations(options) {
        const { mode, payload, settings, modelText, residueStyles, markerResidueStyles } = options;
        const fragmentResidues = mainFragmentResidues(payload);
        const partnerResidues = payload?.partner_fragment_residue_ids || [];
        const contextResidues = differenceResidues(pdbResidueIds(modelText), fragmentResidues, partnerResidues, residueStyleIds(residueStyles), residueStyleIds(markerResidueStyles));
        await this.addContextCartoon(settings, contextResidues);
        if (mode === "representative") {
            await this.addRepresentativeRepresentations(options);
            return;
        }
        await this.addStructureRepresentations(options);
    }
    async addContextCartoon(settings, residueIds) {
        const ids = numberList(residueIds);
        if (ids.length > 0) {
            await this.addResidueCartoon(ids, WHOLE_PROTEIN_COLOR, settings, "protein-context", "Protein context", {
                alpha: clamp(settings.contextAlpha ?? 0.24, 0.05, 0.9),
            });
            return;
        }
        await this.addStaticCartoon("all", WHOLE_PROTEIN_COLOR, settings, "Protein context", {
            alpha: clamp(settings.contextAlpha ?? 0.24, 0.05, 0.9),
        });
    }
    async addStaticCartoon(componentType, color, settings, label, options = {}) {
        try {
            const component = await this.plugin.builders.structure.tryCreateComponentStatic(this.structureRef, componentType, { label });
            if (!component) {
                return;
            }
            await this.plugin.builders.structure.representation.addRepresentation(component, {
                type: "cartoon",
                typeParams: typeParamsFor(settings, { alpha: options.alpha ?? 1 }),
                color: "uniform",
                colorParams: { value: colorFromHex(color) },
            });
        }
        catch (_error) {
        }
    }
    async addStructureRepresentations(options) {
        const { payload, columnView, contactsVisible, residueStyles, markerResidueStyles = [], settings, modelText, mode, } = options;
        const fragmentResidues = mainFragmentResidues(payload);
        const interfaceResidues = payload?.interface_residue_ids || [];
        const markerResidues = residueStyleIds(markerResidueStyles);
        const surfaceOnlyResidues = differenceResidues(payload?.surface_residue_ids, interfaceResidues, markerResidues);
        const domainOnlyResidues = differenceResidues(fragmentResidues, payload?.surface_residue_ids, interfaceResidues, markerResidues);
        const visibleInterfaceResidues = differenceResidues(interfaceResidues, markerResidues);
        const partnerInterfaceResidues = payload?.partner_interface_residue_ids || [];
        const partnerSurfaceOnlyResidues = differenceResidues(payload?.partner_surface_residue_ids, partnerInterfaceResidues);
        const partnerDomainOnlyResidues = differenceResidues(payload?.partner_fragment_residue_ids, payload?.partner_surface_residue_ids, partnerInterfaceResidues);
        if (columnView) {
            await this.addResiduesByColor(stylesToColorMap(filterResidueStyles(residueStyles, markerResidues)), settings, "column");
        }
        else {
            await this.addResidueCartoon(domainOnlyResidues, MAIN_DOMAIN_COLOR, settings, "main-domain", "Main domain");
            await this.addResidueCartoon(surfaceOnlyResidues, MAIN_SURFACE_COLOR, settings, "main-surface", "Main surface");
            await this.addResidueCartoon(visibleInterfaceResidues, MAIN_INTERFACE_COLOR, settings, "main-interface", "Main interface");
        }
        await this.addResiduesByColor(stylesToColorMap(markerResidueStyles), settings, "structure-marker");
        await this.addResidueCartoon(partnerDomainOnlyResidues, PARTNER_DOMAIN_COLOR, settings, "partner-domain", "Partner domain");
        await this.addResidueCartoon(partnerSurfaceOnlyResidues, PARTNER_SURFACE_COLOR, settings, "partner-surface", "Partner surface");
        await this.addResidueCartoon(partnerInterfaceResidues, PARTNER_INTERFACE_COLOR, settings, "partner-interface", "Partner interface");
        if (mode !== "compare" && contactsVisible) {
            await this.addResidueContactRepresentation(modelText, payload, settings);
        }
    }
    async addRepresentativeRepresentations(options) {
        const { payload, residueStyles, clusterLensData, representativeLens, settings } = options;
        const styledResidues = residueStyleIds(residueStyles);
        await this.addResidueCartoon(differenceResidues(mainFragmentResidues(payload), styledResidues), MAIN_DOMAIN_COLOR, settings, "representative-domain", "Representative domain");
        await this.addResiduesByColor(stylesToColorMap(residueStyles), settings, "representative");
    }
    async addResiduesByColor(residueColorMap, settings, keyPrefix) {
        for (const [color, residueIds] of residueColorMap.entries()) {
            await this.addResidueCartoon(residueIds, color, settings, `${keyPrefix}-${String(color).replace(/[^a-z0-9]/gi, "")}`, "Residue group");
        }
    }
    async addResidueCartoon(residueIds, color, settings, key, label, options = {}) {
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
            await this.plugin.builders.structure.representation.addRepresentation(component, {
                type: "cartoon",
                typeParams: typeParamsFor(settings, { alpha: options.alpha ?? 1 }),
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
                colorParams: { value: colorFromHex(RESIDUE_CONTACT_COLOR) },
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
            const residueId = residueIdForLocation(location);
            if (residueId === null) {
                onHoverEnd?.();
                return;
            }
            onHover({
                residueId,
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
    focusResidues(residueIds, extraRadius = 6) {
        const loci = this.getResidueLoci(residueIds);
        if (loci && !StructureElement.Loci.isEmpty(loci)) {
            this.plugin?.managers?.camera?.focusLoci(loci, { durationMs: 0, extraRadius });
            return;
        }
        if (this.plugin) {
            PluginCommands.Camera.Reset(this.plugin, { durationMs: 0 });
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
            highlights.highlightOnly({ loci }, false);
            this.render();
            return;
        }
        this.clearHighlight();
    }
    clearHighlight() {
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
        if (this.plugin) {
            void this.plugin.clear();
        }
        this.structure = null;
        this.structureRef = null;
    }
    destroy() {
        this.loadGeneration += 1;
        this.detachHover();
        this.structure = null;
        this.structureRef = null;
        this.viewer?.dispose?.();
        if (!this.viewer?.dispose && this.plugin) {
            this.plugin.dispose?.();
        }
        this.viewer = null;
        this.plugin = null;
    }
    applyDisplaySettings(settings = {}) {
        if (!this.plugin?.canvas3d) {
            return;
        }
        const resolved = displaySettingsWithPreset(settings);
        const canvas = this.plugin.canvas3d;
        const background = colorFromHex(resolved.background || "#fdfcf8");
        const occlusionStrength = clamp(resolved.occlusionStrength ?? 0.8, 0, 2);
        const antialiasing = antialiasingName(resolved.antialiasing);
        const sampleLevel = multisampleLevel(resolved);
        try {
            canvas.setProps({
                camera: {
                    mode: resolved.cameraMode || "perspective",
                    helper: { axes: { name: "off", params: {} } },
                    fov: clamp(resolved.fieldOfView ?? 45, 20, 90),
                },
                cameraClipping: {
                    far: false,
                    minNear: 0.01,
                },
                cameraFog: resolved.fog
                    ? { name: "on", params: { intensity: clamp(resolved.fogIntensity ?? 18, 1, 80) } }
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
                illumination: { enabled: Boolean(resolved.illumination ?? false) },
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
                                radius: 5,
                                bias: occlusionStrength,
                                blurKernelSize: 15,
                                blurDepthBias: 0.5,
                                samples: 32,
                                resolutionScale: clamp(resolved.quality === "low" ? 0.6 : 1, 0.1, 1),
                                color: Color(0x000000),
                                transparentThreshold: 0.4,
                            },
                        }
                        : { name: "off", params: {} },
                    shadow: resolved.shadows
                        ? {
                            name: "on",
                            params: {
                                steps: 2,
                                maxDistance: clamp(resolved.shadowDistance ?? 4, 0, 24),
                                tolerance: 1.0,
                            },
                        }
                        : { name: "off", params: {} },
                    outline: resolved.outline
                        ? {
                            name: "on",
                            params: {
                                scale: clamp(resolved.outlineScale ?? 1, 1, 5),
                                threshold: 0.33,
                                color: Color(0x1e1b17),
                                includeTransparent: true,
                            },
                        }
                        : { name: "off", params: {} },
                    dof: resolved.depthOfField
                        ? {
                            name: "on",
                            params: {
                                blurSize: clamp(resolved.dofBlur ?? 8, 1, 24),
                                blurSpread: 1.0,
                                inFocus: 0,
                                PPM: clamp(resolved.dofFocusRange ?? 28, 1, 160),
                                center: "camera-target",
                                mode: "sphere",
                            },
                        }
                        : { name: "off", params: {} },
                    antialiasing: { name: antialiasing, params: {} },
                    sharpening: resolved.sharpen
                        ? { name: "on", params: { sharpness: 0.35, denoise: true } }
                        : { name: "off", params: {} },
                    bloom: { name: "off", params: {} },
                },
            });
            this.render();
        }
        catch (error) {
            console.debug("[molstar] display settings update failed", error);
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
