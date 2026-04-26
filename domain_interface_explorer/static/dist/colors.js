import { CLUSTER_COLOR_PALETTE, PARTNER_COLOR_PALETTE } from "./constants.js";
const DOMAIN_LENS_BASE = [143, 138, 130];
export function conservationColor(score) {
    const clamped = Math.max(0, Math.min(100, Number(score) || 0)) / 100;
    const start = [241, 229, 191];
    const end = [47, 125, 90];
    const r = Math.round(start[0] + (end[0] - start[0]) * clamped);
    const g = Math.round(start[1] + (end[1] - start[1]) * clamped);
    const b = Math.round(start[2] + (end[2] - start[2]) * clamped);
    return `rgb(${r}, ${g}, ${b})`;
}
export function hslToRgb(hue, saturation, lightness) {
    const h = ((Number(hue) % 360) + 360) % 360;
    const s = Math.max(0, Math.min(1, Number(saturation)));
    const l = Math.max(0, Math.min(1, Number(lightness)));
    const chroma = (1 - Math.abs((2 * l) - 1)) * s;
    const sector = h / 60;
    const x = chroma * (1 - Math.abs((sector % 2) - 1));
    let red = 0;
    let green = 0;
    let blue = 0;
    if (sector >= 0 && sector < 1) {
        red = chroma;
        green = x;
    }
    else if (sector < 2) {
        red = x;
        green = chroma;
    }
    else if (sector < 3) {
        green = chroma;
        blue = x;
    }
    else if (sector < 4) {
        green = x;
        blue = chroma;
    }
    else if (sector < 5) {
        red = x;
        blue = chroma;
    }
    else {
        red = chroma;
        blue = x;
    }
    const match = l - (chroma / 2);
    return [
        Math.round((red + match) * 255),
        Math.round((green + match) * 255),
        Math.round((blue + match) * 255),
    ];
}
export function columnColor(columnIndex, maxIndex) {
    const normalized = maxIndex <= 0 ? 0 : Math.max(0, Math.min(1, Number(columnIndex) / maxIndex));
    const hue = Math.round(normalized * 360);
    const [red, green, blue] = hslToRgb(hue, 0.8, 0.58);
    return `rgb(${red}, ${green}, ${blue})`;
}
export function interpolateColor(start, end, fraction) {
    const clamped = Math.max(0, Math.min(1, fraction));
    const r = Math.round(start[0] + (end[0] - start[0]) * clamped);
    const g = Math.round(start[1] + (end[1] - start[1]) * clamped);
    const b = Math.round(start[2] + (end[2] - start[2]) * clamped);
    return `rgb(${r}, ${g}, ${b})`;
}
export function colorToRgb(color) {
    if (typeof color !== "string") {
        return null;
    }
    const value = color.trim();
    const hexMatch = value.match(/^#([0-9a-f]{6})$/i);
    if (hexMatch) {
        const hex = hexMatch[1];
        return [
            Number.parseInt(hex.slice(0, 2), 16),
            Number.parseInt(hex.slice(2, 4), 16),
            Number.parseInt(hex.slice(4, 6), 16),
        ];
    }
    const rgbMatch = value.match(/^rgb\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*\)$/i);
    if (rgbMatch) {
        return rgbMatch.slice(1).map((channel) => Number.parseInt(channel, 10));
    }
    return null;
}
export function buildPartnerColorMap(partnerDomains) {
    return new Map(partnerDomains.map((partner, index) => [
        partner,
        PARTNER_COLOR_PALETTE[index % PARTNER_COLOR_PALETTE.length],
    ]));
}
export function partnerColorFromMap(partnerColors, partnerDomain) {
    return partnerColors?.get(partnerDomain) || "#817a71";
}
export function partnerLensColor(partnerColor, partnerDomain, fraction) {
    const base = colorToRgb(partnerColor(partnerDomain));
    if (!base) {
        return partnerColor(partnerDomain);
    }
    const clamped = Math.max(0, Math.min(1, fraction));
    const boostedFraction = 0.14 + clamped * 0.86;
    return interpolateColor(DOMAIN_LENS_BASE, base, boostedFraction);
}
export function embeddingClusterColor(clusterLabel) {
    const numericLabel = Number(clusterLabel);
    if (numericLabel < 0) {
        return "#8a847a";
    }
    return CLUSTER_COLOR_PALETTE[numericLabel % CLUSTER_COLOR_PALETTE.length];
}
export function nonZeroRoundedPercent(value) {
    const rounded = Math.round(Number(value) || 0);
    if (value > 0 && rounded === 0) {
        return 1;
    }
    return rounded;
}
export function clusterLensColor(clusterLabel, supportFraction = 0) {
    const base = colorToRgb(embeddingClusterColor(clusterLabel));
    if (!base) {
        return embeddingClusterColor(clusterLabel);
    }
    const boostedFraction = 0.12 + Math.max(0, Math.min(1, supportFraction)) * 0.88;
    return interpolateColor(DOMAIN_LENS_BASE, base, boostedFraction);
}
export function clusterHoverColor(clusterLabel) {
    const base = colorToRgb(embeddingClusterColor(clusterLabel));
    if (!base) {
        return embeddingClusterColor(clusterLabel);
    }
    return interpolateColor(DOMAIN_LENS_BASE, base, 0.94);
}
