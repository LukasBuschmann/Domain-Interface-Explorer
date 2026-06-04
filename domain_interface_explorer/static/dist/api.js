function datasetWindow() {
    return window;
}
export function activeDatasetKey() {
    return String(datasetWindow().__DIE_ACTIVE_DATASET__ || "").trim();
}
export function setActiveDatasetKey(datasetKey) {
    datasetWindow().__DIE_ACTIVE_DATASET__ = String(datasetKey || "").trim();
}
function withActiveDataset(url) {
    const datasetKey = activeDatasetKey();
    if (!datasetKey || typeof url !== "string" || !url.startsWith("/api/")) {
        return url;
    }
    const parsed = new URL(url, window.location.origin);
    if (!parsed.searchParams.has("dataset")) {
        parsed.searchParams.set("dataset", datasetKey);
    }
    return `${parsed.pathname}${parsed.search}${parsed.hash}`;
}
export async function fetchJson(url, options = {}) {
    const response = await fetch(withActiveDataset(url), options);
    if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.error || `Request failed: ${response.status}`);
    }
    return response.json();
}
export async function fetchText(url, options = {}) {
    const response = await fetch(withActiveDataset(url), options);
    if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
    }
    return response.text();
}
