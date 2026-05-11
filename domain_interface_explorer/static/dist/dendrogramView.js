import { fetchJson } from "./api.js";
import { appendSelectionSettingsToParams, selectionSettingsKey, } from "./selectionSettings.js";
export function createDendrogramViewController({ state, elements, interfaceSelect, appendClusteringSettingsToParams, embeddingClusteringSettingsKey, embeddingClusterColor, embeddingClusterLabel, partnerColor, }) {
    let dendrogramRenderFrameId = 0;
    let dendrogramLoadTimer = 0;
    function dendrogramSettingsKey() {
        return [
            interfaceSelect.value || "",
            selectionSettingsKey(state.selectionSettings),
            embeddingClusteringSettingsKey(),
            Number(state.dendrogramDepth || 5),
        ].join("|");
    }
    function currentDendrogramQuery() {
        const params = new URLSearchParams({
            file: interfaceSelect.value,
            merge_depth: String(Math.max(1, Number(state.dendrogramDepth || 5))),
        });
        appendSelectionSettingsToParams(params, state.selectionSettings);
        appendClusteringSettingsToParams(params);
        return `/api/dendrogram?${params.toString()}`;
    }
    function setDendrogramInfo(message) {
        if (elements.dendrogramInfo) {
            elements.dendrogramInfo.textContent = message;
        }
    }
    function dendrogramLegendMode() {
        return state.dendrogramColorMode || "cluster";
    }
    function dendrogramClusterSelectionKey() {
        return [
            interfaceSelect.value || "",
            selectionSettingsKey(state.selectionSettings),
            embeddingClusteringSettingsKey(),
        ].join("|");
    }
    function sortedClusterLabels(labels) {
        return Array.from(new Set((labels || []).map((label) => String(label)))).sort((left, right) => Number(left) - Number(right));
    }
    function allDendrogramClusterLabels() {
        if (Array.isArray(state.dendrogram?.cluster_labels)) {
            return sortedClusterLabels(state.dendrogram.cluster_labels);
        }
        const labels = [];
        for (const node of state.dendrogram?.nodes || []) {
            for (const label of Object.keys(node.cluster_counts || {})) {
                labels.push(label);
            }
        }
        return sortedClusterLabels(labels);
    }
    function allDendrogramPartnerDomains() {
        const interfacePartners = state.interface?.partnerDomains || [];
        if (interfacePartners.length) {
            return interfacePartners;
        }
        return Array.from(new Set(state.dendrogram?.domain_labels || [])).sort();
    }
    function resetDendrogramPartnerSelection() {
        state.dendrogramVisiblePartners = new Set(allDendrogramPartnerDomains());
    }
    function resetDendrogramClusterSelection() {
        const clusterLabels = allDendrogramClusterLabels();
        state.dendrogramVisibleClusters = new Set(clusterLabels);
        state.dendrogramClusterSelectionKey = clusterLabels.length
            ? dendrogramClusterSelectionKey()
            : null;
    }
    function visibleDendrogramClusters() {
        return allDendrogramClusterLabels().filter((clusterKey) => state.dendrogramVisibleClusters.has(clusterKey));
    }
    function visibleDendrogramPartners() {
        return allDendrogramPartnerDomains().filter((partnerDomain) => state.dendrogramVisiblePartners.has(partnerDomain));
    }
    function syncDendrogramClusterSelection() {
        const selectionKey = dendrogramClusterSelectionKey();
        if (state.dendrogramClusterSelectionKey !== selectionKey) {
            resetDendrogramClusterSelection();
            return;
        }
        const clusterKeys = new Set(allDendrogramClusterLabels());
        state.dendrogramVisibleClusters = new Set([...state.dendrogramVisibleClusters].filter((clusterKey) => clusterKeys.has(clusterKey)));
    }
    function syncDendrogramControls() {
        if (!elements.dendrogramDepthSlider || !elements.dendrogramDepthValue) {
            return;
        }
        const maxDepth = Math.max(1, Number(state.dendrogram?.max_merge_depth || 5));
        const depth = Math.max(1, Math.min(maxDepth, Number(state.dendrogramDepth || 5)));
        elements.dendrogramDepthSlider.min = "1";
        elements.dendrogramDepthSlider.max = String(maxDepth);
        elements.dendrogramDepthSlider.value = String(depth);
        const controlsDisabled = !interfaceSelect.value || state.embeddingClusteringSettings.method !== "hierarchical";
        elements.dendrogramDepthSlider.disabled = controlsDisabled;
        elements.dendrogramDepthValue.textContent = `${depth} / ${maxDepth}`;
        const radiusMode = state.dendrogramRadiusMode === "distance" ? "distance" : "depth";
        elements.dendrogramRadiusMode
            ?.querySelectorAll("[data-dendrogram-radius-mode]")
            .forEach((button) => {
            const isActive = button.dataset.dendrogramRadiusMode === radiusMode;
            button.disabled = controlsDisabled;
            button.classList.toggle("active", isActive);
            button.setAttribute("aria-pressed", String(isActive));
        });
    }
    function renderDendrogramLegend() {
        if (!elements.dendrogramPartnerLegend) {
            return;
        }
        const colorMode = dendrogramLegendMode();
        const partners = allDendrogramPartnerDomains();
        if (partners.length === 0) {
            elements.dendrogramPartnerLegend.innerHTML = "";
            return;
        }
        const clusterKeys = allDendrogramClusterLabels();
        const modeControls = `
      <div class="embedding-legend-header">
        <div class="embedding-legend-mode" role="tablist" aria-label="Dendrogram color mode">
          <button type="button" class="embedding-legend-mode-button ${colorMode === "domain" ? "active" : ""}" data-legend-mode="domain" aria-pressed="${colorMode === "domain"}">Domains</button>
          <button type="button" class="embedding-legend-mode-button ${colorMode === "cluster" ? "active" : ""}" data-legend-mode="cluster" aria-pressed="${colorMode === "cluster"}">Clusters</button>
        </div>
      </div>
    `;
        const legendEntries = colorMode === "cluster"
            ? clusterKeys.length === 0
                ? '<p class="embedding-legend-empty">Clustering not loaded yet.</p>'
                : clusterKeys
                    .map((clusterKey) => `
          <button class="embedding-partner-chip ${state.dendrogramVisibleClusters.has(clusterKey) ? "active" : "inactive"}" type="button" data-cluster-label="${clusterKey}" aria-pressed="${state.dendrogramVisibleClusters.has(clusterKey)}" title="${embeddingClusterLabel(clusterKey)}">
            <span class="representative-partner-filter-swatch" style="background: ${embeddingClusterColor(clusterKey)};"></span>
            <span class="embedding-partner-chip-label">${embeddingClusterLabel(clusterKey)}</span>
          </button>
        `)
                    .join("")
            : partners
                .map((partner) => `
          <button class="embedding-partner-chip ${state.dendrogramVisiblePartners.has(partner) ? "active" : "inactive"}" type="button" data-partner-domain="${partner}" aria-pressed="${state.dendrogramVisiblePartners.has(partner)}" title="${partner}">
            <span class="representative-partner-filter-swatch" style="background: ${partnerColor(partner)};"></span>
            <span class="embedding-partner-chip-label">${partner}</span>
          </button>
        `)
                .join("");
        elements.dendrogramPartnerLegend.innerHTML = `${modeControls}<div class="embedding-legend-list">${legendEntries}</div>`;
    }
    function resizeDendrogramCanvas() {
        if (!elements.dendrogramCanvas || !elements.dendrogramRoot) {
            return;
        }
        const width = Math.max(1, Math.round(elements.dendrogramRoot.clientWidth));
        const height = Math.max(1, Math.round(elements.dendrogramRoot.clientHeight));
        const dpr = window.devicePixelRatio || 1;
        elements.dendrogramCanvas.width = Math.round(width * dpr);
        elements.dendrogramCanvas.height = Math.round(height * dpr);
        elements.dendrogramCanvas.style.width = `${width}px`;
        elements.dendrogramCanvas.style.height = `${height}px`;
    }
    function requestDendrogramRender() {
        if (dendrogramRenderFrameId) {
            return;
        }
        dendrogramRenderFrameId = window.requestAnimationFrame(() => {
            dendrogramRenderFrameId = 0;
            if (state.msaPanelView === "dendrogram") {
                renderDendrogram();
            }
        });
    }
    function clearDendrogram() {
        state.dendrogramRequestId += 1;
        state.dendrogram = null;
        state.dendrogramLoading = false;
        state.dendrogramLoadingKey = null;
        state.dendrogramPromise = null;
        syncDendrogramControls();
        renderDendrogramLegend();
        requestDendrogramRender();
    }
    async function ensureDendrogramLoaded(options = {}) {
        if (!interfaceSelect.value || state.embeddingClusteringSettings.method !== "hierarchical") {
            clearDendrogram();
            return;
        }
        const requestKey = dendrogramSettingsKey();
        if (!options.force &&
            state.dendrogram?.settingsKey === requestKey &&
            !state.dendrogram?.error) {
            syncDendrogramControls();
            requestDendrogramRender();
            return;
        }
        if (state.dendrogramLoading &&
            state.dendrogramLoadingKey === requestKey &&
            state.dendrogramPromise) {
            syncDendrogramControls();
            requestDendrogramRender();
            return state.dendrogramPromise;
        }
        const requestId = ++state.dendrogramRequestId;
        state.dendrogramLoading = true;
        state.dendrogramLoadingKey = requestKey;
        if (elements.dendrogramLoadingLabel) {
            elements.dendrogramLoadingLabel.textContent = "Loading dendrogram";
        }
        renderDendrogram();
        state.dendrogramPromise = (async () => {
            try {
                const payload = await fetchJson(currentDendrogramQuery());
                if (requestId !== state.dendrogramRequestId) {
                    return;
                }
                state.dendrogramDepth = Number(payload.merge_depth || state.dendrogramDepth || 5);
                state.dendrogram = {
                    ...payload,
                    settingsKey: dendrogramSettingsKey(),
                };
                syncDendrogramClusterSelection();
            }
            catch (error) {
                if (requestId !== state.dendrogramRequestId) {
                    return;
                }
                state.dendrogram = {
                    error: error.message,
                    settingsKey: requestKey,
                    nodes: [],
                    edges: [],
                };
            }
            finally {
                if (requestId === state.dendrogramRequestId) {
                    state.dendrogramLoading = false;
                    state.dendrogramLoadingKey = null;
                    state.dendrogramPromise = null;
                    syncDendrogramControls();
                    renderDendrogramLegend();
                    renderDendrogram();
                }
            }
        })();
        return state.dendrogramPromise;
    }
    function scheduleDendrogramLoad() {
        window.clearTimeout(dendrogramLoadTimer);
        dendrogramLoadTimer = window.setTimeout(() => {
            void ensureDendrogramLoaded({ force: true });
        }, 180);
    }
    function drawCenteredMessage(ctx, width, height, message) {
        ctx.fillStyle = "#6f6658";
        ctx.font = '13px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(message, width / 2, height / 2);
    }
    function nodeScreenPosition(node, centerX, centerY, radius) {
        const radiusMode = state.dendrogramRadiusMode === "distance" ? "distance" : "depth";
        const radiusValue = Number(radiusMode === "distance"
            ? node.radius_distance ?? node.radius
            : node.radius_depth ?? node.radius);
        const angle = Number(node.angle);
        if (Number.isFinite(radiusValue) && Number.isFinite(angle)) {
            return {
                x: centerX + Math.cos(angle) * radiusValue * radius,
                y: centerY + Math.sin(angle) * radiusValue * radius,
            };
        }
        return {
            x: centerX + Number(node.x || 0) * radius,
            y: centerY + Number(node.y || 0) * radius,
        };
    }
    function normalizedCountEntries(counts) {
        if (!counts || typeof counts !== "object") {
            return [];
        }
        return Object.entries(counts)
            .map(([key, count]) => [String(key), Math.max(0, Number(count) || 0)])
            .filter(([, count]) => count > 0);
    }
    function countVisibleMembers(counts, visibleKeys) {
        if (!visibleKeys?.size) {
            return 0;
        }
        return normalizedCountEntries(counts).reduce((total, [key, count]) => total + (visibleKeys.has(key) ? count : 0), 0);
    }
    function dominantCountEntry(counts, visibleKeys) {
        const entries = normalizedCountEntries(counts).filter(([key]) => visibleKeys.has(key));
        const total = entries.reduce((sum, [, count]) => sum + count, 0);
        if (total <= 0) {
            return null;
        }
        entries.sort((left, right) => {
            if (right[1] !== left[1]) {
                return right[1] - left[1];
            }
            return String(left[0]).localeCompare(String(right[0]), undefined, { numeric: true });
        });
        return {
            key: entries[0][0],
            count: entries[0][1],
            purity: entries[0][1] / total,
            total,
        };
    }
    function dendrogramNodeVisible(node, colorMode, visibleKeys) {
        const counts = colorMode === "cluster" ? node.cluster_counts : node.domain_counts;
        return countVisibleMembers(counts, visibleKeys) > 0;
    }
    function dendrogramVisualForNode(node, colorMode, visibleKeys) {
        const counts = colorMode === "cluster" ? node.cluster_counts : node.domain_counts;
        const dominant = dominantCountEntry(counts, visibleKeys);
        if (!dominant) {
            return {
                color: "#7f766a",
                label: null,
                purity: 0,
            };
        }
        return {
            color: colorMode === "cluster" ? embeddingClusterColor(dominant.key) : partnerColor(dominant.key),
            label: dominant.key,
            purity: dominant.purity,
        };
    }
    function drawDendrogramEdge(ctx, source, target, targetNode, colorMode, visibleKeys) {
        const visual = dendrogramVisualForNode(targetNode, colorMode, visibleKeys);
        const purity = Math.max(0.15, Math.min(1, Number(visual.purity || 0)));
        ctx.save();
        ctx.globalAlpha = visual.label === null ? 0.2 : 0.22 + purity * 0.58;
        ctx.strokeStyle = visual.label === null ? "rgba(71, 62, 49, 0.38)" : visual.color;
        ctx.lineWidth = Math.max(0.7, 1.15 + purity * 1.2);
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.lineTo(target.x, target.y);
        ctx.stroke();
        ctx.restore();
    }
    function drawDendrogramNode(ctx, position, node, colorMode, visibleKeys) {
        const visual = dendrogramVisualForNode(node, colorMode, visibleKeys);
        const purity = Math.max(0.15, Math.min(1, Number(visual.purity || 0)));
        const subtreeLeaves = Math.max(1, Number(node?.subtree_leaf_count || 1));
        const baseRadius = node?.collapsed ? 2.4 + Math.log1p(subtreeLeaves) * 0.7 : 2.2;
        const radius = Math.min(node?.collapsed ? 8 : 4, baseRadius);
        ctx.save();
        ctx.globalAlpha = visual.label === null ? 0.42 : 0.4 + purity * 0.55;
        ctx.fillStyle = visual.label === null ? "#7f766a" : visual.color;
        ctx.beginPath();
        ctx.arc(position.x, position.y, radius, 0, Math.PI * 2);
        ctx.fill();
        ctx.restore();
    }
    function drawDendrogramOriginMarker(ctx, position, node, colorMode, visibleKeys) {
        const visual = dendrogramVisualForNode(node, colorMode, visibleKeys);
        ctx.save();
        ctx.shadowColor = "rgba(31, 23, 16, 0.18)";
        ctx.shadowBlur = 8;
        ctx.fillStyle = "#fffdf8";
        ctx.strokeStyle = "#2e261d";
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(position.x, position.y, 8.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
        ctx.shadowBlur = 0;
        ctx.fillStyle = visual.label === null ? "#d49a38" : visual.color;
        ctx.beginPath();
        ctx.arc(position.x, position.y, 3.7, 0, Math.PI * 2);
        ctx.fill();
        ctx.restore();
    }
    function formatDendrogramDistance(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "0";
        }
        if (numeric >= 1) {
            return numeric.toFixed(2);
        }
        if (numeric >= 0.1) {
            return numeric.toFixed(3);
        }
        return numeric.toPrecision(2);
    }
    function drawDendrogramCutoffCircle(ctx, centerX, centerY, radius) {
        if (state.dendrogramRadiusMode !== "distance") {
            return;
        }
        if (state.dendrogram?.hierarchical_target !== "distance_threshold") {
            return;
        }
        const cutoffRadius = Number(state.dendrogram?.cutoff_radius_distance);
        const cutoffDistance = Number(state.dendrogram?.cutoff_distance);
        if (!Number.isFinite(cutoffRadius) || !Number.isFinite(cutoffDistance)) {
            return;
        }
        const screenRadius = Math.max(0, cutoffRadius) * radius;
        ctx.save();
        ctx.strokeStyle = "rgba(72, 70, 66, 0.62)";
        ctx.lineWidth = 1.4;
        ctx.setLineDash([4, 6]);
        ctx.beginPath();
        ctx.arc(centerX, centerY, screenRadius, 0, Math.PI * 2);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = "rgba(72, 70, 66, 0.78)";
        ctx.font = '11px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "left";
        ctx.textBaseline = "middle";
        ctx.fillText(`cutoff ${formatDendrogramDistance(cutoffDistance)}`, centerX + screenRadius + 8, centerY);
        ctx.restore();
    }
    function drawDendrogramDistanceGuides(ctx, centerX, centerY, radius) {
        if (state.dendrogramRadiusMode !== "distance") {
            return;
        }
        const maxMergeDistance = Number(state.dendrogram?.max_merge_distance || 0);
        if (!Number.isFinite(maxMergeDistance) || maxMergeDistance <= 0 || radius <= 0) {
            return;
        }
        ctx.save();
        ctx.strokeStyle = "rgba(72, 70, 66, 0.24)";
        ctx.lineWidth = 1.1;
        ctx.setLineDash([3, 7]);
        for (let index = 0; index <= 10; index += 1) {
            const distance = index / 10;
            if (distance > maxMergeDistance) {
                continue;
            }
            const guideRadius = Math.max(0, 1 - distance / maxMergeDistance) * radius;
            ctx.beginPath();
            ctx.arc(centerX, centerY, guideRadius, 0, Math.PI * 2);
            ctx.stroke();
        }
        ctx.restore();
    }
    function drawDendrogramDistanceScale(ctx, width, height, radius) {
        if (state.dendrogramRadiusMode !== "distance") {
            return;
        }
        const maxMergeDistance = Number(state.dendrogram?.max_merge_distance || 0);
        if (!Number.isFinite(maxMergeDistance) || maxMergeDistance <= 0 || radius <= 0) {
            return;
        }
        const pixelsPerCssCm = 96 / 2.54;
        const barWidth = Math.min(pixelsPerCssCm, Math.max(28, width - 34));
        const distancePerBar = (barWidth / radius) * maxMergeDistance;
        const x = width - barWidth - 18;
        const y = height - 24;
        ctx.save();
        ctx.strokeStyle = "rgba(55, 50, 43, 0.86)";
        ctx.fillStyle = "rgba(55, 50, 43, 0.9)";
        ctx.lineWidth = 1.6;
        ctx.beginPath();
        ctx.moveTo(x, y - 5);
        ctx.lineTo(x, y);
        ctx.lineTo(x + barWidth, y);
        ctx.lineTo(x + barWidth, y - 5);
        ctx.stroke();
        ctx.font = '11px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "center";
        ctx.textBaseline = "bottom";
        ctx.fillText(`1 cm = ${formatDendrogramDistance(distancePerBar)}`, x + barWidth / 2, y - 7);
        ctx.restore();
    }
    function renderDendrogram() {
        if (!elements.dendrogramCanvas || !elements.dendrogramRoot) {
            return;
        }
        const ctx = elements.dendrogramCanvas.getContext("2d");
        const dpr = window.devicePixelRatio || 1;
        const width = Math.max(1, Math.round(elements.dendrogramRoot.clientWidth));
        const height = Math.max(1, Math.round(elements.dendrogramRoot.clientHeight));
        if (!ctx || width <= 0 || height <= 0) {
            return;
        }
        const expectedCanvasWidth = Math.round(width * dpr);
        const expectedCanvasHeight = Math.round(height * dpr);
        if (elements.dendrogramCanvas.width !== expectedCanvasWidth ||
            elements.dendrogramCanvas.height !== expectedCanvasHeight) {
            resizeDendrogramCanvas();
            requestDendrogramRender();
            return;
        }
        syncDendrogramControls();
        elements.dendrogramLoading?.classList.toggle("hidden", !(state.dendrogramLoading && !(state.dendrogram?.nodes || []).length));
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, width, height);
        ctx.fillStyle = "#fffdf8";
        ctx.fillRect(0, 0, width, height);
        if (!interfaceSelect.value) {
            drawCenteredMessage(ctx, width, height, "Load an interface selection to view a dendrogram.");
            setDendrogramInfo("Radial hierarchy.");
            return;
        }
        if (state.embeddingClusteringSettings.method !== "hierarchical") {
            drawCenteredMessage(ctx, width, height, "Dendrogram requires hierarchical clustering.");
            setDendrogramInfo("Switch clustering method to Hierarchical.");
            return;
        }
        if (state.dendrogram?.error) {
            drawCenteredMessage(ctx, width, height, state.dendrogram.error);
            setDendrogramInfo(state.dendrogram.error);
            return;
        }
        if (!(state.dendrogram?.nodes || []).length) {
            drawCenteredMessage(ctx, width, height, state.dendrogramLoading ? "Preparing dendrogram..." : "Load dendrogram data.");
            setDendrogramInfo("Radial hierarchy.");
            return;
        }
        const colorMode = dendrogramLegendMode();
        const visibleKeys = new Set(colorMode === "cluster" ? visibleDendrogramClusters() : visibleDendrogramPartners());
        if (visibleKeys.size === 0) {
            drawCenteredMessage(ctx, width, height, colorMode === "cluster"
                ? "Select at least one cluster in the legend."
                : "Select at least one domain in the legend.");
            setDendrogramInfo(colorMode === "cluster"
                ? "Dendrogram filter hides all clusters. Click legend items to show them again."
                : "Dendrogram filter hides all domains. Click legend items to show them again.");
            return;
        }
        const view = state.dendrogramView || { x: 0, y: 0, scale: 1 };
        const baseRadius = Math.max(1, Math.min(width, height) * 0.41);
        const radius = baseRadius * Math.max(0.08, Math.min(16, Number(view.scale || 1)));
        const centerX = width / 2 + Number(view.x || 0);
        const centerY = height / 2 + Number(view.y || 0);
        const nodesById = new Map((state.dendrogram.nodes || []).map((node) => [node.id, node]));
        const visibleNodeIds = new Set((state.dendrogram.nodes || [])
            .filter((node) => dendrogramNodeVisible(node, colorMode, visibleKeys))
            .map((node) => node.id));
        if (visibleNodeIds.size === 0) {
            drawCenteredMessage(ctx, width, height, colorMode === "cluster"
                ? "Selected clusters are not visible at this merge depth."
                : "Selected domains are not visible at this merge depth.");
            setDendrogramInfo(colorMode === "cluster"
                ? "No selected clusters are represented at the current merge depth."
                : "No selected domains are represented at the current merge depth.");
            return;
        }
        const positions = new Map();
        for (const node of state.dendrogram.nodes || []) {
            if (!visibleNodeIds.has(node.id)) {
                continue;
            }
            positions.set(node.id, nodeScreenPosition(node, centerX, centerY, radius));
        }
        drawDendrogramDistanceGuides(ctx, centerX, centerY, radius);
        drawDendrogramCutoffCircle(ctx, centerX, centerY, radius);
        for (const edge of state.dendrogram.edges || []) {
            if (!visibleNodeIds.has(edge.source) || !visibleNodeIds.has(edge.target)) {
                continue;
            }
            const source = positions.get(edge.source);
            const target = positions.get(edge.target);
            const targetNode = nodesById.get(edge.target);
            if (!source || !target || !targetNode) {
                continue;
            }
            drawDendrogramEdge(ctx, source, target, targetNode, colorMode, visibleKeys);
        }
        let originNode = null;
        let originPosition = null;
        for (const node of state.dendrogram.nodes || []) {
            if (!visibleNodeIds.has(node.id)) {
                continue;
            }
            const position = positions.get(node.id);
            if (!position) {
                continue;
            }
            if (node.parent_id === null) {
                originNode = node;
                originPosition = position;
                continue;
            }
            drawDendrogramNode(ctx, position, node, colorMode, visibleKeys);
        }
        if (originNode && originPosition) {
            drawDendrogramOriginMarker(ctx, originPosition, originNode, colorMode, visibleKeys);
        }
        drawDendrogramDistanceScale(ctx, width, height, radius);
        const maxDepth = Number(state.dendrogram.max_merge_depth || 1);
        const radiusModeLabel = state.dendrogramRadiusMode === "distance" ? "distance" : "depth";
        const visibleMemberCount = (state.dendrogram.nodes || [])
            .filter((node) => node.parent_id === null)
            .reduce((total, node) => total +
            countVisibleMembers(colorMode === "cluster" ? node.cluster_counts : node.domain_counts, visibleKeys), 0);
        setDendrogramInfo(`Depth ${state.dendrogram.merge_depth}/${maxDepth} · radius by ${radiusModeLabel} · ${visibleNodeIds.size} visible nodes · ${visibleMemberCount} rows · ${state.dendrogram.cluster_count} clusters · ${state.dendrogram.hierarchy_source}`);
    }
    function dendrogramCanvasPoint(event) {
        const rect = elements.dendrogramCanvas.getBoundingClientRect();
        return {
            x: event.clientX - rect.left,
            y: event.clientY - rect.top,
        };
    }
    function handleDendrogramPointerDown(event) {
        if (event.button !== 0) {
            return;
        }
        const point = dendrogramCanvasPoint(event);
        state.dendrogramDrag = {
            x: point.x,
            y: point.y,
            viewX: Number(state.dendrogramView?.x || 0),
            viewY: Number(state.dendrogramView?.y || 0),
        };
        elements.dendrogramCanvas.setPointerCapture?.(event.pointerId);
    }
    function handleDendrogramPointerMove(event) {
        if (!state.dendrogramDrag) {
            return;
        }
        const point = dendrogramCanvasPoint(event);
        state.dendrogramView = {
            ...state.dendrogramView,
            x: state.dendrogramDrag.viewX + point.x - state.dendrogramDrag.x,
            y: state.dendrogramDrag.viewY + point.y - state.dendrogramDrag.y,
        };
        requestDendrogramRender();
    }
    function handleDendrogramPointerUp(event) {
        state.dendrogramDrag = null;
        elements.dendrogramCanvas?.releasePointerCapture?.(event.pointerId);
    }
    function handleDendrogramWheel(event) {
        event.preventDefault();
        const point = dendrogramCanvasPoint(event);
        const previousScale = Math.max(0.08, Math.min(16, Number(state.dendrogramView?.scale || 1)));
        const zoomFactor = Math.exp(-Number(event.deltaY || 0) * 0.0015);
        const nextScale = Math.max(0.08, Math.min(16, previousScale * zoomFactor));
        const width = Math.max(1, Math.round(elements.dendrogramRoot.clientWidth));
        const height = Math.max(1, Math.round(elements.dendrogramRoot.clientHeight));
        const centerX = width / 2 + Number(state.dendrogramView?.x || 0);
        const centerY = height / 2 + Number(state.dendrogramView?.y || 0);
        const ratio = nextScale / previousScale;
        state.dendrogramView = {
            x: point.x - width / 2 - (point.x - centerX) * ratio,
            y: point.y - height / 2 - (point.y - centerY) * ratio,
            scale: nextScale,
        };
        requestDendrogramRender();
    }
    return {
        allDendrogramClusterLabels,
        allDendrogramPartnerDomains,
        clearDendrogram,
        ensureDendrogramLoaded,
        handleDendrogramPointerDown,
        handleDendrogramPointerMove,
        handleDendrogramPointerUp,
        handleDendrogramWheel,
        renderDendrogramLegend,
        renderDendrogram,
        requestDendrogramRender,
        resetDendrogramClusterSelection,
        resetDendrogramPartnerSelection,
        resizeDendrogramCanvas,
        scheduleDendrogramLoad,
        syncDendrogramControls,
    };
}
