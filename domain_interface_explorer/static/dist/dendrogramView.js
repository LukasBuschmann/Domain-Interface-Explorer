import { fetchJson } from "./api.js";
import { appendSelectionSettingsToParams, selectionSettingsKey, } from "./selectionSettings.js";
export function createDendrogramViewController({ state, elements, interfaceSelect, appendClusteringSettingsToParams, embeddingClusteringSettingsKey, embeddingClusterColor, embeddingClusterLabel, onCutoffDistanceChange = () => { }, partnerColor, }) {
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
    function dendrogramStyle() {
        return state.dendrogramStyle === "linear" ? "linear" : "radial";
    }
    function dendrogramScaleMode() {
        return state.dendrogramRadiusMode === "distance" ? "distance" : "depth";
    }
    function clampUnit(value) {
        return Math.max(0, Math.min(1, Number(value) || 0));
    }
    function dendrogramLayoutForSize(width, height) {
        const view = state.dendrogramView || { x: 0, y: 0, scale: 1 };
        const viewScale = Math.max(0.08, Math.min(16, Number(view.scale || 1)));
        const style = dendrogramStyle();
        if (style === "linear") {
            const plotWidth = Math.max(1, width - 112) * viewScale;
            const plotHeight = Math.max(1, height - 120) * viewScale;
            const left = width / 2 - plotWidth / 2 + Number(view.x || 0);
            const top = height / 2 - plotHeight / 2 + Number(view.y || 0);
            return {
                style,
                left,
                right: left + plotWidth,
                top,
                bottom: top + plotHeight,
                width: plotWidth,
                height: plotHeight,
            };
        }
        const baseRadius = Math.max(1, Math.min(width, height) * 0.41);
        return {
            style,
            centerX: width / 2 + Number(view.x || 0),
            centerY: height / 2 + Number(view.y || 0),
            radius: baseRadius * viewScale,
        };
    }
    function currentDendrogramLayout() {
        if (!elements.dendrogramRoot) {
            return null;
        }
        const width = Math.max(1, Math.round(elements.dendrogramRoot.clientWidth));
        const height = Math.max(1, Math.round(elements.dendrogramRoot.clientHeight));
        return dendrogramLayoutForSize(width, height);
    }
    function currentHierarchicalTarget() {
        return String(state.embeddingClusteringSettings?.hierarchicalTarget ||
            state.dendrogram?.hierarchical_target ||
            "");
    }
    function dendrogramMaxMergeDistance() {
        const maxMergeDistance = Number(state.dendrogram?.max_merge_distance || 0);
        return Number.isFinite(maxMergeDistance) && maxMergeDistance > 0 ? maxMergeDistance : 0;
    }
    function currentCutoffDistance() {
        if (dendrogramScaleMode() !== "distance") {
            return null;
        }
        if (currentHierarchicalTarget() !== "distance_threshold") {
            return null;
        }
        const cutoffDistance = Number(state.embeddingClusteringSettings?.distanceThreshold ??
            state.dendrogram?.cutoff_distance);
        return Number.isFinite(cutoffDistance) ? Math.max(0, cutoffDistance) : null;
    }
    function cutoffProgressForDistance(distance) {
        const maxMergeDistance = dendrogramMaxMergeDistance();
        if (maxMergeDistance <= 0) {
            return null;
        }
        return clampUnit(1 - Math.max(0, Number(distance) || 0) / maxMergeDistance);
    }
    function cutoffScreenRadius(layout) {
        const cutoffDistance = currentCutoffDistance();
        const progress = cutoffDistance === null
            ? null
            : cutoffProgressForDistance(cutoffDistance);
        if (progress === null) {
            return null;
        }
        return progress * (layout.style === "linear" ? layout.height : layout.radius);
    }
    function cutoffDistanceForPoint(point, layout) {
        const maxMergeDistance = dendrogramMaxMergeDistance();
        if (maxMergeDistance <= 0 ||
            dendrogramScaleMode() !== "distance" ||
            currentHierarchicalTarget() !== "distance_threshold") {
            return null;
        }
        const progress = layout.style === "linear"
            ? clampUnit((point.y - layout.top) / layout.height)
            : clampUnit(Math.hypot(point.x - layout.centerX, point.y - layout.centerY) / layout.radius);
        return Math.max(0, maxMergeDistance * (1 - progress));
    }
    function pointNearDendrogramCutoff(point, layout) {
        const screenRadius = cutoffScreenRadius(layout);
        if (screenRadius === null) {
            return false;
        }
        const tolerance = 12;
        if (layout.style === "linear") {
            const y = layout.top + screenRadius;
            return (Math.abs(point.y - y) <= tolerance &&
                point.x >= layout.left - 18 &&
                point.x <= layout.right + 92);
        }
        const distanceFromCenter = Math.hypot(point.x - layout.centerX, point.y - layout.centerY);
        return (Math.abs(distanceFromCenter - screenRadius) <= tolerance &&
            distanceFromCenter <= layout.radius + 24);
    }
    function cutoffCursor(layout) {
        return layout?.style === "linear" ? "ns-resize" : "grab";
    }
    function updateDendrogramCutoffFromPoint(point, commit = false) {
        const layout = currentDendrogramLayout();
        if (!layout) {
            return;
        }
        const cutoffDistance = cutoffDistanceForPoint(point, layout);
        if (cutoffDistance === null) {
            return;
        }
        const roundedDistance = Number(cutoffDistance.toFixed(4));
        onCutoffDistanceChange(roundedDistance, { commit });
        requestDendrogramRender();
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
        if (controlsDisabled) {
            state.dendrogramSettingsOpen = false;
        }
        elements.dendrogramSettingsToggle?.setAttribute("aria-expanded", String(Boolean(state.dendrogramSettingsOpen)));
        elements.dendrogramSettingsToggle?.classList.toggle("active", Boolean(state.dendrogramSettingsOpen));
        if (elements.dendrogramSettingsToggle) {
            elements.dendrogramSettingsToggle.disabled = controlsDisabled;
        }
        elements.dendrogramSettingsPanel?.classList.toggle("hidden", !state.dendrogramSettingsOpen);
        const style = dendrogramStyle();
        elements.dendrogramStyleMode
            ?.querySelectorAll("[data-dendrogram-style]")
            .forEach((button) => {
            const isActive = button.dataset.dendrogramStyle === style;
            button.disabled = controlsDisabled;
            button.classList.toggle("active", isActive);
            button.setAttribute("aria-pressed", String(isActive));
        });
        elements.dendrogramDepthSlider.disabled = controlsDisabled;
        elements.dendrogramDepthValue.textContent = `${depth} / ${maxDepth}`;
        const radiusMode = dendrogramScaleMode();
        elements.dendrogramRadiusMode
            ?.querySelectorAll("[data-dendrogram-radius-mode]")
            .forEach((button) => {
            const isActive = button.dataset.dendrogramRadiusMode === radiusMode;
            button.disabled = controlsDisabled;
            button.classList.toggle("active", isActive);
            button.setAttribute("aria-pressed", String(isActive));
        });
        elements.dendrogramTrueDistanceOption?.classList.toggle("hidden", radiusMode !== "distance");
        elements.dendrogramTrueDistanceOption?.classList.toggle("disabled", controlsDisabled || radiusMode !== "distance");
        if (elements.dendrogramTrueDistanceToggle) {
            elements.dendrogramTrueDistanceToggle.setAttribute("aria-checked", String(Boolean(state.dendrogramTrueDistanceEdges)));
            elements.dendrogramTrueDistanceToggle.disabled = controlsDisabled || radiusMode !== "distance";
        }
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
    function normalizedAngle(angle) {
        const numeric = Number(angle);
        if (!Number.isFinite(numeric)) {
            return 0;
        }
        const fullTurn = Math.PI * 2;
        return ((numeric % fullTurn) + fullTurn) % fullTurn;
    }
    function nodeScaleProgress(node) {
        const radiusMode = dendrogramScaleMode();
        const radiusValue = Number(radiusMode === "distance"
            ? node.radius_distance ?? node.radius
            : node.radius_depth ?? node.radius);
        if (!Number.isFinite(radiusValue)) {
            return 0;
        }
        return Math.max(0, Math.min(1, radiusValue));
    }
    function nodeScreenPosition(node, layout) {
        const progress = nodeScaleProgress(node);
        const angle = Number(node.angle);
        const normalized = normalizedAngle(angle);
        if (layout.style === "linear") {
            const order = normalized / (Math.PI * 2);
            return {
                x: layout.left + order * layout.width,
                y: layout.top + progress * layout.height,
                angle,
                order,
                progress,
            };
        }
        if (Number.isFinite(angle)) {
            return {
                x: layout.centerX + Math.cos(angle) * progress * layout.radius,
                y: layout.centerY + Math.sin(angle) * progress * layout.radius,
                angle,
                order: normalized / (Math.PI * 2),
                progress,
            };
        }
        return {
            x: layout.centerX + Number(node.x || 0) * layout.radius,
            y: layout.centerY + Number(node.y || 0) * layout.radius,
            angle,
            order: normalized / (Math.PI * 2),
            progress,
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
    function shortestAngleDelta(startAngle, endAngle) {
        let delta = endAngle - startAngle;
        while (delta <= -Math.PI) {
            delta += Math.PI * 2;
        }
        while (delta > Math.PI) {
            delta -= Math.PI * 2;
        }
        return delta;
    }
    function drawDendrogramTrueDistanceEdge(ctx, source, target, sourceNode, targetNode, layout, colorMode, visibleKeys) {
        const visual = dendrogramVisualForNode(targetNode, colorMode, visibleKeys);
        const purity = Math.max(0.15, Math.min(1, Number(visual.purity || 0)));
        ctx.save();
        ctx.globalAlpha = visual.label === null ? 0.2 : 0.22 + purity * 0.58;
        ctx.strokeStyle = visual.label === null ? "rgba(71, 62, 49, 0.38)" : visual.color;
        ctx.lineWidth = Math.max(0.7, 1.15 + purity * 1.2);
        ctx.lineJoin = "round";
        ctx.lineCap = "round";
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        if (layout.style === "linear") {
            if (Math.abs(target.x - source.x) > 0.5) {
                ctx.lineTo(target.x, source.y);
            }
            ctx.lineTo(target.x, target.y);
            ctx.stroke();
            ctx.restore();
            return;
        }
        const sourceAngle = Number(sourceNode.angle);
        const targetAngle = Number(targetNode.angle);
        if (!Number.isFinite(sourceAngle) || !Number.isFinite(targetAngle)) {
            ctx.lineTo(target.x, target.y);
            ctx.stroke();
            ctx.restore();
            return;
        }
        const sourceScreenRadius = Math.max(0, source.progress || 0) * layout.radius;
        const targetScreenRadius = Math.max(sourceScreenRadius, Math.max(0, target.progress || 0) * layout.radius);
        const radialStartX = layout.centerX + Math.cos(targetAngle) * sourceScreenRadius;
        const radialStartY = layout.centerY + Math.sin(targetAngle) * sourceScreenRadius;
        const radialEndX = layout.centerX + Math.cos(targetAngle) * targetScreenRadius;
        const radialEndY = layout.centerY + Math.sin(targetAngle) * targetScreenRadius;
        const delta = shortestAngleDelta(sourceAngle, targetAngle);
        if (sourceScreenRadius > 0.5 && Math.abs(delta) > 0.0001) {
            ctx.arc(layout.centerX, layout.centerY, sourceScreenRadius, sourceAngle, sourceAngle + delta, delta < 0);
        }
        else if (Math.hypot(radialStartX - source.x, radialStartY - source.y) > 0.5) {
            ctx.lineTo(radialStartX, radialStartY);
        }
        ctx.lineTo(radialEndX, radialEndY);
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
    function drawDendrogramCutoffCircle(ctx, layout) {
        if (dendrogramScaleMode() !== "distance") {
            return;
        }
        if (currentHierarchicalTarget() !== "distance_threshold") {
            return;
        }
        const cutoffDistance = currentCutoffDistance();
        const screenRadius = cutoffScreenRadius(layout);
        if (cutoffDistance === null || screenRadius === null) {
            return;
        }
        ctx.save();
        ctx.strokeStyle = "rgba(72, 70, 66, 0.72)";
        ctx.lineWidth = 1.8;
        ctx.setLineDash([4, 6]);
        ctx.beginPath();
        if (layout.style === "linear") {
            const y = layout.top + screenRadius;
            ctx.moveTo(layout.left, y);
            ctx.lineTo(layout.right, y);
        }
        else {
            ctx.arc(layout.centerX, layout.centerY, screenRadius, 0, Math.PI * 2);
        }
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = "rgba(72, 70, 66, 0.78)";
        ctx.font = '11px "IBM Plex Sans", sans-serif';
        ctx.textAlign = "left";
        ctx.textBaseline = "middle";
        const labelX = layout.style === "linear" ? layout.right + 8 : layout.centerX + screenRadius + 8;
        const labelY = layout.style === "linear" ? layout.top + screenRadius : layout.centerY;
        ctx.fillText(`cutoff ${formatDendrogramDistance(cutoffDistance)}`, labelX, labelY);
        ctx.restore();
    }
    function drawDendrogramDistanceGuides(ctx, layout) {
        if (dendrogramScaleMode() !== "distance") {
            return;
        }
        const maxMergeDistance = Number(state.dendrogram?.max_merge_distance || 0);
        const scaleLength = layout.style === "linear" ? layout.height : layout.radius;
        if (!Number.isFinite(maxMergeDistance) || maxMergeDistance <= 0 || scaleLength <= 0) {
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
            const guideRadius = Math.max(0, 1 - distance / maxMergeDistance) * scaleLength;
            ctx.beginPath();
            if (layout.style === "linear") {
                const y = layout.top + guideRadius;
                ctx.moveTo(layout.left, y);
                ctx.lineTo(layout.right, y);
            }
            else {
                ctx.arc(layout.centerX, layout.centerY, guideRadius, 0, Math.PI * 2);
            }
            ctx.stroke();
        }
        ctx.restore();
    }
    function drawDendrogramDistanceScale(ctx, width, height, layout) {
        if (dendrogramScaleMode() !== "distance") {
            return;
        }
        const maxMergeDistance = Number(state.dendrogram?.max_merge_distance || 0);
        const scaleLength = layout.style === "linear" ? layout.height : layout.radius;
        if (!Number.isFinite(maxMergeDistance) || maxMergeDistance <= 0 || scaleLength <= 0) {
            return;
        }
        const pixelsPerCssCm = 96 / 2.54;
        const barWidth = Math.min(pixelsPerCssCm, Math.max(28, width - 34));
        const distancePerBar = (barWidth / scaleLength) * maxMergeDistance;
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
        const style = dendrogramStyle();
        const layout = dendrogramLayoutForSize(width, height);
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
            positions.set(node.id, nodeScreenPosition(node, layout));
        }
        drawDendrogramDistanceGuides(ctx, layout);
        drawDendrogramCutoffCircle(ctx, layout);
        const trueDistanceEdges = dendrogramScaleMode() === "distance" && Boolean(state.dendrogramTrueDistanceEdges);
        for (const edge of state.dendrogram.edges || []) {
            if (!visibleNodeIds.has(edge.source) || !visibleNodeIds.has(edge.target)) {
                continue;
            }
            const source = positions.get(edge.source);
            const target = positions.get(edge.target);
            const sourceNode = nodesById.get(edge.source);
            const targetNode = nodesById.get(edge.target);
            if (!source || !target || !sourceNode || !targetNode) {
                continue;
            }
            if (trueDistanceEdges) {
                drawDendrogramTrueDistanceEdge(ctx, source, target, sourceNode, targetNode, layout, colorMode, visibleKeys);
            }
            else {
                drawDendrogramEdge(ctx, source, target, targetNode, colorMode, visibleKeys);
            }
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
        drawDendrogramDistanceScale(ctx, width, height, layout);
        const maxDepth = Number(state.dendrogram.max_merge_depth || 1);
        const scaleModeLabel = dendrogramScaleMode();
        const styleLabel = style === "linear" ? "linear" : "radial";
        const distanceLayoutLabel = trueDistanceEdges ? " · true lengths" : "";
        const visibleMemberCount = (state.dendrogram.nodes || [])
            .filter((node) => node.parent_id === null)
            .reduce((total, node) => total +
            countVisibleMembers(colorMode === "cluster" ? node.cluster_counts : node.domain_counts, visibleKeys), 0);
        setDendrogramInfo(`Style ${styleLabel} · depth ${state.dendrogram.merge_depth}/${maxDepth} · scale by ${scaleModeLabel}${distanceLayoutLabel} · ${visibleNodeIds.size} visible nodes · ${visibleMemberCount} rows · ${state.dendrogram.cluster_count} clusters · ${state.dendrogram.hierarchy_source}`);
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
        const layout = currentDendrogramLayout();
        if (layout && pointNearDendrogramCutoff(point, layout)) {
            event.preventDefault();
            state.dendrogramDrag = {
                type: "cutoff",
            };
            elements.dendrogramCanvas.style.cursor =
                layout.style === "linear" ? "ns-resize" : "grabbing";
            updateDendrogramCutoffFromPoint(point, false);
            elements.dendrogramCanvas.setPointerCapture?.(event.pointerId);
            return;
        }
        state.dendrogramDrag = {
            type: "pan",
            x: point.x,
            y: point.y,
            viewX: Number(state.dendrogramView?.x || 0),
            viewY: Number(state.dendrogramView?.y || 0),
        };
        elements.dendrogramCanvas.setPointerCapture?.(event.pointerId);
    }
    function handleDendrogramPointerMove(event) {
        const point = dendrogramCanvasPoint(event);
        if (!state.dendrogramDrag) {
            const layout = currentDendrogramLayout();
            if (layout && pointNearDendrogramCutoff(point, layout)) {
                elements.dendrogramCanvas.style.cursor = cutoffCursor(layout);
            }
            else {
                elements.dendrogramCanvas.style.cursor = "";
            }
            return;
        }
        if (state.dendrogramDrag.type === "cutoff") {
            event.preventDefault();
            updateDendrogramCutoffFromPoint(point, false);
            return;
        }
        state.dendrogramView = {
            ...state.dendrogramView,
            x: state.dendrogramDrag.viewX + point.x - state.dendrogramDrag.x,
            y: state.dendrogramDrag.viewY + point.y - state.dendrogramDrag.y,
        };
        requestDendrogramRender();
    }
    function handleDendrogramPointerUp(event) {
        if (state.dendrogramDrag?.type === "cutoff") {
            updateDendrogramCutoffFromPoint(dendrogramCanvasPoint(event), true);
        }
        state.dendrogramDrag = null;
        elements.dendrogramCanvas.style.cursor = "";
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
