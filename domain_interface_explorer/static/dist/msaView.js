import { CELL_WIDTH, DEFAULT_CLUSTERING_SETTINGS, DEFAULT_EMBEDDING_SETTINGS, HEADER_HEIGHT, LABEL_WIDTH, ROW_HEIGHT, TEXT_FONT, } from "./constants.js";
import { fetchJson } from "./api.js";
import { interactionRowKey, interfaceFileStem, parseInteractionRowKey } from "./interfaceModel.js";
import { appendSelectionSettingsToParams, normalizeSelectionSettings, } from "./selectionSettings.js";
export function createMsaViewController({ state, elements, buildPairs, activeConservationVector, conservationColor, overlayStateForRow, representativeLens, embeddingDistanceLabel, syncColumnLegends, syncRepresentativeLensControls, syncEmbeddingLoadingUi, syncEmbeddingMemberControls, syncEmbeddingSettingsUi, resizeEmbeddingCanvas, resizeColumnsCanvas, renderEmbeddingPlot, renderColumnsChart, renderColumnsClusterLegend, setEmbeddingInfo, setColumnsInfo, ensureEmbeddingDataLoaded, ensureEmbeddingClusteringLoaded, resetColumnsClusterSelection, resetEmbeddingPartnerSelection, resetEmbeddingClusterSelection, resetRepresentativePartnerSelection, resetRepresentativeClusterSelection, renderRepresentativePartnerFilter, renderEmbeddingLegend, refreshRepresentativeSelection, loadInteractiveStructure, handleStructureLoadFailure, resetRepresentativePanel, resetStructurePanel, closeClusterCompareModal, closeStructureModal, resizeClusterCompareViewers, buildOverlayMaps, buildPartnerColorMap, embeddingClusterColor, embeddingClusterLabel, allColumnsClusterLabels, visibleColumnsClusters, updatePartnerOptions, }) {
    const { appStatus, cellDetailsPanel, columnCount, columnsClusterLegend, detailsList, detailsBar, embeddingRoot, columnsRoot, gridCanvas, gridScroll, gridSpacer, headerCanvas, infoRoot, interfaceSelect, labelsCanvas, loadingDetail, loadingLabel, loadingPanel, loadStructureButton, msaLegend, msaPanelTabs, msaClusterLegend, msaPickerButton, msaPickerFilters, msaPickerMenu, msaPickerOptions, msaPickerSearch, msaPickerSelection, msaSelect, selectionSettingsPanel, selectionSettingsToggle, selectionMinInterfaceSizeInput, partnerSelect, progressBar, representativeShell, representativeViewerRoot, rowCount, selectedRowCopy, statsPanel, structureModal, viewerPanel, viewerRoot, } = elements;
    let layoutSyncScheduled = false;
    let cachedMsaClusterSource = null;
    let cachedMsaRowClusterAssignments = new Map();
    let cachedMsaRowClusterMemberships = new Map();
    let cachedMsaClusterCounts = new Map();
    let pfamMetadataPollHandle = 0;
    let pfamMetadataPollAttempts = 0;
    let currentMsaStreamFilename = "";
    let filesRequestPromise = null;
    const pfamInfoRequests = new Map();
    const INITIAL_MSA_ROW_LIMIT = 240;
    const MSA_ROW_CHUNK_SIZE = 1200;
    const PFAM_METADATA_POLL_DELAY_MS = 30000;
    const PFAM_METADATA_POLL_MAX_ATTEMPTS = 12;
    const pfamNumberFormatter = new Intl.NumberFormat();
    function activeMsaPanelView() {
        return state.msaPanelView;
    }
    function defaultPointMethodLabel() {
        return DEFAULT_EMBEDDING_SETTINGS.method === "pca" ? "PCA" : "openTSNE";
    }
    function numericStyleValue(style, property) {
        const value = Number.parseFloat(style[property] || "0");
        return Number.isFinite(value) ? value : 0;
    }
    function outerHeight(element) {
        if (!element) {
            return 0;
        }
        if (element.classList?.contains("hidden") ||
            element.classList?.contains("panel-view-hidden")) {
            return 0;
        }
        const style = window.getComputedStyle(element);
        return (element.offsetHeight +
            numericStyleValue(style, "marginTop") +
            numericStyleValue(style, "marginBottom"));
    }
    function activePanelRoot() {
        if (activeMsaPanelView() === "info") {
            return infoRoot;
        }
        if (activeMsaPanelView() === "msa") {
            return viewerRoot;
        }
        if (activeMsaPanelView() === "embeddings") {
            return embeddingRoot;
        }
        return columnsRoot;
    }
    function syncPaneHeights() {
        const panelRoots = [infoRoot, viewerRoot, embeddingRoot, columnsRoot];
        panelRoots.forEach((root) => {
            if (root) {
                root.style.height = "";
            }
        });
        const activeRoot = activePanelRoot();
        if (viewerPanel && activeRoot) {
            const panelStyle = window.getComputedStyle(viewerPanel);
            const availableHeight = viewerPanel.clientHeight -
                numericStyleValue(panelStyle, "paddingTop") -
                numericStyleValue(panelStyle, "paddingBottom") -
                outerHeight(msaPanelTabs) -
                (activeMsaPanelView() === "msa" ? outerHeight(msaLegend) + outerHeight(detailsBar) : 0);
            activeRoot.style.height = `${Math.max(0, Math.floor(availableHeight))}px`;
        }
        if (representativeShell) {
            const stage = representativeShell.querySelector(".representative-stage");
            const title = representativeShell.querySelector("h2");
            const copy = representativeShell.querySelector("#representative-copy");
            const shellStyle = window.getComputedStyle(representativeShell);
            const shellChildren = [...representativeShell.children].filter((child) => !child.classList.contains("hidden") && !child.classList.contains("panel-view-hidden"));
            const shellGap = numericStyleValue(shellStyle, "rowGap") || numericStyleValue(shellStyle, "gap");
            const reservedHeight = outerHeight(title) +
                outerHeight(copy) +
                shellGap * Math.max(0, shellChildren.length - 1);
            const availableHeight = representativeShell.clientHeight -
                numericStyleValue(shellStyle, "paddingTop") -
                numericStyleValue(shellStyle, "paddingBottom") -
                reservedHeight;
            if (stage) {
                stage.style.height = `${Math.max(0, Math.floor(availableHeight))}px`;
            }
        }
    }
    function syncMsaPanelView() {
        const isInfoView = activeMsaPanelView() === "info";
        const isMsaView = activeMsaPanelView() === "msa";
        const isEmbeddingView = activeMsaPanelView() === "embeddings";
        const isColumnsView = activeMsaPanelView() === "columns";
        [...msaPanelTabs.querySelectorAll("[data-panel-view]")].forEach((button) => {
            const isActive = button.dataset.panelView === state.msaPanelView;
            button.classList.toggle("active", isActive);
            button.setAttribute("aria-selected", String(isActive));
        });
        detailsBar.classList.toggle("panel-view-hidden", !isMsaView);
        cellDetailsPanel.classList.toggle("panel-view-hidden", !isMsaView);
        statsPanel.classList.add("panel-view-hidden");
        infoRoot.classList.toggle("panel-view-hidden", !isInfoView);
        msaLegend.classList.toggle("panel-view-hidden", !isMsaView);
        viewerRoot.classList.toggle("panel-view-hidden", !isMsaView);
        embeddingRoot.classList.toggle("panel-view-hidden", !isEmbeddingView);
        columnsRoot.classList.toggle("panel-view-hidden", !isColumnsView);
    }
    function setOptions(select, options, value = null) {
        select.innerHTML = "";
        for (const option of options) {
            const el = document.createElement("option");
            el.value = option.value;
            el.textContent = option.label;
            select.appendChild(el);
        }
        if (value !== null) {
            select.value = value;
        }
    }
    function setLoading(progress, label, detail) {
        if (label) {
            loadingLabel.textContent = label;
        }
        if (detail) {
            loadingDetail.textContent = detail;
        }
        loadingPanel.classList.remove("hidden");
        progressBar.style.width = `${Math.max(0, Math.min(100, progress))}%`;
    }
    function hideLoading() {
        loadingPanel.classList.add("hidden");
    }
    function matchesFilterDirection(category, direction) {
        if (typeof category !== "string") {
            return false;
        }
        if (direction === "big") {
            return category === "big" || category === "very_big";
        }
        if (direction === "small") {
            return category === "small" || category === "very_small";
        }
        return false;
    }
    function optionMatchesBadgeFilters(option) {
        const stats = option.stats;
        return Object.entries(state.msaFilterState).every(([metricKey, directions]) => {
            if (!directions || directions.size === 0) {
                return true;
            }
            const category = stats?.[`${metricKey}_category`];
            for (const direction of directions) {
                if (matchesFilterDirection(category, direction)) {
                    return true;
                }
            }
            return false;
        });
    }
    function badgeCategoryClass(key) {
        if (key === "big") {
            return "warm";
        }
        if (key === "very_big") {
            return "warm-strong";
        }
        if (key === "small") {
            return "cold";
        }
        if (key === "very_small") {
            return "cold-strong";
        }
        return null;
    }
    function badgeCategoryLabel(key) {
        if (key === "big") {
            return "larger than average";
        }
        if (key === "very_big") {
            return "much larger than average";
        }
        if (key === "small") {
            return "smaller than average";
        }
        if (key === "very_small") {
            return "much smaller than average";
        }
        return "near average";
    }
    function formatBadgeValue(metricKey, value) {
        if (metricKey === "avg_interface_residues_per_row") {
            return Number(value || 0).toFixed(1);
        }
        return String(value ?? 0);
    }
    function pfamBadges(stats) {
        if (!stats) {
            return [];
        }
        const specs = [
            {
                metricKey: "alignment_length",
                categoryKey: "alignment_length_category",
                symbol: "↔",
                label: "Alignment length",
            },
            {
                metricKey: "interface_rows",
                categoryKey: "interface_rows_category",
                symbol: "≣",
                label: "Interface rows",
            },
            {
                metricKey: "interaction_partners",
                categoryKey: "interaction_partners_category",
                symbol: "⋈",
                label: "Interaction partners",
            },
            {
                metricKey: "avg_interface_residues_per_row",
                categoryKey: "avg_interface_residues_per_row_category",
                symbol: "◍",
                label: "Interface residues per row",
            },
        ];
        return specs
            .map((spec) => {
            const category = stats[spec.categoryKey];
            const className = badgeCategoryClass(category);
            if (!className) {
                return null;
            }
            return {
                symbol: spec.symbol,
                className,
                title: `${spec.label}: ${badgeCategoryLabel(category)} (${formatBadgeValue(spec.metricKey, stats[spec.metricKey])})`,
            };
        })
            .filter(Boolean);
    }
    function createBadgeStrip(badges) {
        const strip = document.createElement("span");
        strip.className = "pfam-badge-strip";
        for (const badge of badges) {
            const item = document.createElement("span");
            item.className = `pfam-badge ${badge.className}`;
            item.textContent = badge.symbol;
            item.title = badge.title;
            strip.appendChild(item);
        }
        return strip;
    }
    function currentMsaOption() {
        return (state.msaOptions || []).find((option) => option.value === msaSelect.value) || null;
    }
    function currentPfamId() {
        return String(currentMsaOption()?.pfamId || "").trim();
    }
    function clearPfamInfoState() {
        state.pfamInfo = null;
        state.pfamInfoError = "";
        state.pfamInfoLoading = false;
        state.pfamInfoRequestId += 1;
    }
    function formatPfamCount(value) {
        const numericValue = Number(value);
        if (!Number.isFinite(numericValue)) {
            return "—";
        }
        return pfamNumberFormatter.format(numericValue);
    }
    function appendPfamInfoMetaItem(container, label, value) {
        const text = String(value || "").trim();
        if (!text) {
            return;
        }
        const item = document.createElement("div");
        item.className = "pfam-info-meta-item";
        const labelElement = document.createElement("span");
        labelElement.className = "pfam-info-meta-label";
        labelElement.textContent = label;
        const valueElement = document.createElement("strong");
        valueElement.className = "pfam-info-meta-value";
        valueElement.textContent = text;
        item.appendChild(labelElement);
        item.appendChild(valueElement);
        container.appendChild(item);
    }
    function appendPfamInfoStat(container, label, value) {
        const numericValue = Number(value);
        if (!Number.isFinite(numericValue)) {
            return;
        }
        const item = document.createElement("div");
        item.className = "pfam-info-stat";
        const valueElement = document.createElement("strong");
        valueElement.className = "pfam-info-stat-value";
        valueElement.textContent = formatPfamCount(numericValue);
        const labelElement = document.createElement("span");
        labelElement.className = "pfam-info-stat-label";
        labelElement.textContent = label;
        item.appendChild(valueElement);
        item.appendChild(labelElement);
        container.appendChild(item);
    }
    function normalizeHistogramEntries(entries) {
        if (!Array.isArray(entries)) {
            return [];
        }
        return entries
            .map((entry) => ({
            size: Number(entry?.size),
            count: Number(entry?.count),
        }))
            .filter((entry) => Number.isFinite(entry.size) && entry.size > 0 && Number.isFinite(entry.count) && entry.count > 0)
            .sort((left, right) => left.size - right.size);
    }
    function interfaceSizeFromPayload(rowPayload) {
        if (!rowPayload || typeof rowPayload !== "object") {
            return 0;
        }
        const sourceValues = Array.isArray(rowPayload.interface_msa_columns_a)
            ? rowPayload.interface_msa_columns_a
            : Array.isArray(rowPayload.interface_residues_a)
                ? rowPayload.interface_residues_a
                : [];
        const interfaceColumns = new Set();
        for (const value of sourceValues) {
            const numericValue = Number(value);
            if (Number.isInteger(numericValue)) {
                interfaceColumns.add(numericValue);
            }
        }
        return interfaceColumns.size;
    }
    function filteredInterfaceEntries(interfacePayload, selectedPartner = state.selectedPartner) {
        const entries = [];
        for (const [partnerDomain, rows] of Object.entries(interfacePayload || {})) {
            if (selectedPartner !== "__all__" && partnerDomain !== selectedPartner) {
                continue;
            }
            if (!rows || typeof rows !== "object") {
                continue;
            }
            for (const [rowKey, rowPayload] of Object.entries(rows)) {
                entries.push({
                    partnerDomain,
                    rowKey,
                    rowPayload,
                });
            }
        }
        return entries;
    }
    function localInterfaceSummary(interfacePayload, selectedPartner = state.selectedPartner) {
        const entries = filteredInterfaceEntries(interfacePayload, selectedPartner);
        const countsBySize = new Map();
        const datasetDomains = new Set();
        for (const entry of entries) {
            const rowKeyParts = String(entry.rowKey || "").split("_", 2);
            const proteinId = rowKeyParts[0] || "";
            const fragmentKey = rowKeyParts[1] || "";
            datasetDomains.add(`${proteinId}@@${fragmentKey}`);
            const interfaceSize = interfaceSizeFromPayload(entry.rowPayload);
            if (interfaceSize <= 0) {
                continue;
            }
            countsBySize.set(interfaceSize, (countsBySize.get(interfaceSize) || 0) + 1);
        }
        return {
            datasetDomains: datasetDomains.size,
            datasetInterfaces: entries.length,
            histogramEntries: normalizeHistogramEntries([...countsBySize.entries()].map(([size, count]) => ({ size, count }))),
        };
    }
    function histogramEntriesFromInterfacePayload(interfacePayload) {
        return localInterfaceSummary(interfacePayload).histogramEntries;
    }
    function compressHistogramEntries(entries, maxBarCount = 24) {
        if (entries.length <= maxBarCount) {
            return entries.map((entry) => ({
                start: entry.size,
                end: entry.size,
                count: entry.count,
            }));
        }
        const minSize = entries[0]?.size || 1;
        const maxSize = entries[entries.length - 1]?.size || minSize;
        const binWidth = Math.max(1, Math.ceil((maxSize - minSize + 1) / maxBarCount));
        const buckets = new Map();
        for (const entry of entries) {
            const bucketIndex = Math.floor((entry.size - minSize) / binWidth);
            const bucketStart = minSize + bucketIndex * binWidth;
            const bucketEnd = Math.min(maxSize, bucketStart + binWidth - 1);
            const bucketKey = `${bucketStart}-${bucketEnd}`;
            const current = buckets.get(bucketKey) || {
                start: bucketStart,
                end: bucketEnd,
                count: 0,
            };
            current.count += entry.count;
            buckets.set(bucketKey, current);
        }
        return [...buckets.values()].sort((left, right) => left.start - right.start);
    }
    function histogramBinLabel(bin) {
        return bin.start === bin.end ? String(bin.start) : `${bin.start}-${bin.end}`;
    }
    function uniprotEntryUrl(accession) {
        return `https://www.uniprot.org/uniprotkb/${encodeURIComponent(String(accession || "").trim())}`;
    }
    function pfamEntryUrl(accession) {
        return `https://www.ebi.ac.uk/interpro/entry/pfam/${encodeURIComponent(String(accession || "").trim())}/`;
    }
    function rowLabelSegments(row, baseColor) {
        const proteinId = String(row?.protein_id || "").trim();
        const partnerDomain = String(row?.partner_domain || "").trim();
        if (proteinId && partnerDomain) {
            return [
                {
                    text: proteinId,
                    color: "#8d5b2c",
                    href: uniprotEntryUrl(proteinId),
                },
                {
                    text: " | ",
                    color: baseColor,
                    href: "",
                },
                {
                    text: partnerDomain,
                    color: "#0b3f78",
                    href: pfamEntryUrl(partnerDomain),
                },
            ];
        }
        return [
            {
                text: String(row?.display_row_key || row?.row_key || ""),
                color: baseColor,
                href: "",
            },
        ];
    }
    function labelHitTargetAtClientPoint(clientX, clientY) {
        if (!state.msa) {
            return null;
        }
        const rect = labelsCanvas.getBoundingClientRect();
        const localY = clientY - rect.top;
        const filteredRowIndex = Math.floor((localY + gridScroll.scrollTop) / ROW_HEIGHT);
        if (filteredRowIndex < 0 || filteredRowIndex >= state.filteredRowIndexes.length) {
            return null;
        }
        const rowIndex = state.filteredRowIndexes[filteredRowIndex];
        const row = state.msa.rows[rowIndex];
        if (!row) {
            return null;
        }
        const localX = clientX - rect.left;
        if (localX < 22) {
            return { row, href: "" };
        }
        const clusterKey = rowClusterKey(row.row_key);
        const baseColor = clusterKey === null ? "#2e261d" : clusterColorForKey(clusterKey);
        const segments = rowLabelSegments(row, baseColor);
        const ctx = labelsCanvas.getContext("2d");
        ctx.font = TEXT_FONT;
        let currentX = 22;
        for (const segment of segments) {
            const width = ctx.measureText(segment.text).width;
            if (localX >= currentX && localX <= currentX + width) {
                return { row, href: segment.href || "" };
            }
            currentX += width;
        }
        return { row, href: "" };
    }
    function setPartnerFilterValue(partnerDomain) {
        const nextValue = String(partnerDomain || "").trim() || "__all__";
        const optionValues = new Set([...(partnerSelect?.options || [])].map((option) => String(option.value || "")));
        state.selectedPartner = optionValues.has(nextValue) ? nextValue : "__all__";
        if (partnerSelect) {
            partnerSelect.value = state.selectedPartner;
        }
    }
    function ensureRowVisible(rowKey) {
        if (!state.msa || !gridScroll) {
            return;
        }
        updateFilteredRows();
        const filteredIndex = state.filteredRowIndexes.findIndex((rowIndex) => state.msa?.rows?.[rowIndex]?.row_key === rowKey);
        if (filteredIndex < 0) {
            return;
        }
        const rowTop = filteredIndex * ROW_HEIGHT;
        const rowBottom = rowTop + ROW_HEIGHT;
        const viewTop = gridScroll.scrollTop;
        const viewBottom = viewTop + gridScroll.clientHeight;
        if (rowTop < viewTop) {
            gridScroll.scrollTop = rowTop;
        }
        else if (rowBottom > viewBottom) {
            gridScroll.scrollTop = Math.max(0, rowBottom - gridScroll.clientHeight);
        }
    }
    function pfamEntryUrl(accession) {
        return `https://www.ebi.ac.uk/interpro/entry/pfam/${encodeURIComponent(String(accession || "").trim())}/`;
    }
    function createPfamEntryLink(accession, label, className = "pfam-info-link") {
        const link = document.createElement("a");
        link.className = className;
        link.href = pfamEntryUrl(accession);
        link.target = "_blank";
        link.rel = "noreferrer";
        link.textContent = label;
        return link;
    }
    function appendDescriptionWithPfamLinks(container, text) {
        const descriptionText = String(text || "");
        if (!descriptionText) {
            return;
        }
        const pattern = /\[pfam:\s*(PF\d+)\]/gi;
        let cursor = 0;
        for (const match of descriptionText.matchAll(pattern)) {
            const matchStart = match.index ?? 0;
            const matchEnd = matchStart + match[0].length;
            if (matchStart > cursor) {
                container.appendChild(document.createTextNode(descriptionText.slice(cursor, matchStart)));
            }
            const accession = String(match[1] || "").toUpperCase();
            container.appendChild(createPfamEntryLink(accession, accession));
            cursor = matchEnd;
        }
        if (cursor < descriptionText.length) {
            container.appendChild(document.createTextNode(descriptionText.slice(cursor)));
        }
    }
    function histogramTargetsForBin(bin) {
        const targets = [];
        for (const { partnerDomain, rowKey, rowPayload } of filteredInterfaceEntries(state.interface?.data)) {
            const interfaceSize = interfaceSizeFromPayload(rowPayload);
            if (interfaceSize < bin.start || interfaceSize > bin.end) {
                continue;
            }
            targets.push({
                rowKey: interactionRowKey(rowKey, partnerDomain),
                partnerDomain: String(partnerDomain || ""),
            });
        }
        return targets;
    }
    async function showRandomInterfaceForHistogramBin(bin) {
        if (!state.interface?.data || !state.interface?.overlayComplete || !state.msa) {
            appStatus.textContent = "Load a filtered interface selection before using the histogram.";
            return;
        }
        const candidates = histogramTargetsForBin(bin);
        if (candidates.length === 0) {
            appStatus.textContent = "No filtered interfaces are available in that histogram range.";
            return;
        }
        const target = candidates[Math.floor(Math.random() * candidates.length)];
        setLoading(5, "Loading selection", "Picking a random interface from the histogram");
        try {
            const selectedRow = selectRowByKey(String(target.rowKey || ""));
            if (!selectedRow) {
                throw new Error("The sampled interface is no longer available in the filtered selection.");
            }
            await loadInteractiveStructure();
            setLoading(100, "Structure ready", String(selectedRow.display_row_key || selectedRow.row_key || ""));
            window.setTimeout(hideLoading, 250);
        }
        catch (error) {
            handleStructureLoadFailure(error);
        }
    }
    function renderPfamInfoHistogram() {
        const histogramEntries = state.interface?.overlayComplete
            ? histogramEntriesFromInterfacePayload(state.interface?.data)
            : [];
        const section = document.createElement("section");
        section.className = "pfam-info-histogram";
        const title = document.createElement("div");
        title.className = "pfam-info-histogram-title";
        title.textContent = "Interface size";
        section.appendChild(title);
        if (!state.interface?.data || !state.interface?.overlayComplete) {
            const empty = document.createElement("p");
            empty.className = "pfam-info-histogram-empty";
            empty.textContent = "Loading filtered interface-size data…";
            section.appendChild(empty);
            return section;
        }
        if (histogramEntries.length === 0) {
            const empty = document.createElement("p");
            empty.className = "pfam-info-histogram-empty";
            empty.textContent = "No local interfaces pass the current minimum-size filter.";
            section.appendChild(empty);
            return section;
        }
        const chartBins = compressHistogramEntries(histogramEntries);
        const maxCount = Math.max(...chartBins.map((entry) => entry.count), 1);
        const chart = document.createElement("div");
        chart.className = "pfam-info-histogram-chart";
        chart.style.gridTemplateColumns = `repeat(${chartBins.length}, minmax(0, 1fr))`;
        if (chartBins.length > 16) {
            chart.classList.add("dense");
        }
        for (const bin of chartBins) {
            const column = document.createElement("button");
            column.type = "button";
            column.className = "pfam-info-histogram-column";
            column.title = `${histogramBinLabel(bin)} residues: ${formatPfamCount(bin.count)} filtered interfaces`;
            column.addEventListener("click", () => {
                void showRandomInterfaceForHistogramBin(bin);
            });
            const barArea = document.createElement("div");
            barArea.className = "pfam-info-histogram-bar-area";
            const bar = document.createElement("div");
            bar.className = "pfam-info-histogram-bar";
            bar.style.height = `${Math.max(6, (bin.count / maxCount) * 100)}%`;
            barArea.appendChild(bar);
            const label = document.createElement("span");
            label.className = "pfam-info-histogram-bin-label";
            label.textContent = histogramBinLabel(bin);
            column.appendChild(barArea);
            column.appendChild(label);
            chart.appendChild(column);
        }
        section.appendChild(chart);
        const footer = document.createElement("div");
        footer.className = "pfam-info-histogram-axis";
        const minLabel = document.createElement("span");
        minLabel.textContent = histogramBinLabel(chartBins[0]);
        const midLabel = document.createElement("span");
        midLabel.textContent = histogramBinLabel(chartBins[Math.floor(chartBins.length / 2)]);
        const maxLabel = document.createElement("span");
        maxLabel.textContent = histogramBinLabel(chartBins[chartBins.length - 1]);
        footer.appendChild(minLabel);
        footer.appendChild(midLabel);
        footer.appendChild(maxLabel);
        section.appendChild(footer);
        return section;
    }
    function renderInfoPanel() {
        if (!infoRoot) {
            return;
        }
        infoRoot.innerHTML = "";
        const selected = currentMsaOption();
        if (!selected) {
            const empty = document.createElement("div");
            empty.className = "pfam-info-empty";
            const title = document.createElement("strong");
            title.textContent = "Choose a PFAM family";
            const copy = document.createElement("p");
            copy.textContent =
                "Select a family from the picker to view its description, key counts, and the official Pfam page link.";
            empty.appendChild(title);
            empty.appendChild(copy);
            infoRoot.appendChild(empty);
            return;
        }
        const pfamId = currentPfamId();
        const cachedInfo = state.pfamInfoByPfamId?.[pfamId] || null;
        const info = state.pfamInfo?.pfam_id === pfamId ? state.pfamInfo : cachedInfo;
        const localSummary = state.interface?.data && state.interface?.overlayComplete
            ? localInterfaceSummary(state.interface.data)
            : null;
        const datasetDomainCount = Number(localSummary?.datasetDomains ?? selected.stats?.dataset_domains);
        const datasetInterfaceCount = Number(localSummary?.datasetInterfaces ?? selected.stats?.dataset_interfaces);
        const pfamDomainCount = Number(info?.stats?.matches);
        const shell = document.createElement("div");
        shell.className = "pfam-info-shell";
        const hero = document.createElement("section");
        hero.className = "pfam-info-hero";
        const kicker = document.createElement("span");
        kicker.className = "pfam-info-kicker";
        kicker.textContent = "PFAM FAMILY";
        hero.appendChild(kicker);
        const title = document.createElement("h2");
        title.className = "pfam-info-title";
        const titleLink = createPfamEntryLink(pfamId, String(info?.display_name || pfamDisplayName(selected) || pfamId), "pfam-info-link pfam-info-title-link");
        title.appendChild(titleLink);
        hero.appendChild(title);
        const titleMeta = document.createElement("div");
        titleMeta.className = "pfam-info-title-meta";
        const accession = document.createElement("span");
        accession.className = "pfam-info-accession";
        accession.textContent = pfamId;
        titleMeta.appendChild(accession);
        if (String(info?.short_name || "").trim()) {
            const shortName = document.createElement("span");
            shortName.className = "pfam-info-chip";
            shortName.textContent = String(info.short_name);
            titleMeta.appendChild(shortName);
        }
        if (String(info?.type || "").trim()) {
            const typeChip = document.createElement("span");
            typeChip.className = "pfam-info-chip";
            typeChip.textContent = String(info.type);
            titleMeta.appendChild(typeChip);
        }
        hero.appendChild(titleMeta);
        const description = document.createElement("p");
        description.className = "pfam-info-description";
        const descriptionText = String(info?.description || "").trim();
        if (descriptionText) {
            appendDescriptionWithPfamLinks(description, descriptionText);
        }
        else {
            description.textContent = state.pfamInfoLoading
                ? "Loading family description from InterPro…"
                : "No description available.";
        }
        hero.appendChild(description);
        const status = document.createElement("div");
        status.className = "pfam-info-status";
        if (state.pfamInfoLoading) {
            status.textContent = "Loading Pfam family details…";
            hero.appendChild(status);
        }
        else if (String(state.pfamInfoError || "").trim()) {
            status.classList.add("error");
            status.textContent = state.pfamInfoError;
            hero.appendChild(status);
        }
        shell.appendChild(hero);
        shell.appendChild(renderPfamInfoHistogram());
        const metaGrid = document.createElement("section");
        metaGrid.className = "pfam-info-meta-grid";
        if (Number.isFinite(datasetDomainCount)) {
            appendPfamInfoMetaItem(metaGrid, "Dataset domains", Number.isFinite(pfamDomainCount) && pfamDomainCount >= 0
                ? `${formatPfamCount(datasetDomainCount)} of ${formatPfamCount(pfamDomainCount)} Pfam domains`
                : formatPfamCount(datasetDomainCount));
        }
        appendPfamInfoMetaItem(metaGrid, "Integrated InterPro", info?.integrated_interpro);
        if (info?.set_info) {
            appendPfamInfoMetaItem(metaGrid, "Clan / set", [info.set_info.name, info.set_info.accession].filter(Boolean).join(" | "));
        }
        if (info?.representative_structure) {
            appendPfamInfoMetaItem(metaGrid, "Representative structure", [info.representative_structure.accession, info.representative_structure.name]
                .filter(Boolean)
                .join(" | "));
        }
        if (metaGrid.children.length > 0) {
            shell.appendChild(metaGrid);
        }
        const statsGrid = document.createElement("section");
        statsGrid.className = "pfam-info-stats-grid";
        appendPfamInfoStat(statsGrid, "Interfaces in dataset", datasetInterfaceCount);
        appendPfamInfoStat(statsGrid, "Pfam domains", info?.stats?.matches);
        appendPfamInfoStat(statsGrid, "Pfam proteins", info?.stats?.proteins);
        appendPfamInfoStat(statsGrid, "Proteomes", info?.stats?.proteomes);
        appendPfamInfoStat(statsGrid, "Taxa", info?.stats?.taxa);
        appendPfamInfoStat(statsGrid, "Structures", info?.stats?.structures);
        appendPfamInfoStat(statsGrid, "AlphaFold models", info?.stats?.alphafold_models);
        appendPfamInfoStat(statsGrid, "Domain architectures", info?.stats?.domain_architectures);
        if (statsGrid.children.length > 0) {
            shell.appendChild(statsGrid);
        }
        infoRoot.appendChild(shell);
    }
    async function ensurePfamInfoLoaded() {
        const pfamId = currentPfamId();
        if (!pfamId) {
            clearPfamInfoState();
            renderInfoPanel();
            return null;
        }
        const cachedInfo = state.pfamInfoByPfamId?.[pfamId] || null;
        if (cachedInfo) {
            state.pfamInfo = cachedInfo;
            state.pfamInfoError = "";
            state.pfamInfoLoading = false;
            renderInfoPanel();
            return cachedInfo;
        }
        const existingRequest = pfamInfoRequests.get(pfamId);
        if (existingRequest) {
            return existingRequest;
        }
        const requestId = state.pfamInfoRequestId + 1;
        state.pfamInfoRequestId = requestId;
        state.pfamInfoLoading = true;
        state.pfamInfoError = "";
        state.pfamInfo = null;
        renderInfoPanel();
        const request = (async () => {
            const payload = await fetchJson(`/api/pfam-info?pfam_id=${encodeURIComponent(pfamId)}`);
            if (requestId !== state.pfamInfoRequestId || currentPfamId() !== pfamId) {
                return payload;
            }
            state.pfamInfoByPfamId = {
                ...(state.pfamInfoByPfamId || {}),
                [pfamId]: payload,
            };
            state.pfamInfo = payload;
            state.pfamInfoError = "";
            return payload;
        })();
        pfamInfoRequests.set(pfamId, request);
        try {
            return await request;
        }
        catch (error) {
            if (requestId !== state.pfamInfoRequestId || currentPfamId() !== pfamId) {
                return null;
            }
            state.pfamInfoError = error.message;
            state.pfamInfo = cachedInfo;
            return null;
        }
        finally {
            if (requestId === state.pfamInfoRequestId && currentPfamId() === pfamId) {
                state.pfamInfoLoading = false;
                renderInfoPanel();
            }
            pfamInfoRequests.delete(pfamId);
        }
    }
    function pfamDisplayName(option) {
        const displayName = String(option?.displayName || "").trim();
        if (!displayName || displayName.toLowerCase() === String(option?.pfamId || "").trim().toLowerCase()) {
            return String(option?.pfamId || "");
        }
        return displayName;
    }
    function pfamShowsSeparateAccession(option) {
        const displayName = String(option?.displayName || "").trim().toLowerCase();
        const pfamId = String(option?.pfamId || "").trim().toLowerCase();
        return Boolean(displayName) && displayName !== pfamId;
    }
    function appendPfamLabel(container, option) {
        const label = document.createElement("span");
        label.className = "msa-picker-option-label";
        const name = document.createElement("span");
        name.className = "msa-picker-option-name";
        name.textContent = pfamDisplayName(option);
        label.appendChild(name);
        if (pfamShowsSeparateAccession(option)) {
            const accession = document.createElement("span");
            accession.className = "msa-picker-option-accession";
            accession.textContent = option.pfamId;
            label.appendChild(accession);
        }
        container.appendChild(label);
    }
    function buildMsaOptionsFromFiles(files) {
        return [...new Set((files?.pairs || []).map((pair) => pair.msaFile))]
            .sort()
            .map((name) => {
            const pfamId = interfaceFileStem(name);
            return {
                value: name,
                pfamId,
                stats: files?.pfam_option_stats?.[pfamId] || null,
                displayName: files?.pfam_option_stats?.[pfamId]?.display_name || "",
            };
        });
    }
    function applyFilesPayload(files, { preserveSelection = true } = {}) {
        const previousSelection = preserveSelection ? msaSelect.value : "";
        state.files = files;
        state.files.pairs = buildPairs(state.files);
        state.msaOptions = buildMsaOptionsFromFiles(state.files);
        setOptions(msaSelect, [{ value: "", label: "Select MSA" }].concat(state.msaOptions.map((option) => ({
            value: option.value,
            label: pfamShowsSeparateAccession(option)
                ? `${pfamDisplayName(option)} (${option.pfamId})`
                : pfamDisplayName(option),
        }))));
        if (previousSelection && state.msaOptions.some((option) => option.value === previousSelection)) {
            msaSelect.value = previousSelection;
        }
    }
    function hasMissingPfamDisplayNames() {
        return (state.msaOptions || []).some((option) => !String(option?.displayName || "").trim() && String(option?.pfamId || "").trim());
    }
    function fetchFilesPayload() {
        if (!filesRequestPromise) {
            filesRequestPromise = fetchJson("/api/files").finally(() => {
                filesRequestPromise = null;
            });
        }
        return filesRequestPromise;
    }
    async function pollPfamMetadataIfNeeded() {
        if (!hasMissingPfamDisplayNames() || pfamMetadataPollAttempts >= PFAM_METADATA_POLL_MAX_ATTEMPTS) {
            return;
        }
        window.clearTimeout(pfamMetadataPollHandle);
        pfamMetadataPollHandle = window.setTimeout(async () => {
            pfamMetadataPollAttempts += 1;
            try {
                const files = await fetchFilesPayload();
                files.pairs = buildPairs(files);
                const nextOptions = buildMsaOptionsFromFiles(files);
                const namesImproved = nextOptions.some((option) => String(option.displayName || "").trim() &&
                    !(state.msaOptions || []).some((current) => current.value === option.value &&
                        String(current.displayName || "").trim() === String(option.displayName || "").trim()));
                if (namesImproved) {
                    applyFilesPayload(files);
                    updatePairedOptions();
                    syncMsaPickerSelection();
                    renderMsaPickerOptions(msaPickerSearch.value || "");
                }
                else {
                    state.files = files;
                    state.files.pairs = files.pairs;
                }
            }
            catch (_error) {
            }
            if (hasMissingPfamDisplayNames()) {
                void pollPfamMetadataIfNeeded();
            }
        }, PFAM_METADATA_POLL_DELAY_MS);
    }
    function syncMsaPickerSelection() {
        msaPickerSelection.innerHTML = "";
        const selected = currentMsaOption();
        if (!selected) {
            msaPickerSelection.textContent = "Select PFAM";
            return;
        }
        appendPfamLabel(msaPickerSelection, selected);
        const badges = pfamBadges(selected.stats);
        if (badges.length > 0) {
            msaPickerSelection.appendChild(createBadgeStrip(badges));
        }
    }
    function syncSelectionSettingsUi() {
        selectionSettingsToggle?.setAttribute("aria-expanded", String(state.selectionSettingsOpen));
        selectionSettingsPanel?.classList.toggle("hidden", !state.selectionSettingsOpen);
        if (selectionMinInterfaceSizeInput) {
            selectionMinInterfaceSizeInput.value = String(normalizeSelectionSettings(state.selectionSettingsDraft).minInterfaceSize);
        }
    }
    function renderMsaPickerOptions(filterText = "") {
        const query = filterText.trim().toLowerCase();
        msaPickerOptions.innerHTML = "";
        const options = (state.msaOptions || []).filter((option) => {
            const searchable = [option.pfamId, option.displayName]
                .map((value) => String(value || "").toLowerCase())
                .join(" ");
            if (query && !searchable.includes(query)) {
                return false;
            }
            return optionMatchesBadgeFilters(option);
        });
        if (options.length === 0) {
            const empty = document.createElement("div");
            empty.className = "msa-picker-option empty";
            empty.textContent = "No PFAM matches the current filter.";
            msaPickerOptions.appendChild(empty);
            return;
        }
        for (const option of options) {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "msa-picker-option";
            if (option.value === msaSelect.value) {
                button.classList.add("active");
            }
            button.dataset.value = option.value;
            appendPfamLabel(button, option);
            const badges = pfamBadges(option.stats);
            if (badges.length > 0) {
                button.appendChild(createBadgeStrip(badges));
            }
            button.addEventListener("click", () => {
                msaSelect.value = option.value;
                syncMsaSelectionInUrl(option.value);
                updatePairedOptions();
                syncMsaPickerSelection();
                closeMsaPicker();
                void loadCurrentSelection();
            });
            msaPickerOptions.appendChild(button);
        }
    }
    function openMsaPicker() {
        msaPickerMenu.classList.remove("hidden");
        msaPickerButton.setAttribute("aria-expanded", "true");
        msaPickerSearch.value = "";
        updateMsaFilterButtons();
        renderMsaPickerOptions("");
        window.setTimeout(() => {
            msaPickerSearch.focus();
            msaPickerSearch.select();
        }, 0);
    }
    function closeMsaPicker() {
        msaPickerMenu.classList.add("hidden");
        msaPickerButton.setAttribute("aria-expanded", "false");
    }
    function toggleMsaPicker() {
        if (msaPickerMenu.classList.contains("hidden")) {
            openMsaPicker();
        }
        else {
            closeMsaPicker();
        }
    }
    function updateMsaFilterButtons() {
        [...msaPickerFilters.querySelectorAll(".msa-filter-chip")].forEach((button) => {
            const metricKey = button.dataset.filterKey;
            const direction = button.dataset.filterDirection;
            const active = state.msaFilterState[metricKey]?.has(direction);
            button.classList.toggle("active", Boolean(active));
        });
    }
    function normalizeResidueIds(residueIds, alignmentLength) {
        const normalized = Array.isArray(residueIds)
            ? residueIds.map((value) => {
                if (value === null || value === undefined || value === "") {
                    return null;
                }
                const parsed = Number(value);
                return Number.isFinite(parsed) ? parsed : null;
            })
            : [];
        if (normalized.length >= alignmentLength) {
            return normalized.slice(0, alignmentLength);
        }
        return normalized.concat(new Array(alignmentLength - normalized.length).fill(null));
    }
    function normalizeInterfaceRows(rows, alignmentLength) {
        const normalizedLength = Math.max(0, Number(alignmentLength || 0));
        return (rows || []).map((row) => {
            const interfaceRowKey = String(row.interface_row_key || "");
            const proteinId = String(row.protein_id || "");
            const partnerDomain = String(row.partner_domain || "");
            const fullInteractionRowKey = String(row.row_key || interactionRowKey(interfaceRowKey, partnerDomain));
            const alignedSequenceRaw = String(row.aligned_sequence || "");
            const alignedSequence = alignedSequenceRaw.length < normalizedLength
                ? alignedSequenceRaw + "-".repeat(normalizedLength - alignedSequenceRaw.length)
                : alignedSequenceRaw;
            const effectiveLength = Math.max(normalizedLength, alignedSequence.length);
            const nextRow = {
                ...row,
                interface_row_key: interfaceRowKey,
                partner_domain: partnerDomain,
                row_key: fullInteractionRowKey,
                display_row_key: String(row.display_row_key || "") ||
                    (partnerDomain ? `${proteinId} | ${partnerDomain}` : proteinId || interfaceRowKey),
                aligned_sequence: alignedSequence,
                alignment_fragment_key: String(row.alignment_fragment_key || row.fragment_key || ""),
                has_alignment: row.has_alignment !== false,
            };
            nextRow.residueIds = normalizeResidueIds(row.residue_ids, effectiveLength);
            return nextRow;
        });
    }
    function computeVisibleColumns(rows, alignmentLength) {
        const visible = [];
        for (let column = 0; column < alignmentLength; column += 1) {
            let keep = false;
            for (const row of rows) {
                if (/^[A-Za-z]$/.test(row.aligned_sequence[column] || "")) {
                    keep = true;
                    break;
                }
            }
            if (keep) {
                visible.push(column);
            }
        }
        if (visible.length > 0) {
            return visible;
        }
        return Array.from({ length: alignmentLength }, (_value, index) => index);
    }
    function displayAlignmentLength() {
        return state.msa?.visible_columns?.length || state.msa?.alignment_length || 0;
    }
    function msaUrlValue(msaFile) {
        return interfaceFileStem(msaFile);
    }
    function syncMsaSelectionInUrl(msaFile) {
        const url = new URL(window.location.href);
        const value = msaUrlValue(msaFile);
        if (value) {
            url.searchParams.set("msa", value);
        }
        else {
            url.searchParams.delete("msa");
        }
        window.history.replaceState({}, "", url);
    }
    function msaFileFromUrl() {
        const url = new URL(window.location.href);
        const requested = String(url.searchParams.get("msa") || "").trim();
        if (!requested) {
            return "";
        }
        const normalizedRequested = interfaceFileStem(requested);
        const match = (state.msaOptions || []).find((option) => option.pfamId === normalizedRequested || interfaceFileStem(option.value) === normalizedRequested);
        return match?.value || "";
    }
    function normalizeFuzzyText(value) {
        return String(value || "")
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "");
    }
    function isFuzzySubsequenceMatch(target, query) {
        if (!query) {
            return true;
        }
        let queryIndex = 0;
        for (const character of target) {
            if (character === query[queryIndex]) {
                queryIndex += 1;
                if (queryIndex === query.length) {
                    return true;
                }
            }
        }
        return false;
    }
    function matchesRowSearch(rowKey, query = state.rowSearchQuery) {
        const rawQuery = String(query || "").trim().toLowerCase();
        if (!rawQuery) {
            return true;
        }
        const label = String(rowKey || "");
        const lowerLabel = label.toLowerCase();
        if (lowerLabel.includes(rawQuery)) {
            return true;
        }
        const condensedQuery = normalizeFuzzyText(rawQuery);
        if (!condensedQuery) {
            return true;
        }
        const condensedLabel = normalizeFuzzyText(label);
        return (condensedLabel.includes(condensedQuery) ||
            isFuzzySubsequenceMatch(condensedLabel, condensedQuery));
    }
    function originalColumnForDisplay(displayColumn) {
        if (!state.msa?.visible_columns) {
            return null;
        }
        return state.msa.visible_columns[displayColumn] ?? null;
    }
    function compareMsaClusterKeys(leftKey, rightKey) {
        return Number(leftKey) - Number(rightKey);
    }
    function rebuildMsaClusterCache() {
        const source = state.embeddingClustering?.points || null;
        if (source === cachedMsaClusterSource) {
            return;
        }
        cachedMsaClusterSource = source;
        cachedMsaRowClusterAssignments = new Map();
        cachedMsaRowClusterMemberships = new Map();
        cachedMsaClusterCounts = new Map();
        if (!source || source.length === 0) {
            return;
        }
        const countsByRow = new Map();
        for (const point of source) {
            const fullRowKey = interactionRowKey(point.row_key, point.partner_domain);
            const clusterLabel = Number(point.cluster_label);
            if (!fullRowKey || !Number.isFinite(clusterLabel)) {
                continue;
            }
            const clusterKey = String(clusterLabel);
            cachedMsaClusterCounts.set(clusterKey, (cachedMsaClusterCounts.get(clusterKey) || 0) + 1);
            let memberships = cachedMsaRowClusterMemberships.get(fullRowKey);
            if (!memberships) {
                memberships = new Set();
                cachedMsaRowClusterMemberships.set(fullRowKey, memberships);
            }
            memberships.add(clusterKey);
            let rowCounts = countsByRow.get(fullRowKey);
            if (!rowCounts) {
                rowCounts = new Map();
                countsByRow.set(fullRowKey, rowCounts);
            }
            rowCounts.set(clusterLabel, (rowCounts.get(clusterLabel) || 0) + 1);
        }
        for (const [rowKey, rowCounts] of countsByRow.entries()) {
            let bestClusterLabel = null;
            let bestCount = -1;
            for (const [clusterLabel, count] of rowCounts.entries()) {
                if (count > bestCount ||
                    (count === bestCount &&
                        (bestClusterLabel === null ||
                            (bestClusterLabel < 0 && clusterLabel >= 0) ||
                            ((bestClusterLabel < 0) === (clusterLabel < 0) &&
                                clusterLabel < bestClusterLabel)))) {
                    bestClusterLabel = clusterLabel;
                    bestCount = count;
                }
            }
            if (bestClusterLabel === null) {
                continue;
            }
            cachedMsaRowClusterAssignments.set(rowKey, String(bestClusterLabel));
        }
    }
    function rowClusterKey(rowKey) {
        rebuildMsaClusterCache();
        return cachedMsaRowClusterAssignments.get(String(rowKey)) || null;
    }
    function rowClusterMemberships(rowKey) {
        rebuildMsaClusterCache();
        return cachedMsaRowClusterMemberships.get(String(rowKey)) || null;
    }
    function allMsaClusterKeys() {
        rebuildMsaClusterCache();
        return [...cachedMsaClusterCounts.keys()].sort(compareMsaClusterKeys);
    }
    function visibleMsaClusterKeys() {
        const allKeys = allMsaClusterKeys();
        if (allKeys.length === 0) {
            return [];
        }
        if (state.msaVisibleClusters.size === 0) {
            state.msaVisibleClusters = new Set(allKeys);
            return allKeys;
        }
        const visible = allKeys.filter((key) => state.msaVisibleClusters.has(key));
        if (visible.length === 0) {
            state.msaVisibleClusters = new Set(allKeys);
            return allKeys;
        }
        return visible;
    }
    function clusterLabelForKey(clusterKey) {
        return embeddingClusterLabel(clusterKey);
    }
    function clusterColorForKey(clusterKey) {
        return embeddingClusterColor(clusterKey);
    }
    function renderMsaClusterLegend() {
        if (!msaClusterLegend) {
            return;
        }
        if (!state.msa || state.embeddingClusteringLoading) {
            msaClusterLegend.classList.toggle("hidden", !(state.embeddingClusteringLoading && state.msa));
            if (!msaClusterLegend.classList.contains("hidden")) {
                msaClusterLegend.innerHTML =
                    '<div class="msa-cluster-legend-header"><span class="msa-cluster-legend-title">Clusters</span><span>Loading clustering...</span></div>';
            }
            else {
                msaClusterLegend.innerHTML = "";
            }
            return;
        }
        const clusterKeys = allMsaClusterKeys();
        const hasClustering = (state.embeddingClustering?.points || []).length > 0;
        if (!hasClustering || clusterKeys.length === 0) {
            msaClusterLegend.classList.add("hidden");
            msaClusterLegend.innerHTML = "";
            return;
        }
        const visibleKeys = new Set(visibleMsaClusterKeys());
        const legendEntries = clusterKeys
            .map((clusterKey) => {
            const active = visibleKeys.has(clusterKey);
            const count = cachedMsaClusterCounts.get(clusterKey) || 0;
            return `
          <button
            type="button"
            class="msa-cluster-chip ${active ? "active" : "inactive"}"
            data-msa-cluster-label="${clusterKey}"
            aria-pressed="${active}"
            title="${clusterLabelForKey(clusterKey)}"
          >
            <span class="representative-partner-filter-swatch" style="background:${clusterColorForKey(clusterKey)};"></span>
            <span class="msa-cluster-chip-label">${clusterLabelForKey(clusterKey)}</span>
            <span class="msa-cluster-chip-value">${count}</span>
          </button>
        `;
        })
            .join("");
        msaClusterLegend.innerHTML = `
      <div class="msa-cluster-legend-header">
        <span class="msa-cluster-legend-title">MSA Cluster Filter</span>
        <span>${visibleKeys.size}/${clusterKeys.length} selected</span>
      </div>
      <div class="msa-cluster-legend-list">${legendEntries}</div>
    `;
        msaClusterLegend.classList.remove("hidden");
    }
    function updateFilteredRows() {
        if (!state.msa) {
            state.filteredRowIndexes = [];
            state.visibleRows = [];
            return;
        }
        const visibleClusterKeys = new Set(visibleMsaClusterKeys());
        const useClusterFilter = visibleClusterKeys.size > 0 && (state.embeddingClustering?.points || []).length > 0;
        const indexes = [];
        state.msa.rows.forEach((row, index) => {
            if (!matchesRowSearch(row.display_row_key || row.row_key)) {
                return;
            }
            const overlay = overlayStateForRow(row);
            if (!overlay || overlay.interface.size === 0) {
                return;
            }
            if (useClusterFilter) {
                const clusterMemberships = rowClusterMemberships(row.row_key);
                if (!clusterMemberships) {
                    return;
                }
                let isVisibleInCluster = false;
                for (const clusterKey of clusterMemberships) {
                    if (visibleClusterKeys.has(clusterKey)) {
                        isVisibleInCluster = true;
                        break;
                    }
                }
                if (!isVisibleInCluster) {
                    return;
                }
            }
            indexes.push(index);
        });
        state.filteredRowIndexes = indexes;
    }
    function resizeCanvases() {
        const headerWidth = gridScroll.clientWidth;
        const bodyHeight = gridScroll.clientHeight;
        headerCanvas.width = headerWidth * window.devicePixelRatio;
        headerCanvas.height = HEADER_HEIGHT * window.devicePixelRatio;
        headerCanvas.style.width = `${headerWidth}px`;
        headerCanvas.style.height = `${HEADER_HEIGHT}px`;
        labelsCanvas.width = LABEL_WIDTH * window.devicePixelRatio;
        labelsCanvas.height = bodyHeight * window.devicePixelRatio;
        labelsCanvas.style.width = `${LABEL_WIDTH}px`;
        labelsCanvas.style.height = `${bodyHeight}px`;
        gridCanvas.width = headerWidth * window.devicePixelRatio;
        gridCanvas.height = bodyHeight * window.devicePixelRatio;
        gridCanvas.style.width = `${headerWidth}px`;
        gridCanvas.style.height = `${bodyHeight}px`;
        const totalWidth = displayAlignmentLength() * CELL_WIDTH;
        const totalHeight = state.filteredRowIndexes.length * ROW_HEIGHT;
        gridSpacer.style.width = `${totalWidth}px`;
        gridSpacer.style.height = `${totalHeight}px`;
    }
    function drawHeader() {
        const ctx = headerCanvas.getContext("2d");
        const dpr = window.devicePixelRatio;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, headerCanvas.width, headerCanvas.height);
        ctx.fillStyle = "#f0e8d6";
        ctx.fillRect(0, 0, headerCanvas.clientWidth, HEADER_HEIGHT);
        ctx.strokeStyle = "#d7ccb3";
        ctx.beginPath();
        ctx.moveTo(0, HEADER_HEIGHT - 0.5);
        ctx.lineTo(headerCanvas.clientWidth, HEADER_HEIGHT - 0.5);
        ctx.stroke();
        if (!state.msa) {
            return;
        }
        const scrollLeft = gridScroll.scrollLeft;
        const firstCol = Math.floor(scrollLeft / CELL_WIDTH);
        const visibleCols = Math.ceil(gridScroll.clientWidth / CELL_WIDTH) + 2;
        const labelStep = CELL_WIDTH >= 16 ? 5 : 10;
        const conservation = activeConservationVector();
        const displayedColumnCount = displayAlignmentLength();
        ctx.font = '11px "IBM Plex Mono", monospace';
        ctx.fillStyle = "#6f6658";
        for (let displayCol = firstCol; displayCol < Math.min(displayedColumnCount, firstCol + visibleCols); displayCol += 1) {
            const originalCol = originalColumnForDisplay(displayCol);
            if (originalCol === null) {
                continue;
            }
            const x = displayCol * CELL_WIDTH - scrollLeft;
            const conservedness = conservation[originalCol];
            if (typeof conservedness === "number") {
                ctx.fillStyle = conservationColor(conservedness);
                ctx.fillRect(x, 22, CELL_WIDTH, HEADER_HEIGHT - 22);
            }
            if (displayCol % labelStep === 0) {
                ctx.fillStyle = "#6f6658";
                ctx.fillText(String(originalCol), x + 2, 15);
            }
            ctx.strokeStyle = originalCol === state.hover?.column ? "#2d6a4f" : "rgba(0,0,0,0.06)";
            ctx.beginPath();
            ctx.moveTo(x + 0.5, 18);
            ctx.lineTo(x + 0.5, HEADER_HEIGHT);
            ctx.stroke();
        }
    }
    function drawLabels(firstRowIndex, visibleRowCount, scrollTop) {
        const ctx = labelsCanvas.getContext("2d");
        const dpr = window.devicePixelRatio;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, labelsCanvas.clientWidth, labelsCanvas.clientHeight);
        ctx.fillStyle = "#fcfaf5";
        ctx.fillRect(0, 0, labelsCanvas.clientWidth, labelsCanvas.clientHeight);
        ctx.font = TEXT_FONT;
        ctx.textBaseline = "middle";
        for (let i = 0; i < visibleRowCount; i += 1) {
            const filteredIndex = firstRowIndex + i;
            if (filteredIndex >= state.filteredRowIndexes.length) {
                break;
            }
            const rowIndex = state.filteredRowIndexes[filteredIndex];
            const row = state.msa.rows[rowIndex];
            const y = filteredIndex * ROW_HEIGHT - scrollTop;
            const isHovered = state.hover?.filteredRowIndex === filteredIndex;
            const isSelected = state.selectedRowKey === row.row_key;
            const isRepresentative = state.representativeRowKey === row.row_key;
            if (isHovered) {
                ctx.fillStyle = "rgba(45, 106, 79, 0.12)";
                ctx.fillRect(0, y, LABEL_WIDTH, ROW_HEIGHT);
            }
            if (isSelected) {
                ctx.fillStyle = "rgba(127, 82, 40, 0.18)";
                ctx.fillRect(0, y, LABEL_WIDTH, ROW_HEIGHT);
            }
            if (isRepresentative) {
                ctx.fillStyle = "#d49a38";
                ctx.beginPath();
                ctx.arc(12, y + ROW_HEIGHT / 2, 4, 0, Math.PI * 2);
                ctx.fill();
                ctx.fillStyle = "#fff9ee";
                ctx.beginPath();
                ctx.arc(12, y + ROW_HEIGHT / 2, 1.6, 0, Math.PI * 2);
                ctx.fill();
            }
            const clusterKey = rowClusterKey(row.row_key);
            const baseColor = clusterKey === null ? "#2e261d" : clusterColorForKey(clusterKey);
            let currentX = 22;
            for (const segment of rowLabelSegments(row, baseColor)) {
                ctx.fillStyle = segment.color;
                ctx.fillText(segment.text, currentX, y + ROW_HEIGHT / 2);
                currentX += ctx.measureText(segment.text).width;
            }
        }
    }
    function drawGrid() {
        const ctx = gridCanvas.getContext("2d");
        const dpr = window.devicePixelRatio;
        const viewportWidth = gridScroll.clientWidth;
        const viewportHeight = gridScroll.clientHeight;
        const scrollLeft = gridScroll.scrollLeft;
        const scrollTop = gridScroll.scrollTop;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, viewportWidth, viewportHeight);
        ctx.fillStyle = "#ffffff";
        ctx.fillRect(0, 0, viewportWidth, viewportHeight);
        if (!state.msa) {
            drawHeader();
            return;
        }
        const firstCol = Math.floor(scrollLeft / CELL_WIDTH);
        const visibleCols = Math.ceil(viewportWidth / CELL_WIDTH) + 2;
        const displayedColumnCount = displayAlignmentLength();
        const firstRowIndex = Math.floor(scrollTop / ROW_HEIGHT);
        const visibleRowCount = Math.ceil(viewportHeight / ROW_HEIGHT) + 2;
        ctx.font = TEXT_FONT;
        ctx.textBaseline = "middle";
        ctx.textAlign = "center";
        for (let i = 0; i < visibleRowCount; i += 1) {
            const filteredIndex = firstRowIndex + i;
            if (filteredIndex >= state.filteredRowIndexes.length) {
                break;
            }
            const rowIndex = state.filteredRowIndexes[filteredIndex];
            const row = state.msa.rows[rowIndex];
            const y = filteredIndex * ROW_HEIGHT - scrollTop;
            const overlay = overlayStateForRow(row);
            const isHoveredRow = state.hover?.filteredRowIndex === filteredIndex;
            const isSelectedRow = state.selectedRowKey === row.row_key;
            if (isHoveredRow) {
                ctx.fillStyle = "rgba(45, 106, 79, 0.08)";
                ctx.fillRect(0, y, viewportWidth, ROW_HEIGHT);
            }
            if (isSelectedRow) {
                ctx.fillStyle = "rgba(127, 82, 40, 0.11)";
                ctx.fillRect(0, y, viewportWidth, ROW_HEIGHT);
            }
            for (let displayCol = firstCol; displayCol < Math.min(displayedColumnCount, firstCol + visibleCols); displayCol += 1) {
                const originalCol = originalColumnForDisplay(displayCol);
                if (originalCol === null) {
                    continue;
                }
                const x = displayCol * CELL_WIDTH - scrollLeft;
                const char = row.aligned_sequence[originalCol] || " ";
                const isHoveredCol = state.hover?.column === originalCol;
                if (overlay?.surface.has(originalCol)) {
                    ctx.fillStyle = "rgba(215, 168, 76, 0.5)";
                    ctx.fillRect(x, y, CELL_WIDTH, ROW_HEIGHT);
                }
                if (overlay?.interface.has(originalCol)) {
                    ctx.fillStyle = "rgba(188, 64, 45, 0.72)";
                    ctx.fillRect(x, y, CELL_WIDTH, ROW_HEIGHT);
                }
                if (isHoveredCol) {
                    ctx.strokeStyle = "#2d6a4f";
                    ctx.strokeRect(x + 0.5, y + 0.5, CELL_WIDTH - 1, ROW_HEIGHT - 1);
                }
                ctx.fillStyle = /^[A-Za-z]$/.test(char) ? "#1f1a14" : "#a7a092";
                ctx.fillText(char, x + CELL_WIDTH / 2, y + ROW_HEIGHT / 2 + 0.5);
            }
        }
        drawHeader();
        drawLabels(firstRowIndex, visibleRowCount, scrollTop);
    }
    function updateStats() {
        if (state.msa && !state.msaRowsComplete && state.msaRowsTotal > state.msaRowsLoaded) {
            rowCount.textContent = `${state.filteredRowIndexes.length} shown (${state.msaRowsLoaded}/${state.msaRowsTotal} loaded)`;
        }
        else {
            rowCount.textContent = String(state.filteredRowIndexes.length);
        }
        columnCount.textContent = String(displayAlignmentLength());
    }
    function fullColumnIndexes(alignmentLength) {
        return Array.from({ length: Math.max(0, Number(alignmentLength || 0)) }, (_value, index) => index);
    }
    function interfacePayloadUrl(filename, options = {}) {
        const params = new URLSearchParams({ file: filename });
        appendSelectionSettingsToParams(params, state.selectionSettings);
        if (options.rowOffset !== undefined) {
            params.set("row_offset", String(options.rowOffset));
        }
        if (options.rowLimit !== undefined && options.rowLimit !== null) {
            params.set("row_limit", String(options.rowLimit));
        }
        if (options.includeData !== undefined) {
            params.set("include_data", options.includeData ? "1" : "0");
        }
        if (options.includeCleanColumnIdentity !== undefined) {
            params.set("include_clean_column_identity", options.includeCleanColumnIdentity ? "1" : "0");
        }
        if (options.dataOffset !== undefined) {
            params.set("data_offset", String(options.dataOffset));
        }
        if (options.dataLimit !== undefined && options.dataLimit !== null) {
            params.set("data_limit", String(options.dataLimit));
        }
        return `/api/interface?${params.toString()}`;
    }
    function partnerCountsFromPayload(payload, fallbackCounts = new Map()) {
        const counts = new Map(fallbackCounts);
        for (const [partnerDomain, count] of Object.entries(payload.interface_partner_counts || {})) {
            counts.set(partnerDomain, Number(count || 0));
        }
        return counts;
    }
    function partnerDomainsFromPayload(payload, maps) {
        const payloadDomains = Array.isArray(payload.interface_partner_domains)
            ? payload.interface_partner_domains.map((partner) => String(partner || ""))
            : [];
        return (payloadDomains.length > 0 ? payloadDomains : maps.partnerDomains || []).sort();
    }
    function mergeInterfaceOverlayPayload(interfaceState, overlayPayload) {
        if (!interfaceState || !overlayPayload) {
            return;
        }
        if (!interfaceState.data) {
            interfaceState.data = {};
        }
        const existingPartners = new Set(interfaceState.partnerDomains || []);
        for (const [partnerDomain, rowsByKey] of Object.entries(overlayPayload || {})) {
            if (!rowsByKey || typeof rowsByKey !== "object") {
                continue;
            }
            existingPartners.add(partnerDomain);
            if (!interfaceState.data[partnerDomain]) {
                interfaceState.data[partnerDomain] = {};
            }
            for (const [rowKey, payload] of Object.entries(rowsByKey)) {
                if (interfaceState.data[partnerDomain][rowKey]) {
                    continue;
                }
                interfaceState.data[partnerDomain][rowKey] = payload;
                const interactionKey = interactionRowKey(rowKey, partnerDomain);
                const partnerState = {
                    interface: new Set(payload.interface_msa_columns_a || []),
                    surface: new Set(payload.surface_msa_columns_a || []),
                };
                let rowState = interfaceState.overlayByRow.get(rowKey);
                if (!rowState) {
                    rowState = {
                        all: { interface: new Set(), surface: new Set() },
                        byPartner: new Map(),
                    };
                    interfaceState.overlayByRow.set(rowKey, rowState);
                }
                rowState.byPartner.set(partnerDomain, partnerState);
                interfaceState.overlayByInteractionRow.set(interactionKey, {
                    all: partnerState,
                    byPartner: new Map([[partnerDomain, partnerState]]),
                });
                const partnerColumnStats = interfaceState.partnerColumnStats.get(partnerDomain) || {
                    denominator: 0,
                    columnCounts: new Map(),
                };
                if (partnerState.interface.size > 0) {
                    partnerColumnStats.denominator += 1;
                    for (const col of partnerState.interface) {
                        partnerColumnStats.columnCounts.set(col, (partnerColumnStats.columnCounts.get(col) || 0) + 1);
                    }
                }
                interfaceState.partnerColumnStats.set(partnerDomain, partnerColumnStats);
                const hadRowInterface = rowState.all.interface.size > 0;
                for (const col of partnerState.interface) {
                    if (!rowState.all.interface.has(col)) {
                        rowState.all.interface.add(col);
                        interfaceState.allColumnStats.columnCounts.set(col, (interfaceState.allColumnStats.columnCounts.get(col) || 0) + 1);
                    }
                }
                for (const col of partnerState.surface) {
                    rowState.all.surface.add(col);
                }
                if (!hadRowInterface && rowState.all.interface.size > 0) {
                    interfaceState.allColumnStats.denominator += 1;
                }
            }
        }
        interfaceState.partnerDomains = [...existingPartners].sort();
    }
    function applyInterfaceOverlayPayload(payload) {
        if (!state.interface || !payload?.data) {
            return;
        }
        mergeInterfaceOverlayPayload(state.interface, payload.data);
        state.interface.overlayRowsTotal = Number(payload.data_row_count || state.interface.overlayRowsTotal || state.msaRowsTotal || 0);
        state.interface.overlayRowsLoaded = Math.max(Number(state.interface.overlayRowsLoaded || 0), Number(payload.data_offset || 0) + Number(payload.data_loaded || 0));
        state.interface.overlayComplete =
            Boolean(payload.data_complete) ||
                (state.interface.overlayRowsTotal > 0 &&
                    state.interface.overlayRowsLoaded >= state.interface.overlayRowsTotal);
    }
    function startMsaRowStreamIfNeeded() {
        if (!state.msa ||
            !currentMsaStreamFilename ||
            state.msaRowsLoading ||
            state.msaRowsComplete ||
            state.msaRowsLoaded >= state.msaRowsTotal ||
            activeMsaPanelView() !== "msa") {
            return;
        }
        const requestId = state.msaRowsRequestId;
        void loadRemainingInterfaceRows(currentMsaStreamFilename, requestId, state.msaRowsLoaded, state.msaRowsTotal).catch((error) => {
            if (requestId === state.msaRowsRequestId) {
                state.msaRowsLoading = false;
                state.msaRowsComplete = false;
                appStatus.textContent = error.message;
                render();
            }
        });
    }
    function applyMsaRowsPayload(payload, rows, { replace = false } = {}) {
        const rowOffset = Number(payload.row_offset || 0);
        const rowCount = Number(payload.row_count || state.msa.row_count || rows.length);
        const nextRows = replace ? rows : state.msa.rows.concat(rows);
        state.msa.rows = nextRows;
        state.msa.row_count = rowCount;
        state.msaRowsLoaded = nextRows.length;
        state.msaRowsTotal = rowCount;
        state.msaRowsComplete = Boolean(payload.rows_complete) || nextRows.length >= rowCount;
        state.msa.rows_complete = state.msaRowsComplete;
        state.msa.loaded_row_count = state.msaRowsLoaded;
        state.msa.visible_columns = state.msa.visible_columns || fullColumnIndexes(state.msa.alignment_length);
        state.msa.hidden_gap_only_columns = Math.max(0, state.msa.alignment_length - state.msa.visible_columns.length);
        if (!replace && rowOffset !== nextRows.length - rows.length) {
            console.debug("[msa-stream] non-contiguous row chunk", {
                expectedOffset: nextRows.length - rows.length,
                rowOffset,
            });
        }
    }
    async function loadRemainingInterfaceRows(filename, requestId, startOffset, totalRows) {
        let nextOffset = Math.max(0, Number(startOffset || 0));
        state.msaRowsLoading = nextOffset < totalRows;
        while (state.msa &&
            requestId === state.msaRowsRequestId &&
            nextOffset < totalRows) {
            if (activeMsaPanelView() !== "msa") {
                state.msaRowsLoading = false;
                return;
            }
            const payload = await fetchJson(interfacePayloadUrl(filename, {
                rowOffset: nextOffset,
                rowLimit: MSA_ROW_CHUNK_SIZE,
                includeData: true,
                dataOffset: nextOffset,
                dataLimit: MSA_ROW_CHUNK_SIZE,
                includeCleanColumnIdentity: false,
            }));
            if (!state.msa || requestId !== state.msaRowsRequestId) {
                return;
            }
            const rows = normalizeInterfaceRows(payload.rows || [], payload.alignment_length);
            if (rows.length === 0) {
                break;
            }
            applyMsaRowsPayload(payload, rows);
            applyInterfaceOverlayPayload(payload);
            nextOffset = state.msaRowsLoaded;
            appStatus.textContent = `Loaded ${nextOffset} of ${totalRows} alignment rows`;
            render();
            await new Promise((resolve) => window.requestAnimationFrame(resolve));
        }
        if (state.msa && requestId === state.msaRowsRequestId) {
            state.msaRowsLoading = false;
            state.msaRowsComplete = nextOffset >= totalRows;
            state.msa.rows_complete = state.msaRowsComplete;
            if (state.msaRowsComplete) {
                appStatus.textContent = `Loaded ${state.msaRowsLoaded} alignment rows`;
            }
            render();
        }
    }
    async function loadInterface(filename) {
        state.interface = null;
        state.msa = null;
        state.selectedRowKey = null;
        state.selectedRowSnapshot = null;
        state.msaRowsRequestId += 1;
        state.msaRowsLoading = false;
        state.msaRowsLoaded = 0;
        state.msaRowsTotal = 0;
        state.msaRowsComplete = false;
        currentMsaStreamFilename = "";
        currentMsaStreamFilename = filename || "";
        state.embedding = null;
        state.embeddingClustering = null;
        state.columnsChart = null;
        state.columnsChartKey = null;
        closeClusterCompareModal();
        state.embeddingHoverRowKey = null;
        state.embeddingMemberSelection = null;
        state.embeddingProjectedPoints = [];
        state.embeddingRequestId += 1;
        state.embeddingLoading = false;
        state.embeddingLoadingKey = null;
        state.embeddingPromise = null;
        state.embeddingClusteringRequestId += 1;
        state.embeddingClusteringLoading = false;
        state.embeddingClusteringLoadingKey = null;
        state.embeddingClusteringPromise = null;
        state.columnsVisibleClusters = new Set();
        syncEmbeddingLoadingUi();
        if (!filename) {
            return;
        }
        const requestId = state.msaRowsRequestId;
        const payload = await fetchJson(interfacePayloadUrl(filename, {
            rowOffset: 0,
            rowLimit: INITIAL_MSA_ROW_LIMIT,
            includeData: true,
            dataOffset: 0,
            dataLimit: INITIAL_MSA_ROW_LIMIT,
            includeCleanColumnIdentity: true,
        }));
        if (requestId !== state.msaRowsRequestId) {
            return;
        }
        setLoading(65, "Loading alignment", `Preparing rows from ${filename}`);
        const rows = normalizeInterfaceRows(payload.rows, payload.alignment_length);
        const alignmentLength = Number(payload.alignment_length || 0);
        const totalRows = Number(payload.row_count || rows.length);
        state.msa = {
            file: payload.file,
            pfam_id: payload.pfam_id,
            alignment_length: alignmentLength,
            row_count: totalRows,
            clean_column_identity: payload.clean_column_identity || [],
            rows,
            loaded_row_count: rows.length,
            rows_complete: Boolean(payload.rows_complete) || rows.length >= totalRows,
        };
        state.msa.visible_columns = fullColumnIndexes(state.msa.alignment_length);
        state.msa.hidden_gap_only_columns = Math.max(0, state.msa.alignment_length - state.msa.visible_columns.length);
        state.msaRowsLoaded = rows.length;
        state.msaRowsTotal = totalRows;
        state.msaRowsComplete = state.msa.rows_complete;
        syncColumnLegends();
        setLoading(78, "Loading interface", `Preparing overlays for ${filename}`);
        const maps = buildOverlayMaps(payload.data || {});
        const partnerDomains = partnerDomainsFromPayload(payload, maps);
        const partnerInterfaceCounts = partnerCountsFromPayload(payload, maps.partnerInterfaceCounts);
        state.interface = {
            ...payload,
            ...maps,
            partnerDomains,
            partnerInterfaceCounts,
            overlayRowsLoaded: Number(payload.data_loaded || rows.length),
            overlayRowsTotal: Number(payload.data_row_count || totalRows),
            overlayComplete: Boolean(payload.data_complete) || rows.length >= totalRows,
            partnerColors: buildPartnerColorMap(partnerDomains),
        };
        resetEmbeddingPartnerSelection();
        resetEmbeddingClusterSelection();
        resetColumnsClusterSelection();
        resetRepresentativePartnerSelection();
        resetRepresentativeClusterSelection();
        renderRepresentativePartnerFilter();
        renderEmbeddingLegend();
        renderColumnsClusterLegend();
        startMsaRowStreamIfNeeded();
    }
    async function refreshData() {
        if (!msaSelect.value || !interfaceSelect.value) {
            return;
        }
        setLoading(10, "Loading selection", "Fetching interface data");
        state.representativeAnchorRowKey = null;
        state.representativeRenderedRowKey = null;
        state.representativeStructure = null;
        state.representativeRequestId += 1;
        state.structureAnchorRowKey = null;
        state.structureRenderedRowKey = null;
        state.structureRenderedModelKey = null;
        state.structureRequestId += 1;
        await loadInterface(interfaceSelect.value);
        updatePartnerOptions();
        setLoading(85, "Loading selection", "Finding representative row");
        state.hover = null;
        updateFilteredRows();
        if (activeMsaPanelView() === "embeddings") {
            void ensureEmbeddingDataLoaded();
            void ensureEmbeddingClusteringLoaded();
        }
        else if (activeMsaPanelView() === "columns") {
            void ensureEmbeddingClusteringLoaded().then(() => {
                if (state.msa && activeMsaPanelView() === "columns") {
                    render();
                }
            });
        }
        render();
        await new Promise((resolve) => window.requestAnimationFrame(() => window.requestAnimationFrame(resolve)));
        render();
        setLoading(96, "Loading selection", "Loading representative structure");
        await refreshRepresentativeSelection("No representative row found.");
        setLoading(100, "Loaded", interfaceSelect.value);
        window.setTimeout(hideLoading, 250);
    }
    async function loadCurrentSelection() {
        syncMsaSelectionInUrl(msaSelect.value);
        if (!msaSelect.value) {
            clearPfamInfoState();
            clearViewer();
            return;
        }
        void ensurePfamInfoLoaded();
        if (!interfaceSelect.value) {
            clearViewer();
            return;
        }
        clearViewer();
        try {
            await refreshData();
        }
        catch (error) {
            loadingPanel.classList.remove("hidden");
            loadingLabel.textContent = "Load failed";
            loadingDetail.textContent = error.message;
            progressBar.style.width = "100%";
        }
    }
    function render() {
        updateFilteredRows();
        renderMsaClusterLegend();
        renderColumnsClusterLegend();
        updateStats();
        renderInfoPanel();
        syncMsaPanelView();
        syncPaneHeights();
        if (activeMsaPanelView() === "info") {
        }
        else if (activeMsaPanelView() === "msa") {
            resizeCanvases();
            drawGrid();
        }
        else if (activeMsaPanelView() === "embeddings") {
            resizeEmbeddingCanvas();
            renderEmbeddingPlot();
        }
        else {
            resizeColumnsCanvas();
            renderColumnsChart();
        }
        setDetails(null);
        startMsaRowStreamIfNeeded();
    }
    function syncLayout() {
        syncPaneHeights();
        if (state.msa && activeMsaPanelView() === "msa") {
            resizeCanvases();
            drawGrid();
        }
        if (activeMsaPanelView() === "embeddings") {
            resizeEmbeddingCanvas();
            renderEmbeddingPlot();
        }
        if (activeMsaPanelView() === "columns") {
            resizeColumnsCanvas();
            renderColumnsChart();
        }
        if (state.representativeViewer) {
            state.representativeViewer.resize();
            state.representativeViewer.render();
        }
        if (state.structureViewer && !structureModal.classList.contains("hidden")) {
            state.structureViewer.resize();
            state.structureViewer.render();
        }
        if (!elements.clusterCompareModal.classList.contains("hidden")) {
            resizeClusterCompareViewers();
        }
    }
    function scheduleLayoutSync() {
        if (layoutSyncScheduled) {
            return;
        }
        layoutSyncScheduled = true;
        window.requestAnimationFrame(() => {
            layoutSyncScheduled = false;
            syncLayout();
        });
    }
    function clearViewer() {
        closeClusterCompareModal();
        state.msa = null;
        state.interface = null;
        state.selectedRowKey = null;
        state.selectedRowSnapshot = null;
        state.msaRowsRequestId += 1;
        state.msaRowsLoading = false;
        state.msaRowsLoaded = 0;
        state.msaRowsTotal = 0;
        state.msaRowsComplete = false;
        state.embedding = null;
        state.columnsChart = null;
        state.columnsChartKey = null;
        state.embeddingClustering = null;
        state.embeddingHoverRowKey = null;
        state.embeddingMemberSelection = null;
        state.embeddingProjectedPoints = [];
        state.embeddingDrag = null;
        state.embeddingRequestId += 1;
        state.embeddingLoading = false;
        state.embeddingLoadingKey = null;
        state.embeddingPromise = null;
        state.embeddingClusteringRequestId += 1;
        state.embeddingClusteringLoading = false;
        state.embeddingClusteringLoadingKey = null;
        state.embeddingClusteringPromise = null;
        state.embeddingSettingsOpen = false;
        state.selectionSettingsOpen = false;
        state.selectionSettingsDraft = {
            ...state.selectionSettings,
        };
        state.embeddingSettingsSection = "clustering";
        state.embeddingColorMode = "cluster";
        state.embeddingVisiblePartners = new Set();
        state.embeddingVisibleClusters = new Set();
        state.columnsVisibleClusters = new Set();
        state.msaVisibleClusters = new Set();
        state.embeddingSettings = { ...DEFAULT_EMBEDDING_SETTINGS };
        state.embeddingSettingsDraft = { ...DEFAULT_EMBEDDING_SETTINGS };
        state.embeddingClusteringSettings = { ...DEFAULT_CLUSTERING_SETTINGS };
        state.embeddingClusteringSettingsDraft = { ...DEFAULT_CLUSTERING_SETTINGS };
        state.embeddingHierarchicalTargetMemory = {
            nClusters: String(DEFAULT_CLUSTERING_SETTINGS.nClusters),
            distanceThreshold: String(DEFAULT_CLUSTERING_SETTINGS.distanceThreshold),
        };
        state.selectedPartner = "__all__";
        state.selectedRowKey = null;
        state.selectedRowSnapshot = null;
        state.representativeRowKey = null;
        state.representativeAnchorRowKey = null;
        state.representativeVisiblePartners = new Set();
        state.representativeVisibleClusters = new Set();
        state.representativeHoveredClusterLabel = null;
        state.representativeRenderedRowKey = null;
        state.representativeStructure = null;
        state.representativeRequestId = 0;
        state.representativePointer = null;
        state.embeddingView = {
            yaw: -0.7,
            pitch: 0.45,
            zoom: 1.0,
        };
        state.structureData = null;
        state.structureAnchorRowKey = null;
        state.structureRenderedRowKey = null;
        state.structureRenderedModelKey = null;
        state.structureRequestId = 0;
        state.structureColumnView = false;
        state.hover = null;
        closeStructureModal();
        updatePartnerOptions();
        syncRepresentativeLensControls();
        renderRepresentativePartnerFilter();
        renderEmbeddingLegend();
        renderColumnsClusterLegend();
        renderMsaClusterLegend();
        updateSelectedRowUi();
        resetRepresentativePanel();
        resetStructurePanel();
        setEmbeddingInfo(`3D ${defaultPointMethodLabel()} points on ${embeddingDistanceLabel(DEFAULT_EMBEDDING_SETTINGS.distance)} input. Drag to rotate.`);
        setColumnsInfo("Stacked per-column cluster interaction profile.");
        syncEmbeddingLoadingUi();
        syncEmbeddingSettingsUi();
        syncSelectionSettingsUi();
        syncColumnLegends();
        render();
    }
    function updatePairedOptions() {
        const selectedMsa = msaSelect.value;
        const matchingPair = state.files.pairs.find((pair) => pair.msaFile === selectedMsa);
        interfaceSelect.value = matchingPair ? matchingPair.interfaceFile : "";
    }
    function updateSelectedRowUi() {
        const row = getSelectedRow();
        if (!row) {
            selectedRowCopy.textContent = "Select a row in the alignment.";
            loadStructureButton.disabled = true;
            return;
        }
        selectedRowCopy.textContent = `Selected row: ${row.display_row_key || row.row_key}`;
        loadStructureButton.disabled = !interfaceSelect.value;
    }
    function getSelectedRow() {
        if (!state.msa || !state.selectedRowKey) {
            return null;
        }
        return (state.msa.rows.find((row) => row.row_key === state.selectedRowKey) ||
            (state.selectedRowSnapshot?.row_key === state.selectedRowKey ? state.selectedRowSnapshot : null));
    }
    function getRowByKey(rowKey) {
        if (!state.msa) {
            return null;
        }
        return state.msa.rows.find((row) => row.row_key === rowKey) || null;
    }
    function syntheticStructureRowFromKey(rowKey) {
        const parsed = parseInteractionRowKey(rowKey);
        const interfaceRowKey = parsed.interfaceRowKey || String(rowKey || "");
        const partnerDomain = parsed.partnerDomain || "";
        const proteinId = parsed.proteinId || "";
        const fragmentKey = parsed.fragmentKey || "";
        const fullRowKey = interactionRowKey(interfaceRowKey, partnerDomain);
        if (!interfaceRowKey || !partnerDomain || !proteinId || !fragmentKey) {
            return null;
        }
        return {
            interface_row_key: interfaceRowKey,
            partner_domain: partnerDomain,
            row_key: fullRowKey,
            display_row_key: `${proteinId} | ${partnerDomain}`,
            protein_id: proteinId,
            fragment_key: fragmentKey,
            alignment_fragment_key: fragmentKey,
            partner_fragment_key: parsed.partnerFragmentKey || "",
            aligned_sequence: "",
            residueIds: [],
            has_alignment: false,
            synthetic: true,
        };
    }
    function clearEmbeddingMemberSelectionUnlessSelected(rowKey) {
        const members = state.embeddingMemberSelection?.members || [];
        if (members.length > 1 &&
            members.some((member) => interactionRowKey(member.row_key, member.partner_domain) === rowKey)) {
            return;
        }
        state.embeddingMemberSelection = null;
        syncEmbeddingMemberControls?.();
    }
    function selectFilteredRow(filteredRowIndex) {
        if (!state.msa) {
            return null;
        }
        if (filteredRowIndex < 0 || filteredRowIndex >= state.filteredRowIndexes.length) {
            return null;
        }
        const rowIndex = state.filteredRowIndexes[filteredRowIndex];
        const row = state.msa.rows[rowIndex];
        state.selectedRowKey = row.row_key;
        state.selectedRowSnapshot = row;
        clearEmbeddingMemberSelectionUnlessSelected(row.row_key);
        updateSelectedRowUi();
        resetStructurePanel("Click a row name or use the button to open the structure.");
        drawGrid();
        return row;
    }
    function selectRowByKey(rowKey) {
        if (!state.msa || !rowKey) {
            return null;
        }
        const row = state.msa.rows.find((entry) => entry.row_key === rowKey) || null;
        const selectedRow = row || syntheticStructureRowFromKey(rowKey);
        if (!selectedRow) {
            return null;
        }
        state.selectedRowKey = selectedRow.row_key;
        state.selectedRowSnapshot = selectedRow;
        clearEmbeddingMemberSelectionUnlessSelected(selectedRow.row_key);
        updateSelectedRowUi();
        resetStructurePanel("Click a row name or use the button to open the structure.");
        if (activeMsaPanelView() === "msa") {
            drawGrid();
        }
        else {
            renderEmbeddingPlot();
        }
        return selectedRow;
    }
    function setDetails(payload) {
        const values = payload || {
            row: "-",
            protein: "-",
            fragment: "-",
            column: "-",
            conservedness: "-",
            residue: "-",
            residueId: "-",
            state: "-",
        };
        const items = [
            values.row,
            values.protein,
            values.fragment,
            values.column,
            values.conservedness,
            values.residue,
            values.residueId,
            values.state,
        ];
        [...detailsList.querySelectorAll("dd")].forEach((el, index) => {
            el.textContent = String(items[index]);
        });
    }
    async function initialize() {
        const files = await fetchFilesPayload();
        applyFilesPayload(files, { preserveSelection: false });
        setOptions(partnerSelect, [{ value: "__all__", label: "All partners" }], "__all__");
        const msaFromUrl = msaFileFromUrl();
        if (msaFromUrl) {
            msaSelect.value = msaFromUrl;
            updatePairedOptions();
        }
        syncMsaPickerSelection();
        renderMsaPickerOptions();
        if (msaFromUrl) {
            void ensurePfamInfoLoaded();
        }
        syncRepresentativeLensControls();
        syncMsaPanelView();
        syncSelectionSettingsUi();
        setEmbeddingInfo(`3D ${defaultPointMethodLabel()} points on ${embeddingDistanceLabel(DEFAULT_EMBEDDING_SETTINGS.distance)} input. Drag to rotate.`);
        syncEmbeddingSettingsUi();
        clearViewer();
        if (hasMissingPfamDisplayNames()) {
            void pollPfamMetadataIfNeeded();
        }
        if (msaFromUrl) {
            await loadCurrentSelection();
        }
        msaClusterLegend?.addEventListener("click", (event) => {
            const button = event.target.closest("[data-msa-cluster-label]");
            if (!button || !state.msa) {
                return;
            }
            const clusterKey = button.dataset.msaClusterLabel;
            if (!clusterKey) {
                return;
            }
            const allKeys = allMsaClusterKeys();
            if (allKeys.length === 0) {
                return;
            }
            const isModifier = event.ctrlKey || event.metaKey;
            const isCurrentlyOnlyOne = state.msaVisibleClusters.size === 1 && state.msaVisibleClusters.has(clusterKey);
            if (isModifier) {
                state.msaVisibleClusters = isCurrentlyOnlyOne ? new Set(allKeys) : new Set([clusterKey]);
            }
            else if (state.msaVisibleClusters.has(clusterKey)) {
                state.msaVisibleClusters.delete(clusterKey);
            }
            else {
                state.msaVisibleClusters.add(clusterKey);
            }
            render();
        });
        columnsClusterLegend?.addEventListener("click", (event) => {
            const button = event.target.closest("[data-columns-cluster-label]");
            if (!button) {
                return;
            }
            const clusterKey = button.dataset.columnsClusterLabel;
            if (!clusterKey) {
                return;
            }
            const allKeys = allColumnsClusterLabels();
            if (allKeys.length === 0) {
                return;
            }
            const currentlyVisible = new Set(visibleColumnsClusters());
            const isModifier = event.ctrlKey || event.metaKey;
            const isCurrentlyOnlyOne = currentlyVisible.size === 1 && currentlyVisible.has(clusterKey);
            if (isModifier) {
                state.columnsVisibleClusters = isCurrentlyOnlyOne
                    ? new Set(allKeys)
                    : new Set([clusterKey]);
            }
            else if (state.columnsVisibleClusters.has(clusterKey)) {
                state.columnsVisibleClusters.delete(clusterKey);
            }
            else {
                state.columnsVisibleClusters.add(clusterKey);
            }
            renderColumnsClusterLegend();
            renderColumnsChart();
        });
    }
    function handleInitializeError(error) {
        appStatus.textContent = error.message;
        loadingPanel.classList.remove("hidden");
        loadingLabel.textContent = "Startup failed";
        loadingDetail.textContent = error.message;
        progressBar.style.width = "100%";
    }
    return {
        clearViewer,
        closeMsaPicker,
        displayAlignmentLength,
        drawGrid,
        ensurePfamInfoLoaded,
        hideLoading,
        initialize,
        handleInitializeError,
        loadCurrentSelection,
        openMsaPicker,
        originalColumnForDisplay,
        render,
        renderMsaPickerOptions,
        scheduleLayoutSync,
        selectFilteredRow,
        labelHitTargetAtClientPoint,
        selectRowByKey,
        setDetails,
        setLoading,
        setOptions,
        syncMsaPanelView,
        syncMsaPickerSelection,
        syncSelectionSettingsUi,
        toggleMsaPicker,
        updateMsaFilterButtons,
        updateSelectedRowUi,
    };
}
