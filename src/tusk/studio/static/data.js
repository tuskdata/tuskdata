// Tusk Data Page JavaScript - ETL Pipeline Builder

// State
let datasets = [];  // Array of datasets {id, name, source_type, path, transforms, joinSources, ...}
let activeDatasetId = null;  // Currently selected dataset
let currentSchema = null;
let selectedTransformType = null;

// Get transforms for active dataset
function getTransforms() {
    const ds = getActiveDataset();
    return ds?.transforms || [];
}

// Set transforms for active dataset
function setTransforms(t) {
    const ds = getActiveDataset();
    if (ds) ds.transforms = t;
}

// Get joinSources for active dataset
function getJoinSources() {
    const ds = getActiveDataset();
    return ds?.joinSources || [];
}

// Set joinSources for active dataset
function setJoinSources(j) {
    const ds = getActiveDataset();
    if (ds) ds.joinSources = j;
}
let generatedCode = '';
let mapInstance = null;
let currentGeoJSON = null;
let currentBrowsePath = '~';

// Get active dataset
function getActiveDataset() {
    return datasets.find(d => d.id === activeDatasetId) || null;
}

// Alias for backward compatibility
function getCurrentSource() {
    return getActiveDataset();
}

// Load state from backend (with localStorage fallback)
async function loadState() {
    try {
        // Try to load from backend first
        const res = await fetch('/api/data/workspace/load?name=default');
        if (res.ok) {
            const state = await res.json();
            if (state.datasets && state.datasets.length > 0) {
                datasets = state.datasets;
                activeDatasetId = state.active_dataset_id || (datasets.length > 0 ? datasets[0].id : null);

                // Ensure all datasets have transforms/joinSources arrays
                datasets.forEach(ds => {
                    ds.transforms = ds.transforms || [];
                    ds.joinSources = ds.joinSources || [];
                });

                currentSchema = null;
                renderDatasets();
                renderTransforms();
                if (typeof lucide !== 'undefined') lucide.createIcons();

                // Auto-preview active dataset
                if (getActiveDataset()) {
                    await previewData();
                }
                console.log('Workspace loaded from backend:', datasets.length, 'datasets');
                return;
            }
        }
    } catch (e) {
        console.warn('Failed to load from backend, trying localStorage:', e);
    }

    // Fallback to localStorage
    try {
        const saved = localStorage.getItem('tusk_data_state');
        if (saved) {
            const state = JSON.parse(saved);

            // Migration: convert old currentSource to datasets array
            if (state.currentSource && !state.datasets) {
                datasets = [{
                    ...state.currentSource,
                    transforms: state.transforms || [],
                    joinSources: state.joinSources || []
                }];
                activeDatasetId = state.currentSource.id;
            } else {
                datasets = state.datasets || [];
                activeDatasetId = state.activeDatasetId || null;

                // Migration: if old global transforms exist, assign to active dataset
                if (state.transforms?.length && activeDatasetId) {
                    const ds = datasets.find(d => d.id === activeDatasetId);
                    if (ds && !ds.transforms?.length) {
                        ds.transforms = state.transforms;
                        ds.joinSources = state.joinSources || [];
                    }
                }

                // Ensure all datasets have transforms/joinSources arrays
                datasets.forEach(ds => {
                    ds.transforms = ds.transforms || [];
                    ds.joinSources = ds.joinSources || [];
                });
            }

            currentSchema = state.currentSchema || null;

            renderDatasets();
            renderTransforms();
            if (typeof lucide !== 'undefined') lucide.createIcons();

            // Auto-preview active dataset
            if (getActiveDataset()) {
                await previewData();
            }

            // Migrate localStorage data to backend
            saveStateToBackend();
        }
    } catch (e) {
        console.error('Failed to load state:', e);
        localStorage.removeItem('tusk_data_state');
    }
}

// Debounced save to backend
let saveTimeout = null;
function saveStateToBackend() {
    if (saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(async () => {
        try {
            await fetch('/api/data/workspace/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: 'default',
                    datasets: datasets,
                    active_dataset_id: activeDatasetId
                })
            });
            // Show subtle save indicator
            showSaveIndicator();
        } catch (e) {
            console.error('Failed to save workspace to backend:', e);
        }
    }, 1000); // Debounce 1 second
}

// Show a subtle "Saved" indicator
function showSaveIndicator() {
    let indicator = document.getElementById('save-indicator');
    if (!indicator) {
        indicator = document.createElement('div');
        indicator.id = 'save-indicator';
        indicator.className = 'fixed bottom-4 right-4 bg-[#238636] text-white text-xs px-3 py-1.5 rounded-full opacity-0 transition-opacity duration-300 flex items-center gap-1.5 pointer-events-none z-50';
        indicator.innerHTML = '<svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg> Saved';
        document.body.appendChild(indicator);
    }
    indicator.style.opacity = '1';
    setTimeout(() => { indicator.style.opacity = '0'; }, 1500);
}

// Save state to localStorage and backend
function saveState() {
    // Save to localStorage (fast, for immediate recovery)
    try {
        localStorage.setItem('tusk_data_state', JSON.stringify({
            datasets,
            activeDatasetId,
            currentSchema
        }));
    } catch (e) {
        console.error('Failed to save state to localStorage:', e);
    }

    // Also save to backend (persistent)
    saveStateToBackend();
}

// Render datasets list in sidebar
function renderDatasets() {
    const container = document.getElementById('datasets-list');
    const emptyMsg = document.getElementById('datasets-empty');

    if (!container) return;

    if (datasets.length === 0) {
        if (emptyMsg) emptyMsg.classList.remove('hidden');
        container.innerHTML = '<div id="datasets-empty" class="text-[#8b949e] text-xs py-2">Click + to add a dataset</div>';
        return;
    }

    container.innerHTML = datasets.map(ds => {
        const isActive = ds.id === activeDatasetId;
        const icon = ds.source_type === 'database' ? 'database' : 'file';
        const iconColor = ds.source_type === 'database' ? 'text-[#58a6ff]' : 'text-[#f0883e]';
        let info = ds.source_type === 'database' ? 'DB' : ds.source_type.toUpperCase();
        if (ds.osm_layer) info += ` · ${ds.osm_layer}`;
        // Show column count for active dataset
        if (isActive && currentSchema) info += ` · ${currentSchema.length} cols`;

        return `
            <div class="sidebar-item flex items-center gap-2 px-2 py-2 rounded cursor-pointer ${isActive ? 'bg-[#21262d] ring-1 ring-indigo-500' : 'hover:bg-[#21262d]'}" onclick="setActiveDataset('${ds.id}')">
                <i data-lucide="${icon}" class="w-4 h-4 ${iconColor}"></i>
                <div class="flex-1 min-w-0">
                    <div class="text-sm font-medium truncate">${ds.name}</div>
                    <div class="text-xs text-[#8b949e]">${info}</div>
                </div>
                <button onclick="event.stopPropagation(); removeDataset('${ds.id}')" class="p-1 hover:bg-[#30363d] rounded text-[#8b949e] hover:text-red-400" title="Remove">
                    <i data-lucide="x" class="w-3 h-3"></i>
                </button>
            </div>
        `;
    }).join('');

    lucide.createIcons();
}

// Set active dataset
window.setActiveDataset = async function(id) {
    if (activeDatasetId === id) return;

    activeDatasetId = id;
    // Each dataset has its own transforms, just switch context
    currentSchema = null;
    currentGeoJSON = null;

    renderDatasets();
    renderTransforms();  // Will render transforms for the new active dataset
    saveState();

    // Auto-preview the data
    if (getActiveDataset()) {
        await previewData();
    }
}

// Remove dataset
window.removeDataset = function(id) {
    datasets = datasets.filter(d => d.id !== id);

    if (activeDatasetId === id) {
        activeDatasetId = datasets.length > 0 ? datasets[0].id : null;
        currentSchema = null;
        currentGeoJSON = null;
        renderTransforms();  // Will show transforms for new active dataset (or empty)

        if (activeDatasetId) {
            previewData();
        } else {
            document.getElementById('results-container').innerHTML = '<div class="text-[#8b949e] text-center py-12">Select a dataset to see preview</div>';
        }
    }

    renderDatasets();
    saveState();
}

// Quick transform shortcut
window.quickTransform = function(type) {
    if (!getActiveDataset()) {
        showToast('Please select a dataset first', 'warning');
        return;
    }
    showAddTransformModal();
    setTimeout(() => selectTransformType(type), 50);
}

// Load state when page loads
document.addEventListener('DOMContentLoaded', () => {
    loadState();

    // Toggle filter value field based on operator
    const filterOp = document.getElementById('filter-operator');
    const filterValueRow = document.getElementById('filter-value')?.parentElement;
    if (filterOp && filterValueRow) {
        filterOp.addEventListener('change', () => {
            const noValueOps = ['is_null', 'is_not_null', 'is_empty', 'is_not_empty'];
            filterValueRow.style.display = noValueOps.includes(filterOp.value) ? 'none' : '';
        });
    }
});

// File Browser
window.showFileBrowser = function() {
    document.getElementById('file-browser-modal').classList.remove('hidden');
    browseTo('~');
}

window.hideFileBrowser = function() {
    document.getElementById('file-browser-modal').classList.add('hidden');
}

async function browseTo(path) {
    currentBrowsePath = path;
    document.getElementById('browser-path').value = path;
    document.getElementById('browser-files').innerHTML = '<div class="text-gray-500 text-center py-8">Loading...</div>';

    const res = await fetch(`/api/files/browse?path=${encodeURIComponent(path)}`);
    const data = await res.json();

    if (data.error) {
        document.getElementById('browser-files').innerHTML = `<div class="text-red-400 text-center py-8">${data.error}</div>`;
        return;
    }

    currentBrowsePath = data.path;
    document.getElementById('browser-path').value = data.path;

    if (data.items.length === 0) {
        document.getElementById('browser-files').innerHTML = '<div class="text-gray-500 text-center py-8">No data files found</div>';
        return;
    }

    const filesEl = document.getElementById('browser-files');
    filesEl.innerHTML = data.items.map(item => {
        if (item.type === 'directory') {
            return `<div class="browser-item flex items-center gap-3 px-3 py-2 hover:bg-[#21262d] rounded-lg cursor-pointer" data-type="dir" data-path="${encodeURIComponent(item.path)}">
                <i data-lucide="folder" class="w-5 h-5 text-yellow-500"></i>
                <span class="flex-1">${item.name}</span>
            </div>`;
        } else {
            return `<div class="browser-item flex items-center gap-3 px-3 py-2 hover:bg-[#21262d] rounded-lg cursor-pointer" data-type="file" data-path="${encodeURIComponent(item.path)}">
                <i data-lucide="file" class="w-5 h-5 text-indigo-400"></i>
                <span class="flex-1">${item.name}</span>
                <span class="text-xs text-gray-500">${item.size}</span>
            </div>`;
        }
    }).join('');

    // Add click handlers
    filesEl.querySelectorAll('.browser-item').forEach(el => {
        el.addEventListener('click', () => {
            const path = decodeURIComponent(el.dataset.path);
            if (el.dataset.type === 'dir') browseTo(path);
            else selectBrowserFile(path);
        });
    });
    lucide.createIcons();
}

window.browseParent = function() {
    const path = document.getElementById('browser-path').value;
    const parts = path.split('/');
    if (parts.length > 1) {
        parts.pop();
        browseTo(parts.join('/') || '/');
    }
}

window.browseTo = browseTo;

let fileBrowserCallback = null;

function selectBrowserFile(path) {
    if (fileBrowserCallback) {
        fileBrowserCallback(path);
        fileBrowserCallback = null;
        hideFileBrowser();
    } else {
        document.getElementById('source-path').value = path;
        detectFileType();
        hideFileBrowser();
    }
}

// Source functions
let currentSourceTab = 'file';
let currentDbSourceType = 'table';
let dbConnections = [];

window.showSourceModal = async function() {
    // Reset form fields
    document.getElementById('source-path').value = '';
    document.getElementById('osm-layer-section').classList.add('hidden');
    document.getElementById('source-schema').classList.add('hidden');

    document.getElementById('source-modal').classList.remove('hidden');
    setSourceTab('file');
    // Load connections for DB tab
    await loadSourceConnections();
    lucide.createIcons();
}

window.hideSourceModal = function() {
    document.getElementById('source-modal').classList.add('hidden');
    document.getElementById('source-schema').classList.add('hidden');
    document.getElementById('osm-layer-section').classList.add('hidden');
    document.getElementById('db-source-schema')?.classList.add('hidden');
    // Reset form
    document.getElementById('source-path').value = '';
}

window.setSourceTab = function(tab) {
    currentSourceTab = tab;
    const fileTab = document.getElementById('source-tab-file');
    const dbTab = document.getElementById('source-tab-database');
    const fileSection = document.getElementById('source-file-section');
    const dbSection = document.getElementById('source-db-section');

    if (tab === 'file') {
        fileTab.classList.add('bg-[#21262d]', 'text-white');
        fileTab.classList.remove('text-[#8b949e]');
        dbTab.classList.remove('bg-[#21262d]', 'text-white');
        dbTab.classList.add('text-[#8b949e]');
        fileSection.classList.remove('hidden');
        dbSection.classList.add('hidden');
    } else {
        dbTab.classList.add('bg-[#21262d]', 'text-white');
        dbTab.classList.remove('text-[#8b949e]');
        fileTab.classList.remove('bg-[#21262d]', 'text-white');
        fileTab.classList.add('text-[#8b949e]');
        dbSection.classList.remove('hidden');
        fileSection.classList.add('hidden');
    }
}

window.setDbSourceType = function(type) {
    currentDbSourceType = type;
    const tableBtn = document.getElementById('db-source-table-btn');
    const queryBtn = document.getElementById('db-source-query-btn');
    const tableSection = document.getElementById('db-table-section');
    const querySection = document.getElementById('db-query-section');

    if (type === 'table') {
        tableBtn.classList.add('bg-[#21262d]', 'text-white');
        tableBtn.classList.remove('bg-[#0d1117]', 'text-[#8b949e]');
        queryBtn.classList.remove('bg-[#21262d]', 'text-white');
        queryBtn.classList.add('bg-[#0d1117]', 'text-[#8b949e]');
        tableSection.classList.remove('hidden');
        querySection.classList.add('hidden');
    } else {
        queryBtn.classList.add('bg-[#21262d]', 'text-white');
        queryBtn.classList.remove('bg-[#0d1117]', 'text-[#8b949e]');
        tableBtn.classList.remove('bg-[#21262d]', 'text-white');
        tableBtn.classList.add('bg-[#0d1117]', 'text-[#8b949e]');
        querySection.classList.remove('hidden');
        tableSection.classList.add('hidden');
    }
}

async function loadSourceConnections() {
    try {
        const res = await fetch('/api/connections');
        dbConnections = await res.json();
        const select = document.getElementById('source-db-connection');
        select.innerHTML = '<option value="">Select a connection...</option>' +
            dbConnections.filter(c => c.type === 'postgres').map(c =>
                `<option value="${c.id}">${c.name} (${c.type})</option>`
            ).join('');
    } catch (e) {
        console.error('Failed to load connections', e);
    }
}

window.loadDbTables = async function() {
    const connId = document.getElementById('source-db-connection').value;
    if (!connId) return;

    const select = document.getElementById('source-db-table');
    select.innerHTML = '<option value="">Loading...</option>';

    try {
        const res = await fetch(`/api/connections/${connId}/schema`);
        const schema = await res.json();

        if (schema.error) {
            select.innerHTML = '<option value="">Error loading tables</option>';
            return;
        }

        const tables = [];
        for (const [schemaName, schemaTables] of Object.entries(schema)) {
            for (const tableName of Object.keys(schemaTables)) {
                tables.push(`${schemaName}.${tableName}`);
            }
        }

        select.innerHTML = '<option value="">Select a table...</option>' +
            tables.map(t => `<option value="${t}">${t}</option>`).join('');
    } catch (e) {
        select.innerHTML = '<option value="">Error loading tables</option>';
    }
}

window.previewDbSource = async function() {
    const connId = document.getElementById('source-db-connection').value;
    if (!connId) { showToast('Please select a connection', 'warning'); return; }

    let sql;
    if (currentDbSourceType === 'table') {
        const table = document.getElementById('source-db-table').value;
        if (!table) { showToast('Please select a table', 'warning'); return; }
        sql = `SELECT * FROM ${table} LIMIT 100`;
    } else {
        sql = document.getElementById('source-db-query').value.trim();
        if (!sql) { showToast('Please enter a query', 'warning'); return; }
    }

    try {
        const res = await fetch('/api/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connection_id: connId, sql })
        });
        const data = await res.json();

        if (data.error) {
            showToast('Error: ' + data.error, 'error');
            return;
        }

        currentSchema = data.columns;
        document.getElementById('db-source-schema').classList.remove('hidden');
        document.getElementById('db-schema-columns').innerHTML = data.columns.map(c =>
            `<div class="flex justify-between"><span>${c.name}</span><span class="text-gray-500">${c.type}</span></div>`
        ).join('');
    } catch (e) {
        showToast('Error: ' + e.message, 'error');
    }
}

window.detectFileType = function() {
    const path = document.getElementById('source-path').value.trim().toLowerCase();
    const osmSection = document.getElementById('osm-layer-section');
    if (path.endsWith('.osm.pbf') || path.endsWith('.pbf')) {
        osmSection.classList.remove('hidden');
    } else {
        osmSection.classList.add('hidden');
    }
}

function getSourceType(path) {
    const lower = path.toLowerCase();
    if (lower.endsWith('.osm.pbf') || lower.endsWith('.pbf')) return 'osm';
    if (lower.endsWith('.parquet')) return 'parquet';
    if (lower.endsWith('.json')) return 'json';
    return 'csv';
}

window.previewSource = async function() {
    const path = document.getElementById('source-path').value.trim();
    if (!path) return;

    let url = `/api/data/files/schema?path=${encodeURIComponent(path)}`;
    const sourceType = getSourceType(path);
    if (sourceType === 'osm') {
        url += `&osm_layer=${encodeURIComponent(document.getElementById('osm-layer').value)}`;
    }

    const res = await fetch(url);
    const data = await res.json();

    if (data.error) {
        showToast('Error: ' + data.error, 'error');
        return;
    }

    currentSchema = data.columns;
    document.getElementById('source-schema').classList.remove('hidden');
    document.getElementById('schema-columns').innerHTML = data.columns.map(c =>
        `<div class="flex justify-between"><span>${c.name}</span><span class="text-gray-500">${c.type}</span></div>`
    ).join('');
}

window.selectSource = async function() {
    // Generate unique ID
    const newId = 'ds_' + Date.now();
    let newDataset;

    if (currentSourceTab === 'file') {
        // File source
        const path = document.getElementById('source-path').value.trim();
        if (!path) { showToast('Please enter a file path', 'warning'); return; }

        const sourceType = getSourceType(path);
        newDataset = {
            id: newId,
            name: path.split('/').pop(),
            source_type: sourceType,
            path: path,
            transforms: [],
            joinSources: []
        };
        if (sourceType === 'osm') newDataset.osm_layer = document.getElementById('osm-layer').value;
    } else {
        // Database source
        const connId = document.getElementById('source-db-connection').value;
        if (!connId) { showToast('Please select a connection', 'warning'); return; }

        const conn = dbConnections.find(c => c.id === connId);
        let name, query;

        if (currentDbSourceType === 'table') {
            const table = document.getElementById('source-db-table').value;
            if (!table) { showToast('Please select a table', 'warning'); return; }
            name = table;
            query = `SELECT * FROM ${table}`;
        } else {
            query = document.getElementById('source-db-query').value.trim();
            if (!query) { showToast('Please enter a query', 'warning'); return; }
            name = 'Custom Query';
        }

        newDataset = {
            id: newId,
            name: name,
            source_type: 'database',
            connection_id: connId,
            connection_name: conn?.name,
            query: query,
            transforms: [],
            joinSources: []
        };
    }

    // Add to datasets array
    datasets.push(newDataset);
    activeDatasetId = newId;
    currentSchema = null;
    currentGeoJSON = null;

    renderDatasets();
    renderTransforms();
    hideSourceModal();
    saveState();
    lucide.createIcons();

    // Auto-preview the data
    await previewData();
}

function getPreviewLimit() {
    return parseInt(document.getElementById('preview-limit')?.value || '100');
}

window.changeLimit = function() {
    if (getActiveDataset()) previewData();
}

async function previewData() {
    const currentSource = getActiveDataset();
    if (!currentSource) return;

    const limit = getPreviewLimit();
    document.getElementById('results-container').innerHTML = '<div class="text-[#8b949e] text-center py-12"><i data-lucide="loader-2" class="w-8 h-8 mx-auto mb-3 animate-spin"></i>Loading preview...</div>';
    lucide.createIcons();

    let data;

    if (currentSource.source_type === 'database') {
        // Database source - run query
        let sql = currentSource.query;
        // Add LIMIT if not already present
        if (!sql.toLowerCase().includes(' limit ')) {
            sql = sql.replace(/;?\s*$/, '') + ` LIMIT ${limit}`;
        }

        const res = await fetch('/api/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connection_id: currentSource.connection_id, sql })
        });
        data = await res.json();
    } else {
        // File source
        let url = `/api/data/files/preview?path=${encodeURIComponent(currentSource.path)}&limit=${limit}`;
        if (currentSource.source_type === 'osm' && currentSource.osm_layer) {
            url += `&osm_layer=${encodeURIComponent(currentSource.osm_layer)}`;
        }
        const res = await fetch(url);
        data = await res.json();
    }

    if (data.error) {
        document.getElementById('results-container').innerHTML = `
            <div class="card rounded-lg p-4">
                <div class="text-red-400 font-medium mb-2">Error loading data</div>
                <pre class="whitespace-pre-wrap font-mono text-sm text-gray-400">${data.error}</pre>
                ${data.hint ? `<p class="text-sm text-gray-500 mt-2">Hint: ${data.hint}</p>` : ''}
            </div>`;
        return;
    }

    // Save schema for transforms
    currentSchema = data.columns;
    saveState();

    // Re-render datasets to update column count
    renderDatasets();

    renderResults(data);
}

window.previewData = previewData;

// Transform functions
window.showAddTransformModal = function() {
    if (!getActiveDataset()) { showToast('Please select a dataset first', 'warning'); return; }
    editingTransformIndex = null;  // Reset editing state
    document.getElementById('transform-modal').classList.remove('hidden');
    document.getElementById('transform-type-select').classList.remove('hidden');
    document.getElementById('transform-config').classList.add('hidden');
    document.getElementById('add-transform-btn').classList.add('hidden');
    document.getElementById('add-transform-btn').textContent = 'Add Transform';
    selectedTransformType = null;
    // Reset filter value field visibility
    const filterValueRow = document.getElementById('filter-value')?.parentElement;
    if (filterValueRow) filterValueRow.style.display = '';
    // Reset operator to default
    const filterOp = document.getElementById('filter-operator');
    if (filterOp) filterOp.value = 'eq';
}

window.hideTransformModal = function() {
    document.getElementById('transform-modal').classList.add('hidden');
    document.querySelectorAll('[id^="config-"]').forEach(el => el.classList.add('hidden'));
    editingTransformIndex = null;  // Reset editing state
}

window.selectTransformType = function(type) {
    selectedTransformType = type;
    document.getElementById('transform-type-select').classList.add('hidden');
    document.getElementById('transform-config').classList.remove('hidden');
    document.getElementById('add-transform-btn').classList.remove('hidden');
    document.querySelectorAll('[id^="config-"]').forEach(el => el.classList.add('hidden'));
    document.getElementById(`config-${type}`).classList.remove('hidden');
    populateColumnSelects();
}

function populateColumnSelects() {
    if (!currentSchema) return;
    const options = currentSchema.map(c => `<option value="${c.name}">${c.name}</option>`).join('');
    document.getElementById('filter-column').innerHTML = options;
    document.getElementById('sort-column').innerHTML = options;
    document.getElementById('agg-column').innerHTML = options;
    document.getElementById('rename-from').innerHTML = options;
    document.getElementById('join-left-on').innerHTML = options;

    const checkboxes = (id, cls) => currentSchema.map(c =>
        `<label class="flex items-center gap-2 cursor-pointer"><input type="checkbox" value="${c.name}" class="${cls} accent-indigo-500"><span class="text-sm">${c.name}</span></label>`
    ).join('');
    document.getElementById('select-columns').innerHTML = currentSchema.map(c =>
        `<label class="flex items-center gap-2 cursor-pointer"><input type="checkbox" value="${c.name}" class="select-col-check accent-indigo-500" checked><span class="text-sm">${c.name}</span></label>`
    ).join('');
    document.getElementById('groupby-columns').innerHTML = checkboxes('groupby-columns', 'groupby-col-check');
    document.getElementById('dropnulls-columns').innerHTML = checkboxes('dropnulls-columns', 'dropnulls-col-check');
}

window.addTransform = function() {
    if (!selectedTransformType) return;
    let t = { type: selectedTransformType };

    if (t.type === 'filter') {
        t.column = document.getElementById('filter-column').value;
        t.operator = document.getElementById('filter-operator').value;
        const val = document.getElementById('filter-value').value;
        t.value = val ? (isNaN(val) ? val : parseFloat(val)) : null;
    } else if (t.type === 'select') {
        t.columns = Array.from(document.querySelectorAll('.select-col-check:checked')).map(el => el.value);
    } else if (t.type === 'sort') {
        t.columns = [document.getElementById('sort-column').value];
        t.descending = [document.getElementById('sort-descending').checked];
    } else if (t.type === 'group_by') {
        t.by = Array.from(document.querySelectorAll('.groupby-col-check:checked')).map(el => el.value);
        t.aggregations = [{ column: document.getElementById('agg-column').value, agg: document.getElementById('agg-function').value }];
    } else if (t.type === 'rename') {
        t.mapping = { [document.getElementById('rename-from').value]: document.getElementById('rename-to').value };
    } else if (t.type === 'drop_nulls') {
        const checked = document.querySelectorAll('.dropnulls-col-check:checked');
        t.subset = checked.length > 0 ? Array.from(checked).map(el => el.value) : null;
    } else if (t.type === 'limit') {
        t.n = parseInt(document.getElementById('limit-n').value);
    } else if (t.type === 'join') {
        const joinPath = document.getElementById('join-file-path').value.trim();
        if (!joinPath) {
            showToast('Please select a file to join with', 'warning');
            return;
        }

        // Check if we're editing an existing join (reuse the source)
        let joinSourceId;
        const currentTransforms = getTransforms();
        const currentJoinSources = getJoinSources();
        if (editingTransformIndex !== null && currentTransforms[editingTransformIndex]?.type === 'join') {
            // Reuse existing join source ID
            joinSourceId = currentTransforms[editingTransformIndex].right_source_id;
            // Update the join source path
            const existingSource = currentJoinSources.find(s => s.id === joinSourceId);
            if (existingSource) {
                existingSource.path = joinPath;
                existingSource.name = joinPath.split('/').pop();
                existingSource.source_type = getSourceType(joinPath);
            }
        } else {
            // Create a new source for the join
            joinSourceId = 'join_' + Date.now();
            const joinSourceType = getSourceType(joinPath);
            currentJoinSources.push({
                id: joinSourceId,
                name: joinPath.split('/').pop(),
                source_type: joinSourceType,
                path: joinPath
            });
            setJoinSources(currentJoinSources);
        }

        t.right_source_id = joinSourceId;
        t.left_on = [document.getElementById('join-left-on').value];
        t.right_on = [document.getElementById('join-right-on').value || document.getElementById('join-left-on').value];
        t.how = document.getElementById('join-how').value;
    }

    // Either update existing transform or add new one
    const transforms = getTransforms();
    if (editingTransformIndex !== null) {
        transforms[editingTransformIndex] = t;
        showToast('Transform updated', 'success', 1500);
    } else {
        transforms.push(t);
    }
    setTransforms(transforms);

    renderTransforms();
    hideTransformModal();
    saveState();
}

function renderTransforms() {
    const list = document.getElementById('transforms-list');
    if (!list) return;

    const transforms = getTransforms();
    if (transforms.length === 0) {
        list.innerHTML = '<div class="text-[#8b949e] text-xs py-2">Add filters, sorts, joins...<br/>(optional)</div>';
        return;
    }

    const iconMap = {
        filter: { icon: 'filter', color: '#58a6ff' },
        select: { icon: 'columns', color: '#a371f7' },
        sort: { icon: 'arrow-up-down', color: '#f0883e' },
        group_by: { icon: 'group', color: '#3fb950' },
        rename: { icon: 'pencil', color: '#58a6ff' },
        drop_nulls: { icon: 'trash-2', color: '#f85149' },
        limit: { icon: 'hash', color: '#8b949e' },
        join: { icon: 'git-merge', color: '#3fb950' }
    };

    list.innerHTML = transforms.map((t, i) => {
        let desc = '';
        if (t.type === 'filter') {
            const opLabels = {
                eq: '=', ne: '!=', gt: '>', gte: '>=', lt: '<', lte: '<=',
                contains: 'contains', starts_with: 'starts with', ends_with: 'ends with',
                is_null: 'is null', is_not_null: 'is not null',
                is_empty: 'is empty', is_not_empty: 'is not empty'
            };
            const opLabel = opLabels[t.operator] || t.operator;
            const needsValue = !['is_null', 'is_not_null', 'is_empty', 'is_not_empty'].includes(t.operator);
            desc = needsValue ? `${t.column} ${opLabel} ${t.value ?? ''}` : `${t.column} ${opLabel}`;
        }
        else if (t.type === 'select') desc = `${t.columns.length} columns`;
        else if (t.type === 'sort') desc = t.columns.join(', ');
        else if (t.type === 'group_by') desc = `by ${t.by.join(', ')}`;
        else if (t.type === 'rename') desc = Object.entries(t.mapping).map(([k,v]) => `${k}→${v}`).join(', ');
        else if (t.type === 'drop_nulls') desc = t.subset ? t.subset.join(', ') : 'all';
        else if (t.type === 'limit') desc = `${t.n} rows`;

        const { icon, color } = iconMap[t.type] || { icon: 'circle', color: '#8b949e' };

        return `<div class="transform-card flex items-center gap-2 px-2 py-2 rounded-lg hover:bg-[#21262d] cursor-pointer" onclick="editTransform(${i})">
            <div class="p-1.5 rounded" style="background: ${color}20">
                <i data-lucide="${icon}" class="w-3.5 h-3.5" style="color: ${color}"></i>
            </div>
            <div class="flex-1 min-w-0">
                <div class="text-sm font-medium capitalize">${t.type.replace('_', ' ')}</div>
                <div class="text-xs text-[#8b949e] truncate">${desc}</div>
            </div>
            <button onclick="event.stopPropagation(); removeTransform(${i})" class="p-1 hover:bg-[#30363d] rounded text-[#8b949e] hover:text-red-400" title="Remove">
                <i data-lucide="x" class="w-3 h-3"></i>
            </button>
        </div>`;
    }).join('');

    lucide.createIcons();
}

// Track if we're editing an existing transform
let editingTransformIndex = null;

window.removeTransform = function(index) {
    const transforms = getTransforms();
    transforms.splice(index, 1);
    setTransforms(transforms);
    renderTransforms();
    saveState();
}

window.editTransform = function(index) {
    const t = getTransforms()[index];
    if (!t) return;

    editingTransformIndex = index;

    // Open modal and select the transform type
    document.getElementById('transform-modal').classList.remove('hidden');
    document.getElementById('transform-type-select').classList.add('hidden');
    document.getElementById('transform-config').classList.remove('hidden');
    document.getElementById('add-transform-btn').classList.remove('hidden');
    document.getElementById('add-transform-btn').textContent = 'Update Transform';

    selectedTransformType = t.type;

    // Show the correct config panel
    document.querySelectorAll('[id^="config-"]').forEach(el => el.classList.add('hidden'));
    document.getElementById(`config-${t.type}`)?.classList.remove('hidden');

    // Populate fields based on transform type
    populateTransformFields(t);

    lucide.createIcons();
}

function populateTransformFields(t) {
    // First populate the column options if needed (for selects that depend on schema)
    if (currentSchema) {
        populateColumnSelects();
    }

    // Small delay to let the selects populate
    setTimeout(() => {
        switch (t.type) {
            case 'filter':
                document.getElementById('filter-column').value = t.column || '';
                document.getElementById('filter-operator').value = t.operator || 'eq';
                document.getElementById('filter-value').value = t.value ?? '';
                // Toggle value field visibility
                const noValueOps = ['is_null', 'is_not_null', 'is_empty', 'is_not_empty'];
                const valueRow = document.getElementById('filter-value')?.parentElement;
                if (valueRow) valueRow.style.display = noValueOps.includes(t.operator) ? 'none' : '';
                break;

            case 'select':
                // Check the appropriate checkboxes
                document.querySelectorAll('.select-col-check').forEach(cb => {
                    cb.checked = t.columns?.includes(cb.value) || false;
                });
                break;

            case 'sort':
                document.getElementById('sort-column').value = t.columns?.[0] || '';
                document.getElementById('sort-descending').checked = t.descending?.[0] || false;
                break;

            case 'group_by':
                // Check group by columns
                document.querySelectorAll('.groupby-col-check').forEach(cb => {
                    cb.checked = t.by?.includes(cb.value) || false;
                });
                // Set aggregation
                if (t.aggregations?.[0]) {
                    document.getElementById('agg-column').value = t.aggregations[0].column || '';
                    document.getElementById('agg-function').value = t.aggregations[0].agg || 'count';
                }
                break;

            case 'rename':
                const entries = Object.entries(t.mapping || {});
                if (entries.length > 0) {
                    document.getElementById('rename-from').value = entries[0][0] || '';
                    document.getElementById('rename-to').value = entries[0][1] || '';
                }
                break;

            case 'drop_nulls':
                document.querySelectorAll('.dropnulls-col-check').forEach(cb => {
                    cb.checked = t.subset ? t.subset.includes(cb.value) : false;
                });
                break;

            case 'limit':
                document.getElementById('limit-n').value = t.n || 100;
                break;

            case 'join':
                // Find the join source to get the path
                const joinSource = getJoinSources().find(s => s.id === t.right_source_id);
                document.getElementById('join-file-path').value = joinSource?.path || '';
                document.getElementById('join-left-on').value = t.left_on?.[0] || '';
                document.getElementById('join-right-on').value = t.right_on?.[0] || '';
                document.getElementById('join-how').value = t.how || 'inner';
                break;
        }
    }, 50);
}

// Pipeline execution
window.runPipeline = async function() {
    const currentSource = getActiveDataset();
    if (!currentSource) { showToast('Please select a dataset first', 'warning'); return; }

    const limit = getPreviewLimit();
    document.getElementById('results-container').innerHTML = '<div class="text-[#8b949e] text-center py-12"><i data-lucide="loader-2" class="w-8 h-8 mx-auto mb-3 animate-spin"></i>Running pipeline...</div>';
    lucide.createIcons();

    // Include join sources if any
    const allSources = [currentSource, ...getJoinSources()];

    const res = await fetch('/api/data/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sources: allSources, transforms: getTransforms(), output_source_id: currentSource.id, limit })
    });
    const data = await res.json();
    if (data.error) {
        document.getElementById('results-container').innerHTML = `<div class="card rounded-lg p-4 text-red-400"><pre class="whitespace-pre-wrap font-mono text-sm">${data.error}</pre></div>`;
        return;
    }
    renderResults(data);
}

// Join file browser
window.browseJoinFile = function() {
    fileBrowserCallback = (path) => {
        document.getElementById('join-file-path').value = path;
    };
    showFileBrowser();
}

// Import to DB functions
let pgConnections = [];

window.showImportModal = async function() {
    const currentSource = getActiveDataset();
    if (!currentSource) { showToast('Please select a dataset first', 'warning'); return; }
    document.getElementById('import-modal').classList.remove('hidden');
    document.getElementById('import-table-name').value = currentSource.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_');
    document.getElementById('import-db-type').value = 'duckdb';
    toggleImportOptions();
    lucide.createIcons();

    // Fetch PostgreSQL connections
    try {
        const res = await fetch('/api/data/connections');
        const data = await res.json();
        pgConnections = data.connections || [];

        const select = document.getElementById('import-pg-connection');
        if (pgConnections.length === 0) {
            select.innerHTML = '<option value="">No PostgreSQL connections available</option>';
        } else {
            select.innerHTML = pgConnections.map(c =>
                `<option value="${c.id}">${c.name} (${c.database})</option>`
            ).join('');
        }
    } catch (e) {
        console.error('Failed to fetch connections:', e);
    }
}

window.hideImportModal = function() {
    document.getElementById('import-modal').classList.add('hidden');
}

window.toggleImportOptions = function() {
    const dbType = document.getElementById('import-db-type').value;
    document.getElementById('import-duckdb-path').classList.toggle('hidden', dbType !== 'duckdb-file');
    document.getElementById('import-postgres-conn').classList.toggle('hidden', dbType !== 'postgres');
}

window.importToDB = async function() {
    const currentSource = getActiveDataset();
    if (!currentSource) { showToast('Please select a dataset first', 'warning'); return; }

    const dbType = document.getElementById('import-db-type').value;
    const tableName = document.getElementById('import-table-name').value.trim();

    if (!tableName) {
        showToast('Please enter a table name', 'warning');
        return;
    }

    const allSources = [currentSource, ...getJoinSources()];
    const payload = {
        sources: allSources,
        transforms: getTransforms(),
        output_source_id: currentSource.id,
        table_name: tableName
    };

    if (dbType === 'duckdb-file') {
        payload.db_path = document.getElementById('import-db-path').value.trim();
        if (!payload.db_path) {
            showToast('Please enter a database file path', 'warning');
            return;
        }
    } else if (dbType === 'postgres') {
        payload.connection_id = document.getElementById('import-pg-connection').value;
        if (!payload.connection_id) {
            showToast('Please select a PostgreSQL connection', 'warning');
            return;
        }
    }

    // For PostgreSQL, use SSE streaming for progress updates
    if (dbType === 'postgres') {
        await importPostgresWithProgress(payload, tableName);
        return;
    }

    // For DuckDB, use regular fetch
    try {
        const res = await fetch('/api/data/import/duckdb', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();

        if (data.error) {
            showToast('Import error: ' + data.error, 'error');
            return;
        }

        showToast(`Successfully imported ${data.rows} rows to DuckDB table "${data.table}"`, 'success');
        hideImportModal();
    } catch (e) {
        showToast('Import error: ' + e.message, 'error');
    }
}

// Import to PostgreSQL with SSE progress streaming
async function importPostgresWithProgress(payload, tableName) {
    // Show progress UI in the modal
    const importBtn = document.querySelector('#import-modal button[onclick="importToDB()"]');
    const originalBtnText = importBtn.innerHTML;
    importBtn.disabled = true;
    importBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> Starting...`;
    lucide.createIcons();

    // Add progress bar if not present
    let progressContainer = document.getElementById('import-progress-container');
    if (!progressContainer) {
        progressContainer = document.createElement('div');
        progressContainer.id = 'import-progress-container';
        progressContainer.className = 'mt-4 hidden';
        progressContainer.innerHTML = `
            <div class="flex justify-between text-xs text-[#8b949e] mb-1">
                <span id="import-progress-text">Starting import...</span>
                <span id="import-progress-pct">0%</span>
            </div>
            <div class="w-full bg-[#30363d] rounded-full h-2">
                <div id="import-progress-bar" class="bg-indigo-500 h-2 rounded-full transition-all duration-300" style="width: 0%"></div>
            </div>
        `;
        importBtn.parentElement.insertBefore(progressContainer, importBtn);
    }
    progressContainer.classList.remove('hidden');

    const progressBar = document.getElementById('import-progress-bar');
    const progressText = document.getElementById('import-progress-text');
    const progressPct = document.getElementById('import-progress-pct');

    try {
        const response = await fetch('/api/data/import/postgres/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop(); // Keep incomplete line in buffer

            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    try {
                        const msg = JSON.parse(line.slice(6));

                        if (msg.type === 'progress') {
                            const pct = msg.current;
                            progressBar.style.width = pct + '%';
                            progressPct.textContent = pct + '%';
                            progressText.textContent = msg.message;
                            importBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 animate-spin"></i> ${pct}%`;
                            lucide.createIcons();
                        } else if (msg.type === 'complete') {
                            progressBar.style.width = '100%';
                            progressPct.textContent = '100%';
                            progressText.textContent = 'Import complete!';
                            const result = msg.result;
                            const elapsed = result.elapsed_sec ? ` in ${result.elapsed_sec}s` : '';
                            showToast(`Imported ${result.rows.toLocaleString()} rows to PostgreSQL table "${result.table}"${elapsed}`, 'success');
                            setTimeout(() => {
                                hideImportModal();
                                progressContainer.classList.add('hidden');
                                progressBar.style.width = '0%';
                            }, 500);
                        } else if (msg.type === 'error') {
                            showToast('Import error: ' + msg.error, 'error');
                            progressContainer.classList.add('hidden');
                        }
                    } catch (e) {
                        // Ignore parse errors for non-JSON lines
                    }
                }
            }
        }
    } catch (e) {
        showToast('Import error: ' + e.message, 'error');
        progressContainer.classList.add('hidden');
    } finally {
        importBtn.disabled = false;
        importBtn.innerHTML = originalBtnText;
        lucide.createIcons();
    }
}

// Save/Load Pipeline functions
window.showSavePipelineModal = function() {
    const currentSource = getActiveDataset();
    if (!currentSource) { showToast('Please select a dataset first', 'warning'); return; }
    document.getElementById('save-pipeline-modal').classList.remove('hidden');
    document.getElementById('save-pipeline-name').value = currentSource.name.replace(/\.[^.]+$/, '') + ' Pipeline';
    document.getElementById('save-pipeline-name').select();
}

window.hideSavePipelineModal = function() {
    document.getElementById('save-pipeline-modal').classList.add('hidden');
}

window.savePipelineWithName = function() {
    const currentSource = getActiveDataset();
    if (!currentSource) { showToast('Please select a dataset first', 'warning'); return; }

    const name = document.getElementById('save-pipeline-name').value.trim();
    if (!name) {
        showToast('Please enter a pipeline name', 'warning');
        return;
    }

    const pipeline = {
        id: 'pipeline_' + Date.now(),
        name: name,
        source: currentSource,
        schema: currentSchema,
        transforms: getTransforms(),
        joinSources: getJoinSources(),
        savedAt: new Date().toISOString()
    };

    // Load existing pipelines
    let pipelines = [];
    try {
        pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]');
    } catch (e) {}

    pipelines.push(pipeline);
    localStorage.setItem('tusk_saved_pipelines', JSON.stringify(pipelines));

    showToast('Pipeline saved!', 'success');
    hideSavePipelineModal();
}

window.showLoadPipelineModal = function() {
    document.getElementById('load-pipeline-modal').classList.remove('hidden');
    renderSavedPipelines();
    lucide.createIcons();
}

window.hideLoadPipelineModal = function() {
    document.getElementById('load-pipeline-modal').classList.add('hidden');
}

function renderSavedPipelines() {
    const container = document.getElementById('saved-pipelines-list');
    let pipelines = [];
    try {
        pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]');
    } catch (e) {}

    if (pipelines.length === 0) {
        container.innerHTML = '<div class="text-[#8b949e] text-center py-8">No saved pipelines</div>';
        return;
    }

    container.innerHTML = pipelines.map((p, i) => `
        <div class="p-3 rounded-lg bg-[#0d1117] hover:bg-[#21262d] cursor-pointer flex items-center gap-3" onclick="loadPipeline(${i})">
            <div class="p-2 bg-indigo-500/20 rounded-lg">
                <i data-lucide="git-branch" class="w-4 h-4 text-indigo-400"></i>
            </div>
            <div class="flex-1 min-w-0">
                <div class="font-medium truncate">${p.name}</div>
                <div class="text-xs text-[#8b949e]">${p.source?.name || 'Unknown'} · ${p.transforms?.length || 0} transforms · ${new Date(p.savedAt).toLocaleDateString()}</div>
            </div>
            <button onclick="event.stopPropagation(); deletePipeline(${i})" class="p-1.5 hover:bg-[#30363d] rounded text-[#8b949e] hover:text-red-400">
                <i data-lucide="trash-2" class="w-4 h-4"></i>
            </button>
        </div>
    `).join('');

    lucide.createIcons();
}

window.loadPipeline = function(index) {
    let pipelines = [];
    try {
        pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]');
    } catch (e) { return; }

    const pipeline = pipelines[index];
    if (!pipeline) return;

    // Add the source to datasets if not already present
    const sourceId = pipeline.source.id || 'ds_' + Date.now();
    const existingIdx = datasets.findIndex(d => d.id === sourceId || d.path === pipeline.source.path);
    if (existingIdx === -1) {
        pipeline.source.id = sourceId;
        pipeline.source.transforms = pipeline.transforms || [];
        pipeline.source.joinSources = pipeline.joinSources || [];
        datasets.push(pipeline.source);
    } else {
        // Update existing dataset with pipeline's transforms
        datasets[existingIdx].transforms = pipeline.transforms || [];
        datasets[existingIdx].joinSources = pipeline.joinSources || [];
    }

    activeDatasetId = sourceId;
    currentSchema = pipeline.schema;

    renderDatasets();
    renderTransforms();
    hideLoadPipelineModal();
    saveState();
    lucide.createIcons();

    // Run the pipeline to show results
    runPipeline();
}

window.deletePipeline = function(index) {
    if (!confirm('Delete this pipeline?')) return;

    let pipelines = [];
    try {
        pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]');
    } catch (e) { return; }

    pipelines.splice(index, 1);
    localStorage.setItem('tusk_saved_pipelines', JSON.stringify(pipelines));
    renderSavedPipelines();
}

function detectGeoColumns(columns, rows) {
    if (!columns) return [];
    // Check column names first
    const geoNames = ['geometry', 'geom', 'wkt', 'the_geom', 'shape', 'geo', 'location', 'coordinates', 'coord', 'latlon', 'point'];
    const byName = columns.map((c, i) => geoNames.includes(c.name.toLowerCase()) ? i : -1).filter(i => i !== -1);
    if (byName.length > 0) return byName;

    // Also check content - look for WKT patterns in first row
    if (rows && rows.length > 0) {
        const wktPattern = /^(POINT|POLYGON|MULTIPOLYGON|LINESTRING|MULTILINESTRING|GEOMETRYCOLLECTION)\s*\(/i;
        for (let i = 0; i < columns.length; i++) {
            const val = rows[0][i];
            if (typeof val === 'string' && wktPattern.test(val)) {
                return [i];
            }
        }
    }
    return [];
}

function parseWKT(wkt) {
    if (!wkt || typeof wkt !== 'string') return null;
    let cleanWkt = wkt.replace(/^SRID=\d+;/i, '').trim();

    // POINT
    const pointMatch = cleanWkt.match(/^POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)/i);
    if (pointMatch) return { type: 'Point', coordinates: [parseFloat(pointMatch[1]), parseFloat(pointMatch[2])] };

    // POLYGON
    const polyMatch = cleanWkt.match(/^POLYGON\s*\(\((.+)\)\)/i);
    if (polyMatch) {
        const coords = polyMatch[1].split(',').map(p => { const [x, y] = p.trim().split(/\s+/).map(parseFloat); return [x, y]; });
        return { type: 'Polygon', coordinates: [coords] };
    }

    // MULTIPOLYGON
    const multiPolyMatch = cleanWkt.match(/^MULTIPOLYGON\s*\(\(\((.+)\)\)\)/i);
    if (multiPolyMatch) {
        // Simplified: treat as single polygon from first ring
        const firstRing = multiPolyMatch[1].split(')),((')[0];
        const coords = firstRing.split(',').map(p => { const [x, y] = p.trim().split(/\s+/).map(parseFloat); return [x, y]; });
        return { type: 'Polygon', coordinates: [coords] };
    }

    // LINESTRING
    const lineMatch = cleanWkt.match(/^LINESTRING\s*\((.+)\)/i);
    if (lineMatch) {
        const coords = lineMatch[1].split(',').map(p => { const [x, y] = p.trim().split(/\s+/).map(parseFloat); return [x, y]; });
        return { type: 'LineString', coordinates: coords };
    }

    // MULTILINESTRING
    const multiLineMatch = cleanWkt.match(/^MULTILINESTRING\s*\(\((.+)\)\)/i);
    if (multiLineMatch) {
        const firstLine = multiLineMatch[1].split('),(')[0];
        const coords = firstLine.split(',').map(p => { const [x, y] = p.trim().split(/\s+/).map(parseFloat); return [x, y]; });
        return { type: 'LineString', coordinates: coords };
    }

    return null;
}

function rowsToGeoJSON(columns, rows, geoColIdx) {
    const features = [];
    for (const row of rows) {
        const geom = parseWKT(row[geoColIdx]);
        if (!geom) continue;
        const properties = {};
        columns.forEach((col, i) => { if (i !== geoColIdx) properties[col.name] = row[i]; });
        features.push({ type: 'Feature', geometry: geom, properties });
    }
    return { type: 'FeatureCollection', features };
}

function renderResults(data) {
    const container = document.getElementById('results-container');
    const statsEl = document.getElementById('results-stats');

    if (!data.columns || data.columns.length === 0) {
        container.innerHTML = '<div class="text-[#8b949e] text-center py-12">No results</div>';
        statsEl.classList.add('hidden');
        return;
    }

    // Update stats badge
    statsEl.textContent = `${data.row_count} rows · ${data.columns.length} cols`;
    statsEl.classList.remove('hidden');

    const geoColIndices = detectGeoColumns(data.columns, data.rows);
    currentGeoJSON = geoColIndices.length > 0 ? rowsToGeoJSON(data.columns, data.rows, geoColIndices[0]) : null;

    let headerActions = '';
    if (geoColIndices.length > 0) {
        headerActions = `
            <span class="text-xs text-[#a371f7] bg-[#a371f7]/10 px-2 py-0.5 rounded-full flex items-center gap-1">
                <i data-lucide="map-pin" class="w-3 h-3"></i>
                Geo detected
            </span>
            <button onclick="showMapModal()" class="flex items-center gap-1.5 px-2.5 py-1 text-sm bg-[#238636] hover:bg-[#2ea043] text-white rounded">
                <i data-lucide="map" class="w-3.5 h-3.5"></i>
                Map
            </button>`;
    }

    container.innerHTML = `
        ${headerActions ? `<div class="mb-3 flex items-center gap-2">${headerActions}</div>` : ''}
        <div class="card rounded-lg overflow-hidden">
            <div class="overflow-x-auto">
                <table id="results-table" class="w-full text-sm" style="table-layout: fixed;">
                    <thead class="bg-[#161b22] sticky top-0">
                        <tr class="text-left text-[#8b949e]">
                            ${data.columns.map((c, idx) => `<th class="resizable-th px-4 py-2 font-medium border-b border-[#30363d]" style="min-width: 100px;">${c.name} <span class="text-xs text-[#484f58]">${c.type}</span><div class="resize-handle" data-col="${idx}"></div></th>`).join('')}
                        </tr>
                    </thead>
                    <tbody class="mono text-xs">
                        ${data.rows.map((row, i) => `
                            <tr class="hover:bg-[#161b22]">
                                ${row.map((cell, j) => {
                                    let content = cell;
                                    let cls = 'px-4 py-2.5 border-b border-[#21262d]';
                                    if (cell === null) {
                                        content = '<span class="text-[#484f58]">null</span>';
                                    } else if (typeof cell === 'object') {
                                        // Handle objects/arrays (like OSM tags)
                                        const json = JSON.stringify(cell);
                                        const escaped = json.replace(/</g, '&lt;').replace(/>/g, '&gt;');
                                        content = `<span class="text-[#f0883e] max-w-[300px] truncate block" title="${escaped.replace(/"/g, '&quot;')}">${escaped}</span>`;
                                    } else if (geoColIndices.includes(j)) {
                                        content = `<span class="text-[#a371f7] max-w-[200px] truncate block">${cell}</span>`;
                                    } else if (typeof cell === 'number') {
                                        content = `<span class="text-[#79c0ff]">${cell}</span>`;
                                    } else {
                                        // Escape HTML in strings
                                        content = String(cell).replace(/</g, '&lt;').replace(/>/g, '&gt;');
                                    }
                                    return `<td class="${cls}">${content}</td>`;
                                }).join('')}
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        </div>`;
    lucide.createIcons();
    initColumnResize();
}

// Column resize functionality
function initColumnResize() {
    const table = document.getElementById('results-table');
    if (!table) return;

    const handles = table.querySelectorAll('.resize-handle');
    let resizing = null;
    let startX = 0;
    let startWidth = 0;

    handles.forEach(handle => {
        handle.addEventListener('mousedown', (e) => {
            e.preventDefault();
            resizing = handle.parentElement;
            startX = e.pageX;
            startWidth = resizing.offsetWidth;
            handle.classList.add('resizing');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
        });
    });

    document.addEventListener('mousemove', (e) => {
        if (!resizing) return;
        const diff = e.pageX - startX;
        const newWidth = Math.max(80, startWidth + diff);
        resizing.style.width = newWidth + 'px';
    });

    document.addEventListener('mouseup', () => {
        if (resizing) {
            resizing.querySelector('.resize-handle')?.classList.remove('resizing');
            resizing = null;
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        }
    });
}

// Code generation
window.showCode = async function() {
    const currentSource = getActiveDataset();
    if (!currentSource) { showToast('Please select a dataset first', 'warning'); return; }
    const allSources = [currentSource, ...getJoinSources()];
    const res = await fetch('/api/data/code', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sources: allSources, transforms: getTransforms(), output_source_id: currentSource.id })
    });
    const data = await res.json();
    generatedCode = data.code || '';
    document.getElementById('code-modal-content').textContent = generatedCode;
    document.getElementById('code-modal').classList.remove('hidden');
}

window.hideCodeModal = function() { document.getElementById('code-modal').classList.add('hidden'); }
window.copyCodeFromModal = function() { navigator.clipboard.writeText(generatedCode); showToast('Copied to clipboard!', 'success', 2000); }

// Export functionality
window.exportResults = async function(format) {
    const currentSource = getActiveDataset();
    if (!currentSource) {
        showToast('Please select a dataset first', 'warning');
        return;
    }

    const filename = currentSource.name.replace(/\.[^.]+$/, '') + '.' + format;
    const allSources = [currentSource, ...getJoinSources()];

    try {
        const res = await fetch(`/api/data/export/${format}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                sources: allSources,
                transforms: getTransforms(),
                output_source_id: currentSource.id,
                filename: filename
            })
        });

        if (!res.ok) {
            const data = await res.json();
            showToast('Export error: ' + (data.error || 'Unknown error'), 'error');
            return;
        }

        // Download the file
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (e) {
        showToast('Export error: ' + e.message, 'error');
    }
}

// Map
let currentPopup = null;

window.showMapModal = function() {
    if (!currentGeoJSON || !currentGeoJSON.features || currentGeoJSON.features.length === 0) {
        showToast('No geographic data available. Please load a dataset with geometry columns first.', 'warning');
        return;
    }
    document.getElementById('map-modal').classList.remove('hidden');
    if (!mapInstance) {
        mapInstance = new maplibregl.Map({
            container: 'map-container',
            style: { version: 8, sources: { 'carto': { type: 'raster', tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'], tileSize: 256 } }, layers: [{ id: 'carto', type: 'raster', source: 'carto' }] },
            center: [0, 20], zoom: 2
        });
        mapInstance.addControl(new maplibregl.NavigationControl());

        // Popup on click for points
        mapInstance.on('click', 'geojson-point', (e) => {
            if (currentPopup) currentPopup.remove();
            const props = e.features[0].properties;
            const html = `<div class="bg-[#161b22] text-white p-3 rounded-lg text-xs max-w-xs max-h-64 overflow-auto">
                ${Object.entries(props).map(([k, v]) => {
                    let val = v;
                    if (typeof v === 'string' && v.startsWith('{')) {
                        try { val = JSON.stringify(JSON.parse(v), null, 2); } catch(e) {}
                    }
                    return `<div class="mb-1"><span class="text-[#8b949e]">${k}:</span> <span class="text-[#58a6ff]">${val}</span></div>`;
                }).join('')}
            </div>`;
            currentPopup = new maplibregl.Popup({ closeButton: true, className: 'geo-popup' })
                .setLngLat(e.lngLat)
                .setHTML(html)
                .addTo(mapInstance);
        });

        // Popup on click for polygons
        mapInstance.on('click', 'geojson-layer', (e) => {
            if (currentPopup) currentPopup.remove();
            const props = e.features[0].properties;
            const html = `<div class="bg-[#161b22] text-white p-3 rounded-lg text-xs max-w-xs max-h-64 overflow-auto">
                ${Object.entries(props).map(([k, v]) => {
                    let val = v;
                    if (typeof v === 'string' && v.startsWith('{')) {
                        try { val = JSON.stringify(JSON.parse(v), null, 2); } catch(e) {}
                    }
                    return `<div class="mb-1"><span class="text-[#8b949e]">${k}:</span> <span class="text-[#58a6ff]">${val}</span></div>`;
                }).join('')}
            </div>`;
            currentPopup = new maplibregl.Popup({ closeButton: true, className: 'geo-popup' })
                .setLngLat(e.lngLat)
                .setHTML(html)
                .addTo(mapInstance);
        });

        // Hover tooltip
        let hoverPopup = null;

        function getFeatureLabel(props) {
            // Try common label fields
            const labelFields = ['name', 'Name', 'NAME', 'title', 'label', 'id', 'ID', 'osm_id'];
            for (const field of labelFields) {
                if (props[field]) return String(props[field]);
            }
            // Check for OSM tags (could be JSON string or object)
            if (props.tags) {
                let tags = props.tags;
                if (typeof tags === 'string') {
                    try { tags = JSON.parse(tags); } catch(e) { return null; }
                }
                if (tags.name) return tags.name;
                if (tags['name:en']) return tags['name:en'];
                // Fallback to any key that contains 'name'
                for (const [k, v] of Object.entries(tags)) {
                    if (k.toLowerCase().includes('name') && v) return String(v);
                }
            }
            // Fallback: first string property that's not too long
            for (const [k, v] of Object.entries(props)) {
                if (typeof v === 'string' && v.length > 0 && v.length < 50 && !k.startsWith('_')) {
                    return v;
                }
            }
            return null;
        }

        function showHoverTooltip(e) {
            if (!e.features || !e.features[0]) return;
            const props = e.features[0].properties;
            const label = getFeatureLabel(props);
            if (!label) return;

            if (hoverPopup) hoverPopup.remove();
            hoverPopup = new maplibregl.Popup({
                closeButton: false,
                closeOnClick: false,
                className: 'hover-tooltip',
                offset: 10
            })
                .setLngLat(e.lngLat)
                .setHTML(`<div class="bg-[#1c2128] text-white px-2 py-1 rounded text-xs font-medium shadow-lg">${label}</div>`)
                .addTo(mapInstance);
        }

        function hideHoverTooltip() {
            if (hoverPopup) {
                hoverPopup.remove();
                hoverPopup = null;
            }
        }

        // Cursor pointer and tooltip on hover
        mapInstance.on('mouseenter', 'geojson-point', (e) => { mapInstance.getCanvas().style.cursor = 'pointer'; showHoverTooltip(e); });
        mapInstance.on('mouseleave', 'geojson-point', () => { mapInstance.getCanvas().style.cursor = ''; hideHoverTooltip(); });
        mapInstance.on('mousemove', 'geojson-point', showHoverTooltip);

        mapInstance.on('mouseenter', 'geojson-layer', (e) => { mapInstance.getCanvas().style.cursor = 'pointer'; showHoverTooltip(e); });
        mapInstance.on('mouseleave', 'geojson-layer', () => { mapInstance.getCanvas().style.cursor = ''; hideHoverTooltip(); });
        mapInstance.on('mousemove', 'geojson-layer', showHoverTooltip);

        mapInstance.on('mouseenter', 'geojson-line', (e) => { mapInstance.getCanvas().style.cursor = 'pointer'; showHoverTooltip(e); });
        mapInstance.on('mouseleave', 'geojson-line', () => { mapInstance.getCanvas().style.cursor = ''; hideHoverTooltip(); });
        mapInstance.on('mousemove', 'geojson-line', showHoverTooltip);
    }
    setTimeout(() => {
        if (currentGeoJSON?.features.length) {
            ['geojson-layer', 'geojson-line', 'geojson-point'].forEach(l => { if (mapInstance.getLayer(l)) mapInstance.removeLayer(l); });
            if (mapInstance.getSource('geojson')) mapInstance.removeSource('geojson');
            mapInstance.addSource('geojson', { type: 'geojson', data: currentGeoJSON });
            mapInstance.addLayer({ id: 'geojson-layer', type: 'fill', source: 'geojson', paint: { 'fill-color': '#6366f1', 'fill-opacity': 0.5 }, filter: ['==', '$type', 'Polygon'] });
            mapInstance.addLayer({ id: 'geojson-line', type: 'line', source: 'geojson', paint: { 'line-color': '#6366f1', 'line-width': 3 }, filter: ['==', '$type', 'LineString'] });
            mapInstance.addLayer({ id: 'geojson-point', type: 'circle', source: 'geojson', paint: { 'circle-color': '#6366f1', 'circle-radius': 6, 'circle-stroke-color': '#fff', 'circle-stroke-width': 1 }, filter: ['==', '$type', 'Point'] });
            const bounds = new maplibregl.LngLatBounds();
            currentGeoJSON.features.forEach(f => {
                if (f.geometry.type === 'Point') bounds.extend(f.geometry.coordinates);
                else if (f.geometry.type === 'LineString') f.geometry.coordinates.forEach(c => bounds.extend(c));
                else if (f.geometry.type === 'Polygon' && f.geometry.coordinates[0]) f.geometry.coordinates[0].forEach(c => bounds.extend(c));
            });
            if (!bounds.isEmpty()) mapInstance.fitBounds(bounds, { padding: 50 });
        }
    }, 100);
}

window.hideMapModal = function() { document.getElementById('map-modal').classList.add('hidden'); }
window.exportGeoJSON = function() {
    if (!currentGeoJSON) return;
    const blob = new Blob([JSON.stringify(currentGeoJSON, null, 2)], { type: 'application/json' });
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'export.geojson'; a.click();
}

// Modal backdrop clicks
['file-browser-modal', 'source-modal', 'transform-modal', 'code-modal', 'map-modal', 'import-modal', 'save-pipeline-modal', 'load-pipeline-modal'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('click', e => { if (e.target === e.currentTarget) e.target.classList.add('hidden'); });
});

// Drag & Drop file support
let dragCounter = 0;

document.addEventListener('dragenter', (e) => {
    e.preventDefault();
    dragCounter++;
    if (dragCounter === 1) {
        document.getElementById('drop-overlay').classList.remove('hidden');
        lucide.createIcons();
    }
});

document.addEventListener('dragleave', (e) => {
    e.preventDefault();
    dragCounter--;
    if (dragCounter === 0) {
        document.getElementById('drop-overlay').classList.add('hidden');
    }
});

document.addEventListener('dragover', (e) => {
    e.preventDefault();
});

document.addEventListener('drop', async (e) => {
    e.preventDefault();
    dragCounter = 0;
    document.getElementById('drop-overlay').classList.add('hidden');

    const files = e.dataTransfer.files;
    if (files.length === 0) return;

    const file = files[0];
    const validExtensions = ['.csv', '.parquet', '.json', '.pbf', '.osm.pbf'];
    const isValid = validExtensions.some(ext => file.name.toLowerCase().endsWith(ext));

    if (!isValid) {
        showToast('Unsupported file type. Use CSV, Parquet, JSON, or OSM/PBF files.', 'error');
        return;
    }

    // Upload the file to a temp location
    showToast('Uploading file...', 'info', 2000);

    const formData = new FormData();
    formData.append('file', file);

    try {
        const res = await fetch('/api/data/upload', {
            method: 'POST',
            body: formData
        });
        const data = await res.json();

        if (data.error) {
            showToast('Upload error: ' + data.error, 'error');
            return;
        }

        // Create new dataset from uploaded file
        const sourceType = getSourceType(data.path);
        const newId = 'ds_' + Date.now();
        const newDataset = {
            id: newId,
            name: file.name,
            source_type: sourceType,
            path: data.path,
            transforms: [],
            joinSources: []
        };

        // Show OSM layer selector for PBF files
        if (sourceType === 'osm') {
            document.getElementById('source-path').value = data.path;
            showSourceModal();
            document.getElementById('osm-layer-section').classList.remove('hidden');
            showToast('Select OSM layer to continue', 'info');
            return;
        }

        // Add to datasets and set as active
        datasets.push(newDataset);
        activeDatasetId = newId;
        currentSchema = null;
        currentGeoJSON = null;

        renderDatasets();
        renderTransforms();
        saveState();
        lucide.createIcons();

        showToast('File loaded successfully', 'success');
        await previewData();
    } catch (err) {
        showToast('Upload failed: ' + err.message, 'error');
    }
});

// DuckDB Extensions Modal
window.showDuckDBExtensionsModal = function() {
    document.getElementById('duckdb-extensions-modal').classList.remove('hidden');
    lucide.createIcons();
    refreshDuckDBExtensions();
}

window.hideDuckDBExtensionsModal = function() {
    document.getElementById('duckdb-extensions-modal').classList.add('hidden');
}

window.refreshDuckDBExtensions = async function() {
    const container = document.getElementById('duckdb-extensions-list');
    container.innerHTML = '<div class="p-4 text-[#8b949e]">Loading...</div>';

    try {
        const res = await fetch('/api/duckdb/extensions');
        const data = await res.json();

        if (data.error) {
            container.innerHTML = `<div class="p-4 text-red-400">${data.error}</div>`;
            return;
        }

        const showAll = document.getElementById('show-all-duckdb-ext').checked;
        let extensions = data.extensions || [];

        // Filter to show only installed if checkbox is unchecked
        if (!showAll) {
            extensions = extensions.filter(e => e.installed);
        }

        if (extensions.length === 0) {
            container.innerHTML = '<div class="p-4 text-[#8b949e] text-center">No extensions found</div>';
            return;
        }

        container.innerHTML = extensions.map(ext => `
            <div class="flex items-center justify-between px-4 py-3 border-b border-[#21262d] hover:bg-[#21262d]/50">
                <div class="flex-1">
                    <div class="flex items-center gap-2">
                        <span class="font-medium">${ext.name}</span>
                        ${ext.loaded ? '<span class="text-xs px-1.5 py-0.5 rounded bg-[#3fb950]/20 text-[#3fb950]">Loaded</span>' : ''}
                        ${ext.installed && !ext.loaded ? '<span class="text-xs px-1.5 py-0.5 rounded bg-[#8b949e]/20 text-[#8b949e]">Installed</span>' : ''}
                    </div>
                    <div class="text-xs text-[#8b949e] mt-0.5">${ext.description || ''}</div>
                </div>
                <div class="flex items-center gap-2">
                    ${!ext.installed ? `
                        <button onclick="installDuckDBExtension('${ext.name}')" class="px-2.5 py-1 text-xs bg-[#238636] hover:bg-[#2ea043] rounded">
                            Install
                        </button>
                    ` : ''}
                    ${ext.installed && !ext.loaded ? `
                        <button onclick="loadDuckDBExtension('${ext.name}')" class="px-2.5 py-1 text-xs bg-[#21262d] hover:bg-[#30363d] rounded">
                            Load
                        </button>
                    ` : ''}
                </div>
            </div>
        `).join('');
    } catch (err) {
        container.innerHTML = `<div class="p-4 text-red-400">Error: ${err.message}</div>`;
    }
}

window.installDuckDBExtension = async function(name) {
    showToast(`Installing ${name}...`, 'info', 3000);

    try {
        const res = await fetch(`/api/duckdb/extensions/${name}/install`, { method: 'POST' });
        const data = await res.json();

        if (data.success) {
            showToast(data.message || `Extension '${name}' installed`, 'success');
            refreshDuckDBExtensions();
        } else {
            showToast(data.error || `Failed to install ${name}`, 'error');
        }
    } catch (err) {
        showToast(`Error: ${err.message}`, 'error');
    }
}

window.loadDuckDBExtension = async function(name) {
    try {
        const res = await fetch(`/api/duckdb/extensions/${name}/load`, { method: 'POST' });
        const data = await res.json();

        if (data.success) {
            showToast(data.message || `Extension '${name}' loaded`, 'success');
            refreshDuckDBExtensions();
        } else {
            showToast(data.error || `Failed to load ${name}`, 'error');
        }
    } catch (err) {
        showToast(`Error: ${err.message}`, 'error');
    }
}
