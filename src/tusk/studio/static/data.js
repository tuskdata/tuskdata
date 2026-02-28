// Tusk Data Page JavaScript - ETL Pipeline Builder

// State
let datasets = [];  // Array of datasets {id, name, source_type, path, transforms, joinSources, ...}
let activeDatasetId = null;  // Currently selected dataset
let currentSchema = null;
let selectedTransformType = null;
let selectedEngine = localStorage.getItem('tusk_data_engine') || 'auto'; // Engine preference
let pipelineCanvasVisible = localStorage.getItem('tusk_pipeline_canvas') === 'true';
let _syncingFromCanvas = false; // Guard against circular sync

// Data pipeline node types
const DATA_NODE_TYPES = {
    source:    { icon: 'database',     color: 'green',  ports: { in: [],        out: ['output'] } },
    filter:    { icon: 'filter',       color: 'blue',   ports: { in: ['input'], out: ['output'] } },
    select:    { icon: 'columns',      color: 'purple', ports: { in: ['input'], out: ['output'] } },
    sort:      { icon: 'arrow-up-down', color: 'orange', ports: { in: ['input'], out: ['output'] } },
    group_by:  { icon: 'group',        color: 'green',  ports: { in: ['input'], out: ['output'] } },
    rename:    { icon: 'pencil',       color: 'blue',   ports: { in: ['input'], out: ['output'] } },
    drop_nulls:{ icon: 'trash-2',      color: 'red',    ports: { in: ['input'], out: ['output'] } },
    limit:     { icon: 'hash',         color: 'gray',   ports: { in: ['input'], out: ['output'] } },
    join:      { icon: 'git-merge',    color: 'green',  ports: { in: ['left', 'right'], out: ['output'] } },
    concat:    { icon: 'layers',       color: 'orange', ports: { in: ['input', 'other'], out: ['output'] } },
    distinct:  { icon: 'fingerprint',  color: 'purple', ports: { in: ['input'], out: ['output'] } },
    window:    { icon: 'bar-chart-3', color: 'blue',   ports: { in: ['input'], out: ['output'] } },
};

// Pipeline canvas instance (lazy init)
let dataCanvas = null;

function initPipelineCanvas() {
    if (dataCanvas) return dataCanvas;
    dataCanvas = tuskPipeline('data-pipeline', {
        nodeTypes: DATA_NODE_TYPES,
        onNodeDoubleClick: (node) => {
            if (node.type === 'source') return;
            // Find the transform index for this node
            const transforms = getTransforms();
            const idx = transforms.findIndex(t =>
                t.type === node.type && JSON.stringify(t) === JSON.stringify(node.config)
            );
            if (idx >= 0) editTransform(idx);
        },
        onPipelineChange: (state) => {
            if (_syncingFromCanvas) return;
            // Sync canvas changes back to transforms array
            const transforms = dataCanvas.toTransforms();
            _syncingFromCanvas = true;
            setTransforms(transforms);
            renderTransforms();
            saveState();
            _syncingFromCanvas = false;
        },
    });
    return dataCanvas;
}

// Sync current transforms → canvas visualization
function syncCanvasFromTransforms() {
    if (!dataCanvas || !pipelineCanvasVisible) return;
    const ds = getActiveDataset();
    if (!ds) {
        dataCanvas.clear();
        return;
    }
    _syncingFromCanvas = true;
    dataCanvas.fromTransforms(ds, ds.transforms || []);
    _syncingFromCanvas = false;
}

// Toggle canvas visibility
window.togglePipelineCanvas = function() {
    pipelineCanvasVisible = !pipelineCanvasVisible;
    localStorage.setItem('tusk_pipeline_canvas', pipelineCanvasVisible);
    const container = document.getElementById('pipeline-canvas-container');
    const btn = document.getElementById('toggle-canvas-btn');
    if (container) {
        container.classList.toggle('hidden', !pipelineCanvasVisible);
    }
    if (btn) {
        btn.classList.toggle('text-indigo-400', pipelineCanvasVisible);
        btn.classList.toggle('text-[#8b949e]', !pipelineCanvasVisible);
    }
    if (pipelineCanvasVisible) {
        initPipelineCanvas();
        // Wait for Alpine to init, then sync
        setTimeout(() => {
            syncCanvasFromTransforms();
            if (dataCanvas) dataCanvas.fitView();
        }, 100);
    }
}

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
        const icon = ds._plugin ? 'puzzle' : ds.source_type === 'database' ? 'database' : 'file';
        const iconColor = ds._plugin ? 'text-[#a371f7]' : ds.source_type === 'database' ? 'text-[#58a6ff]' : 'text-[#f0883e]';
        let info = ds._plugin ? ds._plugin : ds.source_type === 'database' ? 'DB' : ds.source_type.toUpperCase();
        if (ds.osm_layer) info += ` · ${ds.osm_layer}`;
        if (isActive && currentSchema) info += ` · ${currentSchema.length} cols`;
        return `
            <div class="sidebar-item flex items-center gap-1.5 px-2 py-2 rounded cursor-pointer group ${isActive ? 'bg-[#21262d] ring-1 ring-indigo-500' : 'hover:bg-[#21262d]'}" onclick="setActiveDataset('${ds.id}')">
                <i data-lucide="${icon}" class="w-4 h-4 ${iconColor} flex-shrink-0"></i>
                <div class="flex-1 min-w-0">
                    <div class="text-sm font-medium truncate" id="ds-name-${ds.id}" title="${ds.name}">${ds.name}</div>
                    <div class="text-xs text-[#8b949e] truncate">${info}</div>
                </div>
                <div class="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0">
                    <button onclick="event.stopPropagation(); renameDataset('${ds.id}')" class="p-1 hover:bg-[#30363d] rounded text-[#8b949e] hover:text-white" title="Rename">
                        <i data-lucide="pencil" class="w-3 h-3"></i>
                    </button>
                    <button onclick="event.stopPropagation(); removeDataset('${ds.id}')" class="p-1 hover:bg-[#30363d] rounded text-[#8b949e] hover:text-red-400" title="Remove">
                        <i data-lucide="x" class="w-3 h-3"></i>
                    </button>
                </div>
            </div>
        `;
    }).join('');

    lucide.createIcons();
}

// Toggle cluster visibility for a dataset
window.toggleClusterVisibility = function(id) {
    const ds = datasets.find(d => d.id === id);
    if (!ds) return;
    ds.cluster_enabled = !ds.cluster_enabled;
    renderDatasets();
    saveState();
    if (ds.cluster_enabled) {
        showToast(`"${ds.name}" is now visible in cluster`, 'success');
    } else {
        showToast(`"${ds.name}" hidden from cluster`, 'info');
    }
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
    syncCanvasFromTransforms();
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

// Rename dataset inline
window.renameDataset = function(id) {
    const ds = datasets.find(d => d.id === id);
    if (!ds) return;
    const el = document.getElementById('ds-name-' + id);
    if (!el) return;

    const input = document.createElement('input');
    input.type = 'text';
    input.value = ds.name;
    input.className = 'bg-[#0d1117] border border-indigo-500 rounded px-1 py-0.5 text-sm w-full focus:outline-none';

    function commit() {
        const val = input.value.trim();
        if (val) ds.name = val;
        renderDatasets();
        saveState();
        lucide.createIcons();
    }

    input.addEventListener('blur', commit);
    input.addEventListener('keydown', e => {
        if (e.key === 'Enter') { e.preventDefault(); commit(); }
        if (e.key === 'Escape') { renderDatasets(); lucide.createIcons(); }
    });

    el.innerHTML = '';
    el.appendChild(input);
    input.focus();
    input.select();
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
// Initialize data page (works with both full page load and hx-boost navigation)
function _initDataPage() {
    loadState();
    renderSidebarPipelines();

    // Toggle filter value field based on operator
    const filterOp = document.getElementById('filter-operator');
    const filterValueRow = document.getElementById('filter-value')?.parentElement;
    if (filterOp && filterValueRow) {
        filterOp.addEventListener('change', () => {
            const noValueOps = ['is_null', 'is_not_null', 'is_empty', 'is_not_empty'];
            filterValueRow.style.display = noValueOps.includes(filterOp.value) ? 'none' : '';
        });
    }

    // Restore pipeline canvas visibility
    if (pipelineCanvasVisible) {
        const container = document.getElementById('pipeline-canvas-container');
        const btn = document.getElementById('toggle-canvas-btn');
        if (container) container.classList.remove('hidden');
        if (btn) {
            btn.classList.add('text-indigo-400');
            btn.classList.remove('text-[#8b949e]');
        }
        initPipelineCanvas();
        setTimeout(() => syncCanvasFromTransforms(), 200);
    }
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initDataPage);
} else {
    _initDataPage();
}

// File Browser
window.showFileBrowser = function() {
    window.dispatchEvent(new Event('open-file-browser'));
    browseTo('~');
}

window.hideFileBrowser = function() {
    window.dispatchEvent(new Event('close-file-browser'));
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
    document.getElementById('source-name').value = '';
    document.getElementById('source-path').value = '';
    document.getElementById('osm-layer-section').classList.add('hidden');
    document.getElementById('source-schema').classList.add('hidden');

    window.dispatchEvent(new Event('open-source-modal'));
    setSourceTab('file');
    // Load connections for DB tab
    await loadSourceConnections();
    lucide.createIcons();
}

window.hideSourceModal = function() {
    window.dispatchEvent(new Event('close-source-modal'));
    document.getElementById('source-schema').classList.add('hidden');
    document.getElementById('osm-layer-section').classList.add('hidden');
    document.getElementById('db-source-schema')?.classList.add('hidden');
    document.getElementById('source-path').value = '';
}

window.setSourceTab = function(tab) {
    currentSourceTab = tab;
    const tabs = ['file', 'database', 'plugin'];
    const sections = {
        file: document.getElementById('source-file-section'),
        database: document.getElementById('source-db-section'),
        plugin: document.getElementById('source-plugin-section'),
    };

    for (const t of tabs) {
        const btn = document.getElementById('source-tab-' + t);
        const section = sections[t];
        if (!btn || !section) continue;
        if (t === tab) {
            btn.classList.add('bg-[#21262d]', 'text-white');
            btn.classList.remove('text-[#8b949e]');
            section.classList.remove('hidden');
        } else {
            btn.classList.remove('bg-[#21262d]', 'text-white');
            btn.classList.add('text-[#8b949e]');
            section.classList.add('hidden');
        }
    }

    if (tab === 'plugin') loadPluginDatasets();
}

let _pluginDatasets = [];
let _selectedPluginDataset = null;

async function loadPluginDatasets() {
    const container = document.getElementById('plugin-datasets-list');
    container.innerHTML = '<div class="text-xs text-[#8b949e] py-4 text-center">Loading...</div>';
    _selectedPluginDataset = null;

    try {
        const res = await fetch('/api/data/plugin-datasets');
        const data = await res.json();
        _pluginDatasets = data.datasets || [];

        if (_pluginDatasets.length === 0) {
            container.innerHTML = '<div class="text-xs text-[#8b949e] py-4 text-center">No plugin datasets available.<br/>Plugins with storage expose datasets here.</div>';
            return;
        }

        container.innerHTML = _pluginDatasets.map((ds, i) => `
            <button onclick="selectPluginDataset(${i})" id="plugin-ds-${i}"
                class="w-full text-left p-3 rounded-lg border border-[#30363d] hover:border-[#6366f1] hover:bg-[#6366f1]/5 transition-colors">
                <div class="flex items-center gap-2">
                    <i data-lucide="database" class="w-4 h-4 text-[#a371f7]"></i>
                    <span class="text-sm font-medium">${ds.table}</span>
                    <span class="text-xs text-[#8b949e] ml-auto">${ds.plugin}</span>
                </div>
                <div class="text-xs text-[#8b949e] mt-1">${ds.description || ''}</div>
                <div class="text-xs text-[#484f58] mt-1 font-mono">sqlite_scan('${ds.db_path}', '${ds.table}')</div>
            </button>
        `).join('');
        lucide.createIcons();
    } catch (e) {
        container.innerHTML = '<div class="text-xs text-red-400 py-4 text-center">Failed to load plugin datasets</div>';
    }
}

window.selectPluginDataset = function(idx) {
    _selectedPluginDataset = _pluginDatasets[idx];
    // Highlight selected
    _pluginDatasets.forEach((_, i) => {
        const el = document.getElementById('plugin-ds-' + i);
        if (el) {
            if (i === idx) {
                el.classList.add('border-[#6366f1]', 'bg-[#6366f1]/10');
            } else {
                el.classList.remove('border-[#6366f1]', 'bg-[#6366f1]/10');
            }
        }
    });
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
    const customName = document.getElementById('source-name').value.trim();
    let newDataset;

    if (currentSourceTab === 'file') {
        // File source
        const path = document.getElementById('source-path').value.trim();
        if (!path) { showToast('Please enter a file path', 'warning'); return; }

        const sourceType = getSourceType(path);
        // Auto-name: folder name if no custom name
        const autoName = path.split('/').filter(Boolean).slice(-1)[0] || path.split('/').pop();
        newDataset = {
            id: newId,
            name: customName || autoName,
            source_type: sourceType,
            path: path,
            transforms: [],
            joinSources: []
        };
        if (sourceType === 'osm') newDataset.osm_layer = document.getElementById('osm-layer').value;
    } else if (currentSourceTab === 'plugin') {
        // Plugin dataset source (via DuckDB sqlite_scan)
        if (!_selectedPluginDataset) { showToast('Please select a plugin dataset', 'warning'); return; }
        const ds = _selectedPluginDataset;
        newDataset = {
            id: newId,
            name: customName || ds.table,
            source_type: 'database',
            query: `SELECT * FROM sqlite_scan('${ds.db_path}', '${ds.table}')`,
            transforms: [],
            joinSources: [],
            _plugin: ds.plugin,
        };
    } else {
        // Database source
        const connId = document.getElementById('source-db-connection').value;
        if (!connId) { showToast('Please select a connection', 'warning'); return; }

        const conn = dbConnections.find(c => c.id === connId);
        let autoName, query;

        if (currentDbSourceType === 'table') {
            const table = document.getElementById('source-db-table').value;
            if (!table) { showToast('Please select a table', 'warning'); return; }
            autoName = table;
            query = `SELECT * FROM ${table}`;
        } else {
            query = document.getElementById('source-db-query').value.trim();
            if (!query) { showToast('Please enter a query', 'warning'); return; }
            autoName = 'Custom Query';
        }

        newDataset = {
            id: newId,
            name: customName || autoName,
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

window.changeEngine = function() {
    selectedEngine = document.getElementById('engine-select').value;
    localStorage.setItem('tusk_data_engine', selectedEngine);
    if (getActiveDataset()) previewData();
}

function updateEngineBadge(engineUsed, elapsedMs, fallback) {
    const badge = document.getElementById('engine-badge');
    if (!badge) return;
    const colors = {
        duckdb: 'text-[#f0c000] bg-[#f0c000]/10',
        polars: 'text-[#58a6ff] bg-[#58a6ff]/10',
    };
    const label = engineUsed === 'duckdb' ? 'DuckDB' : 'Polars';
    const fallbackNote = fallback ? ' (fallback)' : '';
    const timeNote = elapsedMs != null ? ` · ${elapsedMs}ms` : '';
    badge.className = `text-xs px-2 py-0.5 rounded-full ${colors[engineUsed] || colors.polars}`;
    badge.textContent = `${label}${fallbackNote}${timeNote}`;
    badge.classList.remove('hidden');
}

// Restore engine selector on page load
document.addEventListener('DOMContentLoaded', function() {
    const sel = document.getElementById('engine-select');
    if (sel) sel.value = selectedEngine;
});

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
        let url = `/api/data/files/preview?path=${encodeURIComponent(currentSource.path)}&limit=${limit}&engine=${selectedEngine}`;
        if (currentSource.source_type === 'osm' && currentSource.osm_layer) {
            url += `&osm_layer=${encodeURIComponent(currentSource.osm_layer)}`;
        }
        const res = await fetch(url);
        data = await res.json();
    }

    // Show engine badge if available
    if (data.engine_used) {
        updateEngineBadge(data.engine_used, data.elapsed_ms, data.engine_fallback);
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
    editingTransformIndex = null;
    document.getElementById('transform-type-select').classList.remove('hidden');
    document.getElementById('transform-config').classList.add('hidden');
    document.getElementById('add-transform-btn').classList.add('hidden');
    document.getElementById('add-transform-btn').textContent = 'Add Transform';
    selectedTransformType = null;
    const filterValueRow = document.getElementById('filter-value')?.parentElement;
    if (filterValueRow) filterValueRow.style.display = '';
    const filterOp = document.getElementById('filter-operator');
    if (filterOp) filterOp.value = 'eq';
    window.dispatchEvent(new Event('open-transform-modal'));
}

window.hideTransformModal = function() {
    window.dispatchEvent(new Event('close-transform-modal'));
    document.querySelectorAll('[id^="config-"]').forEach(el => el.classList.add('hidden'));
    editingTransformIndex = null;
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
    document.getElementById('rename-from').innerHTML = options;
    document.getElementById('join-left-on').innerHTML = options;

    // Initialize agg rows if empty
    const aggContainer = document.getElementById('agg-rows');
    if (aggContainer && aggContainer.children.length === 0) {
        addAggRow();
    }
    // Update existing agg row selects
    document.querySelectorAll('.agg-col').forEach(sel => sel.innerHTML = options);

    const checkboxes = (id, cls) => currentSchema.map(c =>
        `<label class="flex items-center gap-2 cursor-pointer"><input type="checkbox" value="${c.name}" class="${cls} accent-indigo-500"><span class="text-sm">${c.name}</span></label>`
    ).join('');
    document.getElementById('select-columns').innerHTML = currentSchema.map(c =>
        `<label class="flex items-center gap-2 cursor-pointer"><input type="checkbox" value="${c.name}" class="select-col-check accent-indigo-500" checked><span class="text-sm">${c.name}</span></label>`
    ).join('');
    document.getElementById('groupby-columns').innerHTML = checkboxes('groupby-columns', 'groupby-col-check');
    document.getElementById('dropnulls-columns').innerHTML = checkboxes('dropnulls-columns', 'dropnulls-col-check');
    document.getElementById('distinct-columns').innerHTML = checkboxes('distinct-columns', 'distinct-col-check');
    // Window function selects
    document.getElementById('window-column').innerHTML = options;
    document.getElementById('window-order-by').innerHTML = options;
    const partOptions = '<option value="">No partition</option>' + options;
    document.getElementById('window-partition-by').innerHTML = partOptions;
}

// Multi-aggregation rows for group_by
window.addAggRow = function(col, fn) {
    const container = document.getElementById('agg-rows');
    if (!container) return;
    const options = currentSchema
        ? currentSchema.map(c => `<option value="${c.name}" ${c.name === col ? 'selected' : ''}>${c.name}</option>`).join('')
        : '';
    const fnOptions = ['sum', 'mean', 'min', 'max', 'count', 'first', 'last'].map(
        f => `<option value="${f}" ${f === fn ? 'selected' : ''}>${f.charAt(0).toUpperCase() + f.slice(1)}</option>`
    ).join('');
    const row = document.createElement('div');
    row.className = 'agg-row flex gap-2 items-center';
    row.innerHTML = `
        <select class="agg-col flex-1 bg-[#0d1117] border border-[#30363d] rounded-lg px-3 py-2 text-sm">${options}</select>
        <select class="agg-fn w-24 bg-[#0d1117] border border-[#30363d] rounded-lg px-3 py-2 text-sm">${fnOptions}</select>
        <button type="button" onclick="this.parentElement.remove()" class="p-1.5 hover:bg-[#30363d] rounded text-[#8b949e] hover:text-red-400" title="Remove">
            <i data-lucide="x" class="w-3.5 h-3.5"></i>
        </button>
    `;
    container.appendChild(row);
    if (typeof lucide !== 'undefined') lucide.createIcons();
}

// Toggle window column/offset fields based on selected function
window.toggleWindowColumnField = function() {
    const fn = document.getElementById('window-function').value;
    const needsCol = ['lag', 'lead', 'cum_sum', 'cum_max', 'cum_min'].includes(fn);
    const needsOffset = ['lag', 'lead'].includes(fn);
    document.getElementById('window-column-field').classList.toggle('hidden', !needsCol);
    document.getElementById('window-offset-field').classList.toggle('hidden', !needsOffset);
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
        // Collect all aggregation rows
        t.aggregations = [];
        document.querySelectorAll('.agg-row').forEach(row => {
            const col = row.querySelector('.agg-col').value;
            const fn = row.querySelector('.agg-fn').value;
            if (col) t.aggregations.push({ column: col, agg: fn });
        });
        if (t.aggregations.length === 0) {
            showToast('Add at least one aggregation', 'warning');
            return;
        }
    } else if (t.type === 'rename') {
        t.mapping = { [document.getElementById('rename-from').value]: document.getElementById('rename-to').value };
    } else if (t.type === 'drop_nulls') {
        const checked = document.querySelectorAll('.dropnulls-col-check:checked');
        t.subset = checked.length > 0 ? Array.from(checked).map(el => el.value) : null;
    } else if (t.type === 'limit') {
        t.n = parseInt(document.getElementById('limit-n').value);
    } else if (t.type === 'join') {
        let joinSourceId;
        const currentTransforms = getTransforms();
        const currentJoinSources = getJoinSources();

        if (joinSourceTab === 'database') {
            const connId = document.getElementById('join-db-connection').value;
            const tableName = document.getElementById('join-db-table').value;
            const customSql = document.getElementById('join-db-sql').value.trim();
            if (!connId) { showToast('Please select a database connection', 'warning'); return; }
            if (!tableName && !customSql) { showToast('Please select a table or write a SQL query', 'warning'); return; }

            const query = customSql || `SELECT * FROM ${tableName}`;
            if (editingTransformIndex !== null && currentTransforms[editingTransformIndex]?.type === 'join') {
                joinSourceId = currentTransforms[editingTransformIndex].right_source_id;
                const existingSource = currentJoinSources.find(s => s.id === joinSourceId);
                if (existingSource) {
                    existingSource.source_type = 'database';
                    existingSource.connection_id = parseInt(connId);
                    existingSource.query = query;
                    existingSource.name = tableName || 'custom_query';
                    delete existingSource.path;
                }
            } else {
                joinSourceId = 'join_' + Date.now();
                currentJoinSources.push({
                    id: joinSourceId,
                    name: tableName || 'custom_query',
                    source_type: 'database',
                    connection_id: parseInt(connId),
                    query: query
                });
                setJoinSources(currentJoinSources);
            }
        } else {
            const joinPath = document.getElementById('join-file-path').value.trim();
            if (!joinPath) { showToast('Please select a file to join with', 'warning'); return; }

            if (editingTransformIndex !== null && currentTransforms[editingTransformIndex]?.type === 'join') {
                joinSourceId = currentTransforms[editingTransformIndex].right_source_id;
                const existingSource = currentJoinSources.find(s => s.id === joinSourceId);
                if (existingSource) {
                    existingSource.path = joinPath;
                    existingSource.name = joinPath.split('/').pop();
                    existingSource.source_type = getSourceType(joinPath);
                    delete existingSource.connection_id;
                    delete existingSource.query;
                }
            } else {
                joinSourceId = 'join_' + Date.now();
                currentJoinSources.push({
                    id: joinSourceId,
                    name: joinPath.split('/').pop(),
                    source_type: getSourceType(joinPath),
                    path: joinPath
                });
                setJoinSources(currentJoinSources);
            }
        }

        t.right_source_id = joinSourceId;
        t.left_on = [document.getElementById('join-left-on').value];
        t.right_on = [document.getElementById('join-right-on').value || document.getElementById('join-left-on').value];
        t.how = document.getElementById('join-how').value;
    } else if (t.type === 'concat') {
        let concatSourceId;
        const currentTransforms = getTransforms();
        const currentJoinSources = getJoinSources();

        if (concatSourceTab === 'database') {
            const connId = document.getElementById('concat-db-connection').value;
            const tableName = document.getElementById('concat-db-table').value;
            const customSql = document.getElementById('concat-db-sql').value.trim();
            if (!connId) { showToast('Please select a database connection', 'warning'); return; }
            if (!tableName && !customSql) { showToast('Please select a table or write a SQL query', 'warning'); return; }

            const query = customSql || `SELECT * FROM ${tableName}`;
            if (editingTransformIndex !== null && currentTransforms[editingTransformIndex]?.type === 'concat') {
                concatSourceId = currentTransforms[editingTransformIndex].source_ids[0];
                const existingSource = currentJoinSources.find(s => s.id === concatSourceId);
                if (existingSource) {
                    existingSource.source_type = 'database';
                    existingSource.connection_id = parseInt(connId);
                    existingSource.query = query;
                    existingSource.name = tableName || 'custom_query';
                    delete existingSource.path;
                }
            } else {
                concatSourceId = 'concat_' + Date.now();
                currentJoinSources.push({
                    id: concatSourceId,
                    name: tableName || 'custom_query',
                    source_type: 'database',
                    connection_id: parseInt(connId),
                    query: query
                });
                setJoinSources(currentJoinSources);
            }
        } else {
            const concatPath = document.getElementById('concat-file-path').value.trim();
            if (!concatPath) { showToast('Please select a file to concat with', 'warning'); return; }

            if (editingTransformIndex !== null && currentTransforms[editingTransformIndex]?.type === 'concat') {
                concatSourceId = currentTransforms[editingTransformIndex].source_ids[0];
                const existingSource = currentJoinSources.find(s => s.id === concatSourceId);
                if (existingSource) {
                    existingSource.path = concatPath;
                    existingSource.name = concatPath.split('/').pop();
                    existingSource.source_type = getSourceType(concatPath);
                    delete existingSource.connection_id;
                    delete existingSource.query;
                }
            } else {
                concatSourceId = 'concat_' + Date.now();
                currentJoinSources.push({
                    id: concatSourceId,
                    name: concatPath.split('/').pop(),
                    source_type: getSourceType(concatPath),
                    path: concatPath
                });
                setJoinSources(currentJoinSources);
            }
        }
        t.source_ids = [concatSourceId];
        t.how = document.getElementById('concat-how').value;
    } else if (t.type === 'distinct') {
        const checked = document.querySelectorAll('.distinct-col-check:checked');
        t.subset = checked.length > 0 ? Array.from(checked).map(el => el.value) : null;
        t.keep = document.getElementById('distinct-keep').value;
    } else if (t.type === 'window') {
        t.function = document.getElementById('window-function').value;
        t.order_by = [document.getElementById('window-order-by').value];
        const partBy = document.getElementById('window-partition-by').value;
        t.partition_by = partBy ? [partBy] : null;
        t.descending = document.getElementById('window-descending').checked;
        t.alias = document.getElementById('window-alias').value || 'window_col';
        const fn = t.function;
        if (['lag', 'lead', 'cum_sum', 'cum_max', 'cum_min'].includes(fn)) {
            t.column = document.getElementById('window-column').value;
            if (!t.column) { showToast('Select a column for this function', 'warning'); return; }
        }
        if (['lag', 'lead'].includes(fn)) {
            t.offset = parseInt(document.getElementById('window-offset').value) || 1;
        }
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
    syncCanvasFromTransforms();
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
        join: { icon: 'git-merge', color: '#3fb950' },
        concat: { icon: 'layers', color: '#f0883e' },
        distinct: { icon: 'fingerprint', color: '#a371f7' },
        window: { icon: 'bar-chart-3', color: '#79c0ff' }
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
        else if (t.type === 'concat') {
            const concatSource = getJoinSources().find(s => t.source_ids?.includes(s.id));
            desc = concatSource ? concatSource.name : `${t.source_ids?.length || 0} sources`;
        }
        else if (t.type === 'distinct') desc = t.subset ? t.subset.join(', ') : 'all columns';
        else if (t.type === 'window') desc = `${t.function}(${t.column || ''}) → ${t.alias}`;

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
    syncCanvasFromTransforms();
    saveState();
}

window.editTransform = function(index) {
    const t = getTransforms()[index];
    if (!t) return;

    editingTransformIndex = index;

    // Open modal and select the transform type
    document.getElementById('transform-type-select').classList.add('hidden');
    document.getElementById('transform-config').classList.remove('hidden');
    document.getElementById('add-transform-btn').classList.remove('hidden');
    document.getElementById('add-transform-btn').textContent = 'Update Transform';
    window.dispatchEvent(new Event('open-transform-modal'));

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
                // Populate aggregation rows
                const aggContainer = document.getElementById('agg-rows');
                aggContainer.innerHTML = '';
                if (t.aggregations?.length > 0) {
                    t.aggregations.forEach(agg => {
                        addAggRow(agg.column, agg.agg);
                    });
                } else {
                    addAggRow();
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
                const joinPath = joinSource?.path || '';
                document.getElementById('join-file-path').value = joinPath;
                document.getElementById('join-left-on').value = t.left_on?.[0] || '';
                document.getElementById('join-how').value = t.how || 'inner';
                // Fetch right table schema, then set the selected value
                if (joinPath) {
                    fetchRightTableColumns(joinPath).then(() => {
                        document.getElementById('join-right-on').value = t.right_on?.[0] || '';
                    });
                }
                break;

            case 'concat':
                const concatSource = getJoinSources().find(s => t.source_ids?.includes(s.id));
                document.getElementById('concat-file-path').value = concatSource?.path || '';
                document.getElementById('concat-how').value = t.how || 'vertical';
                break;

            case 'distinct':
                document.querySelectorAll('.distinct-col-check').forEach(cb => {
                    cb.checked = t.subset ? t.subset.includes(cb.value) : false;
                });
                document.getElementById('distinct-keep').value = t.keep || 'first';
                break;

            case 'window':
                document.getElementById('window-function').value = t.function || 'row_number';
                document.getElementById('window-order-by').value = t.order_by?.[0] || '';
                document.getElementById('window-partition-by').value = t.partition_by?.[0] || '';
                document.getElementById('window-descending').checked = t.descending || false;
                document.getElementById('window-alias').value = t.alias || 'window_col';
                if (t.column) document.getElementById('window-column').value = t.column;
                if (t.offset) document.getElementById('window-offset').value = t.offset;
                toggleWindowColumnField();
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
        body: JSON.stringify({ sources: allSources, transforms: getTransforms(), output_source_id: currentSource.id, limit, engine: selectedEngine })
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
        fetchRightTableColumns(path);
    };
    showFileBrowser();
}

async function fetchRightTableColumns(path) {
    const sel = document.getElementById('join-right-on');
    if (!sel) return;
    sel.innerHTML = '<option value="">Loading...</option>';
    try {
        const res = await fetch(`/api/data/files/schema?path=${encodeURIComponent(path)}`);
        const data = await res.json();
        if (data.columns?.length > 0) {
            sel.innerHTML = data.columns.map(c =>
                `<option value="${c.name}">${c.name} <span class="text-xs">(${c.type})</span></option>`
            ).join('');
        } else {
            sel.innerHTML = '<option value="">No columns found</option>';
        }
    } catch {
        sel.innerHTML = '<option value="">Error loading schema</option>';
    }
}

window.browseConcatFile = function() {
    fileBrowserCallback = (path) => {
        document.getElementById('concat-file-path').value = path;
    };
    showFileBrowser();
}

// ─── Join/Concat Database Source Support ─────────────────────
let joinSourceTab = 'file';
let concatSourceTab = 'file';

function _populateConnectionDropdown(selectId) {
    const select = document.getElementById(selectId);
    if (!select) return;
    select.innerHTML = '<option value="">Select a connection...</option>' +
        dbConnections.filter(c => c.type === 'postgres').map(c =>
            `<option value="${c.id}">${c.name} (${c.type})</option>`
        ).join('');
}

window.setJoinSourceTab = function(tab) {
    joinSourceTab = tab;
    const fileEl = document.getElementById('join-source-file');
    const dbEl = document.getElementById('join-source-database');
    const fileTab = document.getElementById('join-tab-file');
    const dbTab = document.getElementById('join-tab-database');
    if (tab === 'file') {
        fileEl.classList.remove('hidden'); dbEl.classList.add('hidden');
        fileTab.classList.add('bg-[#21262d]', 'text-white'); fileTab.classList.remove('text-[#8b949e]');
        dbTab.classList.remove('bg-[#21262d]', 'text-white'); dbTab.classList.add('text-[#8b949e]');
    } else {
        fileEl.classList.add('hidden'); dbEl.classList.remove('hidden');
        dbTab.classList.add('bg-[#21262d]', 'text-white'); dbTab.classList.remove('text-[#8b949e]');
        fileTab.classList.remove('bg-[#21262d]', 'text-white'); fileTab.classList.add('text-[#8b949e]');
        _populateConnectionDropdown('join-db-connection');
    }
}

window.setConcatSourceTab = function(tab) {
    concatSourceTab = tab;
    const fileEl = document.getElementById('concat-source-file');
    const dbEl = document.getElementById('concat-source-database');
    const fileTab = document.getElementById('concat-tab-file');
    const dbTab = document.getElementById('concat-tab-database');
    if (tab === 'file') {
        fileEl.classList.remove('hidden'); dbEl.classList.add('hidden');
        fileTab.classList.add('bg-[#21262d]', 'text-white'); fileTab.classList.remove('text-[#8b949e]');
        dbTab.classList.remove('bg-[#21262d]', 'text-white'); dbTab.classList.add('text-[#8b949e]');
    } else {
        fileEl.classList.add('hidden'); dbEl.classList.remove('hidden');
        dbTab.classList.add('bg-[#21262d]', 'text-white'); dbTab.classList.remove('text-[#8b949e]');
        fileTab.classList.remove('bg-[#21262d]', 'text-white'); fileTab.classList.add('text-[#8b949e]');
        _populateConnectionDropdown('concat-db-connection');
    }
}

window.loadJoinDbTables = async function() {
    const connId = document.getElementById('join-db-connection').value;
    const tableSelect = document.getElementById('join-db-table');
    if (!connId) { tableSelect.innerHTML = '<option value="">Select connection first...</option>'; return; }
    tableSelect.innerHTML = '<option value="">Loading...</option>';
    try {
        const res = await fetch(`/api/connections/${connId}/schema`);
        const data = await res.json();
        const tables = data.tables || data.schema || [];
        tableSelect.innerHTML = '<option value="">Select a table...</option>' +
            tables.map(t => {
                const name = typeof t === 'string' ? t : (t.table_name || t.name);
                return `<option value="${name}">${name}</option>`;
            }).join('');
    } catch {
        tableSelect.innerHTML = '<option value="">Error loading tables</option>';
    }
}

window.selectJoinDbTable = async function() {
    const connId = document.getElementById('join-db-connection').value;
    const tableName = document.getElementById('join-db-table').value;
    const rightOnSelect = document.getElementById('join-right-on');
    if (!connId || !tableName) return;
    rightOnSelect.innerHTML = '<option value="">Loading...</option>';
    try {
        const res = await fetch(`/api/connections/${connId}/schema`);
        const data = await res.json();
        const tables = data.tables || data.schema || [];
        const table = tables.find(t => (typeof t === 'string' ? t : (t.table_name || t.name)) === tableName);
        const columns = table?.columns || [];
        if (columns.length > 0) {
            rightOnSelect.innerHTML = columns.map(c => {
                const name = typeof c === 'string' ? c : (c.column_name || c.name);
                return `<option value="${name}">${name}</option>`;
            }).join('');
        } else {
            rightOnSelect.innerHTML = '<option value="">No columns found</option>';
        }
    } catch {
        rightOnSelect.innerHTML = '<option value="">Error loading columns</option>';
    }
}

window.loadConcatDbTables = async function() {
    const connId = document.getElementById('concat-db-connection').value;
    const tableSelect = document.getElementById('concat-db-table');
    if (!connId) { tableSelect.innerHTML = '<option value="">Select connection first...</option>'; return; }
    tableSelect.innerHTML = '<option value="">Loading...</option>';
    try {
        const res = await fetch(`/api/connections/${connId}/schema`);
        const data = await res.json();
        const tables = data.tables || data.schema || [];
        tableSelect.innerHTML = '<option value="">Select a table...</option>' +
            tables.map(t => {
                const name = typeof t === 'string' ? t : (t.table_name || t.name);
                return `<option value="${name}">${name}</option>`;
            }).join('');
    } catch {
        tableSelect.innerHTML = '<option value="">Error loading tables</option>';
    }
}

// Import to DB functions
let pgConnections = [];

window.showImportModal = async function() {
    const currentSource = getActiveDataset();
    if (!currentSource) { showToast('Please select a dataset first', 'warning'); return; }
    window.dispatchEvent(new Event('open-import-modal'));
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
    window.dispatchEvent(new Event('close-import-modal'));
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
    const importBtn = document.getElementById('import-btn');
    if (!importBtn) { showToast('Import button not found', 'error'); return; }
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
    window.dispatchEvent(new Event('open-save-modal'));
    document.getElementById('save-pipeline-name').value = currentSource.name.replace(/\.[^.]+$/, '') + ' Pipeline';
    document.getElementById('save-pipeline-name').select();
}

window.hideSavePipelineModal = function() {
    window.dispatchEvent(new Event('close-save-modal'));
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
    renderSidebarPipelines();
}

window.showLoadPipelineModal = function() {
    window.dispatchEvent(new Event('open-load-modal'));
    renderSavedPipelines();
    lucide.createIcons();
}

window.hideLoadPipelineModal = function() {
    window.dispatchEvent(new Event('close-load-modal'));
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

window.deletePipeline = async function(index) {
    if (!await tuskConfirm('Delete this pipeline?')) return;

    let pipelines = [];
    try {
        pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]');
    } catch (e) { return; }

    pipelines.splice(index, 1);
    localStorage.setItem('tusk_saved_pipelines', JSON.stringify(pipelines));
    renderSavedPipelines();
    renderSidebarPipelines();
}

function renderSidebarPipelines() {
    const container = document.getElementById('sidebar-pipelines');
    if (!container) return;

    let pipelines = [];
    try {
        pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]');
    } catch (e) {}

    if (pipelines.length === 0) {
        container.innerHTML = '<div class="text-[#8b949e] text-xs py-1">No saved pipelines</div>';
        return;
    }

    container.innerHTML = pipelines.map((p, i) => {
        const clusterColor = p.cluster_enabled ? 'text-green-400' : 'text-[#484f58]';
        const clusterTitle = p.cluster_enabled ? 'Shared with cluster' : 'Share with cluster';
        return `
        <div class="flex items-center gap-1.5 px-2 py-1.5 rounded hover:bg-[#21262d] cursor-pointer group text-sm" onclick="loadPipeline(${i})">
            <i data-lucide="git-branch" class="w-3.5 h-3.5 text-indigo-400 flex-shrink-0"></i>
            <span class="truncate flex-1" title="${p.name}">${p.name}</span>
            <span class="text-xs text-[#8b949e] flex-shrink-0">${p.transforms?.length || 0}t</span>
            <button onclick="event.stopPropagation(); togglePipelineCluster(${i})" class="p-0.5 ${clusterColor} opacity-0 group-hover:opacity-100" title="${clusterTitle}">
                <i data-lucide="globe" class="w-3 h-3"></i>
            </button>
            <button onclick="event.stopPropagation(); deletePipeline(${i})" class="p-0.5 hover:text-red-400 text-[#8b949e] opacity-0 group-hover:opacity-100" title="Delete">
                <i data-lucide="x" class="w-3 h-3"></i>
            </button>
        </div>`;
    }).join('');

    lucide.createIcons();
}

window.togglePipelineCluster = function(index) {
    let pipelines = [];
    try { pipelines = JSON.parse(localStorage.getItem('tusk_saved_pipelines') || '[]'); } catch (e) {}
    if (index < 0 || index >= pipelines.length) return;

    pipelines[index].cluster_enabled = !pipelines[index].cluster_enabled;
    localStorage.setItem('tusk_saved_pipelines', JSON.stringify(pipelines));
    renderSidebarPipelines();

    const name = pipelines[index].name;
    if (pipelines[index].cluster_enabled) {
        showToast(`"${name}" shared with cluster`, 'success');
    } else {
        showToast(`"${name}" removed from cluster`, 'info');
    }
}

function _flattenCoords(geom) {
    const coords = [];
    const t = geom.type, c = geom.coordinates;
    if (t === 'Point') coords.push(c);
    else if (t === 'MultiPoint' || t === 'LineString') coords.push(...c);
    else if (t === 'MultiLineString' || t === 'Polygon') c.forEach(r => coords.push(...r));
    else if (t === 'MultiPolygon') c.forEach(p => p.forEach(r => coords.push(...r)));
    return coords.filter(c => Array.isArray(c) && c.length >= 2 && isFinite(c[0]) && isFinite(c[1]));
}

function detectGeoColumns(columns, rows) {
    if (!columns) return [];
    // Check column names first
    const geoNames = ['geometry', 'geom', 'wkt', 'the_geom', 'shape', 'geo', 'location', 'coordinates', 'coord', 'latlon', 'point'];
    const byName = columns.map((c, i) => geoNames.includes(c.name.toLowerCase()) ? i : -1).filter(i => i !== -1);
    if (byName.length > 0) return byName;

    // Also check content - look for GeoJSON or WKT patterns in first row
    if (rows && rows.length > 0) {
        const wktPattern = /^(POINT|POLYGON|MULTIPOLYGON|LINESTRING|MULTILINESTRING|GEOMETRYCOLLECTION)\s*(Z|M|ZM)?\s*\(/i;
        const geoTypes = ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection'];
        for (let i = 0; i < columns.length; i++) {
            const val = rows[0][i];
            if (typeof val !== 'string') continue;
            // GeoJSON string (from ST_AsGeoJSON)
            if (val.startsWith('{') && val.includes('"type"')) {
                try { if (geoTypes.includes(JSON.parse(val).type)) return [i]; } catch (e) {}
            }
            // WKT string (fallback / manual queries)
            if (wktPattern.test(val)) return [i];
        }
    }
    return [];
}

function parseWKT(wkt) {
    if (!wkt || typeof wkt !== 'string') return null;
    let cleanWkt = wkt.replace(/^SRID=\d+;/i, '').trim();

    // Strip Z/M/ZM dimension modifiers: "POLYGON Z ((" → "POLYGON (("
    cleanWkt = cleanWkt.replace(/^(MULTI)?(POINT|LINESTRING|POLYGON|GEOMETRYCOLLECTION)\s+(Z|M|ZM)\s*\(/i, '$1$2 (');

    // POINT
    const pointMatch = cleanWkt.match(/^POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)/i);
    if (pointMatch) return { type: 'Point', coordinates: [parseFloat(pointMatch[1]), parseFloat(pointMatch[2])] };

    // POLYGON
    if (/^POLYGON\s*\(/i.test(cleanWkt)) {
        const rings = _extractPolygonRings(cleanWkt);
        if (rings.length > 0) return { type: 'Polygon', coordinates: rings };
    }

    // MULTIPOLYGON
    if (/^MULTIPOLYGON\s*\(/i.test(cleanWkt)) {
        const polygons = _extractMultiPolygonCoords(cleanWkt);
        if (polygons.length > 0) return { type: 'MultiPolygon', coordinates: polygons };
    }

    // LINESTRING
    const lineMatch = cleanWkt.match(/^LINESTRING\s*\((.+)\)/i);
    if (lineMatch) {
        const coords = lineMatch[1].split(',').map(p => { const [x, y] = p.trim().split(/\s+/).map(parseFloat); return [x, y]; });
        return { type: 'LineString', coordinates: coords };
    }

    // MULTILINESTRING
    if (/^MULTILINESTRING\s*\(/i.test(cleanWkt)) {
        const lines = _extractMultiLineCoords(cleanWkt);
        if (lines.length > 0) return { type: 'MultiLineString', coordinates: lines };
    }

    return null;
}

function _extractPolygonRings(wkt) {
    const start = wkt.indexOf('(');
    const end = wkt.lastIndexOf(')');
    if (start === -1 || end === -1) return [];
    const content = wkt.slice(start + 1, end);
    const rings = [];
    let depth = 0, current = '';
    for (const char of content) {
        if (char === '(') { depth++; if (depth === 1) { current = ''; continue; } }
        else if (char === ')') {
            depth--;
            if (depth === 0) {
                const coords = [];
                current.split(',').forEach(p => {
                    const nums = p.trim().split(/\s+/);
                    if (nums.length >= 2) coords.push([parseFloat(nums[0]), parseFloat(nums[1])]);
                });
                if (coords.length > 0) rings.push(coords);
                current = '';
                continue;
            }
        }
        if (depth >= 1) current += char;
    }
    return rings;
}

function _extractMultiPolygonCoords(wkt) {
    // MULTIPOLYGON(((x y, x y, ...), (hole)), ((x y, x y, ...)))
    // Find the opening paren after MULTIPOLYGON keyword
    const mIdx = wkt.toUpperCase().indexOf('MULTIPOLYGON');
    if (mIdx === -1) return [];
    const outerStart = wkt.indexOf('(', mIdx);
    if (outerStart === -1) return [];

    const polygons = [];
    let depth = 0;
    let polyStart = -1;

    // Walk through, tracking depth to split individual polygons
    // depth 1 = inside multipolygon wrapper
    // depth 2 = inside a polygon's ring group
    // depth 3 = inside a ring
    for (let i = outerStart; i < wkt.length; i++) {
        const ch = wkt[i];
        if (ch === '(') {
            depth++;
            if (depth === 2) polyStart = i;
        } else if (ch === ')') {
            depth--;
            if (depth === 1 && polyStart !== -1) {
                const polyWkt = 'POLYGON' + wkt.slice(polyStart, i + 1);
                const rings = _extractPolygonRings(polyWkt);
                if (rings.length > 0) polygons.push(rings);
                polyStart = -1;
            }
            if (depth === 0) break;
        }
    }
    return polygons;
}

function _extractMultiLineCoords(wkt) {
    const start = wkt.indexOf('(');
    const end = wkt.lastIndexOf(')');
    if (start === -1 || end === -1) return [];
    const content = wkt.slice(start + 1, end);
    const lines = [];
    let depth = 0, current = '';
    for (const char of content) {
        if (char === '(') { depth++; if (depth === 1) { current = ''; continue; } }
        else if (char === ')') {
            depth--;
            if (depth === 0) {
                const coords = [];
                current.split(',').forEach(p => {
                    const nums = p.trim().split(/\s+/);
                    if (nums.length >= 2) coords.push([parseFloat(nums[0]), parseFloat(nums[1])]);
                });
                if (coords.length > 0) lines.push(coords);
                current = '';
                continue;
            }
        }
        if (depth >= 1) current += char;
    }
    return lines;
}

function geometryToGeoJSON(value) {
    if (!value) return null;
    if (typeof value === 'object' && value.type) return value;
    if (typeof value !== 'string') return null;
    const v = value.trim();
    // Try GeoJSON string
    if (v.startsWith('{')) {
        try {
            const data = JSON.parse(v);
            if (data.type) return data;
        } catch (e) {}
    }
    // Try WKT/EWKT
    return parseWKT(v);
}

function rowsToGeoJSON(columns, rows, geoColIdx) {
    const features = [];
    for (const row of rows) {
        const geom = geometryToGeoJSON(row[geoColIdx]);
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
    // Defer GeoJSON conversion until map is opened (avoids blocking render for large geometries)
    currentGeoJSON = null;
    window._pendingGeoData = geoColIndices.length > 0 ? { columns: data.columns, rows: data.rows, geoIdx: geoColIndices[0] } : null;

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
                            ${data.columns.map((c, idx) => { const n = String(c.name).replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); const t = String(c.type).replace(/</g,'&lt;').replace(/>/g,'&gt;'); return `<th class="resizable-th px-4 py-2 font-medium border-b border-[#30363d]" style="min-width: 100px;">${n} <span class="text-xs text-[#484f58]">${t}</span><div class="resize-handle" data-col="${idx}"></div></th>`; }).join('')}
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
                                        const geoEsc = String(cell).replace(/</g, '&lt;').replace(/>/g, '&gt;');
                                        content = `<span class="text-[#a371f7] max-w-[200px] truncate block">${geoEsc}</span>`;
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
    window.dispatchEvent(new Event('open-code-modal'));
}

window.hideCodeModal = function() { window.dispatchEvent(new Event('close-code-modal')); }
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

// Map — destroy and recreate each time (same approach as studio.js)

window.showMapModal = function() {
    // Lazy-build GeoJSON on first map open
    if (!currentGeoJSON && window._pendingGeoData) {
        const { columns, rows, geoIdx } = window._pendingGeoData;
        currentGeoJSON = rowsToGeoJSON(columns, rows, geoIdx);
        window._pendingGeoData = null;
    }
    if (!currentGeoJSON || !currentGeoJSON.features || currentGeoJSON.features.length === 0) {
        showToast('No geographic data available. Please load a dataset with geometry columns first.', 'warning');
        return;
    }

    // Check for projected coordinates and show CRS bar if needed
    if (window.tuskReproject) {
        currentGeoJSON = window.tuskReproject(currentGeoJSON);
    }

    // Register callback for CRS reproject button
    window._tuskMapUpdateCallback = function(reprojected, epsgCode) {
        currentGeoJSON = reprojected;
        if (mapInstance) { mapInstance.remove(); mapInstance = null; }
        setTimeout(() => _initDataMap(), 50);
    };

    window.dispatchEvent(new Event('open-map-modal'));
    // Destroy previous map if any
    if (mapInstance) {
        mapInstance.remove();
        mapInstance = null;
    }
    // Wait for modal to be visible so container has dimensions
    setTimeout(() => _initDataMap(), 100);
}

window.hideMapModal = function() {
    window.dispatchEvent(new Event('close-map-modal'));
    if (mapInstance) {
        mapInstance.remove();
        mapInstance = null;
    }
}

function _initDataMap() {
    const container = document.getElementById('map-container');
    if (!container) return;
    container.innerHTML = '';

    mapInstance = new maplibregl.Map({
        container: container,
        style: { version: 8, sources: { 'carto': { type: 'raster', tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'], tileSize: 256 } }, layers: [{ id: 'carto', type: 'raster', source: 'carto' }] },
        center: [0, 20], zoom: 2
    });
    mapInstance.addControl(new maplibregl.NavigationControl());

    mapInstance.on('load', () => {
        if (!currentGeoJSON?.features.length) return;

        // Add GeoJSON source and layers
        mapInstance.addSource('geojson', { type: 'geojson', data: currentGeoJSON });
        mapInstance.addLayer({
            id: 'geojson-layer', type: 'fill', source: 'geojson',
            paint: { 'fill-color': '#6366f1', 'fill-opacity': 0.5 },
            filter: ['any', ['==', ['geometry-type'], 'Polygon'], ['==', ['geometry-type'], 'MultiPolygon']]
        });
        mapInstance.addLayer({
            id: 'geojson-line', type: 'line', source: 'geojson',
            paint: { 'line-color': '#6366f1', 'line-width': 3 },
            filter: ['any', ['==', ['geometry-type'], 'LineString'], ['==', ['geometry-type'], 'MultiLineString']]
        });
        mapInstance.addLayer({
            id: 'geojson-point', type: 'circle', source: 'geojson',
            paint: { 'circle-color': '#6366f1', 'circle-radius': 6, 'circle-stroke-color': '#fff', 'circle-stroke-width': 1 },
            filter: ['any', ['==', ['geometry-type'], 'Point'], ['==', ['geometry-type'], 'MultiPoint']]
        });

        // Fit bounds
        const bounds = new maplibregl.LngLatBounds();
        currentGeoJSON.features.forEach(f => {
            const t = f.geometry.type, co = f.geometry.coordinates;
            if (t === 'Point') bounds.extend(co);
            else if (t === 'MultiPoint') co.forEach(c => bounds.extend(c));
            else if (t === 'LineString') co.forEach(c => bounds.extend(c));
            else if (t === 'MultiLineString') co.forEach(line => line.forEach(c => bounds.extend(c)));
            else if (t === 'Polygon' && co[0]) co[0].forEach(c => bounds.extend(c));
            else if (t === 'MultiPolygon') co.forEach(poly => { if (poly[0]) poly[0].forEach(c => bounds.extend(c)); });
        });
        if (!bounds.isEmpty()) mapInstance.fitBounds(bounds, { padding: 50, maxZoom: 15 });

        // Escape HTML to prevent XSS in popups
        function escHtml(s) {
            const d = document.createElement('div');
            d.textContent = String(s ?? '');
            return d.innerHTML;
        }

        // Click popup for features
        let clickPopup = null;
        function showClickPopup(e) {
            if (!e.features || !e.features[0]) return;
            if (clickPopup) clickPopup.remove();
            const props = e.features[0].properties;
            const html = `<div class="bg-[#161b22] text-white p-3 rounded-lg text-xs max-w-xs max-h-64 overflow-auto">
                ${Object.entries(props).map(([k, v]) => {
                    let val = v;
                    if (typeof v === 'string' && v.startsWith('{')) {
                        try { val = JSON.stringify(JSON.parse(v), null, 2); } catch(e) {}
                    }
                    return `<div class="mb-1"><span class="text-[#8b949e]">${escHtml(k)}:</span> <span class="text-[#58a6ff]">${escHtml(val)}</span></div>`;
                }).join('')}
            </div>`;
            clickPopup = new maplibregl.Popup({ closeButton: true, className: 'geo-popup' })
                .setLngLat(e.lngLat).setHTML(html).addTo(mapInstance);
        }
        mapInstance.on('click', 'geojson-point', showClickPopup);
        mapInstance.on('click', 'geojson-layer', showClickPopup);
        mapInstance.on('click', 'geojson-line', showClickPopup);

        // Hover tooltip
        let hoverPopup = null;
        function getFeatureLabel(props) {
            const labelFields = ['name', 'Name', 'NAME', 'title', 'label', 'id', 'ID', 'osm_id'];
            for (const field of labelFields) {
                if (props[field]) return String(props[field]);
            }
            if (props.tags) {
                let tags = props.tags;
                if (typeof tags === 'string') { try { tags = JSON.parse(tags); } catch(e) { return null; } }
                if (tags.name) return tags.name;
                if (tags['name:en']) return tags['name:en'];
                for (const [k, v] of Object.entries(tags)) {
                    if (k.toLowerCase().includes('name') && v) return String(v);
                }
            }
            for (const [k, v] of Object.entries(props)) {
                if (typeof v === 'string' && v.length > 0 && v.length < 50 && !k.startsWith('_')) return v;
            }
            return null;
        }
        function showHoverTooltip(e) {
            if (!e.features || !e.features[0]) return;
            const label = getFeatureLabel(e.features[0].properties);
            if (!label) return;
            if (hoverPopup) hoverPopup.remove();
            hoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, className: 'hover-tooltip', offset: 10 })
                .setLngLat(e.lngLat)
                .setHTML(`<div class="bg-[#1c2128] text-white px-2 py-1 rounded text-xs font-medium shadow-lg">${escHtml(label)}</div>`)
                .addTo(mapInstance);
        }
        function hideHoverTooltip() { if (hoverPopup) { hoverPopup.remove(); hoverPopup = null; } }

        ['geojson-point', 'geojson-layer', 'geojson-line'].forEach(layer => {
            mapInstance.on('mouseenter', layer, (e) => { mapInstance.getCanvas().style.cursor = 'pointer'; showHoverTooltip(e); });
            mapInstance.on('mouseleave', layer, () => { mapInstance.getCanvas().style.cursor = ''; hideHoverTooltip(); });
            mapInstance.on('mousemove', layer, showHoverTooltip);
        });
    });
}
window.exportGeoJSON = function() {
    if (!currentGeoJSON) return;
    const blob = new Blob([JSON.stringify(currentGeoJSON, null, 2)], { type: 'application/json' });
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'export.geojson'; a.click();
}

// Modal backdrop clicks handled by Alpine.js x-show

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
    window.dispatchEvent(new Event('open-extensions'));
}

window.hideDuckDBExtensionsModal = function() {
    window.dispatchEvent(new Event('close-extensions'));
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
