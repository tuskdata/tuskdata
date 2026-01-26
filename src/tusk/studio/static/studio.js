// Tusk Studio - Main JavaScript Module
// CodeMirror 6 imports
import {EditorState} from "https://esm.sh/@codemirror/state@6"
import {EditorView, keymap} from "https://esm.sh/@codemirror/view@6"
import {defaultKeymap, indentWithTab} from "https://esm.sh/@codemirror/commands@6"
import {sql, PostgreSQL, SQLite} from "https://esm.sh/@codemirror/lang-sql@6"
import {autocompletion, completeFromList} from "https://esm.sh/@codemirror/autocomplete@6"
import {oneDark} from "https://esm.sh/@codemirror/theme-one-dark@6"

let currentConnection = null;
let currentSchema = {};
let editor = null;

// Query execution state
let queryAbortController = null;

// Query tabs state
let tabs = [];
let activeTabId = null;
let tabCounter = 0;

// Load tabs from localStorage on start
function loadTabsFromLocalStorage() {
    try {
        const saved = localStorage.getItem('tusk_tabs');
        if (saved) {
            const data = JSON.parse(saved);
            tabs = data.tabs || [];
            activeTabId = data.activeTabId;
            tabCounter = data.tabCounter || 0;

            // Clean up results from loaded tabs (to avoid memory issues)
            tabs.forEach(t => t.results = null);

            return tabs.length > 0;
        }
    } catch (e) {
        console.error('Failed to load tabs from localStorage:', e);
    }
    return false;
}

// Save tabs to localStorage
function saveTabsToLocalStorage() {
    try {
        // Save current editor content to active tab before saving
        if (activeTabId && editor) {
            const currentTab = tabs.find(t => t.id === activeTabId);
            if (currentTab) {
                currentTab.sql = editor.state.doc.toString();
            }
        }

        // Don't save results (too large), just save the SQL and tab info
        const tabsToSave = tabs.map(t => ({
            id: t.id,
            name: t.name,
            sql: t.sql,
            connectionId: t.connectionId
        }));

        localStorage.setItem('tusk_tabs', JSON.stringify({
            tabs: tabsToSave,
            activeTabId,
            tabCounter
        }));
    } catch (e) {
        console.error('Failed to save tabs to localStorage:', e);
    }
}

// Results state for current tab
let currentResults = null;
let sortColumn = null;
let sortDirection = 'asc';
let filterText = '';
let currentPage = 1;
const PAGE_SIZE = 100;

// Generate a unique tab ID
function generateTabId() {
    return `tab-${++tabCounter}`;
}

// Create a new tab
window.createTab = function(name = null, sqlText = "SELECT * FROM ") {
    const id = generateTabId();
    const tab = {
        id,
        name: name || `Query ${tabCounter}`,
        sql: sqlText,
        connectionId: currentConnection?.id || null,
        results: null
    };
    tabs.push(tab);
    switchTab(id);
    saveTabsToLocalStorage();
    return id;
}

// Switch to a tab
window.switchTab = function(tabId) {
    // Save current tab content before switching
    if (activeTabId && editor) {
        const currentTab = tabs.find(t => t.id === activeTabId);
        if (currentTab) {
            currentTab.sql = editor.state.doc.toString();
            currentTab.results = currentResults;
        }
    }

    activeTabId = tabId;
    const tab = tabs.find(t => t.id === tabId);

    if (tab && editor) {
        // Restore tab content
        editor.dispatch({
            changes: { from: 0, to: editor.state.doc.length, insert: tab.sql }
        });

        // Restore results
        currentResults = tab.results;
        sortColumn = null;
        sortDirection = 'asc';
        filterText = '';
        currentPage = 1;
        renderResults();
    }

    renderTabs();
    saveTabsToLocalStorage();
}

// Close a tab
window.closeTab = function(tabId, event) {
    if (event) {
        event.stopPropagation();
    }

    const tabIndex = tabs.findIndex(t => t.id === tabId);
    if (tabIndex === -1) return;

    // Don't close the last tab
    if (tabs.length === 1) {
        // Reset it instead
        tabs[0].sql = "SELECT * FROM ";
        tabs[0].results = null;
        currentResults = null;
        if (editor) {
            editor.dispatch({
                changes: { from: 0, to: editor.state.doc.length, insert: tabs[0].sql }
            });
        }
        renderResults();
        renderTabs();
        saveTabsToLocalStorage();
        return;
    }

    tabs.splice(tabIndex, 1);

    // If we closed the active tab, switch to an adjacent one
    if (activeTabId === tabId) {
        const newIndex = Math.min(tabIndex, tabs.length - 1);
        switchTab(tabs[newIndex].id);
    } else {
        renderTabs();
        saveTabsToLocalStorage();
    }
}

// Render tabs UI
function renderTabs() {
    const container = document.getElementById('query-tabs');
    container.innerHTML = tabs.map(tab => `
        <div class="query-tab ${tab.id === activeTabId ? 'active' : ''}"
             onclick="switchTab('${tab.id}')">
            <svg class="query-tab-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"></path>
            </svg>
            <span>${tab.name}</span>
            <button class="close-btn" onclick="closeTab('${tab.id}', event)" title="Close tab">
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                </svg>
            </button>
        </div>
    `).join('');
}

// Format cell value for display
function formatCell(value, type = '') {
    if (value === null) {
        return '<span class="null-badge">NULL</span>';
    }
    if (value === true) return '<span class="text-green-400">✓</span>';
    if (value === false) return '<span class="text-red-400">✗</span>';
    if (typeof value === 'object') {
        return `<span class="text-purple-400 font-mono text-xs">${JSON.stringify(value)}</span>`;
    }
    // Escape HTML
    const escaped = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    // Truncate long values
    if (escaped.length > 100) {
        return `<span title="${escaped.replace(/"/g, '&quot;')}">${escaped.slice(0, 100)}...</span>`;
    }
    return escaped;
}

// Get filtered and sorted data
function getProcessedRows() {
    if (!currentResults || !currentResults.rows) return [];

    let rows = [...currentResults.rows];

    // Apply filter
    if (filterText) {
        const lowerFilter = filterText.toLowerCase();
        rows = rows.filter(row =>
            row.some(cell =>
                cell !== null && String(cell).toLowerCase().includes(lowerFilter)
            )
        );
    }

    // Apply sort
    if (sortColumn !== null && currentResults.columns[sortColumn]) {
        rows.sort((a, b) => {
            const aVal = a[sortColumn];
            const bVal = b[sortColumn];

            if (aVal === null && bVal === null) return 0;
            if (aVal === null) return sortDirection === 'asc' ? 1 : -1;
            if (bVal === null) return sortDirection === 'asc' ? -1 : 1;

            if (typeof aVal === 'number' && typeof bVal === 'number') {
                return sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
            }

            const aStr = String(aVal).toLowerCase();
            const bStr = String(bVal).toLowerCase();
            if (sortDirection === 'asc') {
                return aStr.localeCompare(bStr);
            }
            return bStr.localeCompare(aStr);
        });
    }

    return rows;
}

// Render results table
function renderResults() {
    const headerEl = document.getElementById('results-header');
    const tableEl = document.getElementById('results-table');

    if (!currentResults) {
        headerEl.innerHTML = '';
        tableEl.innerHTML = '';
        return;
    }

    if (currentResults.error) {
        headerEl.innerHTML = '<span class="text-red-400">Error</span>';
        tableEl.innerHTML = `
            <div class="card rounded-lg p-4 text-red-400">
                <pre class="whitespace-pre-wrap font-mono text-sm">${currentResults.error}</pre>
            </div>
        `;
        return;
    }

    if (!currentResults.columns || currentResults.columns.length === 0) {
        headerEl.innerHTML = `
            <span class="text-green-400">${currentResults.row_count || 0} rows</span>
            <span class="text-gray-500">·</span>
            <span class="text-gray-400">${currentResults.execution_time_ms || 0}ms</span>
        `;
        tableEl.innerHTML = '<div class="text-gray-400">Query executed successfully (no results)</div>';
        return;
    }

    const processedRows = getProcessedRows();
    const totalFiltered = processedRows.length;
    const totalPages = Math.ceil(totalFiltered / PAGE_SIZE);
    const startIdx = (currentPage - 1) * PAGE_SIZE;
    const endIdx = Math.min(startIdx + PAGE_SIZE, totalFiltered);
    const pageRows = processedRows.slice(startIdx, endIdx);

    // Header with stats, filter, and export buttons
    headerEl.innerHTML = `
        <div class="flex items-center justify-between flex-wrap gap-2">
            <div class="flex items-center gap-2">
                <span class="text-green-400">${currentResults.row_count} rows</span>
                <span class="text-gray-500">·</span>
                <span class="text-gray-400">${currentResults.execution_time_ms}ms</span>
                ${filterText ? `<span class="text-gray-500">·</span><span class="text-yellow-400">${totalFiltered} filtered</span>` : ''}
            </div>
            <div class="flex items-center gap-2">
                <input type="text"
                       id="results-filter"
                       placeholder="Filter results..."
                       value="${filterText}"
                       onkeyup="filterResults(this.value)"
                       class="bg-[#0d1117] border border-[#30363d] rounded px-2 py-1 text-xs w-40 focus:outline-none focus:border-indigo-500">
                ${hasGeoColumn() ? '<button onclick="showMapModal()" class="text-xs px-2 py-1 rounded bg-emerald-600 hover:bg-emerald-700 text-white flex items-center gap-1"><span>🗺️</span> Map</button>' : ''}
                <button onclick="exportCSV()" class="text-xs px-2 py-1 rounded bg-[#21262d] hover:bg-[#30363d] text-gray-400 hover:text-white">CSV</button>
                <button onclick="exportJSON()" class="text-xs px-2 py-1 rounded bg-[#21262d] hover:bg-[#30363d] text-gray-400 hover:text-white">JSON</button>
                ${currentEngine === 'duckdb' ? '<button onclick="showParquetModal()" class="text-xs px-2 py-1 rounded bg-[#21262d] hover:bg-[#30363d] text-gray-400 hover:text-white">Parquet</button>' : ''}
            </div>
        </div>
    `;

    // Table
    tableEl.innerHTML = `
        <div class="card rounded-lg overflow-hidden">
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead class="bg-[#21262d] sticky top-0">
                        <tr>${currentResults.columns.map((c, i) => `
                            <th class="px-4 py-2 text-left text-gray-400 font-medium border-b border-[#30363d] cursor-pointer hover:bg-[#30363d] select-none"
                                onclick="sortByColumn(${i})">
                                <div class="flex items-center gap-1">
                                    ${c.name}
                                    ${sortColumn === i ? (sortDirection === 'asc' ? '↑' : '↓') : '<span class="text-gray-600">↕</span>'}
                                </div>
                            </th>
                        `).join('')}</tr>
                    </thead>
                    <tbody class="font-mono text-xs">
                        ${pageRows.map((row, i) => `
                            <tr class="${i % 2 === 0 ? '' : 'bg-[#0d1117]/50'} hover:bg-[#21262d]">
                                ${row.map(cell => `
                                    <td class="px-4 py-2 border-b border-[#30363d]/50">${formatCell(cell)}</td>
                                `).join('')}
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            ${totalPages > 1 ? `
                <div class="flex items-center justify-between px-4 py-2 bg-[#21262d] border-t border-[#30363d]">
                    <span class="text-xs text-gray-400">
                        Showing ${startIdx + 1}-${endIdx} of ${totalFiltered}
                    </span>
                    <div class="flex items-center gap-1">
                        <button onclick="goToPage(1)" ${currentPage === 1 ? 'disabled' : ''}
                                class="px-2 py-1 text-xs rounded hover:bg-[#30363d] disabled:opacity-50 disabled:cursor-not-allowed">««</button>
                        <button onclick="goToPage(${currentPage - 1})" ${currentPage === 1 ? 'disabled' : ''}
                                class="px-2 py-1 text-xs rounded hover:bg-[#30363d] disabled:opacity-50 disabled:cursor-not-allowed">«</button>
                        <span class="px-2 text-xs text-gray-400">Page ${currentPage} of ${totalPages}</span>
                        <button onclick="goToPage(${currentPage + 1})" ${currentPage === totalPages ? 'disabled' : ''}
                                class="px-2 py-1 text-xs rounded hover:bg-[#30363d] disabled:opacity-50 disabled:cursor-not-allowed">»</button>
                        <button onclick="goToPage(${totalPages})" ${currentPage === totalPages ? 'disabled' : ''}
                                class="px-2 py-1 text-xs rounded hover:bg-[#30363d] disabled:opacity-50 disabled:cursor-not-allowed">»»</button>
                    </div>
                </div>
            ` : ''}
        </div>
    `;
}

// Sort by column
window.sortByColumn = function(colIndex) {
    if (sortColumn === colIndex) {
        sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
        sortColumn = colIndex;
        sortDirection = 'asc';
    }
    currentPage = 1;
    renderResults();
}

// Filter results
window.filterResults = function(text) {
    filterText = text;
    currentPage = 1;
    renderResults();
}

// Go to page
window.goToPage = function(page) {
    const processedRows = getProcessedRows();
    const totalPages = Math.ceil(processedRows.length / PAGE_SIZE);
    if (page >= 1 && page <= totalPages) {
        currentPage = page;
        renderResults();
    }
}

// Export to CSV
window.exportCSV = function() {
    if (!currentResults || !currentResults.columns) return;

    const rows = getProcessedRows();
    const headers = currentResults.columns.map(c => c.name);

    const csvContent = [
        headers.join(','),
        ...rows.map(row =>
            row.map(cell => {
                if (cell === null) return '';
                const str = String(cell);
                if (str.includes(',') || str.includes('"') || str.includes('\n')) {
                    return `"${str.replace(/"/g, '""')}"`;
                }
                return str;
            }).join(',')
        )
    ].join('\n');

    downloadFile(csvContent, 'query_results.csv', 'text/csv');
}

// Export to JSON
window.exportJSON = function() {
    if (!currentResults || !currentResults.columns) return;

    const rows = getProcessedRows();
    const headers = currentResults.columns.map(c => c.name);

    const jsonData = rows.map(row => {
        const obj = {};
        headers.forEach((h, i) => obj[h] = row[i]);
        return obj;
    });

    downloadFile(JSON.stringify(jsonData, null, 2), 'query_results.json', 'application/json');
}

// Download file helper
function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// Initialize CodeMirror editor
function initEditor(schema = {}, connType = 'postgres') {
    const sqlDialect = connType === 'sqlite' ? SQLite : PostgreSQL;

    // Build schema for autocomplete: { tableName: [columns] }
    const schemaForAutocomplete = {};
    // Also build a flat list of all completions
    const allCompletions = [];
    const seenColumns = new Set();
    const seenSchemas = new Set();

    for (const [schemaName, tables] of Object.entries(schema)) {
        // Add schema name to completions (for "public." etc)
        if (!seenSchemas.has(schemaName)) {
            seenSchemas.add(schemaName);
            allCompletions.push({
                label: schemaName,
                type: "namespace",
                detail: `schema (${Object.keys(tables).length} tables)`
            });
        }

        for (const [tableName, columns] of Object.entries(tables)) {
            schemaForAutocomplete[tableName] = columns.map(c => c.name);
            // Also add schema-qualified version
            schemaForAutocomplete[`${schemaName}.${tableName}`] = columns.map(c => c.name);

            // Add table to completions (simple name)
            allCompletions.push({
                label: tableName,
                type: "class",
                detail: `${schemaName} (${columns.length} cols)`
            });

            // Add schema-qualified table name (public.users)
            allCompletions.push({
                label: `${schemaName}.${tableName}`,
                type: "class",
                detail: `table (${columns.length} cols)`
            });

            // Add columns to completions (deduplicated)
            for (const col of columns) {
                if (!seenColumns.has(col.name)) {
                    seenColumns.add(col.name);
                    allCompletions.push({
                        label: col.name,
                        type: "property",
                        detail: `${col.type} (${tableName})`
                    });
                }
            }
        }
    }

    // Custom completer that provides tables and columns globally
    const schemaCompleter = completeFromList(allCompletions);

    // Get the active tab's SQL or default
    const activeTab = tabs.find(t => t.id === activeTabId);
    const initialDoc = activeTab ? activeTab.sql : (editor ? editor.state.doc.toString() : "SELECT * FROM ");

    const state = EditorState.create({
        doc: initialDoc,
        extensions: [
            keymap.of([
                ...defaultKeymap,
                indentWithTab,
                { key: "Ctrl-Enter", run: () => { runQuery(); return true; } },
                { key: "Cmd-Enter", run: () => { runQuery(); return true; } }
            ]),
            sql({
                dialect: sqlDialect,
                schema: schemaForAutocomplete,
                upperCaseKeywords: true
            }),
            autocompletion({
                override: [schemaCompleter]
            }),
            oneDark,
            EditorView.theme({
                "&": { height: "180px" },
                ".cm-scroller": { overflow: "auto" }
            })
        ]
    });

    const editorEl = document.getElementById('sql-editor');
    editorEl.innerHTML = '';

    editor = new EditorView({
        state,
        parent: editorEl
    });
}

// Initialize with empty schema
initEditor();

// Try to restore tabs from localStorage, or create initial tab
if (!loadTabsFromLocalStorage() || tabs.length === 0) {
    createTab();
} else {
    // Restore tabs and switch to the active one
    renderTabs();
    if (activeTabId) {
        const tab = tabs.find(t => t.id === activeTabId);
        if (tab && editor) {
            editor.dispatch({
                changes: { from: 0, to: editor.state.doc.length, insert: tab.sql }
            });
        }
    }
}

// Toggle connection fields
window.toggleConnFields = function() {
    const type = document.querySelector('input[name="type"]:checked').value;
    document.getElementById('postgres-fields').classList.toggle('hidden', type !== 'postgres');

    const isFileDb = type === 'sqlite' || type === 'duckdb';
    document.getElementById('file-db-fields').classList.toggle('hidden', !isFileDb);

    // Update placeholder and hint based on type
    const pathInput = document.getElementById('conn-path');
    const pathHint = document.getElementById('path-hint');
    if (type === 'sqlite') {
        pathInput.placeholder = '~/data/app.db';
        pathHint.textContent = 'Full path to your SQLite database file';
    } else if (type === 'duckdb') {
        pathInput.placeholder = '~/data/analytics.duckdb';
        pathHint.textContent = 'Full path to your DuckDB database file';
    }
}

// Load connections and history on start
loadConnections().then(() => {
    // Auto-select last used connection
    try {
        const lastConn = localStorage.getItem('tusk_last_connection');
        if (lastConn && !currentConnection) {
            const conn = JSON.parse(lastConn);
            // Verify connection still exists by checking the list
            const connEl = document.querySelector(`[onclick*="selectConnection('${conn.id}'"]`);
            if (connEl) {
                selectConnection(conn.id, conn.name, conn.type);
            }
        }
    } catch (e) {
        console.error('Failed to restore last connection:', e);
    }
});
loadHistory();

async function loadConnections() {
    const res = await fetch('/api/connections');
    const conns = await res.json();

    const list = document.getElementById('connections-list');
    if (conns.length === 0) {
        list.innerHTML = '<div class="text-gray-500 text-sm py-2">No connections yet</div>';
        return;
    }

    list.innerHTML = conns.map(c => {
        const icon = c.type === 'duckdb' ? '🦆' : c.type === 'sqlite' ? '🗃️' : '🐘';
        return `
        <div class="group flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer hover:bg-[#21262d] ${currentConnection?.id === c.id ? 'bg-[#21262d] ring-1 ring-indigo-500/50' : ''}"
             onclick="selectConnection('${c.id}', '${c.name}', '${c.type}')">
            <span class="w-2 h-2 rounded-full ${currentConnection?.id === c.id ? 'bg-green-500' : 'bg-gray-500'}"></span>
            <span>${icon}</span>
            <span class="text-sm flex-1 truncate" title="${c.name}">${c.name}</span>
            <div class="opacity-0 group-hover:opacity-100 flex items-center gap-1">
                ${c.type === 'postgres' ? `
                    <button onclick="event.stopPropagation(); showDatabasesModal('${c.id}')"
                            class="text-gray-500 hover:text-indigo-400 transition-colors text-xs" title="Browse databases">
                        <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"></path>
                        </svg>
                    </button>
                ` : ''}
                <button onclick="event.stopPropagation(); showEditConnModal('${c.id}')"
                        class="text-gray-500 hover:text-blue-400 transition-colors text-xs" title="Edit connection">
                    <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z"></path>
                    </svg>
                </button>
                <button onclick="event.stopPropagation(); deleteConnection('${c.id}')"
                        class="text-gray-500 hover:text-red-400 transition-colors">×</button>
            </div>
        </div>
    `}).join('');
}

window.selectConnection = async function(id, name, type) {
    currentConnection = { id, name, type };

    // Save last connection to localStorage
    localStorage.setItem('tusk_last_connection', JSON.stringify({ id, name, type }));

    loadConnections();

    // Load schema
    const res = await fetch(`/api/connections/${id}/schema`);
    const schema = await res.json();

    if (schema.error) {
        document.getElementById('schema-tree').innerHTML = `<div class="text-red-400 py-2">${schema.error}</div>`;
        return;
    }

    currentSchema = schema;

    // Reinitialize editor with schema for autocomplete
    initEditor(schema, type);

    // Render schema tree
    renderSchemaTree(schema);
}

// Render schema tree
function renderSchemaTree(schema) {
    const tree = document.getElementById('schema-tree');
    tree.innerHTML = `
        <div class="flex items-center justify-between mb-2">
            <button onclick="refreshSchema()" class="text-xs text-gray-500 hover:text-white flex items-center gap-1" title="Refresh schema (F5)">
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
                </svg>
                Refresh
            </button>
        </div>
    ` + Object.entries(schema).map(([schemaName, tables]) => `
        <details open class="mt-1">
            <summary class="cursor-pointer text-gray-400 hover:text-white py-1">📁 ${schemaName}</summary>
            <div class="ml-3">
                ${Object.entries(tables).map(([tableName, cols]) => `
                    <details class="mt-0.5">
                        <summary class="cursor-pointer hover:text-white py-0.5 flex items-center gap-1"
                                 ondblclick="event.stopPropagation(); insertTable('${tableName}')">
                            <span>📋</span> ${tableName}
                            <span class="text-xs text-gray-600">(${cols.length})</span>
                        </summary>
                        <div class="ml-4 text-xs text-gray-500">
                            ${cols.map(c => {
                                let icon = '<span class="text-gray-600">⋮</span>';
                                let tooltip = '';
                                if (c.is_primary_key && c.is_foreign_key) {
                                    icon = '<span class="text-yellow-500" title="Primary Key">🔑</span><span class="text-blue-400" title="FK: ' + (c.references || '') + '">🔗</span>';
                                } else if (c.is_primary_key) {
                                    icon = '<span class="text-yellow-500" title="Primary Key">🔑</span>';
                                } else if (c.is_foreign_key) {
                                    icon = '<span class="text-blue-400" title="FK: ' + (c.references || '') + '">🔗</span>';
                                    tooltip = ' → ' + (c.references || '').split('.').pop();
                                }
                                return `
                                <div class="py-0.5 hover:text-gray-300 cursor-pointer flex items-center gap-1" onclick="insertColumn('${c.name}')" title="${c.references || ''}">
                                    ${icon}
                                    ${c.name} <span class="text-gray-600">${c.type}</span>${tooltip ? '<span class="text-blue-400/60 text-[10px]">' + tooltip + '</span>' : ''}
                                </div>
                            `}).join('')}
                        </div>
                    </details>
                `).join('')}
            </div>
        </details>
    `).join('');
}

// Refresh schema
window.refreshSchema = async function() {
    if (!currentConnection) return;

    const res = await fetch(`/api/connections/${currentConnection.id}/schema`);
    const schema = await res.json();

    if (!schema.error) {
        currentSchema = schema;
        initEditor(schema, currentConnection.type);
        renderSchemaTree(schema);
    }
}

window.insertTable = function(table) {
    if (editor) {
        const cursor = editor.state.selection.main.head;
        editor.dispatch({
            changes: { from: cursor, insert: table }
        });
        editor.focus();
    }
}

window.insertColumn = function(column) {
    if (editor) {
        const cursor = editor.state.selection.main.head;
        editor.dispatch({
            changes: { from: cursor, insert: column }
        });
        editor.focus();
    }
}

window.deleteConnection = async function(id) {
    if (!confirm('Delete this connection?')) return;
    await fetch(`/api/connections/${id}`, { method: 'DELETE' });
    if (currentConnection?.id === id) {
        currentConnection = null;
        document.getElementById('schema-tree').innerHTML = '<div class="text-gray-500 py-2">Select a connection</div>';
    }
    loadConnections();
}

// Query execution state
let isRunning = false;

window.runQuery = async function() {
    if (!currentConnection) {
        alert('Select a connection first');
        return;
    }

    if (isRunning) return;

    const sqlText = editor.state.doc.toString();

    isRunning = true;
    queryAbortController = new AbortController();

    // Update Run button to show Cancel
    const runBtn = document.getElementById('run-btn');
    runBtn.disabled = false;
    runBtn.onclick = cancelQuery;
    runBtn.innerHTML = `
        <svg class="w-4 h-4 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
        </svg>
        Cancel
    `;
    runBtn.classList.remove('bg-green-600', 'hover:bg-green-700');
    runBtn.classList.add('bg-red-600', 'hover:bg-red-700');

    document.getElementById('results-header').innerHTML = `
        <span class="text-gray-400 flex items-center gap-2">
            <svg class="w-4 h-4 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <circle cx="12" cy="12" r="10" stroke-opacity="0.25" stroke-width="4"></circle>
                <path d="M12 2a10 10 0 0110 10" stroke-width="4"></path>
            </svg>
            Running query... <span class="text-gray-500">(press Escape or click Cancel to stop)</span>
        </span>
    `;
    document.getElementById('results-table').innerHTML = '';

    try {
        const res = await fetch('/api/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connection_id: currentConnection.id, sql: sqlText }),
            signal: queryAbortController.signal
        });

        currentResults = await res.json();

        // Reset sort/filter/page for new results
        sortColumn = null;
        sortDirection = 'asc';
        filterText = '';
        currentPage = 1;

        // Store results in active tab
        const activeTab = tabs.find(t => t.id === activeTabId);
        if (activeTab) {
            activeTab.sql = sqlText;
            activeTab.results = currentResults;
        }

        renderResults();
    } catch (err) {
        if (err.name === 'AbortError') {
            currentResults = { error: 'Query cancelled by user' };
        } else {
            currentResults = { error: err.message };
        }
        renderResults();
    } finally {
        isRunning = false;
        queryAbortController = null;
        resetRunButton();

        // Refresh history after query execution
        loadHistory();
    }
}

window.cancelQuery = function() {
    if (queryAbortController) {
        queryAbortController.abort();
    }
}

function resetRunButton() {
    const runBtn = document.getElementById('run-btn');
    runBtn.disabled = false;
    runBtn.onclick = runQuery;
    runBtn.innerHTML = '<span>▶</span> Run';
    runBtn.classList.remove('bg-red-600', 'hover:bg-red-700');
    runBtn.classList.add('bg-green-600', 'hover:bg-green-700');
}

// Test connection
window.testConnection = async function() {
    const form = document.getElementById('conn-form');
    const formData = new FormData(form);
    const data = Object.fromEntries(formData);

    const editId = document.getElementById('conn-edit-id').value;
    const btn = document.getElementById('test-conn-btn');
    const originalText = btn.innerHTML;

    btn.disabled = true;
    btn.innerHTML = 'Testing...';

    try {
        let res;
        if (editId) {
            // Test existing connection
            res = await fetch(`/api/connections/${editId}/test`, { method: 'POST' });
        } else {
            // Create temp connection and test
            res = await fetch('/api/connections/test', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
        }

        const result = await res.json();

        if (result.success) {
            btn.innerHTML = '✓ Connected!';
            btn.classList.remove('bg-[#21262d]');
            btn.classList.add('bg-green-600');
            setTimeout(() => {
                btn.innerHTML = originalText;
                btn.classList.remove('bg-green-600');
                btn.classList.add('bg-[#21262d]');
            }, 2000);
        } else {
            btn.innerHTML = '✗ Failed';
            btn.classList.remove('bg-[#21262d]');
            btn.classList.add('bg-red-600');
            alert('Connection failed: ' + result.message);
            setTimeout(() => {
                btn.innerHTML = originalText;
                btn.classList.remove('bg-red-600');
                btn.classList.add('bg-[#21262d]');
            }, 2000);
        }
    } catch (err) {
        alert('Test failed: ' + err.message);
        btn.innerHTML = originalText;
    } finally {
        btn.disabled = false;
    }
}

window.showConnModal = function(editMode = false) {
    // Reset form to add mode
    document.getElementById('conn-modal-title').textContent = 'Add Connection';
    document.getElementById('conn-edit-id').value = '';
    document.getElementById('conn-submit-btn').textContent = 'Connect';
    document.getElementById('password-hint').classList.add('hidden');
    document.getElementById('conn-type-selector').classList.remove('hidden');

    // Reset form fields
    if (!editMode) {
        document.getElementById('conn-form').reset();
        document.getElementById('conn-host').value = 'localhost';
        document.getElementById('conn-port').value = '5432';
        toggleConnFields();
    }

    document.getElementById('conn-modal').classList.remove('hidden');
    document.getElementById('conn-name').focus();
}

window.showEditConnModal = async function(connId) {
    // Fetch connection details
    const res = await fetch(`/api/connections/${connId}`);
    const conn = await res.json();

    if (conn.error) {
        alert('Could not load connection: ' + conn.error);
        return;
    }

    // Set form to edit mode
    document.getElementById('conn-modal-title').textContent = 'Edit Connection';
    document.getElementById('conn-edit-id').value = connId;
    document.getElementById('conn-submit-btn').textContent = 'Save Changes';
    document.getElementById('password-hint').classList.remove('hidden');

    // Hide type selector in edit mode (can't change type)
    document.getElementById('conn-type-selector').classList.add('hidden');

    // Fill form fields
    document.getElementById('conn-name').value = conn.name || '';

    // Set type radio (hidden but needed for form)
    document.querySelector(`input[name="type"][value="${conn.type}"]`).checked = true;
    toggleConnFields();

    if (conn.type === 'postgres') {
        document.getElementById('conn-host').value = conn.host || '';
        document.getElementById('conn-port').value = conn.port || 5432;
        document.getElementById('conn-database').value = conn.database || '';
        document.getElementById('conn-user').value = conn.user || '';
        document.getElementById('conn-password').value = ''; // Don't show password
    } else if (conn.type === 'sqlite' || conn.type === 'duckdb') {
        document.getElementById('conn-path').value = conn.path || '';
    }

    document.getElementById('conn-modal').classList.remove('hidden');
    document.getElementById('conn-name').focus();
}

window.hideConnModal = function() {
    document.getElementById('conn-modal').classList.add('hidden');
    // Reset edit state
    document.getElementById('conn-edit-id').value = '';
}

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
    // Escape - cancel query or close modals
    if (e.key === 'Escape') {
        if (isRunning && queryAbortController) {
            cancelQuery();
        } else {
            hideConnModal();
            hideSettingsModal();
            hideDatabasesModal();
            hideSaveQueryModal();
            hideFolderModal();
            hideFilePreviewModal();
            hideCreateFileModal();
            hideParquetModal();
        }
    }
    // Ctrl+T / Cmd+T - new tab
    if ((e.ctrlKey || e.metaKey) && e.key === 't') {
        e.preventDefault();
        createTab();
    }
    // Ctrl+W / Cmd+W - close current tab
    if ((e.ctrlKey || e.metaKey) && e.key === 'w') {
        e.preventDefault();
        if (activeTabId) {
            closeTab(activeTabId);
        }
    }
    // F5 - refresh schema
    if (e.key === 'F5') {
        e.preventDefault();
        refreshSchema();
    }
});

// Close modal on backdrop click
document.getElementById('conn-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideConnModal();
});

document.getElementById('settings-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideSettingsModal();
});

document.getElementById('conn-form').onsubmit = async (e) => {
    e.preventDefault();
    const form = new FormData(e.target);
    const data = Object.fromEntries(form);

    const editId = document.getElementById('conn-edit-id').value;
    const isEdit = editId && editId.length > 0;

    let res;
    if (isEdit) {
        // Update existing connection
        res = await fetch(`/api/connections/${editId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
    } else {
        // Create new connection
        res = await fetch('/api/connections', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
    }

    const result = await res.json();

    if (result.error) {
        alert('Error: ' + result.error);
        return;
    }

    hideConnModal();
    loadConnections();
    e.target.reset();

    // Auto-select the connection (new or edited)
    if (result.id) {
        setTimeout(() => selectConnection(result.id, result.name, result.type), 100);
    }
};

// Settings Modal Functions
window.showSettingsModal = async function() {
    document.getElementById('settings-modal').classList.remove('hidden');
    await loadPgPaths();
}

window.hideSettingsModal = function() {
    document.getElementById('settings-modal').classList.add('hidden');
}

async function loadPgPaths() {
    const res = await fetch('/api/settings/pg-bin-path/detect');
    const data = await res.json();

    const list = document.getElementById('pg-paths-list');
    const input = document.getElementById('pg-bin-path-input');

    if (data.current) {
        input.value = data.current;
    }

    if (data.available.length === 0) {
        list.innerHTML = '<div class="text-yellow-400 text-sm">No PostgreSQL installations detected</div>';
        return;
    }

    list.innerHTML = data.available.map(p => `
        <div class="flex items-center justify-between p-2 rounded-lg bg-[#0d1117] border border-[#30363d] ${data.current === p.path || (!data.current && data.detected.startsWith(p.path)) ? 'ring-1 ring-indigo-500' : ''}">
            <div class="flex-1 min-w-0">
                <div class="font-mono text-sm truncate">${p.path}</div>
                <div class="text-xs text-gray-500">${p.version}</div>
            </div>
            <button onclick="selectPgPath('${p.path}')" class="text-sm text-indigo-400 hover:text-indigo-300 px-2">Use</button>
        </div>
    `).join('');
}

window.selectPgPath = async function(path) {
    document.getElementById('pg-bin-path-input').value = path;
    await setPgBinPath();
}

window.setPgBinPath = async function() {
    const path = document.getElementById('pg-bin-path-input').value.trim();

    const res = await fetch('/api/settings/pg-bin-path', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path })
    });

    const result = await res.json();

    if (result.success) {
        await loadPgPaths();
    } else {
        alert(result.error);
    }
}

window.clearPgBinPath = async function() {
    document.getElementById('pg-bin-path-input').value = '';
    await setPgBinPath();
}

// History Functions
async function loadHistory() {
    const res = await fetch('/api/history?limit=20');
    const data = await res.json();

    const list = document.getElementById('history-list');

    if (!data.history || data.history.length === 0) {
        list.innerHTML = '<div class="text-gray-500 py-1">No history yet</div>';
        return;
    }

    list.innerHTML = data.history.map(h => {
        // Truncate SQL for display
        const sqlPreview = h.sql.length > 35 ? h.sql.slice(0, 35) + '...' : h.sql;
        const statusIcon = h.status === 'success' ? '✓' : '✗';
        const statusColor = h.status === 'success' ? 'text-green-500' : 'text-red-500';
        const connName = h.connection_name || 'Unknown';
        const connShort = connName.length > 10 ? connName.slice(0, 10) + '..' : connName;

        return `
            <div class="group px-2 py-1.5 rounded hover:bg-[#21262d] cursor-pointer"
                 onclick="useHistoryQuery(${h.id})"
                 title="${h.sql.replace(/"/g, '&quot;')}">
                <div class="flex items-center gap-1 text-[10px] text-gray-600 mb-0.5">
                    <span class="${statusColor}">${statusIcon}</span>
                    <span title="${connName}">${connShort}</span>
                    <span class="ml-auto">${h.execution_time_ms}ms</span>
                </div>
                <div class="truncate text-gray-400 font-mono text-xs">${sqlPreview}</div>
            </div>
        `;
    }).join('');
}

window.useHistoryQuery = async function(id) {
    // Find the query in history and set it in the editor
    const res = await fetch('/api/history?limit=100');
    const data = await res.json();

    const entry = data.history.find(h => h.id === id);
    if (entry && editor) {
        // Check if this is a DuckDB query
        if (entry.connection_id === 'duckdb-local') {
            setEngine('duckdb');
        } else if (entry.connection_id && entry.connection_id !== currentConnection?.id) {
            // Try to connect to the same database
            const connRes = await fetch('/api/connections');
            const connections = await connRes.json();
            const conn = connections.find(c => c.id === entry.connection_id);

            if (conn) {
                await selectConnection(conn.id, conn.name, conn.type);
            } else {
                // Connection no longer exists, just show a warning
                console.warn(`Connection "${entry.connection_name}" no longer exists`);
            }
        }

        // Check if current tab has real content (not empty or default)
        const currentSql = editor.state.doc.toString().trim();
        const isEmptyOrDefault = !currentSql ||
                                  currentSql === 'SELECT * FROM' ||
                                  currentSql === 'SELECT * FROM ';

        if (isEmptyOrDefault) {
            // Replace current tab content
            editor.dispatch({
                changes: { from: 0, to: editor.state.doc.length, insert: entry.sql }
            });
        } else {
            // Open in new tab
            createTab(`History ${id}`, entry.sql);
        }

        editor.focus();
    }
}

window.clearHistory = async function() {
    if (!confirm('Clear all query history?')) return;

    await fetch('/api/history', { method: 'DELETE' });
    loadHistory();
}

// Databases Modal Functions
let currentDatabasesConnId = null;

window.showDatabasesModal = async function(connId) {
    currentDatabasesConnId = connId;
    document.getElementById('databases-modal').classList.remove('hidden');
    document.getElementById('databases-list').innerHTML = '<div class="text-gray-500">Loading databases...</div>';

    const res = await fetch(`/api/connections/${connId}/databases`);
    const data = await res.json();

    const list = document.getElementById('databases-list');

    if (data.error) {
        list.innerHTML = `<div class="text-red-400">${data.error}</div>`;
        return;
    }

    if (!data.databases || data.databases.length === 0) {
        list.innerHTML = '<div class="text-gray-500">No databases found</div>';
        return;
    }

    list.innerHTML = data.databases.map(db => `
        <div class="flex items-center justify-between p-3 rounded-lg hover:bg-[#21262d] ${db.is_current ? 'bg-[#21262d] ring-1 ring-green-500/50' : ''}">
            <div class="flex-1 min-w-0">
                <div class="flex items-center gap-2">
                    <span class="font-medium">${db.name}</span>
                    ${db.is_current ? '<span class="text-xs px-2 py-0.5 rounded-full bg-green-500/20 text-green-400">current</span>' : ''}
                </div>
                <div class="text-xs text-gray-500 mt-1">
                    ${db.size_human} · Owner: ${db.owner}
                </div>
            </div>
            ${!db.is_current ? `
                <button onclick="connectToDatabase('${db.name}')"
                        class="text-indigo-400 hover:text-indigo-300 hover:bg-indigo-500/20 px-3 py-1 rounded transition-colors text-sm">
                    Connect
                </button>
            ` : ''}
        </div>
    `).join('');
}

window.hideDatabasesModal = function() {
    document.getElementById('databases-modal').classList.add('hidden');
    currentDatabasesConnId = null;
}

window.connectToDatabase = async function(dbName) {
    if (!currentDatabasesConnId) return;

    const res = await fetch(`/api/connections/${currentDatabasesConnId}/clone`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ database: dbName })
    });

    const result = await res.json();

    if (result.error) {
        alert('Error: ' + result.error);
        return;
    }

    hideDatabasesModal();
    await loadConnections();

    // Select the new or existing connection
    setTimeout(() => selectConnection(result.id, result.name, result.type), 100);
}

// Close databases modal on Escape and backdrop click
document.getElementById('databases-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideDatabasesModal();
});

// Saved Queries Functions
loadSavedQueries();

async function loadSavedQueries() {
    const res = await fetch('/api/saved-queries');
    const data = await res.json();

    const list = document.getElementById('saved-queries-list');

    if (!data.queries || data.queries.length === 0) {
        list.innerHTML = '<div class="text-gray-500 py-1">No saved queries</div>';
        return;
    }

    // Group by folder
    const folders = {};
    const noFolder = [];

    data.queries.forEach(q => {
        if (q.folder) {
            if (!folders[q.folder]) folders[q.folder] = [];
            folders[q.folder].push(q);
        } else {
            noFolder.push(q);
        }
    });

    let html = '';

    // Render queries without folder first
    noFolder.forEach(q => {
        html += renderSavedQueryItem(q);
    });

    // Render folders
    for (const [folder, queries] of Object.entries(folders)) {
        html += `
            <details class="mt-1">
                <summary class="cursor-pointer text-gray-400 hover:text-white py-1 text-xs">📁 ${folder}</summary>
                <div class="ml-2">
                    ${queries.map(q => renderSavedQueryItem(q)).join('')}
                </div>
            </details>
        `;
    }

    list.innerHTML = html;
}

function renderSavedQueryItem(q) {
    const sqlPreview = q.sql.length > 30 ? q.sql.slice(0, 30) + '...' : q.sql;
    return `
        <div class="group flex items-center gap-2 px-2 py-1 rounded hover:bg-[#21262d] cursor-pointer"
             onclick="loadSavedQuery(${q.id})"
             title="${q.sql.replace(/"/g, '&quot;')}">
            <span class="text-indigo-400 text-xs">⭐</span>
            <span class="flex-1 truncate text-gray-300 text-xs">${q.name}</span>
            <button onclick="event.stopPropagation(); editSavedQuery(${q.id})"
                    class="opacity-0 group-hover:opacity-100 text-gray-500 hover:text-blue-400 text-xs">✎</button>
            <button onclick="event.stopPropagation(); deleteSavedQuery(${q.id})"
                    class="opacity-0 group-hover:opacity-100 text-gray-500 hover:text-red-400 text-xs">×</button>
        </div>
    `;
}

window.loadSavedQuery = async function(id) {
    const res = await fetch(`/api/saved-queries/${id}`);
    const query = await res.json();

    if (query.error) {
        alert('Error loading query: ' + query.error);
        return;
    }

    if (editor) {
        editor.dispatch({
            changes: { from: 0, to: editor.state.doc.length, insert: query.sql }
        });
        editor.focus();
    }
}

window.showSaveQueryModal = function(editId = null) {
    const sql = editor ? editor.state.doc.toString() : '';

    if (!sql.trim()) {
        alert('No query to save');
        return;
    }

    document.getElementById('save-query-id').value = editId || '';
    document.getElementById('save-query-title').textContent = editId ? 'Edit Saved Query' : 'Save Query';
    document.getElementById('save-query-name').value = '';
    document.getElementById('save-query-folder').value = '';
    document.getElementById('save-query-preview').textContent = sql.slice(0, 200) + (sql.length > 200 ? '...' : '');

    document.getElementById('save-query-modal').classList.remove('hidden');
    document.getElementById('save-query-name').focus();
}

window.hideSaveQueryModal = function() {
    document.getElementById('save-query-modal').classList.add('hidden');
}

window.editSavedQuery = async function(id) {
    const res = await fetch(`/api/saved-queries/${id}`);
    const query = await res.json();

    if (query.error) {
        alert('Error loading query: ' + query.error);
        return;
    }

    document.getElementById('save-query-id').value = id;
    document.getElementById('save-query-title').textContent = 'Edit Saved Query';
    document.getElementById('save-query-name').value = query.name;
    document.getElementById('save-query-folder').value = query.folder || '';
    document.getElementById('save-query-preview').textContent = query.sql.slice(0, 200) + (query.sql.length > 200 ? '...' : '');

    document.getElementById('save-query-modal').classList.remove('hidden');
    document.getElementById('save-query-name').focus();
}

window.deleteSavedQuery = async function(id) {
    if (!confirm('Delete this saved query?')) return;

    await fetch(`/api/saved-queries/${id}`, { method: 'DELETE' });
    loadSavedQueries();
}

document.getElementById('save-query-form').onsubmit = async (e) => {
    e.preventDefault();

    const id = document.getElementById('save-query-id').value;
    const name = document.getElementById('save-query-name').value.trim();
    const folder = document.getElementById('save-query-folder').value.trim();
    const sql = editor ? editor.state.doc.toString() : '';

    if (!name) {
        alert('Please enter a query name');
        return;
    }

    let res;
    if (id) {
        // Update existing
        res = await fetch(`/api/saved-queries/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, folder: folder || null, sql })
        });
    } else {
        // Create new
        res = await fetch('/api/saved-queries', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name,
                sql,
                folder: folder || null,
                connection_id: currentConnection?.id || null
            })
        });
    }

    const result = await res.json();

    if (result.error) {
        alert('Error: ' + result.error);
        return;
    }

    hideSaveQueryModal();
    loadSavedQueries();
}

// Add Ctrl+S shortcut for saving queries
document.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        showSaveQueryModal();
    }
});

// Close save query modal on backdrop click
document.getElementById('save-query-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideSaveQueryModal();
});

// Save tabs before leaving the page
window.addEventListener('beforeunload', () => {
    saveTabsToLocalStorage();
});

// Periodically save tabs (every 5 seconds if there are changes)
setInterval(() => {
    saveTabsToLocalStorage();
}, 5000);

// ============================================================================
// Engine Selection (PostgreSQL vs DuckDB)
// ============================================================================

let currentEngine = 'postgres'; // 'postgres', 'sqlite', or 'duckdb'

window.setEngine = function(engine) {
    currentEngine = engine;

    // Update button styles
    document.getElementById('engine-postgres').classList.toggle('active', engine === 'postgres');
    document.getElementById('engine-sqlite').classList.toggle('active', engine === 'sqlite');
    document.getElementById('engine-duckdb').classList.toggle('active', engine === 'duckdb');

    // Update info text
    const info = document.getElementById('engine-info');
    if (engine === 'postgres') {
        info.textContent = currentConnection ? `→ ${currentConnection.name}` : '(select a connection)';
    } else if (engine === 'sqlite') {
        info.textContent = currentConnection ? `→ ${currentConnection.name}` : '(select a connection)';
    } else {
        info.textContent = '→ In-Memory (supports Parquet, CSV, JSON)';
    }
}

window.selectDuckDB = function() {
    setEngine('duckdb');

    // Update visual indicator
    document.getElementById('duckdb-status').classList.remove('bg-gray-500');
    document.getElementById('duckdb-status').classList.add('bg-green-500');

    // Deselect Postgres connections visually
    if (currentConnection) {
        loadConnections();
    }
}

// Update engine info when connection changes
const originalSelectConnection = window.selectConnection;
window.selectConnection = async function(id, name, type) {
    await originalSelectConnection(id, name, type);

    // Set engine based on connection type
    if (type === 'sqlite') {
        setEngine('sqlite');
    } else if (type === 'duckdb') {
        setEngine('duckdb');
    } else {
        setEngine('postgres');
    }

    // Reset DuckDB indicator if not using DuckDB
    if (type !== 'duckdb') {
        document.getElementById('duckdb-status').classList.remove('bg-green-500');
        document.getElementById('duckdb-status').classList.add('bg-gray-500');
    }
}

// Initialize engine display
setEngine('postgres');

// ============================================================================
// Files Management
// ============================================================================

let currentPreviewFile = null;

// Load files on start
loadFiles();

async function loadFiles() {
    const res = await fetch('/api/files/folders');
    const data = await res.json();

    const list = document.getElementById('files-list');

    if (!data.folders || data.folders.length === 0) {
        list.innerHTML = '<div class="text-gray-500 py-1">No folders added</div>';
        return;
    }

    list.innerHTML = data.folders.map(folder => `
        <details open class="mt-1">
            <summary class="cursor-pointer text-gray-400 hover:text-white py-1 text-xs flex items-center gap-1">
                <span>📁</span> ${folder.name}
                <button onclick="event.stopPropagation(); removeFolder('${folder.path}')"
                        class="ml-auto text-gray-600 hover:text-red-400 text-xs">×</button>
            </summary>
            <div class="ml-3 mt-1 space-y-0.5">
                ${folder.files.length === 0 ? '<div class="text-gray-600 text-xs py-1">No data files</div>' : ''}
                ${folder.files.map(f => `
                    <div class="flex items-center gap-1.5 px-2 py-1 rounded hover:bg-[#21262d] cursor-pointer text-xs"
                         onclick="previewFile('${f.path}', '${f.file_type}', '${f.name}')">
                        <span>${f.icon}</span>
                        <span class="flex-1 truncate text-gray-300" title="${f.name}">${f.name}</span>
                        <span class="text-gray-600">${f.size_human}</span>
                    </div>
                `).join('')}
            </div>
        </details>
    `).join('');
}

window.showAddFolderModal = function() {
    document.getElementById('folder-modal').classList.remove('hidden');
    document.getElementById('folder-path').focus();
}

// ============================================================================
// Create New Database File
// ============================================================================

window.showCreateFileModal = function() {
    document.getElementById('create-file-modal').classList.remove('hidden');
    document.getElementById('create-file-path').focus();
}

window.hideCreateFileModal = function() {
    document.getElementById('create-file-modal').classList.add('hidden');
    document.getElementById('create-file-path').value = '';
}

document.getElementById('create-file-form').onsubmit = async (e) => {
    e.preventDefault();

    const fileType = document.querySelector('input[name="file_type"]:checked').value;
    const path = document.getElementById('create-file-path').value.trim();

    if (!path) return;

    try {
        const res = await fetch('/api/files/create', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type: fileType, path })
        });

        const result = await res.json();

        if (result.success) {
            hideCreateFileModal();
            loadFiles();
            alert(`Created ${fileType.toUpperCase()} database: ${result.path}`);
        } else {
            alert('Error: ' + result.error);
        }
    } catch (err) {
        alert('Error: ' + err.message);
    }
}

// Close create file modal on backdrop click
document.getElementById('create-file-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideCreateFileModal();
});

window.hideFolderModal = function() {
    document.getElementById('folder-modal').classList.add('hidden');
    document.getElementById('folder-path').value = '';
}

window.removeFolder = async function(path) {
    if (!confirm('Remove this folder from the list?')) return;

    await fetch('/api/files/folders', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path })
    });

    loadFiles();
}

document.getElementById('folder-form').onsubmit = async (e) => {
    e.preventDefault();

    const path = document.getElementById('folder-path').value.trim();
    if (!path) return;

    const res = await fetch('/api/files/folders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path })
    });

    const result = await res.json();

    if (!result.success) {
        alert('Error: ' + result.error);
        return;
    }

    hideFolderModal();
    loadFiles();
}

// Close folder modal on backdrop click
document.getElementById('folder-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideFolderModal();
});

// ============================================================================
// File Preview
// ============================================================================

window.previewFile = async function(path, fileType, name) {
    currentPreviewFile = { path, fileType, name };

    document.getElementById('file-preview-modal').classList.remove('hidden');
    document.getElementById('file-preview-title').textContent = name;
    document.getElementById('file-preview-info').innerHTML = 'Loading...';
    document.getElementById('file-preview-content').innerHTML = '<div class="text-gray-500">Loading preview...</div>';

    // Get file info
    const infoRes = await fetch(`/api/files/info?path=${encodeURIComponent(path)}`);
    const info = await infoRes.json();

    if (info.error) {
        document.getElementById('file-preview-info').innerHTML = `<span class="text-red-400">${info.error}</span>`;
        return;
    }

    // Display info
    if (fileType === 'sqlite') {
        document.getElementById('file-preview-info').innerHTML = `
            <span class="text-gray-400">SQLite Database</span> ·
            <span class="text-gray-500">${info.tables?.length || 0} tables</span>
        `;

        // Show tables
        document.getElementById('file-preview-content').innerHTML = `
            <div class="space-y-2">
                ${info.tables?.map(t => `
                    <div class="flex items-center justify-between p-2 rounded bg-[#0d1117] hover:bg-[#21262d] cursor-pointer"
                         onclick="previewSqliteTable('${path}', '${t.name}')">
                        <span class="font-mono text-sm">${t.name}</span>
                        <span class="text-xs text-gray-500">${t.row_count?.toLocaleString() || 0} rows</span>
                    </div>
                `).join('') || '<div class="text-gray-500">No tables found</div>'}
            </div>
        `;
    } else {
        document.getElementById('file-preview-info').innerHTML = `
            <span class="text-gray-400">${fileType.toUpperCase()}</span> ·
            <span class="text-gray-500">${info.row_count_human || info.row_count?.toLocaleString() + ' rows'}</span> ·
            <span class="text-gray-500">${info.columns?.length || 0} columns</span>
        `;

        // Get preview data
        const previewRes = await fetch(`/api/files/preview?path=${encodeURIComponent(path)}`);
        const preview = await previewRes.json();

        if (preview.error) {
            document.getElementById('file-preview-content').innerHTML = `<span class="text-red-400">${preview.error}</span>`;
            return;
        }

        // Render preview table
        renderPreviewTable(preview);
    }
}

window.previewSqliteTable = async function(path, table) {
    document.getElementById('file-preview-info').innerHTML = `Loading ${table}...`;

    const previewRes = await fetch(`/api/files/preview?path=${encodeURIComponent(path)}&table=${encodeURIComponent(table)}`);
    const preview = await previewRes.json();

    if (preview.error) {
        document.getElementById('file-preview-content').innerHTML = `<span class="text-red-400">${preview.error}</span>`;
        return;
    }

    document.getElementById('file-preview-info').innerHTML = `
        <span class="text-gray-400">SQLite</span> ·
        <span class="font-mono text-sm">${table}</span> ·
        <span class="text-gray-500">${preview.row_count} rows (showing first 100)</span>
    `;

    currentPreviewFile.table = table;
    renderPreviewTable(preview);
}

function renderPreviewTable(data) {
    if (!data.columns || data.columns.length === 0) {
        document.getElementById('file-preview-content').innerHTML = '<div class="text-gray-500">No data</div>';
        return;
    }

    document.getElementById('file-preview-content').innerHTML = `
        <div class="overflow-x-auto">
            <table class="w-full text-xs">
                <thead class="bg-[#21262d] sticky top-0">
                    <tr>
                        ${data.columns.map(c => `
                            <th class="px-3 py-2 text-left text-gray-400 font-medium border-b border-[#30363d]">
                                ${c.name}
                                <span class="text-gray-600 font-normal ml-1">${c.type}</span>
                            </th>
                        `).join('')}
                    </tr>
                </thead>
                <tbody class="font-mono">
                    ${data.rows.slice(0, 50).map((row, i) => `
                        <tr class="${i % 2 === 0 ? '' : 'bg-[#0d1117]/50'}">
                            ${row.map(cell => `
                                <td class="px-3 py-1.5 border-b border-[#30363d]/50 max-w-[200px] truncate">
                                    ${cell === null ? '<span class="null-badge">NULL</span>' : String(cell)}
                                </td>
                            `).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
}

window.hideFilePreviewModal = function() {
    document.getElementById('file-preview-modal').classList.add('hidden');
    currentPreviewFile = null;
}

window.insertFileQuery = function() {
    if (!currentPreviewFile) return;

    let query = '';
    const path = currentPreviewFile.path;

    if (currentPreviewFile.fileType === 'parquet') {
        query = `SELECT * FROM read_parquet('${path}') LIMIT 100`;
    } else if (currentPreviewFile.fileType === 'csv' || currentPreviewFile.fileType === 'tsv') {
        query = `SELECT * FROM read_csv_auto('${path}') LIMIT 100`;
    } else if (currentPreviewFile.fileType === 'json') {
        query = `SELECT * FROM read_json_auto('${path}') LIMIT 100`;
    } else if (currentPreviewFile.fileType === 'sqlite' && currentPreviewFile.table) {
        query = `SELECT * FROM sqlite_scan('${path}', '${currentPreviewFile.table}') LIMIT 100`;
    }

    if (query && editor) {
        editor.dispatch({
            changes: { from: 0, to: editor.state.doc.length, insert: query }
        });
        setEngine('duckdb');
        hideFilePreviewModal();
        editor.focus();
    }
}

// Close file preview modal on backdrop click
document.getElementById('file-preview-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideFilePreviewModal();
});

// ============================================================================
// Update runQuery to support DuckDB
// ============================================================================

const originalRunQuery = window.runQuery;
window.runQuery = async function() {
    if (currentEngine === 'duckdb') {
        await runDuckDBQuery();
    } else {
        await originalRunQuery();
    }
}

// ============================================================================
// Export to Parquet
// ============================================================================

window.showParquetModal = function() {
    if (!currentResults || !currentResults.columns || currentResults.columns.length === 0) {
        alert('No results to export');
        return;
    }

    if (currentEngine !== 'duckdb') {
        alert('Parquet export only available with DuckDB engine');
        return;
    }

    document.getElementById('parquet-modal').classList.remove('hidden');
    document.getElementById('parquet-path').focus();
}

window.hideParquetModal = function() {
    document.getElementById('parquet-modal').classList.add('hidden');
    document.getElementById('parquet-path').value = '';
}

document.getElementById('parquet-form').onsubmit = async (e) => {
    e.preventDefault();

    const path = document.getElementById('parquet-path').value.trim();
    if (!path) return;

    const sql = editor ? editor.state.doc.toString() : '';
    if (!sql.trim()) {
        alert('No query to export');
        return;
    }

    try {
        const res = await fetch('/api/duckdb/export-parquet', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, path })
        });

        const result = await res.json();

        if (result.success) {
            alert('Exported successfully to: ' + result.path);
            hideParquetModal();
        } else {
            alert('Export failed: ' + result.error);
        }
    } catch (err) {
        alert('Export failed: ' + err.message);
    }
}

// Close parquet modal on backdrop click
document.getElementById('parquet-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideParquetModal();
});

async function runDuckDBQuery() {
    if (isRunning) return;

    const sqlText = editor.state.doc.toString();

    isRunning = true;
    queryAbortController = new AbortController();

    // Update Run button to show Cancel
    const runBtn = document.getElementById('run-btn');
    runBtn.disabled = false;
    runBtn.onclick = cancelQuery;
    runBtn.innerHTML = `
        <svg class="w-4 h-4 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
        </svg>
        Cancel
    `;
    runBtn.classList.remove('bg-green-600', 'hover:bg-green-700');
    runBtn.classList.add('bg-red-600', 'hover:bg-red-700');

    document.getElementById('results-header').innerHTML = `
        <span class="text-gray-400 flex items-center gap-2">
            <svg class="w-4 h-4 animate-spin" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <circle cx="12" cy="12" r="10" stroke-opacity="0.25" stroke-width="4"></circle>
                <path d="M12 2a10 10 0 0110 10" stroke-width="4"></path>
            </svg>
            Running DuckDB query...
        </span>
    `;
    document.getElementById('results-table').innerHTML = '';

    try {
        const res = await fetch('/api/duckdb/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql: sqlText }),
            signal: queryAbortController.signal
        });

        currentResults = await res.json();

        // Reset sort/filter/page for new results
        sortColumn = null;
        sortDirection = 'asc';
        filterText = '';
        currentPage = 1;

        // Store results in active tab
        const activeTab = tabs.find(t => t.id === activeTabId);
        if (activeTab) {
            activeTab.sql = sqlText;
            activeTab.results = currentResults;
        }

        renderResults();
    } catch (err) {
        if (err.name === 'AbortError') {
            currentResults = { error: 'Query cancelled by user' };
        } else {
            currentResults = { error: err.message };
        }
        renderResults();
    } finally {
        isRunning = false;
        queryAbortController = null;
        resetRunButton();
    }
}

// ============================================================================
// Geo Visualization
// ============================================================================

let currentGeoData = null;
let geoColumnIndex = null;
let map = null;

function hasGeoColumn() {
    if (!currentResults || !currentResults.columns || !currentResults.rows) return false;
    const geoIndices = detectGeoColumns(currentResults.columns, currentResults.rows);
    return geoIndices.length > 0;
}

// WKT patterns for geo detection
const WKT_PATTERN = /^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(/i;
const EWKT_PATTERN = /^SRID=\d+;(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(/i;

function isGeometryValue(value) {
    if (value === null || value === undefined) return false;
    if (typeof value !== 'string') return false;

    const v = value.trim();

    // Check WKT/EWKT
    if (WKT_PATTERN.test(v) || EWKT_PATTERN.test(v)) return true;

    // Check GeoJSON
    if (v.startsWith('{') && v.includes('"type"')) {
        try {
            const data = JSON.parse(v);
            if (data.type && ['Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon', 'GeometryCollection', 'Feature', 'FeatureCollection'].includes(data.type)) {
                return true;
            }
        } catch (e) {}
    }

    // Check hex WKB (starts with 01 or 00, all hex chars)
    if (v.length >= 10 && /^(01|00)[0-9a-fA-F]+$/.test(v)) return true;

    return false;
}

function detectGeoColumns(columns, rows) {
    const geoIndices = [];
    const geoColNames = ['geom', 'geometry', 'the_geom', 'shape', 'geo', 'location', 'wkb_geometry'];
    const geoTypeNames = ['geometry', 'geography', 'point', 'polygon', 'linestring'];

    for (let i = 0; i < columns.length; i++) {
        const colName = (columns[i].name || '').toLowerCase();
        const colType = (columns[i].type || '').toLowerCase();

        // Check by type
        if (geoTypeNames.some(t => colType.includes(t))) {
            geoIndices.push(i);
            continue;
        }

        // Check by name
        if (geoColNames.includes(colName) || colName.endsWith('_geom') || colName.endsWith('_geometry')) {
            geoIndices.push(i);
            continue;
        }

        // Check actual values
        if (rows && rows.length > 0) {
            for (let j = 0; j < Math.min(5, rows.length); j++) {
                if (rows[j] && rows[j][i] && isGeometryValue(rows[j][i])) {
                    geoIndices.push(i);
                    break;
                }
            }
        }
    }

    return geoIndices;
}

function parseWKT(wkt) {
    if (!wkt) return null;
    wkt = wkt.trim();

    // Handle EWKT
    if (wkt.toUpperCase().startsWith('SRID=')) {
        wkt = wkt.split(';')[1] || wkt;
    }

    const upper = wkt.toUpperCase();

    try {
        if (upper.startsWith('POINT')) {
            const match = wkt.match(/POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)/i);
            if (match) {
                return { type: 'Point', coordinates: [parseFloat(match[1]), parseFloat(match[2])] };
            }
        } else if (upper.startsWith('LINESTRING')) {
            const coords = extractCoords(wkt);
            if (coords.length > 0) {
                return { type: 'LineString', coordinates: coords };
            }
        } else if (upper.startsWith('POLYGON')) {
            const rings = extractPolygonRings(wkt);
            if (rings.length > 0) {
                return { type: 'Polygon', coordinates: rings };
            }
        } else if (upper.startsWith('MULTIPOINT')) {
            const coords = extractCoords(wkt);
            if (coords.length > 0) {
                return { type: 'MultiPoint', coordinates: coords };
            }
        } else if (upper.startsWith('MULTILINESTRING')) {
            const lines = extractMultiCoords(wkt);
            if (lines.length > 0) {
                return { type: 'MultiLineString', coordinates: lines };
            }
        } else if (upper.startsWith('MULTIPOLYGON')) {
            const polygons = extractMultiPolygonCoords(wkt);
            if (polygons.length > 0) {
                return { type: 'MultiPolygon', coordinates: polygons };
            }
        }
    } catch (e) {}

    return null;
}

function extractCoords(wkt) {
    const start = wkt.indexOf('(');
    const end = wkt.lastIndexOf(')');
    if (start === -1 || end === -1) return [];

    const content = wkt.slice(start + 1, end).replace(/\(|\)/g, '');
    const coords = [];

    content.split(',').forEach(part => {
        const nums = part.trim().split(/\s+/);
        if (nums.length >= 2) {
            coords.push([parseFloat(nums[0]), parseFloat(nums[1])]);
        }
    });

    return coords;
}

function extractPolygonRings(wkt) {
    const start = wkt.indexOf('(');
    const end = wkt.lastIndexOf(')');
    if (start === -1 || end === -1) return [];

    const content = wkt.slice(start + 1, end);
    const rings = [];
    let depth = 0;
    let current = '';

    for (const char of content) {
        if (char === '(') {
            depth++;
            if (depth === 1) { current = ''; continue; }
        } else if (char === ')') {
            depth--;
            if (depth === 0) {
                const coords = [];
                current.split(',').forEach(part => {
                    const nums = part.trim().split(/\s+/);
                    if (nums.length >= 2) {
                        coords.push([parseFloat(nums[0]), parseFloat(nums[1])]);
                    }
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

function extractMultiCoords(wkt) {
    const start = wkt.indexOf('(');
    const end = wkt.lastIndexOf(')');
    if (start === -1 || end === -1) return [];

    const content = wkt.slice(start + 1, end);
    const lines = [];
    let depth = 0;
    let current = '';

    for (const char of content) {
        if (char === '(') {
            depth++;
            if (depth === 1) { current = ''; continue; }
        } else if (char === ')') {
            depth--;
            if (depth === 0) {
                const coords = [];
                current.split(',').forEach(part => {
                    const nums = part.trim().split(/\s+/);
                    if (nums.length >= 2) {
                        coords.push([parseFloat(nums[0]), parseFloat(nums[1])]);
                    }
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

function extractMultiPolygonCoords(wkt) {
    // Simplified extraction
    const start = wkt.indexOf('((');
    const end = wkt.lastIndexOf('))');
    if (start === -1 || end === -1) return [];

    // Split by polygon boundaries
    const content = wkt.slice(start, end + 2);
    const polygons = [];

    // This is a simplified parser - works for basic cases
    let depth = 0;
    let polygonContent = '';

    for (let i = 0; i < content.length; i++) {
        const char = content[i];
        if (char === '(') depth++;
        else if (char === ')') depth--;

        polygonContent += char;

        if (depth === 1 && content[i+1] === ',' && content[i+2] === '(') {
            // End of polygon
            const rings = extractPolygonRings(polygonContent);
            if (rings.length > 0) polygons.push(rings);
            polygonContent = '';
            i += 2; // Skip ", ("
        }
    }

    // Last polygon
    if (polygonContent) {
        const rings = extractPolygonRings(polygonContent);
        if (rings.length > 0) polygons.push(rings);
    }

    return polygons;
}

function geometryToGeoJSON(value) {
    if (!value) return null;

    // Already GeoJSON object
    if (typeof value === 'object' && value.type) return value;

    if (typeof value === 'string') {
        const v = value.trim();

        // Try GeoJSON string
        if (v.startsWith('{')) {
            try {
                const data = JSON.parse(v);
                if (data.type) return data;
            } catch (e) {}
        }

        // Try WKT
        return parseWKT(v);
    }

    return null;
}

function rowsToGeoJSON(columns, rows, geoColIdx) {
    const features = [];
    const colNames = columns.map((c, i) => c.name || `col_${i}`);

    for (const row of rows) {
        if (geoColIdx >= row.length) continue;

        const geom = geometryToGeoJSON(row[geoColIdx]);
        if (!geom) continue;

        const properties = {};
        for (let i = 0; i < row.length; i++) {
            if (i === geoColIdx) continue;
            const colName = colNames[i] || `col_${i}`;
            const val = row[i];

            if (val === null || val === undefined) {
                properties[colName] = null;
            } else if (typeof val === 'object') {
                properties[colName] = JSON.stringify(val);
            } else {
                properties[colName] = val;
            }
        }

        features.push({
            type: 'Feature',
            geometry: geom,
            properties: properties
        });
    }

    return { type: 'FeatureCollection', features: features };
}

window.showMapModal = function() {
    if (!currentResults || !currentResults.columns) return;

    const geoIndices = detectGeoColumns(currentResults.columns, currentResults.rows || []);
    if (geoIndices.length === 0) {
        alert('No geometry columns detected');
        return;
    }

    geoColumnIndex = geoIndices[0]; // Use first geo column
    currentGeoData = rowsToGeoJSON(currentResults.columns, currentResults.rows || [], geoColumnIndex);

    if (currentGeoData.features.length === 0) {
        alert('No valid geometries found');
        return;
    }

    document.getElementById('map-modal').classList.remove('hidden');
    document.getElementById('map-feature-count').textContent = `${currentGeoData.features.length} features`;

    // Initialize map after modal is visible
    setTimeout(() => initMap(), 100);
}

window.hideMapModal = function() {
    document.getElementById('map-modal').classList.add('hidden');
    if (map) {
        map.remove();
        map = null;
    }
}

function initMap() {
    const container = document.getElementById('map-container');
    container.innerHTML = '';

    // Calculate bounds from features
    let bounds = null;

    for (const feature of currentGeoData.features) {
        const geom = feature.geometry;
        if (!geom) continue;

        const coords = getAllCoords(geom);
        for (const coord of coords) {
            if (!bounds) {
                bounds = [[coord[0], coord[1]], [coord[0], coord[1]]];
            } else {
                bounds[0][0] = Math.min(bounds[0][0], coord[0]);
                bounds[0][1] = Math.min(bounds[0][1], coord[1]);
                bounds[1][0] = Math.max(bounds[1][0], coord[0]);
                bounds[1][1] = Math.max(bounds[1][1], coord[1]);
            }
        }
    }

    // Default center if no bounds
    const center = bounds
        ? [(bounds[0][0] + bounds[1][0]) / 2, (bounds[0][1] + bounds[1][1]) / 2]
        : [0, 0];

    map = new maplibregl.Map({
        container: container,
        style: {
            version: 8,
            sources: {
                'carto-dark': {
                    type: 'raster',
                    tiles: ['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'],
                    tileSize: 256,
                    attribution: '&copy; <a href="https://carto.com/">CARTO</a>'
                }
            },
            layers: [{
                id: 'carto-dark-layer',
                type: 'raster',
                source: 'carto-dark',
                minzoom: 0,
                maxzoom: 22
            }]
        },
        center: center,
        zoom: 2
    });

    map.on('load', () => {
        // Add GeoJSON source
        map.addSource('data', {
            type: 'geojson',
            data: currentGeoData
        });

        // Add polygon layer
        map.addLayer({
            id: 'polygons',
            type: 'fill',
            source: 'data',
            filter: ['any',
                ['==', ['geometry-type'], 'Polygon'],
                ['==', ['geometry-type'], 'MultiPolygon']
            ],
            paint: {
                'fill-color': '#6366f1',
                'fill-opacity': 0.4
            }
        });

        // Add polygon outline
        map.addLayer({
            id: 'polygon-outlines',
            type: 'line',
            source: 'data',
            filter: ['any',
                ['==', ['geometry-type'], 'Polygon'],
                ['==', ['geometry-type'], 'MultiPolygon']
            ],
            paint: {
                'line-color': '#818cf8',
                'line-width': 2
            }
        });

        // Add line layer
        map.addLayer({
            id: 'lines',
            type: 'line',
            source: 'data',
            filter: ['any',
                ['==', ['geometry-type'], 'LineString'],
                ['==', ['geometry-type'], 'MultiLineString']
            ],
            paint: {
                'line-color': '#22c55e',
                'line-width': 3
            }
        });

        // Add point layer
        map.addLayer({
            id: 'points',
            type: 'circle',
            source: 'data',
            filter: ['any',
                ['==', ['geometry-type'], 'Point'],
                ['==', ['geometry-type'], 'MultiPoint']
            ],
            paint: {
                'circle-radius': 6,
                'circle-color': '#f59e0b',
                'circle-stroke-width': 2,
                'circle-stroke-color': '#fbbf24'
            }
        });

        // Fit to bounds
        if (bounds) {
            map.fitBounds(bounds, { padding: 50, maxZoom: 15 });
        }

        // Add popup on click
        let clickPopup = null;

        function showClickPopup(e) {
            if (!e.features || e.features.length === 0) return;

            // Remove existing popup
            if (clickPopup) clickPopup.remove();

            const feature = e.features[0];
            const props = feature.properties;

            let html = '<div class="text-xs max-w-xs max-h-48 overflow-auto">';
            html += '<table class="w-full">';
            for (const [key, value] of Object.entries(props)) {
                const displayVal = value === null ? '<span class="text-gray-400">NULL</span>' : String(value).slice(0, 100);
                html += `<tr><td class="font-bold pr-2 text-gray-300">${key}</td><td class="text-gray-400">${displayVal}</td></tr>`;
            }
            html += '</table></div>';

            clickPopup = new maplibregl.Popup({ className: 'dark-popup' })
                .setLngLat(e.lngLat)
                .setHTML(html)
                .addTo(map);
        }

        // Register click handlers for each layer
        map.on('click', 'points', showClickPopup);
        map.on('click', 'lines', showClickPopup);
        map.on('click', 'polygons', showClickPopup);
        map.on('click', 'polygon-outlines', showClickPopup);

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
                .setHTML(`<div class="bg-[#1c2128] text-white px-2 py-1 rounded text-xs font-medium shadow-lg border border-[#30363d]">${label}</div>`)
                .addTo(map);
        }

        function hideHoverTooltip() {
            if (hoverPopup) {
                hoverPopup.remove();
                hoverPopup = null;
            }
        }

        // Change cursor and show tooltip on hover - register for each layer
        const layers = ['points', 'lines', 'polygons'];

        layers.forEach(layer => {
            map.on('mouseenter', layer, (e) => {
                map.getCanvas().style.cursor = 'pointer';
                showHoverTooltip(e);
            });
            map.on('mouseleave', layer, () => {
                map.getCanvas().style.cursor = '';
                hideHoverTooltip();
            });
            map.on('mousemove', layer, showHoverTooltip);
        });
    });
}

function getAllCoords(geom) {
    const coords = [];
    const type = geom.type;
    const c = geom.coordinates;

    if (type === 'Point') {
        coords.push(c);
    } else if (type === 'MultiPoint' || type === 'LineString') {
        coords.push(...c);
    } else if (type === 'MultiLineString' || type === 'Polygon') {
        c.forEach(ring => coords.push(...ring));
    } else if (type === 'MultiPolygon') {
        c.forEach(poly => poly.forEach(ring => coords.push(...ring)));
    }

    return coords;
}

window.exportGeoJSON = function() {
    if (!currentGeoData) return;

    const blob = new Blob([JSON.stringify(currentGeoData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'export.geojson';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// Close map modal on backdrop click
document.getElementById('map-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideMapModal();
});

// Close map modal on Escape
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !document.getElementById('map-modal').classList.contains('hidden')) {
        hideMapModal();
    }
});
