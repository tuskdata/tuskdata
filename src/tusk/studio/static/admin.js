// Tusk Admin Page JavaScript

let currentServer = null;
let refreshInterval = null;

// Load servers on start
document.addEventListener('DOMContentLoaded', loadServers);

async function loadServers() {
    const res = await fetch('/api/connections');
    const conns = await res.json();

    // Filter to PostgreSQL only
    const pgConns = conns.filter(c => c.type === 'postgres');

    const list = document.getElementById('servers-list');
    if (pgConns.length === 0) {
        list.innerHTML = `
            <div class="text-gray-500 text-sm py-2">No PostgreSQL connections</div>
            <a href="/" class="text-sm text-indigo-400 hover:text-indigo-300">Add one in Studio →</a>
        `;
        return;
    }

    list.innerHTML = pgConns.map(c => `
        <div class="flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer hover:bg-[#21262d] ${currentServer?.id === c.id ? 'bg-[#21262d] ring-1 ring-indigo-500/50' : ''}"
             onclick="selectServer('${c.id}', '${c.name}')">
            <span class="w-2 h-2 rounded-full ${currentServer?.id === c.id ? 'bg-green-500' : 'bg-gray-500'}"></span>
            <span>🐘</span>
            <span class="text-sm flex-1 truncate">${c.name}</span>
        </div>
    `).join('');
}

window.selectServer = async function(id, name) {
    currentServer = { id, name };
    loadServers();

    // Enable action buttons
    document.getElementById('backup-btn').disabled = false;
    document.getElementById('backups-btn').disabled = false;

    // Show admin content
    document.getElementById('no-server').classList.add('hidden');
    document.getElementById('admin-content').classList.remove('hidden');
    document.getElementById('server-name').textContent = name;

    // Load all sections
    await Promise.all([
        refreshStats(),
        refreshProcesses(),
        refreshLocks(),
        refreshTableBloat(),
        refreshExtensions(),
        refreshRoles(),
        refreshSettings(),
        refreshSlowQueries(),
        refreshIndexUsage(),
        refreshReplication(),
        refreshPITR(),
        refreshLogs()
    ]);

    // Start auto-refresh based on selected interval
    setRefreshInterval();
}

window.setRefreshInterval = function() {
    if (refreshInterval) clearInterval(refreshInterval);

    const interval = parseInt(document.getElementById('refresh-interval').value);
    if (interval > 0 && currentServer) {
        refreshInterval = setInterval(() => {
            refreshStats();
            refreshProcesses();
        }, interval);
    }
}

window.refreshAll = function() {
    if (!currentServer) return;
    Promise.all([
        refreshStats(),
        refreshProcesses(),
        refreshLocks(),
        refreshTableBloat(),
        refreshExtensions(),
        refreshSlowQueries(),
        refreshIndexUsage(),
        refreshReplication(),
        refreshPITR(),
        refreshLogs()
    ]);
}

async function refreshStats() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/stats`);
    const stats = await res.json();

    if (stats.error) {
        console.error('Stats error:', stats.error);
        return;
    }

    document.getElementById('stat-connections').textContent = `${stats.connections}/${stats.max_connections}`;
    document.getElementById('conn-bar').style.width = `${stats.connection_pct}%`;

    document.getElementById('stat-active').textContent = stats.active_queries;

    document.getElementById('stat-cache').textContent = `${stats.cache_hit_ratio}%`;
    document.getElementById('cache-bar').style.width = `${stats.cache_hit_ratio}%`;

    document.getElementById('stat-size').textContent = stats.db_size_human;
    document.getElementById('stat-uptime').textContent = `Up ${stats.uptime}`;

    // Parse version (e.g., "PostgreSQL 16.1 on ...")
    const versionMatch = stats.version.match(/PostgreSQL [\d.]+/);
    document.getElementById('server-version').textContent = versionMatch ? versionMatch[0] : stats.version.slice(0, 50);
}

window.refreshProcesses = async function() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/processes`);
    const data = await res.json();

    const list = document.getElementById('processes-list');

    if (data.error) {
        list.innerHTML = `<div class="p-4 text-red-400">${data.error}</div>`;
        return;
    }

    if (data.processes.length === 0) {
        list.innerHTML = `<div class="p-4 text-gray-500">No active processes</div>`;
        return;
    }

    list.innerHTML = data.processes.map(p => {
        const isSlow = p.duration_seconds > 10;
        const isIdleInTransaction = p.state === 'idle in transaction';
        const needsAttention = isSlow || isIdleInTransaction;

        return `
        <div class="p-4 flex items-start justify-between gap-4 hover:bg-[#21262d]/50 ${needsAttention ? 'bg-red-500/5 border-l-2 border-red-500' : ''}">
            <div class="flex-1 min-w-0">
                <div class="flex items-center gap-3 mb-1 flex-wrap">
                    <span class="inline-flex items-center gap-1.5">
                        <span class="w-2 h-2 rounded-full ${p.state === 'active' ? 'bg-green-500' : p.state === 'idle' ? 'bg-gray-500' : 'bg-yellow-500'}"></span>
                        <span class="font-mono text-sm">PID ${p.pid}</span>
                    </span>
                    <span class="text-gray-500 text-sm">${p.user}@${p.database}</span>
                    <span class="${isSlow ? 'text-red-400 font-medium' : 'text-gray-500'} text-sm">${p.duration_human}</span>
                    <span class="text-xs px-2 py-0.5 rounded-full ${
                        p.state === 'active' ? 'bg-green-500/20 text-green-400' :
                        p.state === 'idle' ? 'bg-gray-500/20 text-gray-400' :
                        isIdleInTransaction ? 'bg-red-500/20 text-red-400' :
                        'bg-yellow-500/20 text-yellow-400'
                    }">${p.state}</span>
                    ${isSlow ? '<span class="text-xs px-2 py-0.5 rounded-full bg-red-500/20 text-red-400">⚠ SLOW</span>' : ''}
                </div>
                <div class="font-mono text-xs text-gray-400 truncate" title="${p.query.replace(/"/g, '&quot;')}">${p.query_preview}</div>
            </div>
            ${p.state !== 'idle' ? `
                <button onclick="killProcess(${p.pid})"
                        class="text-red-400 hover:text-red-300 hover:bg-red-500/20 px-2 py-1 rounded transition-colors text-sm"
                        title="Terminate process">
                    ✖ Kill
                </button>
            ` : ''}
        </div>
    `}).join('');
}

window.killProcess = async function(pid) {
    if (!confirm(`Terminate process ${pid}?`)) return;

    const res = await fetch(`/api/admin/${currentServer.id}/kill/${pid}`, { method: 'POST' });
    const result = await res.json();

    if (result.success) {
        refreshProcesses();
    } else {
        showToast('Failed to terminate: ' + result.message, 'error');
    }
}

window.refreshLocks = async function() {
    if (!currentServer) return;

    const showAll = document.getElementById('show-all-locks').checked;
    const endpoint = showAll ? 'locks/all' : 'locks';
    const res = await fetch(`/api/admin/${currentServer.id}/${endpoint}`);
    const data = await res.json();

    const list = document.getElementById('locks-list');

    if (data.error) {
        list.innerHTML = `<div class="p-4 text-red-400">${data.error}</div>`;
        return;
    }

    if (data.locks.length === 0) {
        list.innerHTML = `<div class="p-4 text-gray-500">${showAll ? 'No locks active' : 'No blocking locks detected ✓'}</div>`;
        return;
    }

    if (showAll) {
        // Show all locks format
        list.innerHTML = data.locks.map(l => `
            <div class="p-4 flex items-start justify-between gap-4 hover:bg-[#21262d]/50 ${!l.granted ? 'bg-yellow-500/5 border-l-2 border-yellow-500' : ''}">
                <div class="flex-1 min-w-0">
                    <div class="flex items-center gap-3 mb-1">
                        <span class="font-mono text-sm">PID ${l.pid}</span>
                        <span class="text-gray-500 text-sm">${l.user || 'unknown'}</span>
                        <span class="text-xs px-2 py-0.5 rounded-full ${l.granted ? 'bg-green-500/20 text-green-400' : 'bg-yellow-500/20 text-yellow-400'}">${l.granted ? 'granted' : 'waiting'}</span>
                        <span class="text-xs px-2 py-0.5 rounded-full bg-blue-500/20 text-blue-400">${l.mode}</span>
                    </div>
                    <div class="text-sm text-gray-400">
                        <span class="text-gray-500">${l.lock_type}:</span> ${l.locked_item}
                        ${l.duration ? `<span class="text-gray-500 ml-2">${l.duration}</span>` : ''}
                    </div>
                    <div class="font-mono text-xs text-gray-500 truncate mt-1">${l.query || ''}</div>
                </div>
                ${!l.granted ? `
                    <button onclick="killProcess(${l.pid})"
                            class="text-red-400 hover:text-red-300 hover:bg-red-500/20 px-2 py-1 rounded transition-colors text-sm">
                        ✖ Kill
                    </button>
                ` : ''}
            </div>
        `).join('');
    } else {
        // Show blocking locks format
        list.innerHTML = data.locks.map(l => `
            <div class="p-4 hover:bg-[#21262d]/50 bg-red-500/5 border-l-2 border-red-500">
                <div class="grid grid-cols-2 gap-4">
                    <div>
                        <div class="text-xs text-gray-500 mb-1">BLOCKED</div>
                        <div class="flex items-center gap-2 mb-1">
                            <span class="font-mono text-sm">PID ${l.blocked_pid}</span>
                            <span class="text-gray-500 text-sm">${l.blocked_user}</span>
                            <span class="text-xs px-2 py-0.5 rounded-full bg-red-500/20 text-red-400">${l.duration}</span>
                        </div>
                        <div class="font-mono text-xs text-gray-400 truncate">${l.blocked_query}</div>
                    </div>
                    <div>
                        <div class="text-xs text-gray-500 mb-1">BLOCKING</div>
                        <div class="flex items-center gap-2 mb-1">
                            <span class="font-mono text-sm">PID ${l.blocking_pid}</span>
                            <span class="text-gray-500 text-sm">${l.blocking_user}</span>
                        </div>
                        <div class="font-mono text-xs text-gray-400 truncate">${l.blocking_query}</div>
                    </div>
                </div>
                <div class="flex items-center gap-4 mt-3">
                    <span class="text-xs px-2 py-0.5 rounded-full bg-blue-500/20 text-blue-400">${l.mode}</span>
                    <span class="text-gray-500 text-sm">${l.locked_item}</span>
                    <button onclick="killProcess(${l.blocking_pid})"
                            class="ml-auto text-red-400 hover:text-red-300 hover:bg-red-500/20 px-2 py-1 rounded transition-colors text-sm">
                        ✖ Kill Blocker
                    </button>
                </div>
            </div>
        `).join('');
    }
}

window.refreshTableBloat = async function() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/tables/bloat`);
    const data = await res.json();

    const list = document.getElementById('bloat-list');

    if (data.error) {
        list.innerHTML = `<tr><td colspan="7" class="p-4 text-red-400">${data.error}</td></tr>`;
        return;
    }

    if (data.tables.length === 0) {
        list.innerHTML = `<tr><td colspan="7" class="p-4 text-gray-500">No tables found</td></tr>`;
        return;
    }

    list.innerHTML = data.tables.map(t => {
        const bloatLevel = t.bloat_ratio > 50 ? 'high' : t.bloat_ratio > 20 ? 'medium' : 'low';
        const bloatColor = bloatLevel === 'high' ? 'text-red-400' : bloatLevel === 'medium' ? 'text-yellow-400' : 'text-gray-400';
        const needsVacuum = t.dead_tuples > 1000 || t.bloat_ratio > 20;

        return `
        <tr class="hover:bg-[#21262d]/50 ${needsVacuum ? 'bg-yellow-500/5' : ''}">
            <td class="px-4 py-3">
                <span class="font-mono text-sm">${t.schema}.${t.table}</span>
            </td>
            <td class="px-4 py-3 text-gray-400">${t.size_human}</td>
            <td class="px-4 py-3">
                <span class="${t.dead_tuples > 1000 ? 'text-yellow-400' : 'text-gray-400'}">${t.dead_tuples.toLocaleString()}</span>
                <span class="text-gray-600 text-xs">/ ${t.live_tuples.toLocaleString()}</span>
            </td>
            <td class="px-4 py-3">
                <span class="${bloatColor} font-medium">${t.bloat_ratio}%</span>
            </td>
            <td class="px-4 py-3 text-gray-500 text-xs">${t.last_vacuum ? new Date(t.last_vacuum).toLocaleDateString() : 'never'}</td>
            <td class="px-4 py-3 text-gray-500 text-xs">${t.last_analyze ? new Date(t.last_analyze).toLocaleDateString() : 'never'}</td>
            <td class="px-4 py-3 text-right">
                <div class="flex items-center justify-end gap-1">
                    <button onclick="vacuumTable('${t.schema}', '${t.table}', false)"
                            class="text-blue-400 hover:bg-blue-500/20 px-2 py-1 rounded text-xs"
                            title="VACUUM">
                        Vacuum
                    </button>
                    <button onclick="vacuumTable('${t.schema}', '${t.table}', true)"
                            class="text-orange-400 hover:bg-orange-500/20 px-2 py-1 rounded text-xs"
                            title="VACUUM FULL (locks table)">
                        Full
                    </button>
                    <button onclick="analyzeTable('${t.schema}', '${t.table}')"
                            class="text-green-400 hover:bg-green-500/20 px-2 py-1 rounded text-xs"
                            title="ANALYZE">
                        Analyze
                    </button>
                    <button onclick="reindexTable('${t.schema}', '${t.table}')"
                            class="text-purple-400 hover:bg-purple-500/20 px-2 py-1 rounded text-xs"
                            title="REINDEX">
                        Reindex
                    </button>
                </div>
            </td>
        </tr>
    `}).join('');
}

window.vacuumTable = async function(schema, table, full = false) {
    if (!currentServer) return;
    const action = full ? 'VACUUM FULL' : 'VACUUM';
    if (full && !confirm(`${action} will lock the table. Continue?`)) return;

    const btn = event.target;
    const originalText = btn.textContent;
    btn.textContent = '...';
    btn.disabled = true;

    const res = await fetch(`/api/admin/${currentServer.id}/tables/${schema}/${table}/vacuum`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ full })
    });
    const result = await res.json();

    btn.textContent = originalText;
    btn.disabled = false;

    if (result.success) {
        await refreshTableBloat();
    } else {
        showToast('Failed: ' + result.error, 'error');
    }
}

window.analyzeTable = async function(schema, table) {
    if (!currentServer) return;

    const btn = event.target;
    const originalText = btn.textContent;
    btn.textContent = '...';
    btn.disabled = true;

    const res = await fetch(`/api/admin/${currentServer.id}/tables/${schema}/${table}/analyze`, { method: 'POST' });
    const result = await res.json();

    btn.textContent = originalText;
    btn.disabled = false;

    if (result.success) {
        await refreshTableBloat();
    } else {
        showToast('Failed: ' + result.error, 'error');
    }
}

window.reindexTable = async function(schema, table) {
    if (!currentServer) return;
    if (!confirm(`REINDEX will lock the table. Continue?`)) return;

    const btn = event.target;
    const originalText = btn.textContent;
    btn.textContent = '...';
    btn.disabled = true;

    const res = await fetch(`/api/admin/${currentServer.id}/tables/${schema}/${table}/reindex`, { method: 'POST' });
    const result = await res.json();

    btn.textContent = originalText;
    btn.disabled = false;

    if (result.success) {
        await refreshTableBloat();
    } else {
        showToast('Failed: ' + result.error, 'error');
    }
}

window.createBackup = async function() {
    if (!currentServer) return;

    const btn = document.getElementById('backup-btn');
    btn.disabled = true;
    btn.innerHTML = '<span>⏳</span> Creating backup...';

    const res = await fetch(`/api/admin/${currentServer.id}/backup`, { method: 'POST' });
    const result = await res.json();

    btn.disabled = false;
    btn.innerHTML = '<span>💾</span> Create Backup';

    if (result.success) {
        showToast(result.message, 'success');
    } else {
        showToast('Backup failed: ' + result.message, 'error');
    }
}

window.showBackups = async function() {
    if (!currentServer) return;

    document.getElementById('backups-modal').classList.remove('hidden');

    const res = await fetch(`/api/admin/${currentServer.id}/backups`);
    const data = await res.json();

    const list = document.getElementById('backups-list');

    if (data.error) {
        list.innerHTML = `<div class="text-red-400">${data.error}</div>`;
        return;
    }

    if (data.backups.length === 0) {
        list.innerHTML = `<div class="text-gray-500">No backups found for this database</div>`;
        return;
    }

    list.innerHTML = data.backups.map(b => `
        <div class="p-3 rounded-lg hover:bg-[#21262d] mb-2 border border-[#30363d]">
            <div class="flex items-center justify-between mb-2">
                <div>
                    <div class="font-mono text-sm">${b.filename}</div>
                    <div class="text-xs text-gray-500">${b.size_human} · ${new Date(b.created).toLocaleString()}</div>
                </div>
                <a href="/api/admin/backups/${b.filename}"
                   class="text-indigo-400 hover:text-indigo-300 text-sm px-2 py-1 rounded hover:bg-indigo-500/20"
                   title="Download backup">
                    ↓
                </a>
            </div>
            <div class="flex gap-2">
                <button onclick="restoreBackup('${b.filename}')"
                        class="flex-1 text-xs px-2 py-1.5 rounded bg-amber-600/20 text-amber-400 hover:bg-amber-600/30">
                    Restore to current DB
                </button>
                <button onclick="showCreateDbFromBackup('${b.filename}')"
                        class="flex-1 text-xs px-2 py-1.5 rounded bg-green-600/20 text-green-400 hover:bg-green-600/30">
                    Create new DB
                </button>
            </div>
        </div>
    `).join('');
}

window.hideBackupsModal = function() {
    document.getElementById('backups-modal').classList.add('hidden');
}

window.restoreBackup = async function(filename) {
    if (!currentServer) return;

    if (!confirm(`This will overwrite the current database "${currentServer.database}" with the backup.\n\nAre you sure?`)) {
        return;
    }

    showToast('Restoring backup...', 'info');

    try {
        const res = await fetch(`/api/admin/${currentServer.id}/restore`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename })
        });
        const data = await res.json();

        if (data.success) {
            showToast('Database restored successfully!', 'success');
            hideBackupsModal();
        } else {
            showToast('Restore failed: ' + data.message, 'error');
        }
    } catch (err) {
        showToast('Restore failed: ' + err.message, 'error');
    }
}

let selectedBackupForNewDb = null;

window.showCreateDbFromBackup = function(filename) {
    selectedBackupForNewDb = filename;
    document.getElementById('create-db-backup-name').textContent = filename;
    document.getElementById('create-db-name').value = '';
    document.getElementById('create-db-modal').classList.remove('hidden');
}

window.hideCreateDbModal = function() {
    document.getElementById('create-db-modal').classList.add('hidden');
    selectedBackupForNewDb = null;
}

window.createDbFromBackup = async function() {
    if (!currentServer || !selectedBackupForNewDb) return;

    const dbName = document.getElementById('create-db-name').value.trim();
    if (!dbName) {
        showToast('Please enter a database name', 'warning');
        return;
    }

    // Validate database name
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(dbName)) {
        showToast('Invalid database name. Use letters, numbers, and underscores only.', 'warning');
        return;
    }

    showToast('Creating database from backup...', 'info');

    try {
        const res = await fetch(`/api/admin/${currentServer.id}/databases/from-backup`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: dbName,
                filename: selectedBackupForNewDb
            })
        });
        const data = await res.json();

        if (data.success) {
            showToast(`Database "${dbName}" created successfully!`, 'success');
            hideCreateDbModal();
            hideBackupsModal();
        } else {
            showToast('Failed: ' + data.message, 'error');
        }
    } catch (err) {
        showToast('Failed: ' + err.message, 'error');
    }
}

window.refreshExtensions = async function() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/extensions`);
    const data = await res.json();

    const list = document.getElementById('extensions-list');
    const showAll = document.getElementById('show-all-extensions').checked;

    if (data.error) {
        list.innerHTML = `<div class="p-4 text-red-400">${data.error}</div>`;
        return;
    }

    // Filter extensions
    let extensions = data.extensions;
    if (!showAll) {
        extensions = extensions.filter(e => e.is_installed);
    }

    if (extensions.length === 0) {
        list.innerHTML = `<div class="p-4 text-gray-500">${showAll ? 'No extensions available' : 'No extensions installed'}</div>`;
        return;
    }

    list.innerHTML = extensions.map(e => `
        <div class="p-4 flex items-start justify-between gap-4 hover:bg-[#21262d]/50">
            <div class="flex-1 min-w-0">
                <div class="flex items-center gap-3 mb-1">
                    <span class="font-medium">${e.name}</span>
                    ${e.is_installed
                        ? `<span class="text-xs px-2 py-0.5 rounded-full bg-green-500/20 text-green-400">v${e.installed_version}</span>`
                        : `<span class="text-xs px-2 py-0.5 rounded-full bg-gray-500/20 text-gray-400">v${e.default_version}</span>`
                    }
                </div>
                <div class="text-sm text-gray-400 truncate" title="${(e.description || '').replace(/"/g, '&quot;')}">${e.description || 'No description'}</div>
            </div>
            ${e.is_installed
                ? `<button onclick="uninstallExtension('${e.name}')"
                          class="text-red-400 hover:text-red-300 hover:bg-red-500/20 px-3 py-1 rounded transition-colors text-sm">
                      Uninstall
                   </button>`
                : `<button onclick="installExtension('${e.name}')"
                          class="text-green-400 hover:text-green-300 hover:bg-green-500/20 px-3 py-1 rounded transition-colors text-sm">
                      Install
                   </button>`
            }
        </div>
    `).join('');
}

window.installExtension = async function(name) {
    if (!currentServer) return;
    if (!confirm(`Install extension "${name}"?`)) return;

    const res = await fetch(`/api/admin/${currentServer.id}/extensions/${name}/install`, { method: 'POST' });
    const result = await res.json();

    if (result.success) {
        await refreshExtensions();
    } else {
        showToast('Failed to install: ' + result.error, 'error');
    }
}

window.uninstallExtension = async function(name) {
    if (!currentServer) return;
    if (!confirm(`Uninstall extension "${name}"? This may fail if other objects depend on it.`)) return;

    const res = await fetch(`/api/admin/${currentServer.id}/extensions/${name}/uninstall`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cascade: false })
    });
    const result = await res.json();

    if (result.success) {
        await refreshExtensions();
    } else {
        // Offer cascade option
        if (result.error.includes('depends on') || result.error.includes('CASCADE')) {
            if (confirm(`Other objects depend on this extension. Remove with CASCADE (will drop dependent objects)?`)) {
                const res2 = await fetch(`/api/admin/${currentServer.id}/extensions/${name}/uninstall`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ cascade: true })
                });
                const result2 = await res2.json();
                if (result2.success) {
                    await refreshExtensions();
                } else {
                    showToast('Failed to uninstall: ' + result2.error, 'error');
                }
            }
        } else {
            showToast('Failed to uninstall: ' + result.error, 'error');
        }
    }
}

// ===== Settings Viewer =====

window.refreshSettings = async function() {
    if (!currentServer) return;

    const showAll = document.getElementById('show-all-settings').checked;
    const url = showAll
        ? `/api/admin/${currentServer.id}/settings`
        : `/api/admin/${currentServer.id}/settings?important_only=true`;

    const res = await fetch(url);
    const data = await res.json();

    const list = document.getElementById('settings-list');

    if (data.error) {
        list.innerHTML = `<tr><td colspan="4" class="p-4 text-red-400">${data.error}</td></tr>`;
        return;
    }

    if (data.settings.length === 0) {
        list.innerHTML = `<tr><td colspan="4" class="p-4 text-gray-500">No settings found</td></tr>`;
        return;
    }

    list.innerHTML = data.settings.map(s => `
        <tr class="hover:bg-[#21262d]/50">
            <td class="px-4 py-2">
                <span class="font-mono text-sm">${s.name}</span>
                ${s.pending_restart ? '<span class="ml-2 text-xs px-2 py-0.5 rounded-full bg-yellow-500/20 text-yellow-400">restart needed</span>' : ''}
            </td>
            <td class="px-4 py-2">
                <span class="text-indigo-400 font-medium">${s.formatted}</span>
                ${s.boot_val && s.setting !== s.boot_val ? `<span class="text-gray-500 text-xs ml-2">(default: ${s.boot_val})</span>` : ''}
            </td>
            <td class="px-4 py-2 text-gray-500 text-xs">${s.category || '-'}</td>
            <td class="px-4 py-2 text-gray-400 text-xs max-w-xs truncate" title="${(s.description || '').replace(/"/g, '&quot;')}">${s.description || '-'}</td>
        </tr>
    `).join('');
}

// ===== Roles Management =====

window.refreshRoles = async function() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/roles`);
    const data = await res.json();

    const list = document.getElementById('roles-list');

    if (data.error) {
        list.innerHTML = `<tr><td colspan="7" class="p-4 text-red-400">${data.error}</td></tr>`;
        return;
    }

    if (data.roles.length === 0) {
        list.innerHTML = `<tr><td colspan="7" class="p-4 text-gray-500">No roles found</td></tr>`;
        return;
    }

    list.innerHTML = data.roles.map(r => `
        <tr class="hover:bg-[#21262d]/50">
            <td class="px-4 py-3">
                <span class="font-medium">${r.name}</span>
                ${r.is_superuser ? '<span class="ml-2 text-xs px-2 py-0.5 rounded-full bg-red-500/20 text-red-400">superuser</span>' : ''}
            </td>
            <td class="px-4 py-3">${r.can_login ? '<span class="text-green-400">✓</span>' : '<span class="text-gray-600">✗</span>'}</td>
            <td class="px-4 py-3">${r.is_superuser ? '<span class="text-green-400">✓</span>' : '<span class="text-gray-600">✗</span>'}</td>
            <td class="px-4 py-3">${r.can_create_db ? '<span class="text-green-400">✓</span>' : '<span class="text-gray-600">✗</span>'}</td>
            <td class="px-4 py-3">${r.can_create_role ? '<span class="text-green-400">✓</span>' : '<span class="text-gray-600">✗</span>'}</td>
            <td class="px-4 py-3 text-gray-400 text-xs">${r.member_of && r.member_of.length ? r.member_of.join(', ') : '-'}</td>
            <td class="px-4 py-3 text-right">
                <div class="flex items-center justify-end gap-1">
                    <button onclick="editRole('${r.name}', ${r.can_login}, ${r.is_superuser}, ${r.can_create_db}, ${r.can_create_role})"
                            class="text-blue-400 hover:bg-blue-500/20 px-2 py-1 rounded text-xs">
                        Edit
                    </button>
                    ${r.name !== 'postgres' ? `
                        <button onclick="deleteRole('${r.name}')"
                                class="text-red-400 hover:bg-red-500/20 px-2 py-1 rounded text-xs">
                            Delete
                        </button>
                    ` : ''}
                </div>
            </td>
        </tr>
    `).join('');
}

window.showCreateRoleModal = function() {
    document.getElementById('role-modal-title').textContent = 'Create Role';
    document.getElementById('role-edit-name').value = '';
    document.getElementById('role-name').value = '';
    document.getElementById('role-name').disabled = false;
    document.getElementById('role-password').value = '';
    document.getElementById('role-login').checked = true;
    document.getElementById('role-superuser').checked = false;
    document.getElementById('role-createdb').checked = false;
    document.getElementById('role-createrole').checked = false;
    document.getElementById('role-modal').classList.remove('hidden');
}

window.editRole = function(name, login, superuser, createdb, createrole) {
    document.getElementById('role-modal-title').textContent = 'Edit Role';
    document.getElementById('role-edit-name').value = name;
    document.getElementById('role-name').value = name;
    document.getElementById('role-name').disabled = true;
    document.getElementById('role-password').value = '';
    document.getElementById('role-login').checked = login;
    document.getElementById('role-superuser').checked = superuser;
    document.getElementById('role-createdb').checked = createdb;
    document.getElementById('role-createrole').checked = createrole;
    document.getElementById('role-modal').classList.remove('hidden');
}

window.hideRoleModal = function() {
    document.getElementById('role-modal').classList.add('hidden');
}

document.getElementById('role-form').addEventListener('submit', async (e) => {
    e.preventDefault();

    const editName = document.getElementById('role-edit-name').value;
    const isEdit = editName !== '';

    const data = {
        name: document.getElementById('role-name').value,
        login: document.getElementById('role-login').checked,
        superuser: document.getElementById('role-superuser').checked,
        createdb: document.getElementById('role-createdb').checked,
        createrole: document.getElementById('role-createrole').checked
    };

    const password = document.getElementById('role-password').value;
    if (password) {
        data.password = password;
    }

    const url = isEdit
        ? `/api/admin/${currentServer.id}/roles/${editName}`
        : `/api/admin/${currentServer.id}/roles`;

    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    const result = await res.json();

    if (result.success) {
        showToast(result.message, 'success');
        hideRoleModal();
        await refreshRoles();
    } else {
        showToast('Failed: ' + result.error, 'error');
    }
});

window.deleteRole = async function(name) {
    if (!confirm(`Delete role "${name}"? This cannot be undone.`)) return;

    const res = await fetch(`/api/admin/${currentServer.id}/roles/${name}/delete`, { method: 'POST' });
    const result = await res.json();

    if (result.success) {
        showToast(result.message, 'success');
        await refreshRoles();
    } else {
        showToast('Failed: ' + result.error, 'error');
    }
}

// Close modal on Escape
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        hideBackupsModal();
        hideRoleModal();
    }
});

// Close modal on backdrop click
document.getElementById('backups-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideBackupsModal();
});
document.getElementById('role-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideRoleModal();
});
document.getElementById('schedule-modal')?.addEventListener('click', (e) => {
    if (e.target === e.currentTarget) hideScheduleModal();
});

// Scheduled Tasks
window.refreshScheduledJobs = async function() {
    const container = document.getElementById('scheduled-jobs-list');
    if (!container) return;

    container.innerHTML = '<div class="p-4 text-gray-500">Loading...</div>';

    try {
        const res = await fetch('/api/scheduler/jobs');
        const data = await res.json();

        const jobs = data.jobs || [];

        if (jobs.length === 0) {
            container.innerHTML = '<div class="p-4 text-gray-500 text-center">No scheduled tasks. Click "Add Schedule" to create one.</div>';
            return;
        }

        container.innerHTML = jobs.map(job => `
            <div class="flex items-center justify-between px-4 py-3 hover:bg-[#21262d]/50">
                <div class="flex-1">
                    <div class="flex items-center gap-2">
                        <span class="font-medium">${job.name}</span>
                        ${job.enabled
                            ? '<span class="text-xs px-1.5 py-0.5 rounded bg-[#3fb950]/20 text-[#3fb950]">Active</span>'
                            : '<span class="text-xs px-1.5 py-0.5 rounded bg-[#8b949e]/20 text-[#8b949e]">Paused</span>'
                        }
                    </div>
                    <div class="text-xs text-gray-500 mt-0.5">
                        ${job.schedule}
                        ${job.next_run ? ` · Next: ${new Date(job.next_run).toLocaleString()}` : ''}
                    </div>
                </div>
                <div class="flex items-center gap-1">
                    <button onclick="runJobNow('${job.id}')" class="p-1.5 hover:bg-[#30363d] rounded text-gray-400 hover:text-white" title="Run now">
                        <i data-lucide="play" class="w-4 h-4"></i>
                    </button>
                    ${job.enabled
                        ? `<button onclick="pauseJob('${job.id}')" class="p-1.5 hover:bg-[#30363d] rounded text-gray-400 hover:text-yellow-400" title="Pause">
                            <i data-lucide="pause" class="w-4 h-4"></i>
                           </button>`
                        : `<button onclick="resumeJob('${job.id}')" class="p-1.5 hover:bg-[#30363d] rounded text-gray-400 hover:text-green-400" title="Resume">
                            <i data-lucide="play-circle" class="w-4 h-4"></i>
                           </button>`
                    }
                    <button onclick="deleteJob('${job.id}')" class="p-1.5 hover:bg-[#30363d] rounded text-gray-400 hover:text-red-400" title="Delete">
                        <i data-lucide="trash-2" class="w-4 h-4"></i>
                    </button>
                </div>
            </div>
        `).join('');

        lucide.createIcons();
    } catch (err) {
        container.innerHTML = `<div class="p-4 text-red-400">Error: ${err.message}</div>`;
    }
}

let currentScheduleMode = 'recurring';

window.showScheduleModal = function() {
    if (!currentServer) {
        showToast('Please select a server first', 'warning');
        return;
    }
    document.getElementById('schedule-modal').classList.remove('hidden');
    setScheduleMode('recurring');
    updateScheduleOptions();
    // Set default datetime to tomorrow at 02:00
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    tomorrow.setHours(2, 0, 0, 0);
    document.getElementById('schedule-datetime').value = tomorrow.toISOString().slice(0, 16);
    lucide.createIcons();
}

window.hideScheduleModal = function() {
    document.getElementById('schedule-modal').classList.add('hidden');
}

window.setScheduleMode = function(mode) {
    currentScheduleMode = mode;
    const recurringBtn = document.getElementById('schedule-mode-recurring');
    const onceBtn = document.getElementById('schedule-mode-once');
    const recurringOptions = document.getElementById('schedule-recurring-options');
    const onceOptions = document.getElementById('schedule-once-options');

    if (mode === 'recurring') {
        recurringBtn.className = 'flex-1 px-3 py-2 rounded-lg text-sm font-medium bg-[#21262d] text-white border border-indigo-500';
        onceBtn.className = 'flex-1 px-3 py-2 rounded-lg text-sm font-medium bg-[#0d1117] text-[#8b949e] hover:text-white border border-[#30363d]';
        recurringOptions.classList.remove('hidden');
        onceOptions.classList.add('hidden');
    } else {
        onceBtn.className = 'flex-1 px-3 py-2 rounded-lg text-sm font-medium bg-[#21262d] text-white border border-indigo-500';
        recurringBtn.className = 'flex-1 px-3 py-2 rounded-lg text-sm font-medium bg-[#0d1117] text-[#8b949e] hover:text-white border border-[#30363d]';
        recurringOptions.classList.add('hidden');
        onceOptions.classList.remove('hidden');
    }
}

window.updateScheduleOptions = function() {
    const type = document.getElementById('schedule-type').value;
    const vacuumOptions = document.getElementById('schedule-vacuum-options');

    if (type === 'vacuum') {
        vacuumOptions.classList.remove('hidden');
    } else {
        vacuumOptions.classList.add('hidden');
    }
}

window.addScheduledTask = async function() {
    if (!currentServer) {
        showToast('Please select a server first', 'warning');
        return;
    }

    const type = document.getElementById('schedule-type').value;
    const data = { connection_id: currentServer.id };

    if (currentScheduleMode === 'once') {
        // One-time scheduled task
        const datetime = document.getElementById('schedule-datetime').value;
        if (!datetime) {
            showToast('Please select a date and time', 'warning');
            return;
        }
        data.run_date = new Date(datetime).toISOString();
    } else {
        // Recurring task
        data.hour = parseInt(document.getElementById('schedule-hour').value) || 2;
        data.minute = parseInt(document.getElementById('schedule-minute').value) || 0;
        data.day_of_week = document.getElementById('schedule-day').value;
    }

    if (type === 'vacuum') {
        data.full = document.getElementById('schedule-vacuum-full').checked;
    }

    try {
        const res = await fetch(`/api/scheduler/jobs/${type}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        const result = await res.json();

        if (result.success) {
            const modeText = currentScheduleMode === 'once' ? 'one-time' : 'recurring';
            showToast(`${modeText} ${type} task scheduled`, 'success');
            hideScheduleModal();
            await refreshScheduledJobs();
        } else {
            showToast('Failed: ' + result.error, 'error');
        }
    } catch (err) {
        showToast('Error: ' + err.message, 'error');
    }
}

window.runJobNow = async function(jobId) {
    try {
        const res = await fetch(`/api/scheduler/jobs/${jobId}/run`, { method: 'POST' });
        const result = await res.json();

        if (result.success) {
            showToast('Job triggered', 'success');
        } else {
            showToast('Failed to trigger job', 'error');
        }
    } catch (err) {
        showToast('Error: ' + err.message, 'error');
    }
}

window.pauseJob = async function(jobId) {
    try {
        const res = await fetch(`/api/scheduler/jobs/${jobId}/pause`, { method: 'POST' });
        const result = await res.json();

        if (result.success) {
            showToast('Job paused', 'success');
            await refreshScheduledJobs();
        } else {
            showToast('Failed to pause job', 'error');
        }
    } catch (err) {
        showToast('Error: ' + err.message, 'error');
    }
}

window.resumeJob = async function(jobId) {
    try {
        const res = await fetch(`/api/scheduler/jobs/${jobId}/resume`, { method: 'POST' });
        const result = await res.json();

        if (result.success) {
            showToast('Job resumed', 'success');
            await refreshScheduledJobs();
        } else {
            showToast('Failed to resume job', 'error');
        }
    } catch (err) {
        showToast('Error: ' + err.message, 'error');
    }
}

window.deleteJob = async function(jobId) {
    if (!confirm('Delete this scheduled task?')) return;

    try {
        const res = await fetch(`/api/scheduler/jobs/${jobId}`, { method: 'DELETE' });
        const result = await res.json();

        if (result.success) {
            showToast('Job deleted', 'success');
            await refreshScheduledJobs();
        } else {
            showToast('Failed to delete job', 'error');
        }
    } catch (err) {
        showToast('Error: ' + err.message, 'error');
    }
}

// Load scheduled jobs on page load
document.addEventListener('DOMContentLoaded', () => {
    refreshScheduledJobs();
});

// ===== Slow Queries (pg_stat_statements) =====

window.refreshSlowQueries = async function() {
    if (!currentServer) return;

    const orderByEl = document.getElementById('slow-queries-order');
    const statusBadge = document.getElementById('slow-queries-status');
    const list = document.getElementById('slow-queries-list');

    if (!orderByEl || !statusBadge || !list) {
        console.warn('Slow queries elements not found');
        return;
    }

    try {
        const orderBy = orderByEl.value;

        // First check if pg_stat_statements is available
        const statusRes = await fetch(`/api/admin/${currentServer.id}/slow-queries/status`);
        const status = await statusRes.json();

        if (status.error) {
            statusBadge.textContent = 'error';
            statusBadge.className = 'text-xs px-2 py-0.5 rounded bg-red-500/20 text-red-400';
            list.innerHTML = `<tr><td colspan="6" class="p-4 text-red-400">${status.error}</td></tr>`;
            return;
        }

        if (!status.installed) {
            statusBadge.textContent = status.available ? 'not installed' : 'not available';
            statusBadge.className = 'text-xs px-2 py-0.5 rounded bg-yellow-500/20 text-yellow-400';
            list.innerHTML = `
                <tr><td colspan="6" class="p-4 text-gray-400">
                    pg_stat_statements extension is not installed.
                    ${status.available ? '<button onclick="installExtension(\'pg_stat_statements\')" class="text-green-400 hover:underline ml-2">Install it now</button>' : ''}
                </td></tr>`;
            return;
        }

        statusBadge.textContent = 'active';
        statusBadge.className = 'text-xs px-2 py-0.5 rounded bg-green-500/20 text-green-400';

        const res = await fetch(`/api/admin/${currentServer.id}/slow-queries?order_by=${orderBy}&limit=20`);
        const data = await res.json();

        if (data.error) {
            list.innerHTML = `<tr><td colspan="6" class="p-4 text-red-400">${data.error}</td></tr>`;
            return;
        }

        if (!data.queries || data.queries.length === 0) {
            list.innerHTML = `<tr><td colspan="6" class="p-4 text-gray-500">No queries recorded yet</td></tr>`;
            return;
        }

        list.innerHTML = data.queries.map(q => {
            const isSlow = q.mean_time_ms > 100;
            const lowCache = q.hit_ratio < 90;
            return `
            <tr class="hover:bg-[#21262d]/50 ${isSlow ? 'bg-red-500/5' : ''}">
                <td class="px-4 py-2 font-mono text-xs max-w-md truncate" title="${q.query.replace(/"/g, '&quot;')}">${q.query.slice(0, 100)}...</td>
                <td class="px-4 py-2 text-right">${q.calls.toLocaleString()}</td>
                <td class="px-4 py-2 text-right ${isSlow ? 'text-red-400 font-medium' : ''}">${formatMs(q.total_time_ms)}</td>
                <td class="px-4 py-2 text-right ${q.mean_time_ms > 100 ? 'text-orange-400' : ''}">${formatMs(q.mean_time_ms)}</td>
                <td class="px-4 py-2 text-right text-gray-400">${q.rows.toLocaleString()}</td>
                <td class="px-4 py-2 text-right ${lowCache ? 'text-yellow-400' : 'text-green-400'}">${q.hit_ratio}%</td>
            </tr>
        `}).join('');
    } catch (e) {
        console.error('refreshSlowQueries error:', e);
        list.innerHTML = `<tr><td colspan="6" class="p-4 text-red-400">Error: ${e.message}</td></tr>`;
    }
}

window.resetSlowQueries = async function() {
    if (!currentServer) return;
    if (!confirm('Reset pg_stat_statements statistics? This clears all query history.')) return;

    const res = await fetch(`/api/admin/${currentServer.id}/slow-queries/reset`, { method: 'POST' });
    const result = await res.json();

    if (result.success) {
        showToast('Statistics reset', 'success');
        await refreshSlowQueries();
    } else {
        showToast('Failed: ' + result.error, 'error');
    }
}

function formatMs(ms) {
    if (ms < 1) return '<1ms';
    if (ms < 1000) return Math.round(ms) + 'ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
    return (ms / 60000).toFixed(1) + 'm';
}

// ===== Index Usage =====

window.refreshIndexUsage = async function() {
    if (!currentServer) return;

    const list = document.getElementById('index-usage-list');
    const unusedOnlyEl = document.getElementById('show-unused-only');

    if (!list) {
        console.warn('Index usage list element not found');
        return;
    }

    try {
        const unusedOnly = unusedOnlyEl ? unusedOnlyEl.checked : false;
        const res = await fetch(`/api/admin/${currentServer.id}/indexes`);
        const data = await res.json();

        if (data.error) {
            list.innerHTML = `<tr><td colspan="6" class="p-4 text-red-400">${data.error}</td></tr>`;
            return;
        }

        let indexes = data.indexes || [];
        if (unusedOnly) {
            indexes = indexes.filter(i => i.is_unused);
        }

        if (indexes.length === 0) {
            list.innerHTML = `<tr><td colspan="6" class="p-4 text-gray-500">${unusedOnly ? 'No unused indexes found' : 'No indexes found'}</td></tr>`;
            return;
        }

        list.innerHTML = indexes.map(i => `
            <tr class="hover:bg-[#21262d]/50 ${i.is_unused ? 'bg-yellow-500/5' : ''}">
                <td class="px-4 py-2 font-mono text-sm">${i.index}</td>
                <td class="px-4 py-2 text-gray-400">${i.schema}.${i.table}</td>
                <td class="px-4 py-2 text-right">${i.size_human}</td>
                <td class="px-4 py-2 text-right ${i.idx_scan === 0 ? 'text-red-400' : ''}">${i.idx_scan.toLocaleString()}</td>
                <td class="px-4 py-2 text-right text-gray-400">${i.idx_tup_read.toLocaleString()}</td>
                <td class="px-4 py-2">
                    ${i.is_unused
                        ? '<span class="text-xs px-2 py-0.5 rounded bg-yellow-500/20 text-yellow-400">Unused</span>'
                        : '<span class="text-xs px-2 py-0.5 rounded bg-green-500/20 text-green-400">Active</span>'
                    }
                </td>
            </tr>
        `).join('');
    } catch (e) {
        console.error('refreshIndexUsage error:', e);
        list.innerHTML = `<tr><td colspan="6" class="p-4 text-red-400">Error: ${e.message}</td></tr>`;
    }
}

window.showDuplicateIndexes = async function() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/indexes/duplicates`);
    const data = await res.json();

    if (data.error) {
        showToast('Error: ' + data.error, 'error');
        return;
    }

    const duplicates = data.duplicates || [];
    if (duplicates.length === 0) {
        showToast('No duplicate indexes found', 'success');
        return;
    }

    // Show in alert for now (could be a modal)
    const msg = duplicates.map(d =>
        `${d.schema}.${d.table}: ${d.index1} and ${d.index2} (${d.columns})`
    ).join('\n');
    alert('Potential duplicate indexes:\n\n' + msg);
}

// ===== Replication =====

window.refreshReplication = async function() {
    if (!currentServer) return;

    const container = document.getElementById('replication-content');

    if (!container) {
        console.warn('Replication content element not found');
        return;
    }

    try {
        const res = await fetch(`/api/admin/${currentServer.id}/replication`);
        const data = await res.json();

        if (data.error) {
            container.innerHTML = `<div class="text-red-400">${data.error}</div>`;
            return;
        }

        let html = '';

    // Role indicator
    html += `<div class="mb-4 flex items-center gap-2">
        <span class="text-xs px-2 py-1 rounded ${data.is_replica ? 'bg-blue-500/20 text-blue-400' : 'bg-green-500/20 text-green-400'}">
            ${data.is_replica ? 'REPLICA' : 'PRIMARY'}
        </span>
    </div>`;

    // Replication slots
    if (data.slots && data.slots.length > 0) {
        html += `<div class="mb-4">
            <h4 class="text-sm font-medium text-gray-400 mb-2">Replication Slots</h4>
            <div class="space-y-2">
                ${data.slots.map(s => `
                    <div class="flex items-center justify-between bg-[#0d1117] rounded px-3 py-2">
                        <span class="font-mono text-sm">${s.slot_name}</span>
                        <div class="flex items-center gap-3 text-xs text-gray-400">
                            <span>${s.slot_type}</span>
                            <span class="${s.active ? 'text-green-400' : 'text-yellow-400'}">${s.active ? 'active' : 'inactive'}</span>
                            ${s.wal_status ? `<span>${s.wal_status}</span>` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>`;
    } else {
        html += `<div class="text-gray-500 text-sm mb-4">No replication slots</div>`;
    }

    // Streaming replicas (if primary)
    if (!data.is_replica && data.replicas && data.replicas.length > 0) {
        html += `<div class="mb-4">
            <h4 class="text-sm font-medium text-gray-400 mb-2">Connected Replicas</h4>
            <div class="space-y-2">
                ${data.replicas.map(r => `
                    <div class="bg-[#0d1117] rounded px-3 py-2">
                        <div class="flex items-center justify-between mb-1">
                            <span class="font-mono text-sm">${r.client_addr}</span>
                            <span class="text-xs px-2 py-0.5 rounded ${r.sync_state === 'sync' ? 'bg-green-500/20 text-green-400' : 'bg-blue-500/20 text-blue-400'}">${r.sync_state}</span>
                        </div>
                        <div class="flex items-center gap-4 text-xs text-gray-400">
                            <span>State: ${r.state}</span>
                            <span>Lag: <span class="${r.lag_bytes > 1024*1024 ? 'text-red-400' : ''}">${r.lag_human}</span></span>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>`;
    }

    // Receiver info (if replica)
    if (data.is_replica && data.receiver) {
        html += `<div class="mb-4">
            <h4 class="text-sm font-medium text-gray-400 mb-2">Upstream Connection</h4>
            <div class="bg-[#0d1117] rounded px-3 py-2">
                <div class="flex items-center gap-3 text-sm">
                    <span>Primary: <span class="font-mono">${data.receiver.sender_host}:${data.receiver.sender_port}</span></span>
                    <span class="text-xs px-2 py-0.5 rounded bg-green-500/20 text-green-400">${data.receiver.status}</span>
                </div>
            </div>
        </div>`;
    }

        container.innerHTML = html || '<div class="text-gray-500">No replication configured</div>';
    } catch (e) {
        console.error('refreshReplication error:', e);
        container.innerHTML = `<div class="text-red-400">Error: ${e.message}</div>`;
    }
}

// ===== PITR =====

window.refreshPITR = async function() {
    if (!currentServer) return;

    const container = document.getElementById('pitr-content');

    if (!container) {
        console.warn('PITR content element not found');
        return;
    }

    try {
        const res = await fetch(`/api/admin/${currentServer.id}/pitr`);
        const data = await res.json();

        if (data.error) {
            container.innerHTML = `<div class="text-red-400">${data.error}</div>`;
            return;
        }

        let html = `
        <div class="grid grid-cols-4 gap-4 mb-4">
            <div class="bg-[#0d1117] rounded-lg p-3">
                <div class="text-gray-400 text-xs mb-1">Base Backups</div>
                <div class="text-xl font-bold">${data.base_backup_count}</div>
                <div class="text-xs text-gray-500">${data.base_backup_size}</div>
            </div>
            <div class="bg-[#0d1117] rounded-lg p-3">
                <div class="text-gray-400 text-xs mb-1">WAL Files</div>
                <div class="text-xl font-bold">${data.wal_file_count}</div>
                <div class="text-xs text-gray-500">${data.wal_size}</div>
            </div>
            <div class="bg-[#0d1117] rounded-lg p-3">
                <div class="text-gray-400 text-xs mb-1">Total Storage</div>
                <div class="text-xl font-bold">${data.total_size}</div>
            </div>
            <div class="bg-[#0d1117] rounded-lg p-3">
                <div class="text-gray-400 text-xs mb-1">Recovery Window</div>
                <div class="text-sm font-medium">${data.oldest_backup ? new Date(data.oldest_backup).toLocaleDateString() : 'N/A'}</div>
                <div class="text-xs text-gray-500">to now</div>
            </div>
        </div>
    `;

    // WAL archiving status
    html += `
        <div class="mb-4 p-3 rounded-lg border ${data.wal_archive_enabled ? 'border-green-500/30 bg-green-500/5' : 'border-yellow-500/30 bg-yellow-500/5'}">
            <div class="flex items-center justify-between">
                <div>
                    <div class="font-medium">${data.wal_archive_enabled ? 'WAL Archiving Active' : 'WAL Archiving Not Configured'}</div>
                    <div class="text-xs text-gray-400 mt-1">
                        ${data.wal_archive_enabled
                            ? 'WAL files are being archived for point-in-time recovery'
                            : 'Configure archive_command in postgresql.conf to enable PITR'}
                    </div>
                </div>
                <button onclick="showArchiveCommand()" class="text-sm text-indigo-400 hover:text-indigo-300">
                    View Setup
                </button>
            </div>
        </div>
    `;

    // Base backups list button
    html += `
        <div class="flex items-center gap-2">
            <button onclick="showBaseBackupsList()" class="text-sm text-indigo-400 hover:text-indigo-300">
                View Base Backups →
            </button>
        </div>
    `;

        container.innerHTML = html;
    } catch (e) {
        console.error('refreshPITR error:', e);
        container.innerHTML = `<div class="text-red-400">Error: ${e.message}</div>`;
    }
}

window.createBaseBackup = async function() {
    if (!currentServer) return;

    const label = prompt('Backup label (optional):');

    showToast('Starting base backup...', 'info');

    const res = await fetch(`/api/admin/${currentServer.id}/pitr/base-backup`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ label: label || undefined })
    });
    const result = await res.json();

    if (result.success) {
        showToast(result.message, 'success');
        await refreshPITR();
    } else {
        showToast('Backup failed: ' + result.message, 'error');
    }
}

window.showArchiveCommand = async function() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/pitr/archive-command`);
    const data = await res.json();

    alert(data.instructions.join('\n'));
}

window.showBaseBackupsList = async function() {
    if (!currentServer) return;

    const res = await fetch(`/api/admin/${currentServer.id}/pitr/base-backups`);
    const data = await res.json();

    if (!data.backups || data.backups.length === 0) {
        alert('No base backups found. Create one first.');
        return;
    }

    const list = data.backups.map(b =>
        `${b.name} - ${b.size_human} - ${new Date(b.created_at).toLocaleString()}`
    ).join('\n');

    alert('Base Backups:\n\n' + list);
}

// ===== Server Logs =====
window.refreshLogs = async function() {
    if (!currentServer) return;

    const container = document.getElementById('logs-content');
    const levelFilter = document.getElementById('log-level-filter');

    if (!container) {
        console.warn('Logs content element not found');
        return;
    }

    try {
        const level = levelFilter ? levelFilter.value : '';
        const url = `/api/admin/${currentServer.id}/logs?limit=100${level ? `&level=${level}` : ''}`;
        const res = await fetch(url);
        const data = await res.json();

        if (data.error) {
            container.innerHTML = `<div class="text-red-400">${data.error}</div>`;
            return;
        }

        let html = '';

        // Show log settings
        if (data.settings && Object.keys(data.settings).length > 0) {
            html += `<div class="mb-4 p-3 bg-[#21262d] rounded-lg">
                <div class="text-xs text-gray-400 mb-2">Log Configuration</div>
                <div class="grid grid-cols-2 gap-2 text-xs">
                    ${Object.entries(data.settings).map(([k, v]) => `
                        <div class="text-gray-500">${k}</div>
                        <div class="text-gray-300 font-mono">${v || 'N/A'}</div>
                    `).join('')}
                </div>
            </div>`;
        }

        if (data.log_file) {
            html += `<div class="text-xs text-gray-500 mb-2">Log file: ${data.log_file}</div>`;
        }

        if (data.note) {
            html += `<div class="text-xs text-yellow-500 mb-2">${data.note}</div>`;
        }

        if (data.logs && data.logs.length > 0) {
            html += `<div class="space-y-1 font-mono text-xs">`;
            for (const log of data.logs) {
                const levelColor = log.level === 'ERROR' ? 'text-red-400'
                    : log.level === 'WARNING' ? 'text-yellow-400'
                    : log.level === 'FATAL' ? 'text-red-500 font-bold'
                    : log.level === 'PANIC' ? 'text-red-600 font-bold'
                    : 'text-gray-400';
                html += `<div class="py-1 border-b border-[#21262d] ${levelColor}">${log.raw}</div>`;
            }
            html += `</div>`;
        } else {
            html += `<div class="text-gray-500">No log entries found. Logs may require superuser or pg_read_server_files role.</div>`;
        }

        container.innerHTML = html;
    } catch (e) {
        console.error('refreshLogs error:', e);
        container.innerHTML = `<div class="text-red-400">Error: ${e.message}</div>`;
    }
}
