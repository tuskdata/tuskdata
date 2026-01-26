// Tusk Cluster Page JavaScript

let refreshInterval = null;
let isConnected = false;

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    await loadConfig();
    await refreshCluster();
    // Auto-refresh every 3 seconds
    refreshInterval = setInterval(refreshCluster, 3000);
});

async function loadConfig() {
    try {
        const [configRes, localRes] = await Promise.all([
            fetch('/api/cluster/config'),
            fetch('/api/cluster/local/status')
        ]);
        const config = await configRes.json();
        const localStatus = await localRes.json();

        document.getElementById('scheduler-host').value = config.scheduler_host || 'localhost';
        document.getElementById('scheduler-port').value = config.scheduler_port || 8814;

        updateConnectionUI(config.connected);
        updateLocalClusterUI(localStatus.running);
    } catch (e) {
        console.error('Failed to load config:', e);
    }
}

function updateLocalClusterUI(running) {
    const btnStart = document.getElementById('btn-start-local');
    const btnStop = document.getElementById('btn-stop-local');
    const localStatus = document.getElementById('local-status');
    const workersSelect = document.getElementById('local-workers');

    if (running) {
        btnStart.classList.add('hidden');
        btnStop.classList.remove('hidden');
        localStatus.innerHTML = '<span class="text-green-400">Running</span>';
        workersSelect.disabled = true;
        workersSelect.classList.add('opacity-50');
    } else {
        btnStart.classList.remove('hidden');
        btnStop.classList.add('hidden');
        localStatus.textContent = 'Stopped';
        workersSelect.disabled = false;
        workersSelect.classList.remove('opacity-50');
    }
    lucide.createIcons();
}

window.startLocalCluster = async function() {
    const workers = parseInt(document.getElementById('local-workers').value) || 3;
    const btn = document.getElementById('btn-start-local');

    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-3.5 h-3.5 animate-spin"></i> Starting...';
    lucide.createIcons();

    try {
        const res = await fetch('/api/cluster/local/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ workers })
        });
        const data = await res.json();

        if (data.started) {
            updateLocalClusterUI(true);
            document.getElementById('local-status').innerHTML = `<span class="text-green-400">Running (PID: ${data.pid})</span>`;
            // Auto-refresh to show workers
            setTimeout(refreshCluster, 2000);
        } else {
            showToast('Error: ' + (data.error || 'Failed to start'), 'error');
        }
    } catch (e) {
        showToast('Error: ' + e.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="play" class="w-3.5 h-3.5"></i> Start';
        lucide.createIcons();
    }
}

window.stopLocalCluster = async function() {
    const btn = document.getElementById('btn-stop-local');

    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-3.5 h-3.5 animate-spin"></i> Stopping...';
    lucide.createIcons();

    try {
        const res = await fetch('/api/cluster/local/stop', { method: 'POST' });
        const data = await res.json();

        if (data.stopped) {
            updateLocalClusterUI(false);
            await refreshCluster();
        } else {
            showToast('Error: ' + (data.error || 'Failed to stop'), 'error');
        }
    } catch (e) {
        showToast('Error: ' + e.message, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="square" class="w-3.5 h-3.5"></i> Stop';
        lucide.createIcons();
    }
}

function updateConnectionUI(connected) {
    isConnected = connected;
    const btnConnect = document.getElementById('btn-connect');
    const btnDisconnect = document.getElementById('btn-disconnect');
    const hostInput = document.getElementById('scheduler-host');
    const portInput = document.getElementById('scheduler-port');

    if (connected) {
        btnConnect.classList.add('hidden');
        btnDisconnect.classList.remove('hidden');
        hostInput.disabled = true;
        portInput.disabled = true;
        hostInput.classList.add('opacity-50');
        portInput.classList.add('opacity-50');
    } else {
        btnConnect.classList.remove('hidden');
        btnDisconnect.classList.add('hidden');
        hostInput.disabled = false;
        portInput.disabled = false;
        hostInput.classList.remove('opacity-50');
        portInput.classList.remove('opacity-50');
    }
    lucide.createIcons();
}

window.connectScheduler = async function() {
    const host = document.getElementById('scheduler-host').value.trim() || 'localhost';
    const port = parseInt(document.getElementById('scheduler-port').value) || 8814;

    const btn = document.getElementById('btn-connect');
    btn.disabled = true;
    btn.innerHTML = '<i data-lucide="loader-2" class="w-3.5 h-3.5 animate-spin"></i> Connecting...';
    lucide.createIcons();

    try {
        const res = await fetch('/api/cluster/connect', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ host, port })
        });
        const data = await res.json();

        if (data.connected) {
            updateConnectionUI(true);
            document.getElementById('scheduler-info').innerHTML = `<span class="text-green-400">Connected to ${data.address}</span>`;
            await refreshCluster();
        } else {
            document.getElementById('scheduler-info').innerHTML = `<span class="text-yellow-400">${data.error || 'Connection failed'}</span>`;
        }
    } catch (e) {
        document.getElementById('scheduler-info').innerHTML = `<span class="text-red-400">Error: ${e.message}</span>`;
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="plug" class="w-3.5 h-3.5"></i> Connect';
        lucide.createIcons();
    }
}

window.disconnectScheduler = async function() {
    try {
        await fetch('/api/cluster/disconnect', { method: 'POST' });
        updateConnectionUI(false);
        document.getElementById('scheduler-info').textContent = 'Not connected';
        await refreshCluster();
    } catch (e) {
        console.error('Failed to disconnect:', e);
    }
}

window.refreshCluster = async function() {
    await Promise.all([
        fetchStatus(),
        fetchWorkers(),
        fetchJobs()
    ]);
}

async function fetchStatus() {
    try {
        const res = await fetch('/api/cluster/status');
        const data = await res.json();

        // Update scheduler status indicator
        const schedulerStatus = document.getElementById('scheduler-status');
        const schedulerInfo = document.getElementById('scheduler-info');

        if (data.scheduler_online) {
            schedulerStatus.classList.remove('bg-gray-600', 'bg-red-500');
            schedulerStatus.classList.add('bg-green-500');
            if (!isConnected) {
                // Scheduler registered itself (local mode)
                schedulerInfo.innerHTML = `<span class="text-green-400">${data.scheduler_address || 'Local'}</span>`;
            }
        } else {
            schedulerStatus.classList.remove('bg-green-500');
            schedulerStatus.classList.add('bg-gray-600');
            if (!isConnected) {
                schedulerInfo.textContent = 'Not connected';
            }
        }

        // Update stats
        document.getElementById('stat-workers').textContent = data.workers_online || 0;
        document.getElementById('stat-active').textContent = data.active_jobs || 0;
        document.getElementById('stat-completed').textContent = data.completed_jobs || 0;
        document.getElementById('stat-bytes').textContent = formatBytes(data.total_bytes_processed || 0);
        document.getElementById('workers-count').textContent = `${data.workers_online || 0}/${data.workers_total || 0}`;

    } catch (e) {
        console.error('Failed to fetch cluster status:', e);
    }
}

async function fetchWorkers() {
    try {
        const res = await fetch('/api/cluster/workers');
        const data = await res.json();

        const container = document.getElementById('workers-list');

        if (!data.workers || data.workers.length === 0) {
            container.innerHTML = '<div class="text-[#8b949e] text-sm py-2">No workers connected</div>';
            return;
        }

        container.innerHTML = data.workers.map(w => {
            const statusColor = w.status === 'idle' ? 'bg-green-500' :
                               w.status === 'busy' ? 'bg-yellow-500' : 'bg-red-500';
            const statusText = w.status === 'idle' ? 'Idle' :
                              w.status === 'busy' ? 'Busy' : 'Offline';

            return `
                <div class="worker-card p-3 rounded-lg bg-[#0d1117] border border-[#30363d]">
                    <div class="flex items-center justify-between mb-2">
                        <div class="flex items-center gap-2">
                            <span class="w-2 h-2 rounded-full ${statusColor}"></span>
                            <span class="font-mono text-sm">${w.id}</span>
                        </div>
                        <span class="text-xs px-1.5 py-0.5 rounded ${
                            w.status === 'idle' ? 'bg-green-500/20 text-green-400' :
                            w.status === 'busy' ? 'bg-yellow-500/20 text-yellow-400' :
                            'bg-red-500/20 text-red-400'
                        }">${statusText}</span>
                    </div>
                    <div class="space-y-1.5">
                        <div>
                            <div class="flex justify-between text-xs mb-0.5">
                                <span class="text-[#8b949e]">CPU</span>
                                <span>${Math.round(w.cpu_percent)}%</span>
                            </div>
                            <div class="h-1 bg-[#21262d] rounded-full overflow-hidden">
                                <div class="h-full bg-indigo-500 progress-bar" style="width: ${w.cpu_percent}%"></div>
                            </div>
                        </div>
                        <div>
                            <div class="flex justify-between text-xs mb-0.5">
                                <span class="text-[#8b949e]">Memory</span>
                                <span>${formatBytes(w.memory_mb * 1024 * 1024)}</span>
                            </div>
                            <div class="h-1 bg-[#21262d] rounded-full overflow-hidden">
                                <div class="h-full bg-purple-500 progress-bar" style="width: ${w.memory_percent}%"></div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }).join('');

        lucide.createIcons();
    } catch (e) {
        console.error('Failed to fetch workers:', e);
    }
}

async function fetchJobs() {
    try {
        const res = await fetch('/api/cluster/jobs');
        const data = await res.json();

        const activeContainer = document.getElementById('active-jobs');
        const historyContainer = document.getElementById('job-history');

        const activeJobs = (data.jobs || []).filter(j => j.status === 'running' || j.status === 'pending');
        const historyJobs = (data.jobs || []).filter(j => j.status !== 'running' && j.status !== 'pending');

        // Render active jobs
        if (activeJobs.length === 0) {
            activeContainer.innerHTML = '<div class="text-[#8b949e] text-sm">No active jobs</div>';
        } else {
            activeContainer.innerHTML = activeJobs.map(j => `
                <div class="job-row p-4 rounded-lg bg-[#0d1117] border border-[#30363d]">
                    <div class="flex items-center justify-between mb-2">
                        <div class="flex items-center gap-2">
                            <i data-lucide="loader-2" class="w-4 h-4 text-indigo-400 ${j.status === 'running' ? 'animate-spin' : ''}"></i>
                            <span class="font-mono text-sm">${j.id}</span>
                            <span class="text-xs px-1.5 py-0.5 rounded ${
                                j.status === 'running' ? 'bg-indigo-500/20 text-indigo-400 status-running' :
                                'bg-yellow-500/20 text-yellow-400'
                            }">${j.status}</span>
                        </div>
                        <button onclick="cancelJob('${j.id}')" class="p-1.5 hover:bg-red-500/20 rounded text-[#8b949e] hover:text-red-400" title="Cancel">
                            <i data-lucide="x" class="w-4 h-4"></i>
                        </button>
                    </div>
                    <div class="text-xs text-[#8b949e] font-mono truncate mb-2">${j.sql}</div>
                    <div class="flex items-center gap-3">
                        <div class="flex-1 h-2 bg-[#21262d] rounded-full overflow-hidden">
                            <div class="h-full bg-gradient-to-r from-indigo-500 to-purple-500 progress-bar" style="width: ${(j.progress || 0) * 100}%"></div>
                        </div>
                        <span class="text-sm font-medium">${Math.round((j.progress || 0) * 100)}%</span>
                    </div>
                    ${j.rows_processed ? `<div class="text-xs text-[#8b949e] mt-1">${j.rows_processed.toLocaleString()} rows processed</div>` : ''}
                </div>
            `).join('');
        }

        // Render history
        if (historyJobs.length === 0) {
            historyContainer.innerHTML = '<div class="text-[#8b949e] text-sm">No job history</div>';
        } else {
            historyContainer.innerHTML = historyJobs.slice(0, 20).map(j => {
                const statusIcon = j.status === 'completed' ? 'check-circle' :
                                  j.status === 'failed' ? 'x-circle' :
                                  j.status === 'cancelled' ? 'slash' : 'circle';
                const statusColor = j.status === 'completed' ? 'text-green-400' :
                                   j.status === 'failed' ? 'text-red-400' :
                                   j.status === 'cancelled' ? 'text-yellow-400' : 'text-gray-400';

                return `
                    <div class="job-row flex items-center gap-3 p-3 rounded-lg hover:bg-[#21262d]">
                        <i data-lucide="${statusIcon}" class="w-4 h-4 ${statusColor}"></i>
                        <span class="font-mono text-sm">${j.id}</span>
                        <span class="flex-1 text-xs text-[#8b949e] font-mono truncate">${j.sql}</span>
                        <span class="text-xs text-[#8b949e]">${j.rows_processed?.toLocaleString() || 0} rows</span>
                        <span class="text-xs px-1.5 py-0.5 rounded ${
                            j.status === 'completed' ? 'bg-green-500/20 text-green-400' :
                            j.status === 'failed' ? 'bg-red-500/20 text-red-400' :
                            'bg-yellow-500/20 text-yellow-400'
                        }">${j.status}</span>
                    </div>
                `;
            }).join('');
        }

        lucide.createIcons();
    } catch (e) {
        console.error('Failed to fetch jobs:', e);
    }
}

window.submitJob = async function() {
    const sqlInput = document.getElementById('job-sql');
    const sql = sqlInput.value.trim();

    if (!sql) {
        showToast('Please enter a SQL query', 'warning');
        return;
    }

    try {
        const res = await fetch('/api/cluster/jobs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql })
        });
        const data = await res.json();

        if (data.error) {
            showToast('Error: ' + data.error, 'error');
            return;
        }

        sqlInput.value = '';
        await fetchJobs();
    } catch (e) {
        showToast('Failed to submit job: ' + e.message, 'error');
    }
}

window.cancelJob = async function(jobId) {
    if (!confirm('Cancel this job?')) return;

    try {
        const res = await fetch(`/api/cluster/jobs/${jobId}/cancel`, { method: 'POST' });
        const data = await res.json();

        if (data.error) {
            showToast('Error: ' + data.error, 'error');
            return;
        }

        await fetchJobs();
    } catch (e) {
        showToast('Failed to cancel job: ' + e.message, 'error');
    }
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
    if (refreshInterval) clearInterval(refreshInterval);
});
