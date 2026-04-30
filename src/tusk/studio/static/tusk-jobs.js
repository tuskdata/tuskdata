/**
 * Background-jobs poller, topnav badge, and slide-in drawer.
 *
 * Loaded on every page via base.html, runs on a single setInterval
 * (3s by default) regardless of which tab the user is on. Long-running
 * operations (pg_dump, pg_restore, plugin scans) submit jobs server-
 * side and return immediately; this file is what surfaces the result
 * back to the user — a completion toast (with download link when
 * applicable) and the live count in the topnav.
 *
 * Wire-up: the topnav has `<button id="tusk-jobs-btn">` and a
 * sibling `<aside id="tusk-jobs-drawer">`. We mount inside both on
 * DOMContentLoaded; if either is absent (older pages, embeds), the
 * file is a no-op.
 */
(function () {
    'use strict';

    const POLL_INTERVAL_MS = 3000;
    const HISTORY_LIMIT = 25;

    // status → { label, css class for the pill, lucide icon }
    const STATUS_META = {
        running:     { label: 'Running',     cls: 'chip',          icon: 'loader-2' },
        done:        { label: 'Done',        cls: 'chip chip-green', icon: 'check' },
        failed:      { label: 'Failed',      cls: 'chip chip-red',   icon: 'x' },
        interrupted: { label: 'Interrupted', cls: 'chip chip-amber', icon: 'pause' },
    };

    // kind → human label + icon for the row
    const KIND_META = {
        backup:                { label: 'Backup',          icon: 'database-backup' },
        restore:               { label: 'Restore',         icon: 'database' },
        create_db:             { label: 'Create database', icon: 'plus' },
        create_db_from_backup: { label: 'Restore to new DB', icon: 'database' },
        dns_fetch:             { label: 'DNS fetch',       icon: 'shield' },
    };

    // Track per-job last-seen status so we only toast on transitions.
    // Initial poll seeds the cache without firing any toast (otherwise
    // every page load would replay the entire job history).
    const lastSeen = new Map();
    let primed = false;

    function fmtDuration(startedAt, endedAt) {
        try {
            const a = new Date(startedAt).getTime();
            const b = endedAt ? new Date(endedAt).getTime() : Date.now();
            const ms = Math.max(0, b - a);
            if (ms < 1500) return `${ms}ms`;
            const s = Math.round(ms / 1000);
            if (s < 90) return `${s}s`;
            const m = Math.floor(s / 60);
            const rem = s % 60;
            return rem ? `${m}m ${rem}s` : `${m}m`;
        } catch {
            return '';
        }
    }

    function escapeHtml(s) {
        if (typeof window.tuskEscapeHtml === 'function') return window.tuskEscapeHtml(s);
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function setBadge(count) {
        const badge = document.getElementById('tusk-jobs-badge');
        if (!badge) return;
        if (count > 0) {
            badge.textContent = String(count);
            badge.classList.remove('hidden');
        } else {
            badge.textContent = '';
            badge.classList.add('hidden');
        }
        const btn = document.getElementById('tusk-jobs-btn');
        if (btn) btn.classList.toggle('is-active', count > 0);
    }

    function renderRow(job) {
        const km = KIND_META[job.kind] || { label: job.kind, icon: 'circle' };
        const sm = STATUS_META[job.status] || STATUS_META.running;
        const dur = fmtDuration(job.started_at, job.ended_at);
        const detail = job.status === 'failed'
            ? job.error
            : (job.status === 'done' ? job.result : null);
        const downloadLink = job.href
            ? `<a href="${escapeHtml(job.href)}" class="btn btn-sm btn-ghost" style="margin-top:6px;color:var(--brand)">
                   <i data-lucide="download" style="width:12px;height:12px"></i>Download
               </a>` : '';
        const detailLine = detail
            ? `<div class="mono" style="font-size:11px;color:var(--fg-3);margin-top:4px;word-break:break-word">${escapeHtml(detail)}</div>`
            : '';
        return `
            <div class="tusk-job-row" data-job-id="${escapeHtml(job.id)}"
                 style="padding:10px 12px;background:var(--surface-2);border-radius:6px;margin-bottom:6px">
                <div style="display:flex;align-items:center;gap:8px">
                    <i data-lucide="${escapeHtml(km.icon)}" style="width:14px;height:14px;color:var(--brand);flex-shrink:0"></i>
                    <div style="flex:1;min-width:0">
                        <div style="font-size:12.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
                            ${escapeHtml(job.label)}
                        </div>
                        <div style="font-size:11px;color:var(--fg-3);margin-top:2px">
                            ${escapeHtml(km.label)} · ${escapeHtml(dur)}
                        </div>
                    </div>
                    <span class="${sm.cls}">${escapeHtml(sm.label)}</span>
                </div>
                ${detailLine}
                ${downloadLink}
            </div>`;
    }

    function renderDrawerContents(jobs) {
        const body = document.getElementById('tusk-jobs-list');
        if (!body) return;
        if (!jobs.length) {
            body.innerHTML = `<div style="padding:24px;text-align:center;color:var(--fg-3);font-size:13px">
                No background jobs yet.<br>
                <span style="font-size:11.5px">Backups, restores, and DNS fetches will appear here.</span>
            </div>`;
            return;
        }
        body.innerHTML = jobs.slice(0, HISTORY_LIMIT).map(renderRow).join('');
        if (window.lucide) window.lucide.createIcons();
    }

    function fireTransitionToast(job) {
        if (typeof window.showToast !== 'function') return;
        if (job.status === 'done') {
            const dl = job.href ? ` (download ready in activity drawer)` : '';
            window.showToast(`${job.label} — completed${dl}`, 'success');
        } else if (job.status === 'failed') {
            const detail = job.error ? `: ${job.error.split('\n')[0].slice(0, 120)}` : '';
            window.showToast(`${job.label} — failed${detail}`, 'error');
        } else if (job.status === 'interrupted') {
            window.showToast(`${job.label} — interrupted (Tusk restarted)`, 'warning');
        }
    }

    async function pollOnce() {
        try {
            const res = await fetch('/api/jobs?limit=' + HISTORY_LIMIT, {
                credentials: 'same-origin',
            });
            if (!res.ok) return;
            const data = await res.json();
            const jobs = Array.isArray(data.jobs) ? data.jobs : [];

            // Compare against last seen — fire toasts on transitions
            // OUT of running (skip the very first poll so reloads
            // don't replay history).
            if (primed) {
                jobs.forEach((j) => {
                    const prev = lastSeen.get(j.id);
                    if (prev === 'running' && j.status !== 'running') {
                        fireTransitionToast(j);
                    }
                });
            }
            jobs.forEach((j) => lastSeen.set(j.id, j.status));
            primed = true;

            setBadge(typeof data.running_count === 'number' ? data.running_count : 0);

            // Only re-render when the drawer is open. Otherwise we're
            // burning DOM cycles for nothing.
            const drawer = document.getElementById('tusk-jobs-drawer');
            if (drawer && !drawer.classList.contains('hidden')) {
                renderDrawerContents(jobs);
            }
        } catch (e) {
            // Silent — poller runs forever, transient errors are fine.
        }
    }

    function toggleDrawer() {
        const drawer = document.getElementById('tusk-jobs-drawer');
        if (!drawer) return;
        const willOpen = drawer.classList.contains('hidden');
        drawer.classList.toggle('hidden');
        if (willOpen) {
            // Pull fresh data the moment the drawer opens, don't wait
            // for the next 3-second tick.
            pollOnce();
        }
    }

    function closeDrawerOnOutsideClick(e) {
        const drawer = document.getElementById('tusk-jobs-drawer');
        const btn = document.getElementById('tusk-jobs-btn');
        if (!drawer || drawer.classList.contains('hidden')) return;
        if (drawer.contains(e.target)) return;
        if (btn && btn.contains(e.target)) return;
        drawer.classList.add('hidden');
    }

    function init() {
        const btn = document.getElementById('tusk-jobs-btn');
        const drawer = document.getElementById('tusk-jobs-drawer');
        if (!btn || !drawer) return;  // no-op on pages without the topnav

        btn.addEventListener('click', toggleDrawer);
        document.addEventListener('click', closeDrawerOnOutsideClick);
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !drawer.classList.contains('hidden')) {
                drawer.classList.add('hidden');
            }
        });

        // Kick off the poller. Single shared interval — visible badge
        // updates regardless of which tab is open.
        pollOnce();
        setInterval(pollOnce, POLL_INTERVAL_MS);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
