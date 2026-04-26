/* studio-views.js — Studio view switching, pager, Explain, Format
 *
 * Pulled out of templates/index.html so the template stays declarative
 * and this file can be cached separately. Loaded after studio.js, which
 * exposes window.editor, window.currentResults, window.currentSql,
 * window.currentConnection, etc.
 */

// Mark the Studio body so studio-only CSS rules can target it.
document.body.classList.add('studio-redesigned');

// ─── Result view switcher ────────────────────────────────────────────
window.setResultView = async function(view) {
    document.querySelectorAll('#result-views button').forEach(b => {
        b.classList.toggle('on', b.dataset.view === view);
    });
    document.querySelectorAll('.view-pane').forEach(el => el.classList.add('hidden'));
    const pane = document.getElementById(`results-${view}-view`);
    if (pane) pane.classList.remove('hidden');

    if (view === 'json') {
        const el = document.getElementById('results-json-content');
        el.textContent = window.currentResults
            ? JSON.stringify(window.currentResults, null, 2)
            : '— no results yet —';
    }
    if (view === 'map' && typeof window.showMapModal === 'function' && window.hasGeoColumn?.()) {
        showMapModal();
    }
    if (view === 'plan') {
        await loadExplainPlan();
    }
};

window.loadExplainPlan = async function() {
    const el = document.getElementById('results-plan-content');
    if (!el) return;
    if (!window.currentConnection || !window.currentSql) {
        el.innerHTML = '<div class="plan-empty">Run a query first.</div>';
        return;
    }
    if (window.currentConnection.type !== 'postgres') {
        el.innerHTML = '<div class="plan-empty">EXPLAIN is only available for PostgreSQL connections.</div>';
        return;
    }
    el.innerHTML = '<div class="plan-empty">Loading plan…</div>';
    try {
        const res = await fetch('/api/explain', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                connection_id: window.currentConnection.id,
                sql: window.currentSql,
                analyze: false,
            }),
        });
        const data = await res.json();
        if (data.error) {
            el.innerHTML = `<div class="inline-error plan-error"><i data-lucide="alert-circle"></i><pre>${data.error}</pre></div>`;
            if (window.lucide) lucide.createIcons();
            return;
        }
        el.textContent = JSON.stringify(data.plan, null, 2);
    } catch (e) {
        el.innerHTML = `<div class="inline-error plan-error"><pre>${e.message}</pre></div>`;
    }
};

// ─── Pager ───────────────────────────────────────────────────────────
window.pagerPrev = function() {
    const cur = window.currentPage || 1;
    if (cur > 1) goToPage(cur - 1);
};
window.pagerNext = function() {
    const cur = window.currentPage || 1;
    const total = window.getTotalPages?.() || 1;
    if (cur < total) goToPage(cur + 1);
};
window.pagerLast = function() {
    const total = window.getTotalPages?.() || 1;
    goToPage(total);
};

// ─── Editor toolbar actions ──────────────────────────────────────────
window.explainCurrentQuery = function() {
    setResultView('plan');
};

// Tiny client-side SQL beautifier — no server round-trip. Uppercases
// keywords and breaks before each major clause. Not a parser; covers
// the common SELECT/INSERT/UPDATE/DELETE shape.
const _SQL_KEYWORDS = [
    'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'GROUP BY', 'ORDER BY',
    'HAVING', 'LIMIT', 'OFFSET', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN',
    'INNER JOIN', 'OUTER JOIN', 'ON', 'UNION', 'UNION ALL',
    'INSERT INTO', 'UPDATE', 'DELETE FROM', 'VALUES', 'SET',
    'RETURNING', 'WITH', 'AS',
];

window.formatCurrentQuery = function() {
    if (!window.editor) return;
    const sql = editor.state.doc.toString();
    if (!sql.trim()) return;
    let out = sql.replace(/\s+/g, ' ').trim();
    _SQL_KEYWORDS.forEach(k => {
        const re = new RegExp('\\b' + k.replace(' ', '\\s+') + '\\b', 'gi');
        out = out.replace(re, '\n' + k);
    });
    out = out.replace(/^\n/, '').replace(/\n+/g, '\n').replace(/,\s*/g, ', ');
    editor.dispatch({changes: {from: 0, to: editor.state.doc.length, insert: out}});
};
