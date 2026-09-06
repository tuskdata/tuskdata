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

window.loadExplainPlan = async function(analyze = false) {
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
                analyze: analyze,
            }),
        });
        const data = await res.json();
        if (data.error) {
            // tuskEscapeHtml: PG error messages echo the offending SQL
            // fragment, so a query containing `<img onerror=…>` would
            // ship raw HTML through the EXPLAIN endpoint into the DOM.
            el.innerHTML = `<div class="inline-error plan-error"><i data-lucide="alert-circle"></i><pre>${tuskEscapeHtml(data.error)}</pre></div>`;
            if (window.lucide) lucide.createIcons();
            return;
        }
        window.currentPlan = data.plan;
        el.innerHTML = '';
        const bar = document.createElement('div');
        bar.className = 'plan-actions';
        bar.innerHTML = `<div class="flex aic gap-2 flex-wrap">
                            <button class="btn btn-sm" onclick="explainPlanWithAI()" title="Ask the AI Copilot where the time goes and what to do about it">
                                <i data-lucide="sparkles"></i>Explain with AI</button>
                            <button class="btn btn-sm btn-ghost" onclick="loadExplainPlan(true)" title="EXPLAIN ANALYZE: runs the query and shows real rows and times">
                                <i data-lucide="timer"></i>Analyze (runs the query)</button>
                            <button class="btn btn-sm btn-ghost" onclick="const p = document.getElementById('plan-json'); p.hidden = !p.hidden;">
                                <i data-lucide="braces"></i>JSON</button>
                         </div>
                         <div id="plan-insight" class="plan-insight" hidden></div>`;
        el.appendChild(bar);
        el.appendChild(renderPlanTree(data.plan));
        const pre = document.createElement('pre');
        pre.className = 'plan-json';
        pre.id = 'plan-json';
        pre.hidden = true;
        pre.textContent = JSON.stringify(data.plan, null, 2);
        el.appendChild(pre);
        if (window.lucide) lucide.createIcons();
    } catch (e) {
        el.innerHTML = `<div class="inline-error plan-error"><pre>${tuskEscapeHtml(e.message)}</pre></div>`;
    }
};

// AI Insight on the EXPLAIN plan: the Copilot reads the plan + SQL (+ the
// schema it already grounds on) and answers with summary / bottlenecks /
// suggestions. Same provider as the rest of the Copilot.
window.explainPlanWithAI = async function() {
    const box = document.getElementById('plan-insight');
    if (!box || !window.currentPlan || !window.currentSql) return;
    box.hidden = false;
    box.innerHTML = '<div class="plan-empty">Reading the plan…</div>';
    try {
        const data = await tuskFetchJSON('/api/ai/plan-insight', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                connection_id: window.currentConnection?.id,
                sql: window.currentSql,
                plan: window.currentPlan,
            }),
        });
        if (data.error) {
            const hint = data.code === 412 ? ' — configure a provider in Settings → AI Copilot' : '';
            box.innerHTML = `<div class="inline-error plan-error"><pre>${tuskEscapeHtml(data.error + hint)}</pre></div>`;
            return;
        }
        const li = (items) => items.map(x => `<li>${tuskEscapeHtml(x)}</li>`).join('');
        box.innerHTML = `
            <p class="plan-insight-summary">${tuskEscapeHtml(data.summary || '')}</p>
            ${data.bottlenecks?.length ? `<div class="plan-insight-title">Bottlenecks</div><ul>${li(data.bottlenecks)}</ul>` : ''}
            ${data.suggestions?.length ? `<div class="plan-insight-title">Suggestions</div><ol>${li(data.suggestions)}</ol>` : ''}`;
    } catch (e) {
        box.innerHTML = `<div class="inline-error plan-error"><pre>${tuskEscapeHtml(e.message)}</pre></div>`;
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

// ─── Graphical plan tree ─────────────────────────────────────────────
// The EXPLAIN JSON as a tree of cards: node type, what it touches, rows
// (estimated vs actual), cost or time, and a bar with the node's own share
// of the total (exclusive of its children). The costliest node is marked.
window.renderPlanTree = function(plan) {
    const root = Array.isArray(plan) ? plan[0]?.Plan : (plan?.Plan || plan);
    const wrap = document.createElement('div');
    wrap.className = 'plan-tree';
    if (!root) { wrap.textContent = 'No plan.'; return wrap; }
    const analyzed = root['Actual Total Time'] !== undefined;
    // Exclusive metric per node: time (ANALYZE) or cost (estimate).
    const nodes = [];
    const metric = (n) => analyzed ? (n['Actual Total Time'] || 0) * (n['Actual Loops'] || 1) : (n['Total Cost'] || 0);
    const walk = (n, depth, parent) => {
        const kids = n.Plans || [];
        const own = Math.max(0, metric(n) - kids.reduce((a, k) => a + metric(k), 0));
        const rec = { n, depth, own, kids: [] };
        nodes.push(rec);
        for (const k of kids) rec.kids.push(walk(k, depth + 1, rec));
        return rec;
    };
    const tree = walk(root, 0, null);
    const total = Math.max(1e-9, metric(root));
    const hottest = nodes.reduce((a, b) => (b.own > a.own ? b : a), nodes[0]);
    const fmt = (v) => v >= 1000 ? Math.round(v).toLocaleString() : (Math.round(v * 10) / 10).toString();
    const label = (n) => {
        const parts = [];
        if (n['Relation Name']) parts.push(n['Relation Name'] + (n.Alias && n.Alias !== n['Relation Name'] ? ` as ${n.Alias}` : ''));
        if (n['Index Name']) parts.push('using ' + n['Index Name']);
        if (n['Join Type']) parts.push(n['Join Type'] + ' join');
        if (n['Hash Cond']) parts.push(n['Hash Cond']);
        if (n['Index Cond']) parts.push(n['Index Cond']);
        if (n['Filter']) parts.push('filter ' + n['Filter']);
        if (n['Sort Key']) parts.push('by ' + [].concat(n['Sort Key']).join(', '));
        return parts.join(' · ');
    };
    const render = (rec) => {
        const n = rec.n;
        const pct = 100 * rec.own / total;
        const card = document.createElement('div');
        card.className = 'plan-node' + (rec === hottest && pct > 15 ? ' hot' : '') + (n['Node Type'] === 'Seq Scan' && (n['Plan Rows'] || 0) > 1000 ? ' warn' : '');
        card.style.marginLeft = (rec.depth * 22) + 'px';
        const rows = analyzed
            ? `${fmt(n['Actual Rows'] || 0)} rows${n['Plan Rows'] !== undefined && Math.abs((n['Actual Rows'] || 0) - n['Plan Rows']) > Math.max(10, n['Plan Rows'] * 0.5) ? ` <span class="plan-mis" title="planner estimated ${fmt(n['Plan Rows'])}">est ${fmt(n['Plan Rows'])}</span>` : ''}`
            : `${fmt(n['Plan Rows'] || 0)} rows est`;
        const own = analyzed ? `${fmt(rec.own)} ms` : `cost ${fmt(rec.own)}`;
        card.innerHTML = `
            <div class="plan-node-head">
                <span class="plan-node-type">${tuskEscapeHtml(n['Node Type'] || '?')}</span>
                <span class="plan-node-label">${tuskEscapeHtml(label(n))}</span>
                <span class="plan-node-nums">${rows} · ${own} · ${pct.toFixed(0)}%</span>
            </div>
            <div class="plan-bar"><div class="plan-bar-fill" style="width:${Math.max(1, pct).toFixed(1)}%"></div></div>`;
        wrap.appendChild(card);
        for (const k of rec.kids) render(k);
    };
    render(tree);
    const foot = document.createElement('div');
    foot.className = 'plan-foot';
    foot.textContent = analyzed
        ? `Total ${fmt(metric(root))} ms · planning ${fmt(plan[0]?.['Planning Time'] || 0)} ms · execution ${fmt(plan[0]?.['Execution Time'] || 0)} ms. Bars are each node's own time.`
        : `Estimated total cost ${fmt(metric(root))}. Bars are each node's own cost; press Analyze for real rows and times.`;
    wrap.appendChild(foot);
    return wrap;
};
