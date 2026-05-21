/* AI panel — Studio's "Ask AI" overlay.
 *
 * Two modes:
 *   1. Empty editor → prompt for natural-language → call /api/ai/sql
 *      → render the SQL response with "Insert" / "Replace" buttons.
 *   2. Editor has SQL → "Explain this" preset → call /api/ai/explain
 *      → render the explanation. Always free-form prompt available.
 *
 * Designed to feel like the cmdk palette: Cmd+I or click "Ask AI".
 * Esc closes. Enter submits. The panel writes results inline; the
 * user decides whether to splat the SQL into the editor.
 *
 * Renders only when called via tuskAI.open() — the markup lives at
 * the bottom of base.html (or studio.html in the future) and stays
 * `display:none` otherwise so an empty/broken page never gets stuck
 * behind it (we already paid that bill once with cmdk).
 */
(function () {
    "use strict";

    const PANEL_ID = "tusk-ai-panel";
    const STATE = {
        open: false,
        loading: false,
        provider_configured: null,  // tri-state — null until first /api/ai/status
        last_response: null,        // {kind: 'sql'|'explain'|'error', payload}
    };

    function _ensurePanel() {
        let el = document.getElementById(PANEL_ID);
        if (el) return el;
        el = document.createElement("div");
        el.id = PANEL_ID;
        el.className = "tusk-ai-mask";
        el.style.display = "none";
        el.innerHTML = `
            <div class="tusk-ai-shell" data-stop>
                <div class="tusk-ai-head">
                    <i data-lucide="sparkles"></i>
                    <span class="title">Ask AI</span>
                    <span class="meta" id="tusk-ai-meta"></span>
                    <button class="close" onclick="window.tuskAI.clearMemory()" title="Forget this conversation"
                            style="margin-right:4px;width:auto;padding:0 8px;font-size:11px">
                        <i data-lucide="eraser"></i>
                    </button>
                    <button class="close" onclick="window.tuskAI.close()" title="Close (Esc)">
                        <i data-lucide="x"></i>
                    </button>
                </div>
                <div class="tusk-ai-input-wrap">
                    <textarea id="tusk-ai-input" rows="2"
                              placeholder="Ask in plain English — e.g. 'Top 10 customers by revenue this month'"></textarea>
                    <div class="tusk-ai-hints">
                        <button type="button" class="hint" data-preset="explain">
                            <i data-lucide="help-circle"></i>Explain current SQL
                        </button>
                        <button type="button" class="hint" data-preset="optimize">
                            <i data-lucide="zap"></i>Optimize current SQL
                        </button>
                        <span class="grow"></span>
                        <button type="button" class="primary" id="tusk-ai-submit">
                            <i data-lucide="send"></i>Ask <span class="kbd">⏎</span>
                        </button>
                    </div>
                </div>
                <div class="tusk-ai-body" id="tusk-ai-body">
                    <div class="tusk-ai-empty">
                        <i data-lucide="message-square"></i>
                        Type a question above or pick a preset. The response shows up here.
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(el);

        // Wire events
        el.addEventListener("click", (e) => {
            if (!e.target.closest("[data-stop]")) window.tuskAI.close();
        });
        el.querySelectorAll("[data-preset]").forEach((b) =>
            b.addEventListener("click", () => _preset(b.dataset.preset))
        );
        el.querySelector("#tusk-ai-submit").addEventListener("click", _submit);
        el.querySelector("#tusk-ai-input").addEventListener("keydown", (e) => {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                _submit();
            }
        });
        if (window.lucide) lucide.createIcons();
        return el;
    }

    function _renderBody(html) {
        const body = document.getElementById("tusk-ai-body");
        if (!body) return;
        body.innerHTML = html;
        if (window.lucide) lucide.createIcons();
    }

    function _renderError(msg) {
        _renderBody(`
            <div class="tusk-ai-error">
                <i data-lucide="alert-circle"></i>
                <div>${tuskEscapeHtml(msg)}</div>
            </div>
        `);
    }

    function _renderConfigure() {
        _renderBody(`
            <div class="tusk-ai-empty">
                <i data-lucide="settings"></i>
                <div>
                    No AI provider configured.
                    <a href="/settings/ai" class="link">Set one up →</a>
                </div>
            </div>
        `);
    }

    function _renderSQL(sql, explanation, danger) {
        // danger: { dangerous: bool, reason: str } — server flags
        // destructive SQL (DROP/TRUNCATE/DELETE-no-WHERE/etc) so the
        // user reads the warning BEFORE pasting it into the editor.
        const warnBanner = (danger && danger.dangerous) ? `
            <div class="tusk-ai-danger">
                <i data-lucide="alert-triangle"></i>
                <div>
                    <strong>Destructive SQL detected${danger.reason ? ` — ${tuskEscapeHtml(danger.reason)}` : ""}.</strong>
                    Review carefully before running. The AI does not know
                    your data — confirm tables and WHERE clauses match
                    what you actually intend to change.
                </div>
            </div>
        ` : "";
        _renderBody(`
            <div class="tusk-ai-result">
                <div class="tusk-ai-result-head">
                    <span class="chip chip-violet"><i data-lucide="terminal"></i>Generated SQL</span>
                    <span class="grow"></span>
                    <button type="button" class="btn btn-sm" onclick="window.tuskAI.replace()">
                        <i data-lucide="refresh-cw"></i>Replace
                    </button>
                    <button type="button" class="btn btn-sm btn-brand" onclick="window.tuskAI.insert()">
                        <i data-lucide="external-link"></i>New tab
                    </button>
                </div>
                ${warnBanner}
                <pre class="tusk-ai-sql"><code>${tuskEscapeHtml(sql)}</code></pre>
                ${explanation ? `<div class="tusk-ai-explanation">${tuskEscapeHtml(explanation)}</div>` : ""}
            </div>
        `);
    }

    function _renderExplanation(text) {
        _renderBody(`
            <div class="tusk-ai-result">
                <div class="tusk-ai-result-head">
                    <span class="chip chip-green"><i data-lucide="book-open"></i>Explanation</span>
                </div>
                <div class="tusk-ai-explanation">${tuskEscapeHtml(text)}</div>
            </div>
        `);
    }

    function _renderLoading(label) {
        _renderBody(`
            <div class="tusk-ai-loading">
                <i data-lucide="loader-2" class="spin"></i>
                ${tuskEscapeHtml(label || "Thinking…")}
            </div>
        `);
    }

    function _currentSQL() {
        try {
            return window.editor && window.editor.state
                ? window.editor.state.doc.toString().trim()
                : "";
        } catch {
            return "";
        }
    }

    function _currentConnectionId() {
        // window.currentConnectionId is set by selectConnection()
        return window.currentConnectionId || null;
    }

    function _preset(kind) {
        const sql = _currentSQL();
        if (kind === "explain") {
            if (!sql) {
                _renderError("Editor is empty — write or paste some SQL first.");
                return;
            }
            _explain(sql);
        } else if (kind === "optimize") {
            if (!sql) {
                _renderError("Editor is empty — write or paste some SQL first.");
                return;
            }
            const input = document.getElementById("tusk-ai-input");
            input.value = "Optimize this SQL — same result, faster execution. Suggest indexes if relevant.";
            _generateSQL(input.value, sql);
        }
    }

    async function _submit() {
        const input = document.getElementById("tusk-ai-input");
        const prompt = (input.value || "").trim();
        if (!prompt) return;

        const sql = _currentSQL();
        // If the prompt obviously asks to "explain" and there's SQL in
        // the editor, route to /explain. Otherwise generate SQL.
        const looksLikeExplain = /^(explain|what does|describe|walk me through)/i.test(prompt);
        if (looksLikeExplain && sql) {
            _explain(sql);
        } else {
            _generateSQL(prompt, sql);
        }
    }

    async function _generateSQL(prompt, contextSQL) {
        _renderLoading("Generating SQL…");
        try {
            const res = await tuskFetchJSON("/api/ai/sql", {
                method: "POST",
                body: JSON.stringify({
                    prompt: contextSQL ? `${prompt}\n\nCurrent SQL:\n${contextSQL}` : prompt,
                    connection_id: _currentConnectionId(),
                }),
            });
            if (res.error) {
                if (res.code === 412) {
                    _renderConfigure();
                    return;
                }
                _renderError(res.error);
                return;
            }
            const danger = { dangerous: !!res.dangerous, reason: res.dangerous_reason || "" };
            STATE.last_response = { kind: "sql", sql: res.sql, explanation: res.explanation, danger };
            _renderSQL(res.sql, res.explanation, danger);
        } catch (e) {
            _renderError(e.message || "Request failed");
        }
    }

    async function _explain(sql) {
        _renderLoading("Explaining…");
        try {
            const res = await tuskFetchJSON("/api/ai/explain", {
                method: "POST",
                body: JSON.stringify({
                    sql,
                    connection_id: _currentConnectionId(),
                }),
            });
            if (res.error) {
                if (res.code === 412) {
                    _renderConfigure();
                    return;
                }
                _renderError(res.error);
                return;
            }
            STATE.last_response = { kind: "explain", text: res.explanation };
            _renderExplanation(res.explanation);
        } catch (e) {
            _renderError(e.message || "Request failed");
        }
    }

    async function _checkProvider() {
        try {
            const res = await fetch("/api/ai/status").then((r) => r.json());
            STATE.provider_configured = !!res.configured;
            const meta = document.getElementById("tusk-ai-meta");
            if (meta) {
                if (res.configured) {
                    meta.textContent = `${res.provider} · ${res.model}`;
                } else {
                    meta.textContent = "not configured";
                }
            }
        } catch {
            STATE.provider_configured = false;
        }
    }

    window.tuskAI = {
        async open() {
            const el = _ensurePanel();
            STATE.open = true;
            el.style.display = "flex";
            await _checkProvider();
            const input = document.getElementById("tusk-ai-input");
            if (input) {
                input.value = "";
                input.focus();
            }
            // If there's SQL in the editor, default body shows the
            // "Explain current SQL" hint highlighted.
            const sql = _currentSQL();
            if (sql) {
                _renderBody(`
                    <div class="tusk-ai-empty">
                        <i data-lucide="terminal"></i>
                        <div>
                            <div>${sql.length > 200 ? sql.slice(0, 200) + "…" : tuskEscapeHtml(sql)}</div>
                            <div style="margin-top:6px;color:var(--fg-3);font-size:12px">
                                Editor has SQL. Try “Explain current SQL” or ask a follow-up.
                            </div>
                        </div>
                    </div>
                `);
            } else {
                _renderBody(`
                    <div class="tusk-ai-empty">
                        <i data-lucide="message-square"></i>
                        <div>Ask anything in plain English — e.g. <code>top 10 customers by revenue this month</code></div>
                    </div>
                `);
            }
            if (STATE.provider_configured === false) {
                _renderConfigure();
            }
        },
        close() {
            const el = document.getElementById(PANEL_ID);
            if (el) el.style.display = "none";
            STATE.open = false;
        },
        insert() {
            // "Insert" now means "open in a new tab" — appending into the
            // current editor produced the bug where running an Optimize
            // round left two copies of the same query stacked in the
            // active tab (v0.4.22 / 0.4.23). A new tab is the only
            // unambiguous interpretation: the current tab keeps whatever
            // the user was working on, and the suggestion lands somewhere
            // they can run independently.
            const r = STATE.last_response;
            if (!r || r.kind !== "sql" || !r.sql) return;
            if (typeof window.createTab === "function") {
                window.createTab("AI suggestion", r.sql);
                tuskToast("SQL opened in new tab", "success");
            } else if (window.editor) {
                // Studio editor not loaded (other page) → fall back to
                // replace-in-place rather than silently doing nothing.
                window.editor.dispatch({
                    changes: { from: 0, to: window.editor.state.doc.length, insert: r.sql },
                });
                tuskToast("SQL replaced", "success");
            }
            window.tuskAI.close();
        },
        async clearMemory() {
            try {
                await tuskFetchJSON("/api/ai/clear-memory", {
                    method: "POST",
                    body: JSON.stringify({
                        connection_id: _currentConnectionId(),
                    }),
                });
                tuskToast("Conversation cleared", "success");
                _renderBody(`
                    <div class="tusk-ai-empty">
                        <i data-lucide="check"></i>
                        Memory cleared. Next prompt starts fresh.
                    </div>
                `);
            } catch (e) {
                tuskToast("Could not clear memory: " + e.message, "error");
            }
        },
        replace() {
            const r = STATE.last_response;
            if (!r || r.kind !== "sql" || !r.sql) return;
            if (!window.editor) return;
            window.editor.dispatch({
                changes: { from: 0, to: window.editor.state.doc.length, insert: r.sql },
            });
            tuskToast("SQL replaced", "success");
            window.tuskAI.close();
        },
    };

    // Cmd+I / Ctrl+I shortcut
    document.addEventListener("keydown", (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "i") {
            e.preventDefault();
            window.tuskAI.open();
        } else if (e.key === "Escape" && STATE.open) {
            window.tuskAI.close();
        }
    });
})();
