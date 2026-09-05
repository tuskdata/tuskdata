// scheduled.js — UI logic for the /scheduled page.
// All HTTP calls go through tuskFetchJSON (timeout + error normalization).

(function () {
    "use strict";

    function defaultForm() {
        return {
            kind: "",
            name: "",
            connection_id: "",
            sql: "",
            save_results_as: "",
            pipeline_id: "",
            workspace: "default",
            plugin_id: "",
            plugin_kind: "",
            plugin_payload: "",
            trigger_type: "cron",
            cron: "0 2 * * *",
            interval_hours: 0,
            interval_minutes: 5,
            interval_seconds: 0,
            run_date: "",
            notify_on_success: false,
            notify_on_failure: true,
            // Only for kind === "backup"
            backup_format: "custom",
            keep_last: 7,
            backup_dir: "",
        };
    }

    function scheduledPage() {
        return {
            loading: true,
            jobs: [],
            runsByJob: {},  // {job_id: [{status, started_at, ...}]}
            submitting: false,
            newModalOpen: false,
            step: "kind",
            form: defaultForm(),
            // Filled by `/api/scheduler/info` on init. Shown next to the
            // cron-expression hint so users aren't guessing whether
            // "0 2 * * *" means 2 AM UTC, browser-local, or server-local
            // (B11 in 0.4.26). The scheduler resolves to either the
            // configured default or America/Santo_Domingo by default.
            serverTimezone: "server time",
            // Pipeline runs drawer state
            pipelineRunsOpen: false,
            pipelineRunsLoading: false,
            pipelineRuns: [],
            pipelineRunsJob: null,
            kindOptions: [
                { id: "query", label: "SQL query", icon: "code", desc: "Run SQL on a connection" },
                { id: "pipeline", label: "Pipeline", icon: "git-branch", desc: "Run a saved Data pipeline" },
                { id: "backup", label: "Backup", icon: "hard-drive-download", desc: "pg_dump a Postgres database" },
                { id: "vacuum", label: "VACUUM", icon: "wand-2", desc: "VACUUM ANALYZE all tables" },
                { id: "analyze", label: "ANALYZE", icon: "activity", desc: "ANALYZE all tables" },
                { id: "plugin", label: "Plugin job", icon: "puzzle", desc: "Plugin-registered handler" },
            ],

            async init() {
                // Resolve the scheduler's configured timezone in parallel
                // with the jobs list — used to label cron hints.
                window.tuskFetchJSON("/api/scheduler/info")
                    .then((d) => { if (d && d.timezone) this.serverTimezone = d.timezone; })
                    .catch(() => {});
                await this.refresh();
                this.$nextTick(() => window.lucide && window.lucide.createIcons());
            },

            get activeCount() {
                return this.jobs.filter((j) => j.enabled).length;
            },
            get pausedCount() {
                return this.jobs.filter((j) => !j.enabled).length;
            },

            async refresh() {
                this.loading = true;
                try {
                    const data = await window.tuskFetchJSON("/api/scheduler/jobs");
                    this.jobs = data.jobs || [];
                    // Pull last 10 runs for each job to render the sparkline
                    await Promise.all(
                        this.jobs.map(async (j) => {
                            try {
                                const r = await window.tuskFetchJSON(`/api/scheduler/jobs/${j.id}/runs`);
                                this.runsByJob[j.id] = r.runs || [];
                            } catch (_) {
                                this.runsByJob[j.id] = [];
                            }
                        })
                    );
                } catch (e) {
                    if (window.tuskToast) window.tuskToast(`Failed to load jobs: ${e.message}`, "error");
                } finally {
                    this.loading = false;
                    this.$nextTick(() => window.lucide && window.lucide.createIcons());
                }
            },

            // Reset the fields of triggers OTHER than the one being
            // selected. Without this, switching Cron→Interval then
            // back to "One-time" left stale interval values around,
            // and stuff like the run_date field could leak from a
            // previous attempt — B8 in 0.4.26.
            onTriggerTypeChange(next) {
                this.form.trigger_type = next;
                if (next !== "cron") {
                    this.form.cron = "0 2 * * *";
                }
                if (next !== "interval") {
                    this.form.interval_hours = 0;
                    this.form.interval_minutes = 5;
                    this.form.interval_seconds = 0;
                }
                if (next !== "date") {
                    this.form.run_date = "";
                }
            },

            // ── Trigger formatting ──────────────────────────────
            formatTrigger(trigger) {
                if (!trigger) return "";
                if (trigger.type === "cron") {
                    if (trigger.cron) return trigger.cron;
                    const dow = trigger.day_of_week || "*";
                    return `${trigger.minute ?? 0} ${trigger.hour ?? 0} ${trigger.day || "*"} ${trigger.month || "*"} ${dow}`;
                }
                if (trigger.type === "interval") {
                    const parts = [];
                    if (trigger.hours) parts.push(`${trigger.hours}h`);
                    if (trigger.minutes) parts.push(`${trigger.minutes}m`);
                    if (trigger.seconds) parts.push(`${trigger.seconds}s`);
                    return `every ${parts.join(" ") || "—"}`;
                }
                if (trigger.type === "date") {
                    return `once at ${trigger.run_date || "—"}`;
                }
                return JSON.stringify(trigger);
            },

            statusChipClass(job) {
                if (!job.enabled) return "chip-neutral";
                if (job.last_run_status === "error") return "chip-amber";
                return "chip-green";
            },
            statusChipText(job) {
                if (!job.enabled) return "paused";
                if (job.last_run_status === "error") return "1 alert";
                return "active";
            },

            nextRunLabel(job) {
                if (!job.enabled) return "paused";
                if (!job.next_run) return "—";
                const dt = new Date(job.next_run);
                const diffMs = dt.getTime() - Date.now();
                if (diffMs <= 0) return "now";
                const sec = Math.floor(diffMs / 1000);
                if (sec < 60) return `next: ${sec}s`;
                const min = Math.floor(sec / 60);
                if (min < 60) return `next: ${min}m`;
                const hr = Math.floor(min / 60);
                if (hr < 24) return `next: ${hr}h`;
                return `next: in ${Math.floor(hr / 24)}d`;
            },

            runBars(job) {
                // Render last 10 runs (oldest left → newest right). Pending if no run yet.
                const runs = (this.runsByJob[job.id] || []).slice(0, 10);
                runs.reverse(); // oldest first
                const bars = [];
                for (let i = 0; i < 10; i += 1) {
                    const r = runs[i];
                    if (!r) {
                        bars.push({ idx: i, height: 4, className: "pending" });
                        continue;
                    }
                    const failed = r.status === "error";
                    const dur = r.duration_ms || 0;
                    const height = Math.max(8, Math.min(18, 10 + Math.log10(Math.max(dur, 1)) * 2));
                    bars.push({ idx: i, height: Math.round(height), className: failed ? "fail" : "" });
                }
                return bars;
            },

            runsTooltip(job) {
                const runs = this.runsByJob[job.id] || [];
                if (!runs.length) return "No runs yet";
                return runs
                    .map((r) => `${r.started_at}: ${r.status}${r.duration_ms ? ` (${r.duration_ms}ms)` : ""}`)
                    .join("\n");
            },

            // ── Pipeline runs drawer ───────────────────────────
            async openPipelineRuns(job) {
                this.pipelineRunsJob = job;
                this.pipelineRunsOpen = true;
                this.pipelineRunsLoading = true;
                this.pipelineRuns = [];
                try {
                    const data = await window.tuskFetchJSON(
                        `/api/scheduler/jobs/${job.id}/pipeline-runs`
                    );
                    this.pipelineRuns = data.runs || [];
                } catch (e) {
                    if (window.tuskToast) {
                        window.tuskToast(`Failed to load runs: ${e.message}`, "error");
                    }
                } finally {
                    this.pipelineRunsLoading = false;
                    this.$nextTick(() => window.lucide && window.lucide.createIcons());
                }
            },

            formatRunTime(iso) {
                if (!iso) return "—";
                try {
                    const dt = new Date(iso);
                    return dt.toLocaleString();
                } catch (_) {
                    return iso;
                }
            },

            // ── Modal flow ─────────────────────────────────────
            openNewModal() {
                this.form = defaultForm();
                this.step = "kind";
                this.newModalOpen = true;
                this.$nextTick(() => window.lucide && window.lucide.createIcons());
            },

            selectKind(kindId) {
                this.form.kind = kindId;
                this.step = "form";
                this.$nextTick(() => window.lucide && window.lucide.createIcons());
            },

            kindLabel(kindId) {
                const k = this.kindOptions.find((o) => o.id === kindId);
                return k ? k.label : kindId;
            },

            canProceedToTrigger() {
                if (!this.form.name) return false;
                if (["backup", "vacuum", "analyze"].includes(this.form.kind)) {
                    return Boolean(this.form.connection_id);
                }
                if (this.form.kind === "query") {
                    return Boolean(this.form.connection_id && this.form.sql);
                }
                if (this.form.kind === "pipeline") {
                    return Boolean(this.form.pipeline_id);
                }
                if (this.form.kind === "plugin") {
                    return Boolean(this.form.plugin_id && this.form.plugin_kind);
                }
                return false;
            },

            buildTrigger() {
                const t = this.form.trigger_type;
                if (t === "cron") {
                    return { type: "cron", cron: this.form.cron };
                }
                if (t === "interval") {
                    return {
                        type: "interval",
                        hours: this.form.interval_hours || 0,
                        minutes: this.form.interval_minutes || 0,
                        seconds: this.form.interval_seconds || 0,
                    };
                }
                if (t === "date") {
                    return { type: "date", run_date: this.form.run_date };
                }
                return { type: "cron", cron: "0 0 * * *" };
            },

            async submit() {
                this.submitting = true;
                try {
                    const trigger = this.buildTrigger();
                    let url;
                    let body;
                    const kind = this.form.kind;

                    if (kind === "query") {
                        url = "/api/scheduler/jobs/query";
                        body = {
                            name: this.form.name,
                            connection_id: this.form.connection_id,
                            sql: this.form.sql,
                            save_results_as: this.form.save_results_as || null,
                            trigger,
                        };
                    } else if (kind === "pipeline") {
                        url = "/api/scheduler/jobs/pipeline";
                        body = {
                            name: this.form.name,
                            pipeline_id: this.form.pipeline_id,
                            workspace: this.form.workspace || "default",
                            trigger,
                        };
                    } else if (kind === "plugin") {
                        url = "/api/scheduler/jobs/plugin";
                        let parsedPayload = {};
                        if (this.form.plugin_payload) {
                            try {
                                parsedPayload = JSON.parse(this.form.plugin_payload);
                            } catch (e) {
                                throw new Error(`Invalid payload JSON: ${e.message}`);
                            }
                        }
                        body = {
                            name: this.form.name,
                            plugin_id: this.form.plugin_id,
                            kind: this.form.plugin_kind,
                            payload: parsedPayload,
                            trigger,
                        };
                    } else if (kind === "backup" || kind === "vacuum" || kind === "analyze") {
                        url = `/api/scheduler/jobs/${kind}`;
                        // Legacy endpoints expect flat cron fields; translate from JobSpec trigger.
                        body = { connection_id: this.form.connection_id };
                        if (kind === "backup") {
                            body.format = this.form.backup_format || "custom";
                            body.keep_last = Number(this.form.keep_last) || 0;
                            if (this.form.backup_dir) body.backup_dir = this.form.backup_dir;
                        }
                        if (trigger.type === "date") {
                            body.run_date = trigger.run_date;
                        } else if (trigger.type === "cron" && trigger.cron) {
                            const parts = trigger.cron.split(/\s+/);
                            if (parts.length >= 5) {
                                body.minute = parts[0];
                                body.hour = parts[1];
                                body.day_of_week = parts[4];
                            }
                        } else if (trigger.type === "cron") {
                            body.minute = trigger.minute;
                            body.hour = trigger.hour;
                            body.day_of_week = trigger.day_of_week;
                        }
                    } else {
                        throw new Error(`Unknown kind: ${kind}`);
                    }

                    const result = await window.tuskFetchJSON(url, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify(body),
                    });

                    if (result.error) throw new Error(result.error);

                    if (window.tuskToast) window.tuskToast("Scheduled job created", "success");
                    this.newModalOpen = false;
                    await this.refresh();
                } catch (e) {
                    if (window.tuskToast) window.tuskToast(e.message, "error");
                } finally {
                    this.submitting = false;
                }
            },

            // ── Row actions ────────────────────────────────────
            async runNow(jobId) {
                try {
                    await window.tuskFetchJSON(`/api/scheduler/jobs/${jobId}/run`, { method: "POST" });
                    if (window.tuskToast) window.tuskToast("Triggered", "success");
                    setTimeout(() => this.refresh(), 800);
                } catch (e) {
                    if (window.tuskToast) window.tuskToast(`Run failed: ${e.message}`, "error");
                }
            },

            async togglePause(job) {
                const path = job.enabled ? "pause" : "resume";
                try {
                    await window.tuskFetchJSON(`/api/scheduler/jobs/${job.id}/${path}`, { method: "POST" });
                    await this.refresh();
                } catch (e) {
                    if (window.tuskToast) window.tuskToast(`Failed: ${e.message}`, "error");
                }
            },

            async deleteJob(job) {
                if (!window.confirm(`Delete schedule "${job.name}"?`)) return;
                try {
                    await window.tuskFetchJSON(`/api/scheduler/jobs/${job.id}`, { method: "DELETE" });
                    if (window.tuskToast) window.tuskToast("Deleted", "success");
                    await this.refresh();
                } catch (e) {
                    if (window.tuskToast) window.tuskToast(`Delete failed: ${e.message}`, "error");
                }
            },

            // Edit trigger only (B9 in 0.4.26). Editing the payload
            // (sql, connection_id, etc.) is bigger surface area — for
            // now you delete-and-recreate. The trigger is the thing
            // that goes wrong most often (wrong cron, wrong type) so
            // that's what we make editable.
            async editJob(job) {
                const t = job.trigger || {};
                let next;
                if (t.type === "cron") {
                    const current = t.cron || `${t.minute ?? 0} ${t.hour ?? 0} * * *`;
                    next = window.prompt(`Edit cron expression for "${job.name}":`, current);
                    if (!next) return;
                    next = { type: "cron", cron: next.trim() };
                } else if (t.type === "interval") {
                    const totalMin = (t.hours || 0) * 60 + (t.minutes || 0) + (t.seconds || 0) / 60;
                    const raw = window.prompt(`Edit interval (minutes) for "${job.name}":`, String(totalMin));
                    if (!raw) return;
                    const minutes = Number(raw);
                    if (!minutes || minutes <= 0) {
                        if (window.tuskToast) window.tuskToast("Invalid minutes", "error");
                        return;
                    }
                    next = { type: "interval", hours: 0, minutes: minutes, seconds: 0 };
                } else {
                    if (window.tuskToast) window.tuskToast("This trigger type isn't editable yet — delete and recreate.", "warning");
                    return;
                }
                try {
                    await window.tuskFetchJSON(`/api/scheduler/jobs/${job.id}/trigger`, {
                        method: "PUT",
                        body: JSON.stringify({ trigger: next }),
                    });
                    if (window.tuskToast) window.tuskToast("Trigger updated", "success");
                    await this.refresh();
                } catch (e) {
                    if (window.tuskToast) window.tuskToast(`Update failed: ${e.message}`, "error");
                }
            },
        };
    }

    window.scheduledPage = scheduledPage;
})();
