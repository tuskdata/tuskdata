/* Command palette — ⌘K / Ctrl+K opens an Alpine overlay that indexes
   connections, saved queries, history, scheduled jobs, and pages.

   The index is fetched once on first open and cached for 60s; navigating
   between pages doesn't re-fetch. The palette listens for a custom
   `tusk-cmdk-open` event so other components (Ask AI button on the
   homepage) can pop it with a preset mode. */

window.cmdkPalette = function () {
  const PAGES = [
    { kind: "page", title: "Home",      icon: "home",        url: "/home" },
    { kind: "page", title: "Studio",    icon: "terminal",    url: "/studio" },
    { kind: "page", title: "Schema",    icon: "git-graph",   url: "/schema" },
    { kind: "page", title: "Explore",   icon: "bar-chart-3", url: "/explore" },
    { kind: "page", title: "Scheduled", icon: "clock",       url: "/scheduled" },
    { kind: "page", title: "Data",      icon: "git-branch",  url: "/data" },
    { kind: "page", title: "Admin",     icon: "database",    url: "/admin" },
    { kind: "page", title: "Settings · AI", icon: "sparkles", url: "/settings/ai" },
    { kind: "page", title: "Settings · Notifications", icon: "bell", url: "/notifications/settings" },
    { kind: "page", title: "Profile",   icon: "user",        url: "/profile" },
    { kind: "page", title: "Users",     icon: "users",       url: "/users" },
  ];

  const ICON_BY_KIND = {
    connection: "database",
    saved: "star",
    history: "history",
    scheduled: "clock",
    page: "arrow-right",
    ai: "sparkles",
  };

  return {
    open: false,
    query: "",
    activeIndex: 0,
    items: [],
    cache: null,
    cacheAt: 0,
    mode: "all", // "all" | "ai"

    init() {
      window.addEventListener("keydown", (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
          e.preventDefault();
          this.openPalette();
        } else if (e.key === "Escape" && this.open) {
          this.close();
        }
      });
      window.addEventListener("tusk-cmdk-open", (e) => {
        this.openPalette((e.detail && e.detail.mode) || "all");
      });
    },

    async openPalette(mode = "all") {
      this.mode = mode;
      this.query = "";
      this.activeIndex = 0;
      this.open = true;
      await this.$nextTick();
      const input = this.$refs.input;
      if (input) input.focus();
      // Refresh index if cache is older than 60s.
      if (!this.cache || Date.now() - this.cacheAt > 60_000) {
        await this.refreshIndex();
      }
      this.recompute();
    },

    close() {
      this.open = false;
    },

    async refreshIndex() {
      const out = [];
      // Static pages always available
      for (const p of PAGES) out.push(p);
      try {
        const conns = await tuskFetchJSON("/api/connections").catch(() => ({ connections: [] }));
        for (const c of (conns.connections || conns || [])) {
          out.push({
            kind: "connection",
            title: c.name,
            subtitle: `${c.type}${c.database ? " · " + c.database : ""}`,
            icon: "database",
            action: { type: "navigate", url: `/studio?connection=${encodeURIComponent(c.id)}` },
          });
        }
      } catch (e) { /* ignore */ }
      try {
        const saved = await tuskFetchJSON("/api/saved-queries").catch(() => ({ queries: [] }));
        for (const s of (saved.queries || saved || [])) {
          out.push({
            kind: "saved",
            title: s.name,
            subtitle: s.folder || "Saved query",
            icon: "star",
            action: { type: "navigate", url: `/studio?saved=${s.id}` },
          });
        }
      } catch (e) { /* ignore */ }
      try {
        const hist = await tuskFetchJSON("/api/history?limit=30").catch(() => ({ history: [] }));
        for (const h of (hist.history || [])) {
          const sql = (h.sql || "").trim().replace(/\s+/g, " ").slice(0, 80);
          out.push({
            kind: "history",
            title: sql,
            subtitle: `${h.connection_name} · ${h.execution_time_ms}ms`,
            icon: "history",
            action: { type: "navigate", url: `/studio?history=${h.id}` },
          });
        }
      } catch (e) { /* ignore */ }
      try {
        const jobs = await tuskFetchJSON("/api/scheduler/jobs").catch(() => ({ jobs: [] }));
        for (const j of (jobs.jobs || [])) {
          out.push({
            kind: "scheduled",
            title: j.name,
            subtitle: `Job · ${j.kind || "task"}`,
            icon: "clock",
            action: { type: "navigate", url: `/scheduled?job=${j.id}` },
          });
        }
      } catch (e) { /* ignore */ }

      this.cache = out;
      this.cacheAt = Date.now();
    },

    recompute() {
      const q = this.query.trim().toLowerCase();
      const all = this.cache || PAGES;
      if (!q) {
        // Top groups: pages first, then a few of each list.
        this.items = all.slice(0, 12);
      } else {
        this.items = all
          .filter((it) => {
            const hay = (it.title + " " + (it.subtitle || "")).toLowerCase();
            return hay.includes(q);
          })
          .slice(0, 30);

        // Always offer "Ask AI: <query>" as an option when the query is non-trivial.
        if (q.length > 3) {
          this.items.unshift({
            kind: "ai",
            title: `Ask AI: "${this.query.trim()}"`,
            subtitle: "Generate SQL or get an explanation",
            icon: "sparkles",
            action: { type: "ai", prompt: this.query.trim() },
          });
        }
      }
      this.activeIndex = 0;
    },

    onInput() { this.recompute(); },

    onKey(e) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        this.activeIndex = Math.min(this.activeIndex + 1, this.items.length - 1);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        this.activeIndex = Math.max(this.activeIndex - 1, 0);
      } else if (e.key === "Enter") {
        e.preventDefault();
        this.run(this.items[this.activeIndex]);
      }
    },

    iconFor(item) {
      return item.icon || ICON_BY_KIND[item.kind] || "arrow-right";
    },

    async run(item) {
      if (!item) return;
      const action = item.action;
      if (item.kind === "page" && item.url) {
        window.location.href = item.url;
        return;
      }
      if (action && action.type === "navigate") {
        window.location.href = action.url;
        return;
      }
      if (action && action.type === "ai") {
        // Inline AI prompt — needs a connection context to be truly useful.
        // For v0.4.4 we route to Studio with a query parameter that the
        // Studio can render the AI panel for.
        window.location.href = `/studio?ai=${encodeURIComponent(action.prompt)}`;
        return;
      }
      this.close();
    },
  };
};
