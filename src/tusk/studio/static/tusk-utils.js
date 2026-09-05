/* Shared client-side utilities — loaded from base.html so every page
   AND every plugin inherits them without bundling a copy.

   Surface (all attached to window):

     tuskFormatBytes(n)   → "1.2 KB" / "4.5 MB" / "2.3 GB"
     tuskTimeAgo(iso)     → "2 min ago" / "3h ago" / "Apr 14"
     tuskEscapeHtml(s)    → HTML-escaped string for safe innerHTML
     tuskFormatNumber(n)  → "12,345" / "1.2K" / "1.5M"
     tuskQS(obj)          → encodeURIComponent-style query string

   Plugins should reach for these instead of re-implementing. The
   v0.4.5 dedup pass replaced ~30 in-plugin copies with calls to
   these globals.
*/
(function () {
    "use strict";

    function tuskFormatBytes(n) {
        if (n == null || isNaN(n)) return "—";
        const units = ["B", "KB", "MB", "GB", "TB", "PB"];
        let i = 0;
        let v = Number(n);
        while (v >= 1024 && i < units.length - 1) {
            v /= 1024;
            i++;
        }
        return (i === 0 ? v.toFixed(0) : v.toFixed(v >= 10 ? 1 : 2)) + " " + units[i];
    }

    function tuskTimeAgo(iso) {
        if (!iso) return "";
        const then = typeof iso === "number" ? new Date(iso) : new Date(iso);
        if (isNaN(then.getTime())) return "";
        const sec = Math.floor((Date.now() - then.getTime()) / 1000);
        if (sec < 5)   return "just now";
        if (sec < 60)  return `${sec}s ago`;
        const min = Math.floor(sec / 60);
        if (min < 60)  return `${min} min ago`;
        const hr = Math.floor(min / 60);
        if (hr < 24)   return `${hr}h ago`;
        const day = Math.floor(hr / 24);
        if (day < 7)   return `${day}d ago`;
        // Past a week, just show the date — we don't have user locale here,
        // so fall back to whatever the browser gives us.
        return then.toLocaleDateString();
    }

    function tuskEscapeHtml(s) {
        if (s == null) return "";
        return String(s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function tuskFormatNumber(n) {
        if (n == null || isNaN(n)) return "—";
        const v = Number(n);
        if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(1).replace(/\.0$/, "") + "B";
        if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
        if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1).replace(/\.0$/, "") + "K";
        return v.toLocaleString();
    }

    function tuskQS(obj) {
        if (!obj) return "";
        const parts = [];
        for (const [k, v] of Object.entries(obj)) {
            if (v === null || v === undefined) continue;
            parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
        }
        return parts.length ? "?" + parts.join("&") : "";
    }

    window.tuskFormatBytes = tuskFormatBytes;
    window.tuskTimeAgo = tuskTimeAgo;
    window.tuskEscapeHtml = tuskEscapeHtml;
    window.tuskFormatNumber = tuskFormatNumber;
    window.tuskQS = tuskQS;
})();

// ─── Basemap ────────────────────────────────────────────────────────
// One place decides what a MapLibre map sits on. Settings → Studio can
// point `map_tiles_url` at any XYZ raster provider (self-hosted OSM, a
// keyed CARTO/Mapbox URL, an internal tile server); without it we use
// OpenFreeMap's vector styles, which need no key. CARTO's free raster
// basemaps started returning "API KEY REQUIRED" watermarks in 2026, which
// is why the old default had to go.
window.tuskBasemapStyle = function () {
    const ui = window.TUSK_UI || {};
    if (ui.map_tiles_url) {
        return {
            version: 8,
            sources: {
                basemap: {
                    type: 'raster',
                    tiles: [ui.map_tiles_url],
                    tileSize: 256,
                    attribution: ui.map_tiles_attribution || '',
                },
            },
            layers: [{ id: 'basemap', type: 'raster', source: 'basemap', minzoom: 0, maxzoom: 22 }],
        };
    }
    const dark = document.body && document.body.getAttribute('data-theme') === 'dark';
    return dark ? 'https://tiles.openfreemap.org/styles/dark' : 'https://tiles.openfreemap.org/styles/positron';
};
