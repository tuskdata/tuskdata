"""Regenerate the documentation screenshots.

Boots a throwaway Tusk (own HOME, single-user, port 8900) wired to the
`tusk_demo` database (see scripts/demo_db.py), drives the UI with
Playwright at a fixed viewport, and writes PNGs (2x, retina-crisp) to
docs/screenshots/. Every scene is a real page with real demo data, so the
docs never show a stale or hand-made UI.

    .venv/bin/python scripts/demo_db.py          # once
    .venv/bin/python scripts/docs_screenshots.py [--only studio,schema] [--theme light|dark]

Requires: playwright + chromium (`playwright install chromium`).
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "screenshots"
PORT = 8900
BASE = f"http://127.0.0.1:{PORT}"
VIEWPORT = {"width": 1440, "height": 900}
DEMO_DSN = "postgresql://postgres@localhost:5432/tusk_demo"


def _tusk_binary() -> str:
    exe = Path(sys.executable)
    script = exe.with_name("tusk")
    return str(script) if script.exists() else "tusk"


def _wait_health(timeout: float = 40.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{BASE}/api/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:  # noqa: BLE001
            time.sleep(0.3)
    return False


def _boot(home: Path) -> subprocess.Popen:
    """Start Tusk with a clean HOME and one connection: the demo database."""
    (home / ".tusk").mkdir(parents=True, exist_ok=True)
    (home / ".tusk" / "connections.toml").write_text(
        '[[connections]]\nid = "demo"\nname = "Demo shop"\ntype = "postgres"\nhost = "localhost"\n'
        'port = 5432\ndatabase = "tusk_demo"\nuser = "postgres"\ncolor = "#30a46c"\n'
    )
    # Reuse the developer's AI config so the Copilot scenes work when Ollama is reachable.
    real_ai = Path.home() / ".tusk" / "ai.toml"
    if real_ai.exists():
        shutil.copy(real_ai, home / ".tusk" / "ai.toml")
    # Run the working tree, not whatever wheel is installed in the venv.
    env = {**os.environ, "HOME": str(home), "TUSK_AUTH_MODE": "single", "PYTHONPATH": str(ROOT / "src")}
    return subprocess.Popen(
        [_tusk_binary(), "studio", "--host", "127.0.0.1", "--port", str(PORT)],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        start_new_session=(sys.platform != "win32"),
    )


def _stop(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True)
    else:
        import signal

        os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


async def _seed(page) -> None:
    """Give the empty-state pages something to show: jobs, channels, a pipeline.

    Runs inside the page so the app's own fetch wrapper adds the CSRF token.
    Nothing here reaches the outside world: the Slack/webhook URLs are never
    called (no job runs during a shoot) and the throwaway HOME is deleted.
    """
    await page.goto(BASE + "/home", wait_until="networkidle")
    await page.evaluate("""async () => {
        const post = (url, body) => fetch(url, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
        // Scheduled: a nightly backup, a schema check and a saved-results query.
        await post('/api/scheduler/jobs/backup', {connection_id: 'demo', hour: 2, minute: 0, format: 'custom', keep_last: 7});
        await post('/api/scheduler/jobs/schema_watch', {connection_id: 'demo', hour: 6, minute: 0});
        await post('/api/scheduler/jobs/query', {
            name: 'Daily revenue snapshot', connection_id: 'demo',
            sql: "SELECT date_trunc('day', created_at)::date AS day, count(*) AS orders, sum(total) AS revenue FROM orders WHERE status <> 'cancelled' GROUP BY 1 ORDER BY 1 DESC LIMIT 30",
            trigger: {type: 'cron', hour: 7, minute: 30, day_of_week: '*'}, save_results_as: 'daily_revenue'});
        // Notifications: two channels and the subscriptions an ops team would set.
        const slack = await (await post('/api/notifications/channels', {name: 'Ops #alerts', channel_type: 'slack', config: {webhook_url: 'https://hooks.slack.com/services/T000/B000/demo'}})).json();
        const hook = await (await post('/api/notifications/channels', {name: 'On-call bridge', channel_type: 'webhook', config: {url: 'https://alerts.example.com/tusk'}})).json();
        await post('/api/notifications/subscriptions', {subscriptions: [
            {event_key: 'scheduler.job.error', channel_id: slack.id, enabled: true},
            {event_key: 'core.backup.failed', channel_id: slack.id, enabled: true},
            {event_key: 'schema.changed', channel_id: slack.id, enabled: true},
            {event_key: 'contract.violated', channel_id: slack.id, enabled: true},
            {event_key: 'contract.violated', channel_id: hook.id, enabled: true},
        ]});
        // Data: an orders dataset with a filter and a sort, as the page would save it.
        await post('/api/data/workspace/save', {name: 'default', active_dataset_id: 'ds_orders', datasets: [{
            id: 'ds_orders', name: 'orders', source_type: 'database', connection_id: 'demo', connection_name: 'Demo shop',
            query: 'SELECT * FROM orders', joinSources: [],
            transforms: [{type: 'filter', column: 'status', operator: 'eq', value: 'shipped'},
                         {type: 'sort', columns: ['total'], descending: [true]}]}]});
    }""")


async def _scenes(page, only: set[str] | None, theme: str) -> list[str]:
    """Each scene: navigate, arrange, shoot. Returns the files written."""
    written: list[str] = []

    async def shot(name: str, clip: dict | None = None, full_page: bool = False):
        if only and name not in only:
            return
        path = OUT / f"{name}.png"
        await page.wait_for_timeout(600)
        await page.screenshot(path=str(path), clip=clip, full_page=full_page)
        written.append(path.name)
        print(f"  {path.name}")

    async def goto(path: str, settle: int = 1200):
        await page.goto(BASE + path, wait_until="networkidle")
        await page.wait_for_timeout(settle)

    # Theme: Tusk persists it client-side; set before the first navigation.
    await page.goto(BASE + "/home", wait_until="networkidle")
    await page.evaluate(f"localStorage.setItem('tusk-theme', '{theme}')")
    await _seed(page)

    # Home
    await goto("/home")
    await shot("home")

    # Studio: run a query, show results
    await goto("/studio")
    await page.click('[data-conn-id="demo"]')
    await page.wait_for_timeout(2500)
    sql = ("SELECT c.country, COUNT(*) AS orders, ROUND(SUM(o.total)) AS revenue\n"
           "FROM orders o JOIN customers c ON c.id = o.customer_id\n"
           "WHERE o.status = 'shipped'\nGROUP BY c.country ORDER BY revenue DESC LIMIT 10")
    await page.evaluate("(sql) => { editor.dispatch({changes: {from: 0, to: editor.state.doc.length, insert: sql}}) }", sql)
    await page.click('button:has-text("Run")')
    await page.wait_for_timeout(2500)
    await shot("studio")
    await page.click('button[data-view="chart"]')
    await page.wait_for_timeout(1500)
    await shot("studio-chart")
    await page.click('button[data-view="plan"]')
    await page.wait_for_timeout(2500)
    await shot("studio-plan")

    # Studio: a spatial query on the map (the demo DB carries OSM POIs + sectors)
    try:
        await page.click('button:has-text("+")', timeout=2000)
    except Exception:  # noqa: BLE001 — tab bar variant without a plus button
        await page.evaluate("window.createTab && window.createTab('Map', '')")
    await page.wait_for_timeout(600)
    geo_sql = ("SELECT o.name, o.tags->>'cuisine' AS cuisine, o.geom, s.name AS sector\n"
               "FROM osm_pois o JOIN sectors s ON ST_Contains(s.geom, o.geom)\n"
               "WHERE s.name = 'Piantini' AND o.tags->>'amenity' = 'restaurant'")
    await page.evaluate("(sql) => { editor.dispatch({changes: {from: 0, to: editor.state.doc.length, insert: sql}}); window._tuskAIWantsMap = true; }", geo_sql)
    await page.click('button:has-text("Run")')
    await page.wait_for_timeout(6000)
    await shot("studio-map")
    await page.keyboard.press("Escape")  # close the map modal before the Copilot scene
    await page.wait_for_timeout(500)

    # Studio: Copilot (only if a provider answers)
    try:
        await page.click('button[title*="Ask AI"]')
        await page.wait_for_timeout(600)
        await page.fill('textarea[placeholder^="Ask in plain"]', "vegetarian restaurants in the Piantini sector, on the map")
        await page.keyboard.press("Enter")
        for _ in range(30):
            await page.wait_for_timeout(5000)
            if await page.evaluate("document.body.innerText.includes('Generated SQL')"):
                break
        await page.wait_for_timeout(800)
        await shot("studio-copilot")
        await page.keyboard.press("Escape")
    except Exception as e:  # noqa: BLE001
        print("  (copilot scene skipped:", str(e)[:80], ")")

    # Schema
    await goto("/schema", settle=2500)
    await page.click('#schema-fit')
    await page.wait_for_timeout(4500)  # let the "Loaded N tables" toast fade
    await shot("schema")

    # Explore
    await goto("/explore", settle=2000)
    await shot("explore")

    # Scheduled
    await goto("/scheduled", settle=1500)
    await shot("scheduled")

    # Data: the seeded pipeline, executed so the preview shows rows
    await goto("/data", settle=2000)
    await page.click('button:has-text("Run Pipeline")')
    await page.wait_for_timeout(3500)
    await shot("data")

    # Admin: pick the server so the dashboard renders
    await goto("/admin", settle=1500)
    await page.click('text=Demo shop')
    await page.wait_for_timeout(4000)
    await shot("admin")

    # Analytics (demo gallery dashboard is created on first boot)
    await goto("/bi", settle=2000)
    await shot("analytics-overview")
    await goto("/bi/dashboards", settle=1500)
    await page.click('text=Chart Gallery (Demo)')
    await page.wait_for_timeout(4000)
    await shot("analytics-dashboard")

    # Notifications settings (channels, subscriptions, history)
    await goto("/notifications/settings", settle=1500)
    await shot("notifications")

    # Settings hub + Studio settings
    await goto("/settings", settle=1000)
    await shot("settings")
    await goto("/settings/studio", settle=1000)
    await shot("settings-studio")

    # Profile (tokens card is multi-user only; single-user shows the page)
    await goto("/profile", settle=1000)
    await shot("profile")

    return written


async def _run(only: set[str] | None, theme: str) -> None:
    from playwright.async_api import async_playwright

    home = Path(tempfile.mkdtemp(prefix="tusk_docs_"))
    proc = _boot(home)
    try:
        if not _wait_health():
            raise SystemExit("Tusk did not start (is tusk_demo there? run scripts/demo_db.py)")
        OUT.mkdir(parents=True, exist_ok=True)
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            ctx = await browser.new_context(viewport=VIEWPORT, device_scale_factor=2, color_scheme=theme)
            page = await ctx.new_page()
            written = await _scenes(page, only, theme)
            await browser.close()
        print(f"{len(written)} screenshots in {OUT}")
    finally:
        _stop(proc)
        shutil.rmtree(home, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated scene names")
    ap.add_argument("--theme", default="light", choices=["dark", "light"])
    args = ap.parse_args()
    only = set(args.only.split(",")) if args.only else None
    import asyncio

    asyncio.run(_run(only, args.theme))


if __name__ == "__main__":
    main()
