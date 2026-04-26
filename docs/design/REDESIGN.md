# Tusk v0.4.x — Redesign Port

The new visual direction lives in [design-system.html](design-system.html)
(canonical reference) and [redesign-v3.html](redesign-v3.html) (page
mockups). This doc is the porting plan from v0.3.x to v0.4.x.

## Direction in one paragraph

**Light-first, warm, editorial.** Off-white `#fbfaf7` body, surfaces in
white and warm cream, ink-black text, **rust orange `#d4502b`** as the
single brand color. **Geist** for UI, **Geist Mono** for data,
**Instrument Serif** for editorial moments (page titles, big numbers).
Dark mode via `data-theme="dark"` is a first-class citizen, not an
afterthought.

Five principles:
1. Calm over clever
2. Data is the hero
3. Warm, not corporate
4. Color = meaning (never decorative)
5. Keyboard-first, mouse-fluent

## Phases

### Round 1 — Foundations *(current)*

**Goal:** new shell that *looks* like v3 without any feature change.

- Vendor / load Geist + Geist Mono + Instrument Serif
- New `design-tokens.css` with all CSS variables from the design
  system, hooked into `base.html`
- New mammoth tusk SVG logo replacing the `🦣` emoji
- Light-first, with `data-theme="dark"` parity
- Top nav redesigned: brand mark + lockup, pill-tab navigation,
  command-K search affordance, kbd shortcut hints, settings/notif/theme
  cluster on the right
- Body shell: warm off-white, sticky top bar, sidebar borders softened

Stays identical for now: every page below the shell, every modal,
every chart. We're moving the frame, not the contents.

### Round 2 — Studio polish

- Connection sidebar with the new "active row" treatment
- Tab bar with file-icon prefix + dirty marker + close affordance
- Editor toolbar with Explain / Format / Save / Run pill row
- Results table: column type chip, sparkline column header, view tabs
  (Table / Map / Chart / JSON / Plan), Pin button
- Inline column stats (n unique, distinct count) in headers

### Round 3 — Admin + Data + plugin tabs

- Admin: stat cards in serif numerals, sparklines built from
  `/admin/{conn}/stats/history`, calmer table styling
- Data tab: pipeline canvas with the design system's node treatment
- BI / CI / Security: bring them into the family — currently look like
  separate apps

### Round 4+ — New features (out of scope until rounds 1–3 land)

The v3 mockup includes pages we don't ship yet. Each is a full
feature, not just UI:

- AI copilot panel (Tusk Copilot) — needs an LLM integration layer
- Notebook page — Hex-style cells (SQL + Python + markdown)
- Schema visualization — auto ER diagram
- Profile page — automatic column profiling at table-open time
- Lineage — track which queries / pipelines touch which columns
- Scheduler page — surface APScheduler jobs
- Landing / marketing page — for the OSS site

These ship one at a time after the visual port stabilizes.

## What stays, what changes

| Area              | v0.3.x today         | v0.4.x target              |
|-------------------|----------------------|----------------------------|
| Body bg           | `#0d1117` (dark)     | `#fbfaf7` (warm off-white) |
| Brand color       | indigo `#6366f1`     | coral `#d4502b`            |
| UI font           | Inter                | Geist                      |
| Mono font         | JetBrains Mono       | Geist Mono                 |
| Display font      | none                 | Instrument Serif           |
| Logo              | 🦣 emoji             | SVG mammoth tusk           |
| Default theme     | dark                 | light                      |
| Dark theme        | second-class         | first-class                |
| Backend           | unchanged            | unchanged                  |
| URLs / JSON shape | unchanged            | unchanged                  |

## Constraints (still)

- MiniJinja templates only (no Python at template time)
- HTMX + Alpine.js for interactivity, no React
- Tailwind utility classes — keep usage; layer the design tokens on top
- Lucide icons — already match the v3 mockup
- Self-vendorable: every font / asset must work offline (`make vendor`)

## Files

- `docs/design/design-system.html` — the canonical token reference
- `docs/design/redesign-v3.html` — page-by-page mockups
- `src/tusk/studio/static/design-tokens.css` — *(round 1)* new file
  with CSS variables for both themes
- `src/tusk/studio/static/styles.css` — existing app styles, gradually
  migrated to the new tokens
- `src/tusk/studio/templates/base.html` — top nav + theme attribute

## Versioning

Each round ships as `v0.4.x`. Feature additions stay frozen during
rounds 1-3 except for security / regression fixes. Once Round 3 ships,
v0.5.x picks up the feature track again (AI copilot first).
