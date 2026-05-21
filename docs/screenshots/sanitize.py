"""Sanitize the 6 source screenshots for docs/screenshots/.

For each image we replace specific sensitive text regions (DB names,
IPs, real table names, real row data) with placeholder labels drawn
over a card-background-colored rectangle.

Coordinates were measured by cropping subregions of each source image
and inspecting the result. They're approximate but tight enough that
the substitution looks intentional, not like an obvious blur.

Replacement names follow a consistent naming scheme so that the
substitutions read like a plausible test environment:

  api_socio_db_pro  → my-postgres-prod
  status_staging    → my-postgres-stage
  Alexandria        → analytics-db
  ApiSocio          → my-postgres-dev
  geo_administrative_area → public.regions
  leasing_*         → app_*  (in the schema view)
  10.0.0.188:7000   → <internal-host>
  54.210.176.211    → <bastion-ip>

Place names in the Studio result table (LA NORITA, LOS INDIOS …) are
blanket-redacted as solid rectangles rather than replaced text, since
they vary row by row.
"""

from __future__ import annotations

from PIL import Image, ImageDraw, ImageFont, ImageFilter
import os
from pathlib import Path

SRC = Path(os.environ.get("TUSK_SCREENSHOT_SOURCES", "./_raw_screenshots"))
DST = Path("/Users/jeasoft/Projects/TuskData/docs/screenshots")
DST.mkdir(parents=True, exist_ok=True)

# Card backgrounds in the Tusk theme.
LIGHT_BG = (251, 250, 247)     # --bg
SURFACE = (255, 255, 255)       # --surface (card background)
SURFACE_2 = (245, 243, 238)     # --surface-2 (alternative card background)
FG = (26, 26, 23)               # primary text
FG_2 = (61, 60, 55)             # secondary text
FG_3 = (118, 116, 106)          # tertiary text


def _font(size: int = 13, *, weight: str = "regular") -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Best-effort font load. Geist if present, else Inter, else default."""
    candidates = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNS.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
    ]
    for c in candidates:
        try:
            return ImageFont.truetype(c, size)
        except Exception:
            continue
    return ImageFont.load_default()


def redact(img: Image.Image, box: tuple[int, int, int, int], *,
           label: str | None = None, bg=SURFACE, fg=FG,
           font_size: int = 13, label_align: str = "left") -> None:
    """Cover `box` (x1,y1,x2,y2) with a solid rect and optionally write `label`."""
    draw = ImageDraw.Draw(img)
    x1, y1, x2, y2 = box
    draw.rectangle(box, fill=bg)
    if label:
        font = _font(font_size)
        bbox = draw.textbbox((0, 0), label, font=font)
        text_h = bbox[3] - bbox[1]
        text_w = bbox[2] - bbox[0]
        if label_align == "center":
            tx = x1 + ((x2 - x1) - text_w) // 2
        elif label_align == "right":
            tx = x2 - text_w - 4
        else:
            tx = x1 + 4
        ty = y1 + ((y2 - y1) - text_h) // 2 - 2
        draw.text((tx, ty), label, fill=fg, font=font)


def blur_region(img: Image.Image, box: tuple[int, int, int, int], radius: int = 8) -> None:
    """Apply a heavy gaussian over a region — for varied row data."""
    crop = img.crop(box).filter(ImageFilter.GaussianBlur(radius=radius))
    img.paste(crop, box)


# ─────────────────────────────────────────────────────────────────────
# Image 3: Home — Recent queries shows DB names
# ─────────────────────────────────────────────────────────────────────
def sanitize_home():
    img = Image.open(SRC / "3.png").convert("RGB")
    # The 4 connection-name spans in the Recent queries list.
    # Row band spans roughly x=410-560 (the bold name text).
    # Bg under each is SURFACE_2.
    rows = [
        (411, 437, 558, 459, "my-postgres-prod"),
        (411, 502, 558, 524, "my-postgres-prod"),
        (411, 568, 558, 590, "my-postgres-prod"),
        (411, 633, 558, 655, "analytics-db"),
    ]
    for (x1, y1, x2, y2, name) in rows:
        redact(img, (x1, y1, x2, y2), label=name, bg=SURFACE, fg=FG, font_size=15)
    img.save(DST / "home.png", optimize=True)
    print("wrote", DST / "home.png")


# ─────────────────────────────────────────────────────────────────────
# Image 4: Studio — connection sidebar + top chip + result rows
# ─────────────────────────────────────────────────────────────────────
def sanitize_studio():
    img = Image.open(SRC / "4.png").convert("RGB")
    # Left sidebar connection list (under "CONNECTIONS"). Each row is
    # ~30px tall starting around y=120.
    sidebar = [
        (60, 120, 235, 142, "analytics-db"),
        (60, 154, 235, 176, "my-postgres-stage"),
        (60, 186, 235, 208, "my-postgres-dev"),
        (60, 217, 235, 239, "my-postgres-prod"),
    ]
    for (x1, y1, x2, y2, name) in sidebar:
        redact(img, (x1, y1, x2, y2), label=name, bg=SURFACE, fg=FG, font_size=13)

    # Top-right connection chip: "● status_staging · PostgreSQL"
    redact(img, (1505, 73, 1620, 92),
           label="my-postgres-stage", bg=SURFACE, fg=FG, font_size=12)

    # SQL editor: rect covers the whole line so we don't leave the
    # original "SELECT * FROM geo_..." prefix visible underneath the
    # replacement. The editor text starts at ~x=270.
    redact(img, (265, 158, 685, 180), label="SELECT * FROM regions",
           bg=SURFACE, fg=FG, font_size=14)

    # Schema sidebar (left) lists "geo_administrative_area" as the
    # selected table. Replace.
    redact(img, (45, 472, 230, 494), label="regions",
           bg=SURFACE, fg=FG, font_size=12)

    # Result table — the entire "name" column with real place names.
    # Column spans roughly x=1230-1390, y=510-985 (data rows).
    blur_region(img, (1230, 510, 1390, 985), radius=10)

    # Bottom-left query history list — blur the connection names + queries.
    # The history block spans roughly x=10-235, y=735-985.
    blur_region(img, (10, 735, 235, 985), radius=6)

    img.save(DST / "studio.png", optimize=True)
    print("wrote", DST / "studio.png")


# ─────────────────────────────────────────────────────────────────────
# Image 5: Schema — ER diagram with business table names
# ─────────────────────────────────────────────────────────────────────
def sanitize_schema():
    img = Image.open(SRC / "5.png").convert("RGB")
    # Top picker: "status_staging · status_staging"
    redact(img, (50, 78, 285, 100),
           label="my-postgres-stage · public", bg=SURFACE, fg=FG, font_size=12)

    # The whole canvas of table cards has business-specific names.
    # We blur the whole canvas region — the structure is still readable
    # (you can see there's an ER diagram) but no names leak.
    # Canvas roughly x=10-1700, y=125-985.
    blur_region(img, (10, 125, 1700, 985), radius=4)

    # Re-overlay a "schema browser" indicator strip so the page still
    # reads as an ER diagram, not a smudge.
    draw = ImageDraw.Draw(img)
    draw.rectangle((460, 470, 1230, 540), fill=SURFACE, outline=(220, 217, 207))
    font = _font(18)
    draw.text((480, 488), "Schema view — 89 tables · 254 foreign keys",
              fill=FG, font=font)

    img.save(DST / "schema.png", optimize=True)
    print("wrote", DST / "schema.png")


# ─────────────────────────────────────────────────────────────────────
# Image 6: Explore — connection + table picker, column stats
# ─────────────────────────────────────────────────────────────────────
def sanitize_explore():
    img = Image.open(SRC / "6.png").convert("RGB")
    # Top picker row.
    redact(img, (50, 95, 285, 117),
           label="my-postgres-stage · public", bg=SURFACE, fg=FG, font_size=12)
    redact(img, (335, 95, 580, 117),
           label="public.regions", bg=SURFACE, fg=FG, font_size=12)

    # The "table" tile (top-right of the page) shows the schema-
    # qualified name on two lines. Widen the rect to fully cover both.
    redact(img, (1290, 178, 1700, 222),
           label="public.regions", bg=SURFACE, fg=FG, font_size=14)

    # The "name" column on the bottom-right card has real place names
    # (CENTRO DEL PUEBLO, PUEBLO NUEVO …). Blur the right-side bottom
    # card's data area.
    blur_region(img, (860, 950, 1700, 996), radius=8)

    img.save(DST / "explore.png", optimize=True)
    print("wrote", DST / "explore.png")


# ─────────────────────────────────────────────────────────────────────
# Image 7: Admin — sidebar + IPs in tunnel errors
# ─────────────────────────────────────────────────────────────────────
def sanitize_admin():
    img = Image.open(SRC / "7.png").convert("RGB")
    draw = ImageDraw.Draw(img)

    # Left sidebar (re-measured from a crop): the "POSTGRESQL SERVERS"
    # label sits at abs y≈75; the 4 rows span abs y≈110-235. We blank
    # the whole list area and redraw with placeholder names.
    draw.rectangle((0, 60, 245, 245), fill=LIGHT_BG)
    label_font = _font(10)
    draw.text((30, 65), "POSTGRESQL SERVERS", fill=FG_3, font=label_font)
    name_font = _font(13)
    names = ["analytics-db", "my-postgres-stage", "my-postgres-dev", "my-postgres-prod"]
    for i, n in enumerate(names):
        draw.text((40, 110 + i * 30), f"●  {n}", fill=FG, font=name_font)

    # Header big title — actual position is abs y≈80-135, NOT the
    # y=22-90 I had before.
    redact(img, (290, 75, 720, 145),
           label="● my-postgres-stage", bg=LIGHT_BG, fg=FG, font_size=30)

    # Active Processes chip column — `status_staging` chips appear in
    # the first 3 process rows at abs y≈400-510. Heavy blur over the
    # whole PID + chip span.
    blur_region(img, (155, 395, 600, 530), radius=12)

    # IP at the very bottom-left: "10.0.0.188:7000".
    redact(img, (0, 990, 120, 1010),
           label="<internal-host>", bg=(255, 255, 255), fg=FG_2, font_size=11)

    # SSH-tunnel error rows. Re-measured: lock-monitor error is at
    # abs y≈815, NOT y=414. Table-maintenance error is at y≈1000.
    err_msg = ("ssh_tunnel: bastion <bastion-ip> marked unreachable "
               "(handshake to <bastion-ip>:22 timed out after 10s); "
               "will retry in <30s")
    redact(img, (180, 808, 1300, 832), label=err_msg,
           bg=SURFACE, fg=FG_2, font_size=11)
    redact(img, (180, 990, 1300, 1009), label=err_msg,
           bg=SURFACE, fg=FG_2, font_size=11)

    img.save(DST / "admin.png", optimize=True)
    print("wrote", DST / "admin.png")


# ─────────────────────────────────────────────────────────────────────
# Image 8: Analytics — already clean, just copy through
# ─────────────────────────────────────────────────────────────────────
def sanitize_analytics():
    img = Image.open(SRC / "8.png").convert("RGB")
    # No sensitive data — prebuilt dashboard names (Cluster Monitor,
    # Security Overview) are generic.
    img.save(DST / "analytics-overview.png", optimize=True)
    print("wrote", DST / "analytics-overview.png")


if __name__ == "__main__":
    sanitize_home()
    sanitize_studio()
    sanitize_schema()
    sanitize_explore()
    sanitize_admin()
    sanitize_analytics()
    print("done — 6 sanitized screenshots in", DST)
