# Running without internet

Tusk's web UI needs nothing from the outside. Every browser asset it uses
(Tailwind, Alpine, HTMX, Lucide icons, MapLibre, proj4, Dagre, Chart.js,
gridstack and the Geist / Instrument Serif fonts) ships inside the Python
wheel and the container image under `static/vendor/`, and the pages
reference those files. A Tusk behind a firewall, on an air-gapped LAN or on
a laptop without Wi-Fi renders exactly like one on the open internet.

What still talks to the outside, and how to keep it inside:

| Feature | Needs | Offline option |
|---|---|---|
| Map basemap (Studio map view, Explore, dashboards) | OpenFreeMap vector tiles by default | **Settings → Studio → Map tiles URL**: point it at your own XYZ raster or vector tiles (a `tileserver-gl`, `martin` or PMTiles server on the LAN). Geometry from your queries is drawn regardless; only the background map is external. |
| AI Copilot | The provider you configure | Ollama on the same machine or LAN. Nothing is sent anywhere unless you point it at a hosted API. |
| Notification channels | The webhook, SMTP or chat endpoint you configure | LAN-only endpoints work; nothing is contacted by default. |
| `pip install tuskdata` | PyPI | Download the wheel once (`pip download tuskdata`) and install it from a file or a local index; the container image is self-contained. |

## Development: CDN mode

When hacking on templates you may prefer the Tailwind play CDN, which
generates classes on the fly instead of requiring a rebuild:

```bash
TUSK_CDN=1 tusk studio
```

That is the only time external URLs appear in the HTML. The default is
always the vendored files, and `tests/test_offline_assets.py` fails the
build if a template references a CDN outside that switch, if a vendored
file is missing, or if the compiled Tailwind lags the templates.

## Regenerating the vendored assets

```bash
make vendor          # downloads the pinned third-party files (see scripts/vendor.sh)
make tailwind        # one-time: fetches the Tailwind 4 standalone CLI
make css             # compiles static/tailwind.css -> static/vendor/tailwind.min.css
```

Run `make css` after adding Tailwind classes to a template and commit the
result; the compiled file is part of the repository so that building the
wheel or the image needs neither Node.js nor the network.
