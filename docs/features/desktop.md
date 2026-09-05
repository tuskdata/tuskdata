# Desktop window (preview)

`tusk app` opens Tusk Studio in a native window instead of a browser tab.
It is marked **preview**: it works, but there is no installer, no code
signing and no auto-update yet — you still install Tusk with `pip`/`uv`.
Those arrive with the desktop release.

## Install

```bash
uv pip install "tuskdata[app]"      # adds pywebview (~1 MB, uses the OS WebView)
```

macOS uses WebKit, Windows uses WebView2 (present on Windows 10/11),
Linux needs GTK WebKit (`gir1.2-webkit2-4.1` or your distro's equivalent).

## Use

```bash
tusk app                       # local: starts Studio on a free port, opens the window
tusk app --port 8000           # local, fixed port
tusk app --url http://10.0.0.188:7000   # remote: a window on an existing Tusk
```

In local mode the server is a child process on `127.0.0.1`; closing the
window stops it. In `--url` mode nothing runs locally — it is a window on
your deployment, with the same login and the same session as the browser.

## Why it is cheap

Tusk already runs as a local web server, exactly like pgAdmin. The window
is the only missing piece, so the preview is a thin wrapper: pick a port,
start `tusk studio`, wait for `/api/health`, open the window.

## Known limits

- No dock/taskbar icon or menu bar integration yet.
- On Windows the console window stays open behind the app; the signed
  release will ship a windowless launcher.
- One window; open a second Tusk in the browser if you need it.
- Set `TUSK_APP_VERBOSE=1` to see the server log in the terminal.
