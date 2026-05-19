# Bug: CSRF middleware returns 500 instead of 403

- **Reported**: 2026-05-19 (during BI v0.3.0 e2e test setup)
- **Versions affected**: 0.3.x through 0.4.12 (likely the entire lifetime of the CSRF middleware on Litestar 2.x)
- **Version that fixes**: 0.4.13
- **Severity**: high (silent — browser users unaffected; any programmatic client hits a 500 the moment it POSTs)

## Symptom

Every state-changing request (POST/PUT/DELETE/PATCH) that **lacked** a valid `X-CSRF-Token` header returned:

```
HTTP/1.1 500 Internal Server Error
{"status_code":500,"detail":"Internal Server Error"}
```

The intended behavior was a 403 with `{"error": "CSRF token missing or invalid"}`. Browser users were unaffected because HTMX in `base.html` auto-attaches the token from the `tusk_csrf` cookie on every request. The failure surfaced when:

- Writing Playwright/`urllib`/curl tests that POST without priming the cookie.
- Any future external SDK or integration would have hit this immediately.

## Root cause

`src/tusk/studio/app.py`, `CSRFMiddleware.__call__` constructed a Litestar `Response` object and tried to invoke it as an ASGI callable:

```python
response = Response(
    content={"error": "CSRF token missing or invalid"},
    status_code=403,
)
await response(scope, receive, send)
```

In Litestar 2.x, `Response` is **not** an ASGI callable — that role belongs to `ASGIResponse`/`ASGIApp`. Calling `await response(...)` raises:

```
TypeError: 'Response' object is not callable
```

The exception bubbled up the middleware stack, was caught by Litestar's internal exception middleware, and converted to the generic 500 with no body details in non-debug mode. There was no traceback in the structlog output because the `litestar` logger's exception handler only logs at DEBUG by default in this app.

Why this stayed silent so long: HTMX in `base.html` always sends the header, so dev/manual testing never reached the 403 branch.

## Fix

Emit the 403 directly via the ASGI `send` channel — no `Response` wrapping:

```python
body = b'{"error": "CSRF token missing or invalid"}'
await send({
    "type": "http.response.start",
    "status": 403,
    "headers": [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
    ],
})
await send({"type": "http.response.body", "body": body})
return
```

This is the right pattern for middleware that short-circuits the request — no need to involve Litestar's Response/route machinery for a static error.

## Lessons

1. **Middleware should never build a Response and call it.** Always work in ASGI message dicts, or call through the actual `self.app` chain. Add a lint rule or code-review checklist item.
2. **Litestar's default exception logger is too quiet.** Errors swallowed by the framework with no traceback are operationally invisible. We should set `LITESTAR_DEBUG=1` or configure Litestar's exception handler to always log at ERROR level on 500s — see follow-up note in tech-debt.md.
3. **API endpoints need a non-browser integration test.** This bug would have been caught the day it was written by a single `client.post(...)` without CSRF priming. Add one to `tests/test_frontend_smoke.py` for every new state-changing route.
4. **CSRF design**: with `SameSite=Lax` cookies and a session-cookie-required model, double-submit CSRF on JSON APIs is belt-and-suspenders. Revisit in 0.5.x whether `/api/` paths should be CSRF-exempt entirely; if yes, document the threat model that justifies it.

## Tests added

- `tests/test_bi_v030_e2e.py::_csrf_token` and `_create_dashboard` — primes the cookie before POSTing.
- Follow-up: add `test_state_changing_without_csrf_returns_403_not_500` to `tests/test_frontend_smoke.py` to lock this in.
