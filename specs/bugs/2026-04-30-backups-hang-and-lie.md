# Bug: Create Backup hangs forever AND shipped backups lie about being verified

- **Reported**: 2026-04-30 (user report — clicked Create Backup, spinner never resolved; separately, old 0.0 KB files in the list showed a green "verified" badge)
- **Versions affected**: 0.4.0–0.4.9 (entire lifetime of the backup feature)
- **Version that fixes**: 0.4.10
- **Severity**: critical (both blocked the feature, one was an integrity lie)

## Symptom

Two distinct bugs that hit users at the same time:

1. **Hang.** Clicking "Create Backup" in the Admin UI spun forever. The HTTP request never returned. The Granian worker stopped serving other requests entirely while the dump ran.
2. **Empty backups shown as verified.** Older 0.0 KB or near-empty backup files in `~/.tusk/backups` showed up in the backup list with a green "verified" chip.

## Root cause

### #1 — Hang

`create_backup` (and `restore_backup`, `create_database`, `create_database_from_backup`) ran `subprocess.Popen.communicate` / `subprocess.run` on `pg_dump`/`pg_restore`. These are blocking calls that can take minutes on a real DB.

The four admin route handlers (`/backup`, `/restore`, `/databases`, `/databases/from-backup`) called these sync functions **directly from `async def` route bodies**. asyncio runs sync code on the event loop unless you explicitly offload. The same Granian worker that was supposed to respond to the browser request was the one stuck in `pg_dump`, so the request hung even after the dump finished — there was no event loop available to write the response back.

### #2 — Empty success → "verified"

`create_backup` piped `pg_dump | gzip > backup.sql.gz`. It only checked `dump_proc.returncode` for the plain-format pipe; it never:

- Checked `gzip_proc.returncode`.
- Checked the resulting file size.

Failure mode: `pg_dump` returns 0 with empty stdout (silent client/server version mismatch is the most common cause). `gzip` happily writes ~23 bytes of gzip preamble. The success path runs to completion, metadata is written, the file shows up in the list. The template's `verified` chip was hardcoded — it didn't actually verify anything; it just rendered green if a sidecar metadata file existed.

## Fix

### #1 — Hang
Wrap all four call sites in `asyncio.to_thread(...)`. The route handler now `await`s a thread-pooled execution of the sync function, so the worker stays free.

### #2 — Lying chip
Three changes in `admin/backup.py` + the backup-list template:
- Check `gzip_proc.returncode` after both processes finish.
- After both procs finish, **fail and unlink** when the resulting file is <100 bytes. Even an empty DB dumps several hundred bytes of `SET`/preamble.
- Template now renders three states:
  - **`empty`** (red) for 0-byte files,
  - **`verified`** (green) only when sidecar metadata is present **and** the file passes a header check,
  - **`unverified`** (neutral) when the file exists but no metadata is on disk.

## Lessons

1. **Never call blocking I/O from an `async def` handler.** The whole point of an async server is the event loop. Wrap subprocess / fs / blocking SDK calls in `asyncio.to_thread` or use the native async variant. **Add a lint rule** that flags `subprocess.run`/`Popen.communicate` inside `async def` bodies.

2. **A UI "verified" affordance must be backed by a real verification call**, not by the existence of metadata. If the verification step is expensive, make it lazy + cache, but never render a green badge based on a proxy that can lie.

3. **File-size guards on subprocess output are cheap** and would have caught this immediately. Whenever we shell out and write the output to disk, add a sanity check on the size before declaring success.

4. **One pg_dump test path is not enough**: this slipped through because tests covered the happy path (a populated DB). Add a regression test that simulates "pg_dump exits 0 but emits nothing" and asserts we fail loudly.

## Tests added

Light — most of the backup test path is mocked, and the empty-dump scenario needs a real `pg_dump` in CI to reproduce faithfully. **Outstanding action**: add a regression test once the CI workflow (P1 in tech-debt) is up with a Postgres service container.
