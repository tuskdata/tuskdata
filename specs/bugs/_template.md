# Bug: <one-line title>

- **Reported**: YYYY-MM-DD (and how it surfaced — user, test, monitor, etc.)
- **Versions affected**: e.g. 0.4.0–0.4.12
- **Version that fixes**: e.g. 0.4.13
- **Severity**: critical | high | medium | low

## Symptom

What the user (or developer) saw. HTTP code + body if API. Log lines if backend. UI behavior if frontend. Be specific — "doesn't work" is not enough.

## Root cause

The line of code that's wrong and the reason it's wrong. If the failure mode required a particular environment or sequence of events, document that. If the bug was silent in some configurations (e.g. browser-only flows), say so and why.

## Fix

What changed and why this approach over alternatives. Link to the commit / PR.

## Lessons

What pattern do we now avoid? What guard / lint / test would have caught this? This is the section that has the most long-term value — be specific and write it as a directive.

## Tests added

List of tests that lock the fix in place. If we knowingly shipped without a regression test, say why.
