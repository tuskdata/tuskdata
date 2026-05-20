# Tusk specs

Internal engineering notes: how we make decisions, why the code is shaped the way it is, what we know is broken, and what we're going to do next. Lives in the repo so it stays close to the code; not part of the user-facing docs site.

## Layout

```
specs/
├── architecture/
│   ├── current-state.md       # Module-by-module snapshot from the audit
│   ├── tech-debt.md           # Ranked list of known debts (P1/P2/P3)
│   ├── test-coverage.md       # Per-module coverage snapshot
│   └── adrs/                  # Architecture Decision Records
│       └── NNNN-<slug>.md
├── features/
│   └── <slug>.md              # Design spec per non-trivial feature
├── bugs/
│   └── YYYY-MM-DD-<slug>.md   # Post-mortem per real bug shipped to users
└── roadmap/
    ├── now.md                 # Currently in-flight
    ├── next.md                # Cola corta (next 1-3 months)
    └── later/<slug>.md        # Deferred, with reasoning per item
```

## Conventions

### Every non-trivial feature gets a spec **before** code
File: `specs/features/<slug>.md`. Skim-able. Includes: what we're building, why, the user-visible contract, the data model, edge cases, what we're explicitly NOT doing in v1, and a "done" definition. The spec is the artifact reviewers read in lieu of a fully fleshed-out PR description.

### Every bug that shipped to users gets a post-mortem
File: `specs/bugs/YYYY-MM-DD-<slug>.md`. Template at the top of `specs/bugs/_template.md`. Sections: symptom, root cause, fix, lessons, tests added. This is what makes us less likely to ship the same shape of bug again.

### Architecture Decision Records
File: `specs/architecture/adrs/NNNN-<slug>.md`. One per "we picked X over Y for these reasons" moment that future-us would regret unwinding without context. Examples: "why Litestar over FastAPI", "why msgspec over pydantic", "why plugins ship as wheels not as git submodules".

### Roadmap doesn't lie
`now.md` is what's actively in flight (use TaskCreate for sub-tasks; this file is the higher-level view). `next.md` is what we've committed to next. `later/` is everything else — every deferred item needs a reason in its file so future-us doesn't accidentally start it without re-evaluating.

## What does NOT go here

- Public-facing user docs → `docs/` (mkdocs-material site, separate)
- Code comments explaining *what* code does → just rename things better
- Anything that duplicates `CHANGELOG.md` — link to the changelog entry instead

## Update discipline

A feature is not done until its `features/<slug>.md` reflects what shipped (not what we planned). A bug is not closed until its `bugs/<date>-<slug>.md` exists.
