# Verified listing aliases implementation and seven-pass verification

> **For agentic workers:** Use superpowers:subagent-driven-development. User approved completing the local repair and at least seven verification passes without routine approval pauses. Review checkpoints are handled internally, not sent back as repeated permission requests.

**Goal:** Accept independently verified address-slug corrections without duplicating a listing or losing history.

**Architecture:** Preserve original transaction URLs/row IDs. Store verified aliases in an additive table in the staged source database; map observed URLs to original identities for refresh, absence and manifest checks. Only official URLs with identical stable numeric ID and non-slug path prefix, exact MLS, and matching town may qualify. Genuine collisions remain fatal. The frozen downstream helper is unchanged.

**Tech Stack:** Python, SQLite, pytest; existing bounded detail cache and atomic complete-feed publication.

## Scope and authority

The user's request supersedes routine design/plan clarification pauses for this repair. Do not spend paid credits, dispatch a workflow, alter reservations/budgets, publish, import into the app, restart services, enable daily refresh or send email. Preserve unrelated edits. Failed historical detail payload is unavailable: browser-confirmed facts support a regression fixture, not an exact historical payload replay.

## Seven passes

- [x] 1. Reproduce: add a fixture with original `1-willow-ridge-ridge.../782016957`, observed `1-willow-ridge-road.../782016957`, MLS1672938 and Biddeford. Run the new regression against unchanged code and confirm collision failure.
- [x] 2. Repair: implement strict source-key parsing, additive verified aliases, explicit fresh identity proof, reuse, and canonical membership. Run the correction and repeated-refresh tests green.
- [x] 3. Adversarial identities: wrong/missing MLS or town, changed source ID/path prefix, duplicate current aliases, and corrupted alias rows must not bypass identity checks or publish partial data. Run focused negative regressions.
- [x] 4. Lifecycle: original URL reappears, new alias appears, confirmed alias disappears, status changes, retries exhaust, and a later failure rolls back publication. Assert original row IDs, old history, and cached/retry evidence remain intact.
- [x] 5. Saved-evidence replay: replay all 154 captured summary responses with network-free callbacks, inspect stable-ID overlaps against the unchanged source database, and distinguish observed real data from synthetic detail fixtures.
- [x] 6. Independent review: fresh spec review first, quality/safety review second; resolve findings and rerun affected tests without asking the user about ordinary implementation choices.
- [x] 7. Final verification: full offline regression suite, scoped formatting/lint/diff checks, protected-file hashes, read-only application audit comparison, and durable release/handoff report with remaining gates.

## File ownership

- Implementer: `src/active_refresh.py`, optional `src/listing_identity.py`, `tests/test_listing_aliases.py`.
- Controller: this plan, `docs/listing-alias-verification-2026-09-27.md`, project checkpoints, read-only evidence collection.
- Reviewers: read-only findings, no overlapping edits.

## Verification commands

From the scraper worktree, use `../production-launch/.venv/bin/python`.

```sh
../production-launch/.venv/bin/python -m pytest tests/test_listing_aliases.py tests/test_active_refresh.py -q
../production-launch/.venv/bin/python -m pytest -q --disable-warnings
git diff --check
```

Full-suite success is local repair evidence, not provider acceptance. No failure is converted into permission for a new paid run. Record exact final counts and unresolved limitations rather than calling the whole outreach platform live-ready.
