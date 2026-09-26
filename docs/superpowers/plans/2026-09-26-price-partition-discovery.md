# One-page Price Discovery Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for the isolated collector task and independent reviews. Use test-driven-development throughout. Work only in the existing scraper-incremental worktree.

**Goal:** Replace unstable Active pagination with bounded, verified single-page price partitions.

**Architecture:** A pure collector validates range coverage; the existing transport owns request reservation and source parsing. The completed manifest atomically stores reusable boundaries, not cached listing facts. The existing refresh publication/enrichment path remains authoritative.

**Tech Stack:** Existing Python, pytest, SQLite and requests. No dependencies or deployment changes.

## Task 1: Pure collector

Files: create `src/price_discovery.py`, `tests/test_price_discovery.py`.

- [x] Test-first API (import inside tests so absent API produces a failing assertion):

```python
RangePage(town, minimum, maximum, page, total_pages, total_results, listings)
result = discover_price_ranges(towns, fetch_range, hints=None, max_requests=90)
# result.listings: URL -> validated source card plus discovery_town
# result.hints: {"schema_version": 1, "towns": {town: [{"minimum": None, "maximum": ...}, ...]}}
# result.requests: town -> actual callback invocation count
# fetch_range(town, minimum, maximum) returns actual first-page RangePage
```

- [x] Run `../production-launch/.venv/bin/python -m pytest tests/test_price_discovery.py -q`; observe absent collector failure.
- [x] Implement strict range/card/page validation and deterministic splitting, parent/child counts, parent probe consistency, final fresh count, global identity uniqueness, exact-price overflow and pre-callback request ceiling. Reuse `_town`, `_valid_url`, `RefreshIncomplete` from active_refresh; integration imports the collector lazily to avoid cycles.
- [x] Test zero/1/24/25, captured200 Wells identities and prices, equal-price permutations, missing address, malformed cards/metadata, all-equal overflow, drift, cross-town duplicates,90-call ceiling and transport/budget exceptions. Test valid fresh hint reuse, corrupt/gapped/overlapping hints discarded, expanded leaves and open tails retained.
- [x] Run focused suite; record cold/warm request counts. Spec review then quality review. Commit only owned files after reviews.

## Task 2: Budgeted transport and atomic integration

Files: modify `src/refresh_transport.py`, `src/active_refresh.py`, `src/incremental_main.py`; tests in existing transport/main/refresh files or focused `tests/test_price_refresh.py`.

- [x] Add failing transport tests for `transport.summary_range(town, minimum, maximum)`: URL has observed inclusive filter names, page1 only; reserved once before I/O and diagnostics retained. Invalid bounds fail before I/O. Missing pagination allowed only for fully parsed0..24 unique results with no contradictory page/navigation evidence; strict typed counts and source response validation retained.
- [x] Run these tests red; implement `summary_range` using existing `_scrape`, parsers and RangePage. Do not change frozen helper/legacy transport.
- [x] Add failing integration tests selecting exactly one mechanism:

```python
run_active_refresh(..., fetch_summary_range=transport.summary_range,
                   fetch_detail=transport.detail, fetch_status=transport.status)
# fetch_summary remains supported for existing callers but both/neither reject.
```

- [x] Load optional previous hints only from a complete checksum-matching manifest; invalid/missing hints never establish coverage. Collector result feeds existing enrichment, and `price_range_hints` plus `discovery_requests` are additive fields in the new manifest before `_publish`. Failure leaves original DB and manifest unchanged.
- [x] Switch incremental CLI explicitly to range callback. Update isolated fake transport tests to exercise the new selection; no flag, allowance, workflow or send changes.
- [x] Run focused integration tests verifying repeated new-only enrichment, atomic preservation on drift/publish failure, failed hint persistence, count evidence and CLI reservation behavior. Spec review then quality review.

## Task 3: Final verification and handoff

- [x] Run full offline pytest suite, scoped Black/Ruff and `git diff --check`; preserve and report pre-existing warnings accurately.
- [x] Independently review combined change against approved specification; resolve material issues using failing regressions first.
- [x] Verify frozen helper, legacy workflow/extractor, source DB and real allowance/usage files unchanged. No real consumer changes or requests.
- [x] Record exact tests, cold/warm request counts and remaining provider gate in verification docs/AGENTS. Commit scoped local changes only. No push, paid run, schedule, import or email.

## Self-review

Tasks cover the approved specification, including same-price overflow, current source drift, single-page evidence and budget accounting. The transport cannot invent a200-result one-page response. Hints are committed with the existing complete publication and never substitute for fresh requests. Current approval permits implementation/offline verification, not publication or another paid run.
