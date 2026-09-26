# Bounded Discovery Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Recover once from inconsistent town pagination without weakening completeness or spending guards.

**Architecture:** Extract one town scan from `_discover` in `src/active_refresh.py`. A private `RefreshIncomplete` subclass identifies only recoverable pagination inconsistencies. `_discover` retries that town once with a new empty accumulator, committing its URLs to the global accumulator only after successful validation. Existing callback reservations, staging/publication and detail enrichment remain unchanged.

**Tech Stack:** Existing Python, pytest, SQLite; no new dependency or live service.

**Approval:** Lucas approved the bounded retry and offline testing after the September 26 investigation. No paid canary, deployment, daily activation or sending is authorized by this approval.

**Execution checkpoint:** Implementation, review correction, independent spec
and quality review, focused 170-test suite and final full 521-test/two-subtest
regression are complete. Documentation and protected-file hash checks pass.
Evidence: `docs/bounded-discovery-retry-verification-2026-09-26.md`.

## Task 1: Implement and test one isolated town retry

Files: modify `src/active_refresh.py`; create `tests/test_discovery_retry.py`;
extend `tests/test_active_refresh.py` and `tests/test_refresh_transport.py` as needed.
Review correction: minimally extend `src/incremental_cards.py` and
`src/refresh_transport.py` to reject malformed/unparsed source cards before a
terminal count mismatch can be classified as pagination drift. The frozen legacy
parser remains untouched.

- [x] Add a regression with `SummaryPage` callbacks: attempt 1 returns duplicate URL `a` across two pages; attempt 2 returns `a,b`. Assert `_discover` returns exactly `a,b` and requests pages `[1,2,1,2]`. Run it before implementing and confirm the existing duplicate error.
- [x] Add cases for changing total/page counts, terminal count mismatch, two failed attempts, no merging of stale attempt URLs and retention of earlier successfully scanned towns. Assert exact callback sequences.
- [x] Add no-retry cases for malformed identity, invalid metadata, missing page, out-of-town/status errors, cross-town duplicates, transport and budget errors. Check both direct failure and failure during the recovery attempt.
- [x] Implement private typed pagination error and one-town helper. Validate every card on a returned page before raising a recoverable error, so malformed content cannot be hidden by an earlier duplicate. Cross-town identity conflicts remain fatal. Use a fixed two-attempt loop, never a broad exception retry; second failure propagates.

```python
# Coordinator structure; the helper retains the existing validation checks.
for town in towns:
    for attempt in range(2):
        try:
            town_found = _discover_town(town, callback, max_pages, found)
        except _PaginationInconsistent:
            if attempt == 1:
                raise
        else:
            found.update(town_found)
            break
```

- [x] Replay the real captured inconsistent Biddeford fixture followed by the complete fixture; assert 118 unique results and no stale union. Retain the existing always-inconsistent rejection test.
- [x] Exercise `run_active_refresh` with a temporary SQLite DB and preexisting manifest: persistent inconsistency and budget exhaustion preserve both bytes; successful retry fetches each new detail at most once, never re-enriches known rows, and targeted absence verification remains intact.
- [x] Run focused offline tests with `/Users/lucasknowles/Documents/ChatGPT/Automated-Realtor-Outreach/.worktrees/production-launch/.venv/bin/python -m pytest tests/test_discovery_retry.py tests/test_active_refresh.py tests/test_refresh_transport.py -q`.
- [x] Self-review; independent spec review followed by independent quality review. Fix findings and rerun affected tests. No provider requests.
- [x] Quality-review regression: feed malformed raw card content first and clean
  content if called again. Prove immediate fatal rejection with exactly one
  reserved simulated request, not recovery acceptance. Include malformed baths,
  terminators, URLs/status and repeated photo-link controls. Replay all ten local
  retained full response exports to ensure legitimate cards are not rejected.

## Task 2: Verification and local handoff

- [x] Run full offline suite: same Python executable, `-m pytest tests/ -q`. Record final count and any preexisting warnings.
- [x] Confirm git diff excludes frozen `src/maine_active.py`, weekly workflow, tracked data, billing/allowance controls and consumer files.
- [x] Update scraper `AGENTS.md` and `docs/incremental-active-refresh.md` with local-only behavior and verification, preserving older checkpoints as history.
- [x] Record red/green and final verification in `docs/bounded-discovery-retry-verification-2026-09-26.md`.
- [x] Commit only this task's source, tests and documentation locally. Do not push, trigger paid workflows or activate schedules. Keep the existing worktree for controlled-release review.

## Acceptance boundaries

At most one retry per inconsistent town. Every request still goes through existing
budget accounting. Never reset a ledger, union incomplete scans, infer Sold from
absence or change historical projections/contact evidence. The full refresh must
still complete before publication. Passing offline tests is not a successful live
refresh. No new transport or API credentials.
