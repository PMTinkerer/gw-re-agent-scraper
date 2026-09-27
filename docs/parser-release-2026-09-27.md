# Parser correction: approved publication and one controlled test

**Subsequent status:** the verified URL-alias repair is now implemented and
offline-verified. See `listing-alias-verification-2026-09-27.md`. The failed run
below remains failed; no replacement paid run or new source publication occurred.

## Terminal outcome — failed identity validation, no complete feed

Run `36325959142` completed **failure** at `2026-09-27T15:08:56Z` after about
42 minutes. Terminal error at `15:08:53Z`:

```text
src/incremental_main.py:123 -> run_active_refresh
src/active_refresh.py:467 -> _retry_detail
src/active_refresh.py:267 -> _identity
src/active_refresh.py:317
src.active_refresh.RefreshIncomplete: MLS identity collision or change
```

This was not the previous count-parser timeout. All ten towns completed price-range
discovery before new-listing detail enrichment began. The 154 saved summary
responses are all HTTP 200, untruncated and SHA-256 checked. Offline replay through
the actual `summary_range` and `discover_price_ranges` code consumed every captured
response, made no network calls, and reproduced complete discovery in 2.926 seconds:

| Town | Unique listing URLs | Summary requests |
| --- | ---: | ---: |
| Kittery | 50 | 8 |
| York | 103 | 16 |
| Ogunquit | 57 | 8 |
| Wells | 199 | 34 |
| Kennebunk | 107 | 18 |
| Kennebunkport | 56 | 8 |
| Biddeford | 119 | 20 |
| Saco | 70 | 10 |
| Old Orchard Beach | 113 | 20 |
| Scarborough | 79 | 12 |
| Total | 953 | 154 |

These are source listing URLs across property types, not 953 outreach-eligible
standalone homes. Discovery success is NOT complete feed acceptance: no new source
database or complete manifest was published, and no consumer import occurred.

The durable request ledger records 154 summaries and 47 new-detail reservations:
201 requests / 1,005 conservative reserved units, NOT measured billed credits.
The retry cache preserves 46 successful detail records and one uncached attempt.
The final request at `15:08:35Z` was:
`https://mainelistings.com/listings/ME/Biddeford/04005/10376/1-willow-ridge-road-biddeford-me-04005/782016957`.
Its summary says 1 Willow Ridge Road, Biddeford, Active, $1,099,000. The unchanged
source database already has MLS 1672938 at a URL with the same terminal numeric
ID but the older slug `1-willow-ridge-ridge-biddeford-me-04005/782016957` and address
1 Willow Ridge Ridge. This is a concrete candidate for a changed-URL identity
collision. The failing detail payload was not retained, so its returned MLS value
and exact rejection predicate cannot be independently replayed from the artifact.
Do not guess, merge identities or bypass the guard. Further investigation is needed.

### Subsequent read-only identity investigation

A free browser check of the exact final-request URL shows MLS **1672938**,
1 Willow Ridge Road, Biddeford, Active, $1,099,000. This matches the stored
MLS and terminal source listing ID **782016957**, while the address slug changed
from `willow-ridge-ridge` to `willow-ridge-road`. A plain HTTP retrieval was
denied with 403; the browser check made no Firecrawl request. This is current
page evidence, not recovery of the missing historical detail response.

The code keys known listings by the entire URL. A corrected slug therefore
enters the new-detail path, where the existing MLS at its old URL triggers the
collision guard. Another closed listing for the same street address has a
different MLS and numeric source ID; address matching alone is unsafe.

Proposed narrow repair: retain the original database row and history, record
a verified URL alias only when the official source URL's stable listing ID,
MLS and town agree, and resolve discovery, absence checks and manifest membership
through that alias. Reject mismatches and duplicate current identities; never
fuzzy-merge addresses. Reuse previously verified aliases without repeat full
enrichment. Test correction, reuse, genuine identity conflicts, completeness
and history preservation offline before any separate publication or paid test.
No implementation or additional paid request has occurred in this investigation.

Existing artifact `10934886465` (`active-refresh-diagnostics-36325959142-1`),
777,707 ZIP bytes, expires `2026-10-27T15:08:53Z`. GitHub-reported ZIP digest:
`sha256:f7ada2c17d4d0014278e5a0d51c7cf15a739716b304610669232d3e26853346c`.
Downloaded evidence is in `.firecrawl/parser-run-36325959142/summaries/` and
read-only inspected checkpoint copies under its `checkpoint/` directory. Artifact
access inherits the PUBLIC scraper repository; it is not a private artifact.

Remote main ends at `d30e3b9` after durable checkpoints for this exact run. Compared
with release `d118a51`, only allowance, request-usage and retry ledgers changed.
Source DB/manifest, billing config, workflows and frozen helper are unchanged.
All prior records remain; the new 5,000-unit whole-run reservation is consumed.
The normal rolling allowance remains exhausted; no reset or additional run.

Post-test private audit `../production-launch/tmp/handoff-audit-parser-release-after.json`
matches the before audit byte-for-byte. Saved projection versions/PDF bytes,
defaults, decisions/contact tables and both sending flags remain unchanged.
Real sandbox outbox/provider attempts/send ledger remain 0/0/0. Manual canary
readback remains `false`; daily-enable absent. No email, import or restart.

Remaining gates: establish and correct the exact identity-resolution issue with
offline regressions, then separately authorize any further paid verification.
Complete feed acceptance and sustainable daily request capacity remain open;
this run's 154 summary requests alone exceed the normal 100-request daily budget.
The follow-up `verify-approved-realtor-feed-test` is now PAUSED after recording
this terminal outcome. No retry was performed.

## Authority and limits

Lucas said **Go** after the verified local fix and the proposed next step of
publication followed by a separately approved controlled live test. This authorizes
one run, not a retry loop, daily activation, downstream import or email sending.

Exact one-time approval: `2026-09-27-parser-performance-test`, UTC September 27.
At most 5,000 new reserved units / 1,000 scrape requests. The explicit 25,000-unit
ceiling retains the existing 20,000 whole-run reservations; these are not dollars
or measured billed credits. Normal caps and the USD 5/month provider cap remain
unchanged. Prior approvals and reservations are preserved verbatim. The previous
timeout's final provider usage remains unverified, not treated as zero.

## Plan and acceptance checklist

- [x] Fetch main and retain the prior run's reservation (`c19c4c4`).
- [x] Record a fresh read-only application audit before publication.
- [x] Test the exact dated extension before implementing it: authorized reservation
  failed under the old code; wrong identity/date/mode/limits remain rejected.
- [x] Run offline checks and independent release review.
- [x] Commit only reviewed source/tests/docs and the additive approval; publish
  without force to main and verify the exact remote hash.
- [x] Enable only the manual canary, dispatch once, and restore it to false as soon
  as the job starts (with cleanup on failure). Keep daily-enable absent.
- [x] Inspect that exact run's terminal outcome and diagnostic artifact. No retry.
- [x] If successful, verify all ten configured towns, complete manifest and source
  database SHA-256; otherwise retain failure evidence without relaxing validation.
- [x] Compare the post-test app audit with the baseline; no app import or restart.

Private baseline: `../production-launch/tmp/handoff-audit-parser-release-before.json`.
Real sandbox: four saved versions, two archived PDFs, zero outbox/provider attempts/
send ledger. Controlled: five saved versions, two archived PDFs, three outbox,
two previous provider attempts and two historical ledger rows. Both outreach flags
are false. No operational data was changed by the baseline audit.

The parser fix's full offline evidence is in
`count-parser-performance-2026-09-27.md`: 723 tests and two subtests passed before
this separately dated authorization. Captured-response replay is parser evidence,
not proof of complete feed acceptance. No progress or completion should be inferred
from elapsed time alone.

## Release verification

Final preflight: 727 tests plus two subtests passed (three unchanged slow mocked
Redfin tests deselected; those passed in the preceding full 723-test run).
Black/Ruff for changed Python files and `git diff --check` pass. Fresh replay of
the ten exact retained responses took 0.1442 seconds. All six historical
reservations and four historical approval objects are unchanged; the new approval
is additive. Protected source DB, manifest, usage/retry ledgers, billing config,
workflows and frozen helper have no differences from fetched main.

Independent release review: no findings; 111 allowance/CLI/price-transport checks
passed independently. The existing CLI refuses request 1,001 before transport,
and expanded allowances cannot run under a scheduled event. Publication is approved.

## Published and running

Release `d118a513c9aba406cf7c8e20ff0bea464e8edaf0` was pushed without force and
independently read back from GitHub main. Exactly one test was dispatched:
[36325959142](https://github.com/PMTinkerer/gw-re-agent-scraper/actions/runs/36325959142),
created `2026-09-27T14:27:07Z`, observed in progress. Manual canary was restored
to `false` at `2026-09-27T14:27:15Z`; daily-enable remains absent. An always-run
cleanup enclosed activation/dispatch/status readback so no enabled gate was left.

Existing follow-up `verify-approved-realtor-feed-test` now monitors this exact run
every five minutes, quietly while unchanged. It forbids dispatch/retry/cancel,
budget or gate changes, emails, imports and restarts. On terminal verification it
records the outcome, notifies Lucas and pauses itself. This is not daily refresh.
Do not commit/push local outcome notes while the running producer checkpoints main.

Readback confirms job `108638651411` is in the incremental-refresh step. Its
single-use reservation `36325959142-1` was committed at
`2026-09-27T14:27:24.366712+00:00`, exactly 5,000 units. This is conservative
reserved capacity, not an actual charge. Terminal failure is recorded above.
