# Incremental Active price-sort verification — 2026-09-26

## Terminal outcome — incomplete feed, not accepted

Run36264703193 failed at19:07:16 UTC with `ActiveCardParseError: Malformed or
unparsed Active result card`, wrapped as `RefreshIncomplete: Malformed Active
discovery page`. Remote evidence fast-forwarded through
`0a0e466dc44bc2ee6a51b88fcf9fb4cbe7638e15`.

The durable ledger records20 summary requests /100 reserved request units:
Biddeford pages1–5, Kennebunk1–5, Kennebunkport1–3, Kittery1–3, Ogunquit1–3,
then Old Orchard Beach1. Sequential discovery cannot advance towns without
validated complete coverage; no town retried. The original Biddeford pagination
failure did not recur. No detail/status requests or complete publication/import.

Free browser inspection of the exact failing query displayed113 Results, page1
of5, Sort: Price. One card showed $1,300,000, Active, The Boulos Company, and no
address/town, linking to https://mainelistings.com/listings/759917817 . The detail
UI identifies MLS1662844, Property Type Land, a permitted condo/commercial site.
The parser requires two bold address/city fields, so this visible shape is
unsupported. Failed Firecrawl markdown was not archived; this diagnostic is not
an exact provider-payload reproduction.

Next design review: identity-first discovery/coverage separate from complete
home facts and eligibility; explicitly account for unresolved/ineligible cards,
never silently drop them or fabricate addresses. Capture bounded failure evidence
for offline compatibility work. No new parser change or paid retry was inferred.

Protected source DB, frozen helper and weekly workflow hashes still match below.
The ledger appended the approved5,000 reservation with all older rows preserved;
whole-run total10,000 now exhausts the rolling allowance. This is reservation
accounting, not measured provider charges. Canary=false, daily-enable absent,
no delayed run scheduled. Both local environments' private before/after audits
match exactly (`tmp/handoff-audit-price-sort-{before,after}.json` in outreach).
Real sandbox retains4 saved versions,2 PDFs,288 defaults and zero send evidence.
Backup: `.local/live-sandbox/history-backups/history-mqj4pnfu`. No app restart.

Atlas card updated; last_verified already2026-09-26. Full Atlas validation found
seven unrelated existing registration/capability errors and one stale-project
warning; none was changed here.

## Published release and dispatched test

Release `928eb3cd7b0eb0a37b4fb535a8316eadca7c0f50` was pushed and independently
read back from GitHub main. Independent review found no issues and reproduced
204 focused passing tests.

Dispatched once: **36264703193**, 2026-09-26T19:03:20Z, release SHA above,
`finalization=true`, `one_time_approval=2026-09-26-price-sort-test`.
Job 108467038909 started at 19:03:24Z. Manual canary flag restored false at
19:03:46Z; daily-enable variable absent. Observe this exact run, never duplicate
an uncertain dispatch. No email or recurring activation. Terminal result and
complete feed remain unverified at this checkpoint.

## Superseding immediate-test approval and release preflight

Lucas approved publication, then explicitly approved one additional test now:
5,000 reserved units / at most 1,000 requests; today's internal ceiling 10,000;
previous records retained; actual USD 5/month provider cap unchanged; no email
or daily refresh. Dated single-use ID: `2026-09-26-price-sort-test`, used only
with manual `finalization=true`. This supersedes the capacity pause below.

The extra approval was appended without altering existing approval/reservation
objects. Three new positive tests failed before implementation, then passed.
Eight new tests cover allowance/CLI limits and negative paths. Focused suite:
204 passed. Latest broad suite: 540 passed, two subtests, three unchanged slow
Redfin tests deselected; those three passed in this turn's preceding full
535-test/two-subtest run (384.81 seconds). No failures. Scoped Black/Ruff and
diff checks passed. Provider execution has not started at this preflight.

The whole-run rolling ceiling remains 10,000: consuming this extra reservation
also consumes its remaining capacity. A later operational spending-policy change
is a separate decision; test success must not silently raise it or enable cron.

## Scope and release state

Implemented locally in `codex/incremental-active-refresh`. Incremental Active
summary requests now use `sort_by=list_price&sort_order=desc` on every page and
the existing single whole-town retry. The legacy weekly Closed/date-order code,
strict parsing, duplicate/completeness rejection, retry count and budgets are
unchanged. No push, paid provider run, source publication, consumer import,
recurring activation or email was performed in this task.

## Test-first evidence

- Before implementation, the new exact-query and retry-order assertions failed:
  11 failed, 34 passed; all failures were the expected `on_market_date` versus
  `list_price` mismatch.
- After implementation, focused transport/discovery/budget/allowance tests:
  182 passed. The nine new combinations cover three towns (including a multiword
  town) and pages 1, 2 and 5. Both captured-duplicate retry cases assert all ten
  requests use price order and retain one reservation per request.
- Independent read-only code review: no findings; 138 focused tests passed,
  including legacy search-order coverage.
- Full regression: **535 passed, two subtests passed**, 1,628 warnings, in
  384.80 seconds. The three slow legacy Redfin tests accounted for 379.32
  seconds; no test failed.
- Final focused rerun after formatting: 182 passed in 3.30 seconds.
- Black check and Ruff passed for both changed Python files; `git diff --check`
  passed. Existing deprecation warnings were not changed by this fix.

The saved date-sorted response replay proves rejection/retry behavior, not live
price-sorted provider acceptance.

## Fresh free-browser evidence

Observed through the public website UI on 2026-09-26, completed at
18:51:12 UTC. Started at
`https://mainelistings.com/listings?city=Biddeford&mls_status=Active&sort_by=list_price&sort_order=desc&page=1`
and followed the visible Next links through page 5. Waited for each visible
page indicator before extracting rendered listing links; an early page-2 capture
that still displayed page 1 was discarded, not counted.

| Page | Cards | First price | Last price |
| --- | ---: | ---: | ---: |
| 1 | 24 | $3,950,000 | $1,100,000 |
| 2 | 24 | $1,099,000 | $690,000 |
| 3 | 24 | $665,000 | $489,900 |
| 4 | 24 | $489,000 | $384,900 |
| 5 | 22 | $375,000 | $18,500 |

Every page displayed 118 Results. Combined: **118 cards, 118 unique listing
URLs, zero duplicate URLs, zero missing prices, globally non-increasing prices**.
Both formerly problematic listings appeared once:

- 350 Main Street, $950,000, listing URL suffix `752305093`.
- 365 Main Street, $940,000, listing URL suffix `752306590`.

This verifies one current public-browser scan, not an atomic snapshot, price-tie
stability, all towns, or Firecrawl rendering. The scraper still rejects any
future overlap, changed totals or incomplete coverage. Browser records were
not imported into either application's database.

## Protected-file evidence

Before/after SHA-256 checks matched for all six protected files:

| File | SHA-256 |
| --- | --- |
| `src/maine_active.py` | `9dc31528ded1ed61dc036a6a7c74354433524725b703ce0b9f89ba3b69881b4a` |
| `src/maine_firecrawl.py` | `fb1ef9c1ccd100a64b8475f36581073e7d4bb6aa5d91b9d127bdf352acfd2aae` |
| `.github/workflows/maine_listings.yml` | `12e79f4c318ca1ea3f9d294d81d2ce41d914eaec4dc9179d8d694021a6eaa40a` |
| `data/maine_listings.db` | `376cc6e38f4dcbe7fa84f5bc08977065cc9b53c052deb0e8ff3bfed05e46a66d` |
| `data/active_refresh_allowances.json` | `2b03c9344b9b864f49fcb747b7bfaebc99ac52f9d1fc29959d997969d3d3cd4d` |
| `data/active_refresh_usage.db` | `c961349098054c58e323cfe30083f7575ba9710efca7faa527ae975878d2ad13` |

## Remaining acceptance gate

Publish the reviewed correction, then complete one explicitly authorized,
bounded all-town provider run and verify a complete manifest before consuming
its feed. Today's 5,000 whole-run reservation was already consumed by earlier
attempts; do not reset it or treat request-level usage as remaining run capacity.
No additional allowance, delayed retry or activation was created here. Manual
canary and daily scheduling were not changed. Email approval gates are unrelated
to this scrape fix and remain intact.
