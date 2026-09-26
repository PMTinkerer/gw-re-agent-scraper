# Sparse discovery release / controlled test

## Terminal result: source pagination rejected

Run [36267949570](https://github.com/PMTinkerer/gw-re-agent-scraper/actions/runs/36267949570)
failed at `2026-09-26T20:08:09Z` with
`_PaginationInconsistent: Duplicate discovery card within town`.
The missing-address correction passed the former Old Orchard Beach blocker.
Eight towns completed: Biddeford (118), Kennebunk (109), Kennebunkport (54),
Kittery (50), Ogunquit (58), Old Orchard Beach (113), Saco (70, after the bounded
retry), and Scarborough (78). Wells failed both attempts; York was not reached.

There were 44 summary requests, representing 220 request-reserved units, NOT a
measurement of provider credits or dollar spend. No detail/status requests ran.
The single-use approval is consumed. Whole-run reservations total 15,000; normal
rolling capacity is exhausted. Do not dispatch another run or imply a daily reset
restores rolling capacity. No emails, recurring activation, app import or restart.

### Reproducible evidence, without another paid request

All 44 response files were retained, none truncated. GitHub artifact
`active-refresh-diagnostics-36267949570-1` (ID 10915045703) has 30-day retention;
access inherits the PUBLIC repository, not private storage. The retained local
copy is `.firecrawl/sparse-run-36267949570/`, with private file permissions.
These contain public source markdown and allowlisted metadata, not app records
or provider envelopes. Offline replay through the actual parser/discovery path
reproduces the exception; all individual pages parse successfully.

Observed repeated URL identities at page boundaries:

- Saco attempt 1, pages 1/2: 60 North Street, $655,000, listing URL ID 748455930.
- Wells attempt 1, pages 8/9: 150 Chapel Road #208, $49,900, ID 747759907.
  Two hundred cards yield only 199 unique URLs.
- Wells retry, pages 1/2: 0 Eastern Avenue, $1,200,000, ID 747546624.

Equal-price boundary ordering is a plausible explanation, not a proven attribution
to Firecrawl or the source website. Price sorting is insufficient to guarantee
complete discovery. Do NOT silently deduplicate and treat missing coverage as
complete, infer sales, or buy another identical retry. After repeated pagination
fixes, the systematic-debugging checkpoint calls for architectural review. A
candidate is nonoverlapping price-range searches sized to fit a single page;
filter semantics and full coverage must be verified before implementation/use.

### Preservation checks

GitHub ledger checkpoint `fe7614a40f36d4c410dfa4e1052c5be52db8a99b` was fetched.
Only the expected allowance/usage records changed upstream. Source database,
frozen helper, legacy extractor and weekly workflow hashes remain unchanged.
Manual canary is false and daily-enable absent. Before/after private app audit
files match byte for byte for sandbox and controlled environments, including
saved figures, PDFs and send evidence. Sandbox outbox/attempts/ledger remain
0/0/0. Backup: `history-78p7g7br`. Thirty consumer persistence/feed/backup checks
passed. Complete all-town source acceptance remains outstanding.

## Historical dispatch checkpoint

Release `af20ca79a48974709d722d93d2e65b766fa1c14b` was published and independently
read back from GitHub main. Preflight: 569 tests + two subtests pass; three
unchanged slow Redfin tests passed in the preceding complete suite and were
excluded from the fresh fast run. Scoped Black/Ruff/diff checks pass.
Independent release review reproduced 31 allowance tests and exact-ledger probes
with no blockers. No original allowance/reservation objects were changed.

User's Go authorized publishing the correction and one bounded controlled test.
Exact approval `2026-09-26-sparse-discovery-test`: max 5,000 reserved units /
1,000 requests; total daily/rolling ceilings 15,000 for this approval only.
Normal constants and USD 5/month provider cap are unchanged. No email authority.

Run **36267949570**, dispatched once at `2026-09-26T19:59:42Z` against the above
release. Job **108476142181** started `19:59:45Z`. Manual canary restored false
at `19:59:58Z`; daily-enable variable remains absent. Do not duplicate/retry this
dispatch. Terminal result is recorded above; this paragraph is the dispatch record.

Before-run private app audit:
`../production-launch/tmp/handoff-audit-sparse-release-before.json`.
Live sandbox: 4 saved versions, 2 archived PDFs, 288 defaults, 669 listings,
325 pursuits, 367 agents; outbox/attempts/ledger 0/0/0, no-email marker 1,
outreach false. Controlled test evidence unchanged at 3/2/2 and outreach false.
No app import, restart or state change has been performed.
