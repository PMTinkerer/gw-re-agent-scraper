# Price-partition discovery: local verification

## Outcome and boundary

Lucas approved the written design. The incremental Active collector now queries
only page1 of disjoint inclusive price ranges, splitting oversized ranges until
each leaf fits. No offset-pagination fallback or automatic retry. This is a
locally implemented candidate, not a deployed or provider-accepted feed.

No network/provider requests, paid scans, new reservations, budget changes, feed
imports, app restarts, recurring activation or emails occurred during this
implementation. No push or production release. Existing worktree retained.

## Implementation

- `src/price_discovery.py`: pure typed range responses, full-domain partitions,
  parent/child counts, all parent/final probe identities and price/status facts,
  fresh final town count, canonical town checks and global duplicate rejection.
- Positive integer source prices; exact-price overflow stops explicitly rather
  than paging. Unpriced/malformed/out-of-range cards cannot disappear silently.
- Maximum90 actual summary callbacks per town, checked before each call and
  including the final probe. Existing persistent global spend gates still apply.
- `summary_range` uses the observed min/max filter names and reserves/archives
  each response through the existing transport. It validates whole result/page
  tokens and complete first-page card counts; no fabricated200-result page.
- Strict source price-comma grouping is confined to the incremental parser;
  frozen helper and legacy weekly parser/extractor/workflow are unchanged.
- Select exactly one discovery mechanism. Incremental CLI selects price ranges;
  original paginated callback remains for existing callers, with no fallback.
- Successful leaf boundaries and request counts are additive complete-manifest
  fields. Old hints are loaded only from a complete checksum-matching snapshot,
  fully validated, and queried afresh. They never establish current coverage.
- Existing atomic publication, new-only enrichment, status checks and unresolved
  observations remain. Failure/failed manifest write preserves prior DB and
  manifest bytes, including prior hints. No separate hint file can partially
  advance ahead of a failed publication.

## Test-first and independent review evidence

Baseline123 existing refresh/transport/CLI checks passed before changes.
Absent collector assertion failed before implementation;83 pure collector tests
then passed.26 transport and9 integration tests failed on absent APIs before
implementation. CLI test failed on old pagination selection before wiring.

Spec review reproduced malformed pagination and comma-format acceptance. Seven
new regression cases failed as expected before tightening the parsers; they now
pass. Independent spec re-review:144 focused tests plus27 sparse/legacy parser
tests passed, with no remaining material deviations. Independent quality review:
144 focused tests passed, no code findings; offline-only readiness confirmed.

Full regression during implementation:675 passed plus2 subtests in334.99s,
including all three slow legacy Redfin cases. After final added regressions,
final-tree run:698 passed plus2 subtests,3 unchanged slow tests deselected,
9.70s. The deselected tests are TestEnrichAgentsFromRedfin's successful_enrichment,
stops_on_consecutive_errors and stops_on_captcha, all passed in the full run.
Existing datetime deprecation warnings remain. Scoped Black/Ruff/diff checks pass.

The end-to-end offline fixture exercises actual source-card parsing, transport
reservation/diagnostics, collector, temp SQLite enrichment, manifest publication
and a second fresh run. All200 observed Wells IDs/prices survive, including
760702704. Other property/detail fields are explicitly synthetic fixture data,
not claims about the actual homes.34 cold requests and19 warm requests were
reserved and verified offline; this is not Firecrawl acceptance.

## Request-capacity finding

Modeling the saved public-price populations (not new source requests):

| Town | Captured listings | Cold summaries | Fresh hinted summaries |
| --- | ---: | ---: | ---: |
| Biddeford |118|20|12|
| Kennebunk |109|18|11|
| Kennebunkport |54|8|6|
| Kittery |50|8|6|
| Ogunquit |58|8|6|
| Old Orchard Beach |113|20|12|
| Saco |70|10|7|
| Scarborough |78|12|8|
| Wells |200|34|19|
| Nine-town total |850|138|87|

York was not reached in the earlier provider run and is not modeled. New-detail
and missing-listing status requests are excluded. Therefore the normal500-unit
daily allowance (100 conservative five-unit requests) is not proven adequate;
cold discovery already exceeds it for these nine towns. Warm discovery leaves
13 requests for York plus detail/status work and may also exceed it. This is
request capacity, not actual credits or dollars. No limit was silently raised.
Resolve normal daily capacity before launch; don't interpret a successful
finalization allowance run as proof of sustainable daily operation.

## Preservation

`git diff --exit-code HEAD -- data src/maine_active.py src/maine_firecrawl.py
.github/workflows` passed: real source DB, allowance/usage/retry records, frozen
helper, legacy source and workflow files unchanged. No live application files
or databases were touched. All mutation tests use temporary fixture databases.
The GitHub-backed downstream frozen feed boundary remains unchanged.

## Next gates

1. Authorize publication of the reviewed candidate.
2. Resolve bounded test capacity without erasing prior reservations; authorize
   a specific all-town provider test. The last approval is consumed and normal
   rolling capacity remains exhausted. Keep the approved USD5/month cap.
3. Verify actual filtered provider markdown and all-town complete publication.
   Import only after matching manifest/database verification; verify consumer.
4. Prove a viable normal daily request allowance, then separately activate
   recurring refresh. Email sending remains outside this feed release.
