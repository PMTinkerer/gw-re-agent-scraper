# Bounded discovery retry — approved design

Lucas approved this recovery design and offline implementation on September 26,
2026. Source investigation is in `docs/pagination-investigation-2026-09-26.md`.

## Required behavior

Retry an inconsistent town's summary discovery exactly once, starting at page 1
with no retained cards from its failed attempt. Keep earlier complete towns but
do not publish any refresh until every requested town passes existing coverage
and identity checks. Accept a complete attempt, never a union of partial scans.

Only within-town duplicate URLs among otherwise valid cards, changed page/result
totals, or a terminal result-count mismatch qualify. Validate the entire returned
page before treating its inconsistency as recoverable. Invalid URLs, malformed
metadata, missing pages, wrong town/status, cross-town duplicates, transport
errors, source blocking and exhausted budgets stop the run. A second inconsistent
attempt also stops. No new URL normalization or MLS identity scheme.

The same existing callback executes all original and retry requests, retaining
pre-request reservations and current whole-run, daily and rolling limits. No
extra allowance, hidden HTTP retries, ledger resets or budget change. Only the
affected town repeats; known detail records remain reused. New details are
processed only after discovery succeeds. Absence still requires targeted status
verification and is never proof of Sold.

## Verification and scope

Tests must prove exact request sequences, isolation between attempts/towns,
captured duplicate-then-complete recovery, unchanged publication bytes on
persistent failure and budget exhaustion, and unchanged new-only enrichment.
Use temporary databases and saved public fixtures only. Follow TDD, independent
spec/quality review, and full offline regression.

This approval does not authorize another paid test, pushing/deploying the fix,
enabling recurring refresh, importing live results, or sending email. Historical
quotes/PDFs, the frozen consumer helper and existing weekly workflow are outside
this change. Preserve the work locally for the next controlled release step.

Self-review: scope is one recovery behavior; no unresolved design choices,
new dependencies, persistence schemas or external integrations.
