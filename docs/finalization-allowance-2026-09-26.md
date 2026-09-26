# Approved temporary finalization allowance

Lucas explicitly requested removing the test allowance or raising it to 5,000
while finalizing. Use the bounded option: manual finalization runs may reserve
up to 5,000 conservative units per UTC day, including earlier reservations.
The 10,000-unit trailing-30-day ceiling, five units per request, matching-account
billing proof and approved USD 5 monthly overage cap remain unchanged.

Normal scheduled/default runs retain their 500-unit limit. Expanded capacity
requires the manual workflow's explicit `finalization` input; it cannot combine
with a one-time exception. Remove/stop using this temporary input when finalizing
is complete. It never enables daily operation or email transport.

The next manual attempt can reserve 4,000 units because today's two previous
500-unit reservations are retained. These are conservative reserved units, not
4,000 actual billed credits. A failed run retains its full reservation.
The redundant 8:05 PM one-shot task was paused before any immediate dispatch.

Verification plan: test preservation, daily/rolling bounds, normal-limit fallback,
manual-only gate, and request/run enforcement; independent review before publishing.
Then dispatch the existing no-email lane once, restore its canary flag false,
and verify complete coverage and source checksum before any downstream import.

## Offline verification

- Five new regression tests failed before implementation, then passed.
- Focused incremental suite: 187 passed; fresh final allowance/CLI subset: 25 passed.
- Independent review: no critical, important or minor findings. An additional
  isolated CLI check allowed 800 five-unit requests under a returned 4,000-unit
  run reservation and rejected request 801.
- Frozen helper, legacy workflow, source DB and both existing ledgers retain
  their pre-change SHA-256 hashes. No historical reservations were rewritten.
- Fresh GitHub check: main at eacfc78, canary false, daily variable absent, no
  conflicting active run. Scheduled duplicate task readback: PAUSED.
- Full regression: 526 passed and two subtests passed in 309.95 seconds;
  1,628 existing deprecation warnings. No failures.

## Immediate controlled attempt

Lucas's latest approval authorizes this controlled finalization test now.
An immediate manual dispatch is planned after publication with finalization=true,
no one_time_approval. This checkpoint must be reconciled with GitHub before any
retry; uncertain dispatch responses are never grounds to dispatch twice.
Run ID and terminal result will be appended below. Do not import partial data.

Dispatched once: **36261585861**, created 2026-09-26T18:10:11Z on
`412da058a6343eccfb9f9ab58a4959f2d3fda878`. Job started at 18:10:15Z.
Manual canary gate restored false after job-start verification; daily flag
remains absent. Observe this run only; do not duplicate it.

## Terminal result and next investigation

Run 36261585861 failed at 18:12:25Z: `Duplicate discovery card within town`
after the bounded recovery attempt. The higher allowance was applied correctly:
remote ledger appended 4,000 finalization units while preserving both earlier
500-unit records. No complete manifest was published and no consumer import ran.
Source DB SHA-256 remains
`376cc6e38f4dcbe7fa84f5bc08977065cc9b53c052deb0e8ff3bfed05e46a66d`.
Remote evidence was fast-forwarded locally through a141e36. The full reservation
is retained under the existing failure policy; this is not actual billed usage.
The request ledger confirms 10 summary requests / 50 conservative request units
for this run. Today's whole-run total is now 5,000; request-level total is 100.

Free browser investigation through the site's own visible Sort: Price menu used
`sort_by=list_price&sort_order=desc`, then its NEXT links through all five pages.
Observed 118 results and page counts 24/24/24/24/22, with 118 unique listing URLs
and zero cross-page duplicates. Multiple image/text anchors per card were counted
once for this browser diagnostic only; no production deduplication was changed.
This is evidence for investigating price ordering instead of unstable date
ordering, NOT a verified all-town transport fix. No extra paid attempt occurred.
Current code still uses date ordering and the complete-coverage guard remains.

No scraping key was exposed. A separate optional balance read did not execute
because its local dotenv dependency was unavailable; no new provider balance is
claimed from that attempt. Billing settings, normal limits and all email gates
remain unchanged. The project is not yet live-operation complete.
