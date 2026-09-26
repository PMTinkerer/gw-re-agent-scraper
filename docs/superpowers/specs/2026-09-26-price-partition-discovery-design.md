# Stable Active discovery through one-page price ranges

Status: approved by Lucas; implemented and offline-verified locally on2026-09-26.
See `docs/price-partition-verification-2026-09-26.md`. Not published or provider-accepted.

## Goal and authority

Replace unstable multi-page Active discovery with complete, nonoverlapping price
queries. Lucas's latest Go authorized investigating/verifying this approach.
No paid request, new allowance, release, feed import, recurring activation or
email is authorized by this document. Existing USD5 provider cap and all usage
records remain unchanged. The normal rolling reservation is already exhausted.

## Evidence and root cause

Run36267949570 retained44 provider responses. All individual pages parse, but
Wells repeats URL747759907 at pages8/9 on one attempt and URL747546624 at pages1/2
on the retry. Thus offset-based pagination cannot establish complete coverage.
The duplicate prices have distinct legitimate same-price peers; unstable tie
ordering is consistent with the evidence, but attribution to the source backend
versus Firecrawl rendering is not proven.

On2026-09-26, a free browser check used the source's visible List Price Min/Max
controls, then verified `min_list_price` and `max_list_price` URL filters. Exact
$1,200,000 returned both15 Cherry Tree Trail and0 Eastern Avenue. Minimum
$1,200,001 returned23 listings and excluded both. Bounds are inclusive in these
observations and retain dollar precision.

Ten adjoining ranges collected all200 Wells listings exactly once, on one page
per range. The unfiltered total was200 both before20:22:02Z and after20:23:22Z.
Every returned price was inside its range and each range's visible total matched
its cards. Counts:21,22,22,22,22,22,22,22,22,3. No paid Firecrawl request was made.
Public identity/price evidence is in
`docs/evidence/wells-price-bands-2026-09-26.json`.

Offline comparison with the earlier paid scan found all199 earlier unique IDs
plus760702704 (430 Post Road #87, $49,900), the same-price peer lost at the prior
page boundary. This is verified free-browser coverage, not all-town provider
acceptance or proof that a live market is an atomic snapshot.

## Alternatives

1. **Recommended: adaptive one-page price ranges.** Uses existing public filters
   and existing paid transport, removes page-boundary dependence. More discovery
   requests on a cold start; reusable range hints reduce later runs.
2. Another sort/retry/deduplication patch: smaller edit, but repeated failures
   already demonstrate it cannot reliably prove coverage. Rejected.
3. A stable licensed MLS/API feed: strongest long-term option if available, but
   access, contractual permission and integration are not established. Separate
   project decision, not an assumed dependency for this fix.

## Proposed architecture

### Isolated range collector

Add a small `price_discovery` module for bounded partition traversal and coverage
validation. Keep source transport and budget reservation in `RefreshTransport`.
Represent the actual requested bounds and source page evidence explicitly;
never fabricate a one-page summary containing200 listings.

Start each town with a fresh unbounded first-page query and its result count.
If all results fit, accept only a fully parsed, unique, count-matching single
page. Otherwise split the price domain into two inclusive ranges `<= P` and
`>= P+1`. Within an existing range, inherit its outer bounds. Choose a split
from observed integer prices that strictly reduces the interval. Never cap the
outer price domain at an arbitrary amount or omit unpriced results silently.

Recursively query page1 of each range until every leaf fits. For varied prices,
choose an interior observed price near the middle of the visible distinct
prices. For an all-equal visible group, isolate that exact price from the open
tails with at most two splits. If that exact price still spans multiple pages,
stop with an explicit equal-price overflow error; no unstable paging fallback.

Each response must have a verified successful transport status, explicit result
count, valid source cards and valid page evidence. Missing pagination is allowed
only when count is0..24 AND the number of fully parsed unique cards equals that
count, with no contradictory navigation/page evidence. A missing count, malformed
card, fractional/invalid price, out-of-range price, unexpected town/status,
duplicate URL or count mismatch stops publication.

Children's counts must sum to their parent's count. Leaf URL identities must be
unique across the entire town and all towns. Every identity observed in a parent
probe must occur in the final leaves with consistent price/status/town facts;
probe observations are evidence only, never extra imported cards. Recheck the
unbounded town count at the end. Reject changed counts or conflicting observed
facts; never merge partial attempts to force a complete result.

These checks detect observed drift but cannot promise an atomic snapshot when
unobserved listings change while preserving counts. Existing targeted status
verification remains mandatory; absence alone must never establish a sale.

### Bounded cost and reuse

Every probe, child range and final count check reserves a request before I/O and
retains redacted diagnostics. Enforce a maximum90 summary requests per town,
including the final check, under the existing whole-run/day/rolling reservation
limits. Exhaustion stops cleanly, preserving the prior published feed.

Persist successful leaf boundaries as versioned hints with the completed scan,
never as completeness evidence. On the next run, validate that hints cover the
whole integer-price domain with no gaps/overlaps, then query each leaf afresh.
Split any leaf that has grown too large. The first/last ranges remain open-ended.
Discard invalid/stale-format hints rather than trusting them. Cold discovery may
cost more than a normal daily allowance; do not raise limits automatically.
Measure cold and warm request counts in offline tests before seeking a paid run.

### Existing publication boundary

Wire this only into the incremental Active lane after offline verification.
Keep the frozen `maine_active.py` contract and legacy weekly scraper unchanged.
Do not repurpose the old paginated callback by inventing metadata. Explicitly
select one discovery mechanism, with no automatic fallback between mechanisms.

All towns must complete before the existing atomic source publication begins.
Retain new-only detail enrichment, bounded persistent detail attempts, unresolved
card handling, status checks, immutable histories, consumer manifest checks and
the no-email boundary. Failed discovery cannot alter the source database or
publish a new complete manifest. Diagnostic/budget records may append as today.

## Test-first acceptance

- Captured Wells fixture:200 unique IDs recovered, including760702704; repeated
  equal-price card ordering cannot affect a complete one-page leaf.
- Inclusive split boundaries, zero/one/24/25 results, unbounded tails, empty
  leaves, multiple MLS listings at one address, missing address with valid URL.
- Single-price overflow, all-equal first-page samples, malformed bounds and
  hints, bool/negative/fractional prices, out-of-range and out-of-town cards.
- Parent-child mismatch, changing totals, duplicate leaves/towns, probe identity
  lost/changed, malformed card, contradictory/missing metadata and blocked page.
- Exact request accounting,90-query town limit, exhausted global reservation,
  failed hint persistence, no writes to source/manifest on any failed discovery.
- Successful fresh hint reuse; expanded leaf split without holes, overlaps or
  omission of prices beyond yesterday's observed maximum.
- Broad scraper regression, frozen-file/source/ledger hash checks, independent
  review, then separately approved bounded all-town provider acceptance.

## Non-goals and remaining gates

No outreach UI change, mailbox operation, transport activation, new credential,
budget expansion, automatic retry authorization or schedule change. No claim
that a browser rehearsal is provider acceptance. After local implementation,
publish only with release authority; a further paid test still needs a bounded
approval. Import only a successful complete source, then verify the consumer.

## Self-review

Scope is one discovery replacement. No placeholders; open-ended ranges, equal-
price overflow, changing markets, cold-start cost and existing immutable records
have explicit behavior. The source filter contract is observed in the public
browser; provider-specific filtered markdown remains an acceptance gate.
