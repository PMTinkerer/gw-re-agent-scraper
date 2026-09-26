# Active discovery pagination investigation — 2026-09-26

Status: diagnosis and proposed recovery only. No runtime code, paid scrape,
activation flag, database, usage ledger or email changes in this investigation.

Subsequent checkpoint: Lucas approved the bounded-retry design and offline
implementation. See `superpowers/specs/2026-09-26-bounded-discovery-retry-design.md`
and `bounded-discovery-retry-verification-2026-09-26.md`. The investigation below
records the pre-implementation findings; it is not a live-acceptance claim.

## Evidence

The two retained Firecrawl captures each contain 118 parsed Biddeford cards.
The first contains 118 distinct listing URLs. The second contains 117:
350 Main Street appears on pages 4 and 5, while 365 Main Street is absent
relative to the first capture. The parser is not dropping that missing card.
This establishes inconsistent source pagination; it does not establish whether
concurrent source changes or unstable sort ties caused the inconsistency.

Local retained responses:

- `.firecrawl/canary-2026-09-26/biddeford-page-{1..5}.json`
- `.firecrawl/canary-2026-09-26-extra/biddeford-page-{1..5}.json`

Free, read-only browser inspection of the public Maine Listings search confirmed
118 results, 24 cards per page and five pages. The visible sort menu offers Date
and Price. Its own Next action requested page 2 from
`https://api-v3.displet.com/properties`, with `per_page=24`,
`sort_by=on_market_date` and `sort_order=desc`; the response contained total 118
and 24 results. No supported single-snapshot, unique-key sort, larger page size
or cursor contract has been verified. This observed endpoint is not an approved
replacement transport, and no credentials were copied from the browser.

The existing transport already requests fresh content (`maxAge=0`). A cache
setting change is therefore not an evidence-backed correction.

## Options for review

1. **Recommended: one bounded, whole-town discovery retry.** Discard the entire
   inconsistent town attempt and restart that town at page 1 once. Accept only
   one independently complete attempt with consistent totals, correct identity,
   town/status validation and unique URLs. Retain previously completed towns
   within the same run, with global duplicate checks intact. Never combine two
   incomplete attempts to manufacture complete coverage.
2. **A supported snapshot or unique-order source.** Potentially stronger than a
   retry, but not verified from the public interface. Adopting the observed API
   directly would need a separately established access and response contract.
3. **Disjoint filtered searches.** Could reduce page overlap but adds partition
   coverage and boundary risks. Not recommended for this focused correction.

## Proposed limits and failure behavior

Retry only clearly identified pagination inconsistency: duplicate URLs within
an otherwise valid town attempt, changing page/result totals, or a final count
mismatch. Malformed identity, out-of-town data, unexpected status, bad metadata,
blocked requests, transport failures and exhausted budgets still stop the run.
Do not treat cross-town duplicate identities as a transient pagination error.

All retry requests use the existing reservation and spending controls. No extra
allowance, provider retry loop, ledger reset or higher page ceiling. Biddeford's
five-page retry would add at most five summary requests if budget remains; it
does not re-enrich known listing detail pages. If no budget remains, or the
second attempt fails, retain the previous database and manifest unchanged.

An absent listing never becomes Sold merely because it was absent. Existing
targeted status verification remains required after complete discovery.

## Implementation acceptance tests after design approval

- Replay inconsistent saved cards, then a complete attempt: accept only the
  complete attempt and preserve exact identities.
- Two inconsistent attempts: stop without publishing or modifying source data.
- Count/page changes and terminal count mismatch: at most one whole-town retry.
- Invalid URL, town/status, malformed metadata, cross-town duplicate, transport
  error or budget exhaustion: no recovery retry.
- No union of incomplete attempts, no repeated detail enrichment, no altered
  budget reservation semantics, no changes to the frozen consumer contract.
- Independent review and full regression before any release; a further paid
  canary remains separately gated. Daily activation and email remain off.

## Verification performed in this investigation

`python -m pytest tests/test_refresh_transport.py -q`: **27 passed**. This includes
the captured complete-discovery and second-scan duplicate rejection regressions.
It verifies existing safeguards, not the proposed retry, which is not implemented.

The design/brainstorming workflow requires user approval of the recovery design
before implementation. This note is an investigation record, not an approved spec.
