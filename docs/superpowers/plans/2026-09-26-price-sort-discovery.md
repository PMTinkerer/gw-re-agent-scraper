# Price-sort Active discovery implementation plan

> Execute the user-approved price-sort alternative with test-first implementation
> and independent code review. Preserve the existing linked worktree.

**Goal:** Change only incremental Active summary requests to the public site's
verified Price descending ordering, retaining strict complete-coverage checks.

**Design:** Use `sort_by=list_price&sort_order=desc` for every summary page,
including the one bounded whole-town retry. Do not change the legacy weekly
Closed search (`close_date`) or its Active date-order path. Price ties and live
updates can still cause overlap: duplicates must still fail, never be silently
deduplicated or unioned across attempts. No new transport or API access.

**Alternatives:** More date-order retries repeat the observed failure and cost
more; direct API/partitioned search introduces an unverified access or coverage
contract. The selected price ordering is the smallest source-supported change.

**Stack:** Existing Python requests adapter, pytest, free browser verification.

## Work

- [x] Add regression assertions in `tests/test_refresh_transport.py`: generated
  request query contains exact town/status/page and price descending; all pages
  and bounded retries use the same ordering. Run and observe date-sort failure.
- [x] In `src/refresh_transport.py`, replace only summary `sort_by` value
  `on_market_date` with `list_price`; explain that completeness guards remain.
- [x] Run focused transport/discovery/budget/allowance tests and full `tests/`.
  Existing duplicate, changed-total, invalid-card and budget failure tests must
  stay green. Saved date-order fixture replay is not live price-sort proof.
- [x] Independently check current public Price/Next browser pages without paid
  scraping. Record actual totals/unique URLs and any ties or overlap. Do not
  import browser data into either application's operational database.
- [x] Independent review, verify frozen helper/legacy workflow/source DB/usage
  and allowance ledger hashes unchanged, then document exact evidence and limits.

No daily activation, email, ledger reset, automatic paid retry or consumer import.
Today's 5,000 whole-run reservation is already consumed; this implementation and
free validation do not authorize exceeding it. Provider-side all-town acceptance
is a separate bounded run when capacity and explicit run authority are available.

Completed local evidence: `docs/price-sort-verification-2026-09-26.md`.
535 tests plus two subtests pass; fresh free-browser coverage is 118/118 unique
Biddeford listings across five pages. Not yet published or provider-accepted.
