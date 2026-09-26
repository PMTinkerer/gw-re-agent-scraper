# Incremental active refresh — controlled acceptance

Publication checkpoint (September 26): following Lucas's approval to proceed,
GitHub main readback confirms recovery fix `5fce0d0`. Fresh focused tests: 170
passed. Canary remains false and daily-enable variable absent. No paid run was
dispatched because the normal and extra September 26 run allowances are already
consumed; both reservations remain intact. Next normal window: September 26,
8:00 PM Eastern (September 27 00:00 UTC). No test has been scheduled, no consumer
import occurred, and neither daily refresh nor email was activated.

Latest local change: Lucas approved bounded whole-town pagination recovery and
offline testing on September 26. An inconsistent town may restart once, within
the existing request budget. This has not been published or exercised through a
new paid canary. No further paid test, import, daily activation or email is
authorized by the implementation approval. See
`bounded-discovery-retry-verification-2026-09-26.md` for final local evidence.

Previous paid acceptance attempt: Lucas explicitly approved one extra capped September26 test. Dated
single-use allowance d88fc5c preserved prior reservations and normal request
limits. Run36257185586 used five additional actual credits and safely rejected
source pages containing118 cards but117 unique URLs (350 Main Street duplicated
on pages4and5). All cards parsed; source pagination itself was inconsistent.
No database/manifest publication or consumer import. Both gates OFF; no further
retry authorized. Full suite457 tests plus two subtests passed. Saved responses
are retained in .firecrawl/canary-2026-09-26-extra. Current balance98,994.

September26 outcome: source code published but canary36256050877 rejected
incomplete card parsing after five summary requests/five actual credits. No
source snapshot published; all allowance/retry evidence retained. Offline repair
now reads all118saved Biddeford cards, with unknown property facts left unknown.
111focused checks and independent review passed. Both activation gates remain
off. Do not reset the whole-run allowance to force another paid attempt today.

This separate lane is approved for publication and one manually gated canary.
Daily activation remains OFF until complete source and consumer acceptance.
It does not import notifications or email transports. The original weekly
closed-transaction workflow is unchanged.

## Behavior

- Completely scan compact Active search cards for all configured towns.
- Reuse existing property/contact/photo details. Full details are requested only
  for a newly discovered exact listing URL, with at most two durable attempts.
- Use exact MLS identity, never fuzzy addresses/names. Preserve historical URL
  aliases; conflicting simultaneous current URLs stop publication.
- Verify missing previously active homes with a status-only extraction. Explicit
  inactive status hides them; ambiguous evidence is Unverified, never guessed Sold.
- Within-town duplicate URLs, changed page/result totals or terminal count
  mismatches trigger at most one restart of that town from page 1. Discard its
  entire failed attempt; never union incomplete scans. Earlier completed towns
  are retained, and cross-town duplicate identities remain fatal.
- Invalid cards/metadata, missing pages, blocked responses, caps and failed
  requests stop immediately. Validate every card on a fetched page before
  classifying an inconsistency as retryable. A second inconsistent scan stops
  too. These failures leave the published database and coverage unchanged.
  Complete empty sets are valid only with explicit zero-result evidence.
- Publish the standalone database and hash-bound `data/active_refresh.json`
  together. Do not change the frozen `maine_active.py` consumer helper.

## Conservative allowances, not measured charges

Each run first reserves 500 credit units in
`data/active_refresh_allowances.json`. Limits are 500 per UTC day and 10,000 in
the trailing 30 days. The reservation is pushed and remotely verified before
any paid request. Failed runs retain their entire reservation, so a second run
that day stops. There are no automatic refunds or raised limits.

Every basic scrape additionally reserves five units in
`data/active_refresh_usage.db`, with live balance checked before each request.
This conservative accounting is NOT actual billed credit usage. Requests use
basic proxy, plain markdown/raw HTML, no PDF parsing or LLM formats, explicit
timeouts and no HTTP retries. New-detail attempt counts are pushed before the
detail request; failed runner retention therefore does not reset retry counts.
The one town-discovery retry also uses this exact reservation path; it does not
grant extra budget or reset any reservation. If the cap is exhausted during
recovery, the next request is denied and the published feed remains unchanged.

Provider pricing reference checked September 26:
https://docs.firecrawl.dev/billing . Basic scrape is documented as one credit;
the five-unit reservation retains headroom and is never advertised as metering.

## Required activation evidence

1. Verify the exact Firecrawl account's Billing page has Pay-as-you-go OFF or
   a USD monthly limit no greater than five, as explicitly approved by Lucas. A
   balance check is not proof. On September 26 the matched local account had
   99,004 credits and Pay-as-you-go ON with a $5 monthly limit. No billing setting
   was changed. UI describes Standard 100,000 credits/month, billed yearly,
   next credit reset October 23; annual API billing dates are not annual credits.
2. Only after verified readback, create `config/active-refresh-billing.json` with
   `api_key_sha256` (SHA-256 of the selected key, NOT the key),
   `pay_as_you_go_enabled`, UTC `verified_at`, and a nonempty `evidence`
   description. It expires after 30 days. It must match the actual GitHub secret.
   Enabled Pay-as-you-go also requires numeric `monthly_limit_usd` between zero
   and five and `currency: "USD"`. Reject unknown/unlimited/larger caps.
   Do not invent this file from the user's approval alone. September 26 current
   readback and approval are recorded in the checked-in proof; no key is stored.
3. Review/publish the source changes with both enable variables absent/false.
   Do not reset allowance or retry ledgers. Preserve the shared concurrency group.
4. Set only `INCREMENTAL_ACTIVE_CANARY=true` for one manually dispatched acceptance
   run. This does NOT enable cron. Verify provider usage, complete all-town
   manifest and matching database checksum, then disable the canary flag.
5. Verify the actual outreach reader imports this complete source via its trusted
   Git clone; prior saved quote/PDF/contact evidence must remain unchanged.
6. Only after that acceptance, `INCREMENTAL_ACTIVE_ENABLED=true` enables the daily
   10:15 UTC schedule. This is 06:15 Eastern during daylight saving time and
   05:15 during standard time, not a fixed Eastern hour.

The no-email outreach app still needs its own scheduled import; a successful
upstream run alone does not prove that the local screen refreshed. No outreach
approval, sending activation, mailbox worker or operational digest email is
authorized by this lane. Report the real last successful source time.

## Failure recovery

Do not delete/reset reservations or the retry DB to get past a cap. Wait for the
next allowance window. Fix parser/coverage failures using offline captured
fixtures before another bounded attempt. Missing required source facts remain
unverified; do not fabricate them or silently run the historical enrichment queue.

An operator must handle a rejected Git push; no force push or automatic conflict
resolution exists. A crash between local database/manifest replacements causes
checksum rejection, not acceptance of a partial feed. Only a successful final
Git commit publishes that pair for consumers.
# Temporary manual testing ceiling (September 26, 2026)

Lucas approved 5,000 conservative units per UTC day during finalization.
The manual workflow input `finalization=true` reserves remaining daily capacity
after prior reservations, up to the unchanged 10,000-unit rolling ceiling.
Normal/default/scheduled runs retain 500. Request-level and whole-run limits
both enforce the selected policy; exceptions cannot stack. No ledger reset,
provider cap increase, email or recurring activation. Stop using the temporary
input after finalization. See `finalization-allowance-2026-09-26.md`.
