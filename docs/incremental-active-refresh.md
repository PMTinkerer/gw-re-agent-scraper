# Incremental active refresh — controlled acceptance

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
- Incomplete pages, blocked responses, caps, changed counts or failed requests
  leave the published database and coverage unchanged. Complete empty sets are
  valid only with explicit zero-result evidence.
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
