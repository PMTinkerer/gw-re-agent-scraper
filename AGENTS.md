# AGENTS.md — gw-re-agent-scraper

## Superseding bounded release authority — 2026-09-27

Lucas explicitly approved publication and up to FIVE TOTAL controlled live
verification/fix attempts without repeated permission requests. Stop on first
verified success; diagnose/fix/test failures before using another slot. Exact
single-use IDs2026-09-27-alias-verification-1 through-5 permit at most5000reserved
units/1000requests each, preserving all prior25000whole-run reservations.
USD5/month billing cap and normal operating limits unchanged. No sixth attempt,
consumed-ID reuse, daily activation, email, app import or restart. Manual canary
is permitted solely for dispatch and must promptly returnfalse. Read
docs/alias-release-batch-2026-09-27.md for attempt register and completion rules.
Earlier local-only/no-publication notes below are historical for this release.

## Verified listing-alias repair — locally complete 2026-09-27

Seven verification passes completed without routine user approval stops.
Strict source-ID/prefix + MLS/town proof now records address-slug aliases while
preserving original rows, URLs, contacts and history. Verified historical
duplicates retain one deterministic active representative; secondaries stay
Unverified even if the legacy importer reactivates them. Pending/Sold, failed
publication, cached proof and two-attempt limits are covered. Independent spec
and quality reviews pass after a reproduced legacy-reactivation defect was fixed.
Final focused86 and broad750 tests +2subtests pass;3unchanged slow Redfin tests
passed in the earlier full745-test run. Scoped Black/Ruff/diff checks pass.

All154captured summaries replay across10towns in a temporary copy. Original
18214rows/7167history records survive, frozen reader matches manifest. Alias
proof in that rehearsal is simulated, NOT live acceptance. Source DB, budgets,
retry ledgers, frozen helper/workflow hashes and application audits unchanged;
real sandbox outbox/attempts/ledger0/0/0, sending/dailyOFF. No paid scan, push,
import or restart. Preserve remote d30e3b9 durable checkpoints on later release.
Read docs/listing-alias-verification-2026-09-27.md for exact proof and release gates.

## Continuous local repair authority — 2026-09-27

Lucas approved the verified-alias repair and directed at least seven internal
implementation/verification passes without routine clarification stops. Use
focused sub-agents for implementation and independent review, resolve ordinary
technical choices internally, and complete the local evidence/report in one
task. No new paid scan, consumed approval reuse, budget change, publication,
consumer import/restart, daily activation or email follows from this authority.
Plan: docs/superpowers/plans/2026-09-27-verified-listing-aliases.md.

## Identity investigation — 2026-09-27

Free browser inspection of the failed run's final URL confirms current MLS1672938
and source listingID782016957, matching the stored row despite an address-slug
correction from ridge-ridge to ridge-road. The code keys known records by full
URL and rejects the same MLS at a different URL. The historic failed detail
payload remains unavailable; do not claim exact payload replay. Proposed fix is
verified aliases with exact stable-ID/MLS/town agreement, preserving original
rows/history and retaining collision guards. Not implemented yet. No paid scan,
publication, import, restart or gate change is authorized by this investigation.
See docs/parser-release-2026-09-27.md.

## Parser test terminal result — 2026-09-27

Run36325959142 failed15:08:56UTC during new-detail identity validation:
`MLS identity collision or change`. All ten towns completed discovery:154 saved
responses,953 unique listing URLs; exact offline replay succeeds in2.926s.
201 request reservations total1005 conservative units, not measured charges;
46 successful details are cached. Last request was1 Willow Ridge Road,Biddeford,
numeric listingID782016957, while stored MLS1672938 uses an older `ridge-ridge`
URL slug. Failed detail response is not retained: this is a specific candidate,
not a fully reproduced root cause. Do not weaken identity checks or retry blindly.
No source DB/complete manifest published/imported; app before/after audits match
exactly. Sending/daily/manual gatesOFF. The5000-unit approval is consumed; prior
ledgers retained. Follow-up pauses after terminal reporting. Read
docs/parser-release-2026-09-27.md for bounded evidence and remaining gates.

## Approved parser release/test — 2026-09-27

Lucas said Go to publishing the verified parser correction and ONE controlled
live test. Exact single-use approval `2026-09-27-parser-performance-test`: at most
1,000 requests / 5,000 newly reserved units. Its 25,000-unit explicit ceilings
preserve all prior 20,000 reservations; normal caps and USD 5/month billing cap
are unchanged. No automatic retry, email, daily activation, app import/restart.
Read docs/parser-release-2026-09-27.md for publication and run evidence. Older
no-publication statements describe the preceding local-only correction.

## Offline parser correction — 2026-09-27

Lucas approved fixing the timeout cause locally. Result-count extraction now
scans whole tokens once, preserving old non-overlapping matches and malformed
evidence rejection. All 10 captured responses replay in 0.1266 seconds total;
51 focused tests and independent re-review pass. Final full suite: 723 tests plus
2 subtests pass; scoped Black/Ruff/diff checks pass. Verification is recorded in
docs/count-parser-performance-2026-09-27.md.
No push, paid scrape, allowance change/reset, app import/restart or email.
Manual/daily gates remain OFF; consumed approvals stay consumed. This is NOT
all-town acceptance. Publication and a separately authorized bounded live test
remain separate steps. Frozen helper, source DB, workflow and ledgers unchanged.

## Price-partition test timed out — 2026-09-27 UTC

Run36277036335 hit GitHub's90-minute limit and endedcancelled00:10:45UTC. Only10
Biddeford summaries captured; no town/feed completion or detail checkpoint.
Offline replay reproduces a local result-count regex stall at
src/refresh_transport.py:217 within3seconds without any network request. Next:
bounded parsing/performance regressions against the saved responses, NOT another
blind paid retry or a larger timeout. Artifact10919077871 retained locally in
.firecrawl/partition-run-36277036335. Remote mainc19c4c4 contains only the initial
allowance checkpoint; source DB/manifest unchanged. Whole-run20,000 reservations
remain, normal rolling capacity exhausted; final per-request ledger checkpoint
was not reached, so actual provider usage remains unverified. App before/after
audits identical, no sends/import/restart. Manual/daily gatesOFF. See
docs/price-partition-release-2026-09-26.md for terminal evidence and next gates.

## Price-partition release/test approval — 2026-09-26

Lucas explicitly approved publication and ONE controlled all-town test, capped
at 1,000 requests / 5,000 reserved units. Exact single-use dated approval:
`2026-09-26-price-partition-test`. Its daily/rolling ceilings20,000 retain the
prior15,000 reservations; normal constants and USD5/month cap unchanged. Email
and recurring refresh stay OFF. Do not reuse consumed approvals or auto-retry.
See docs/price-partition-release-2026-09-26.md for dispatch and terminal evidence.

## Price-range implementation — locally verified 2026-09-26

Approved design now implemented: incremental CLI selects disjoint price ranges,
page1 only, strict source evidence/probe/count reconciliation, exact-price
overflow rejection and90-request town cap. Fresh validated hints persist with
the existing atomic complete manifest; no paging fallback or retries. Source
DB/ledgers/frozen helper/legacy workflow unchanged. No paid call, new allowance,
push/import/activation/email. Final-tree698 tests plus2 subtests pass;3 unchanged
slow Redfin tests passed in the preceding full675-test run. Independent spec and
quality reviews pass. Offline Wells end-to-end200 identities:34 cold/19 warm
requests. Nine-town modeled warm total87, excluding York/detail/status, so normal
100-request daily capacity is NOT proven adequate. Do not activate automatically.
See docs/price-partition-verification-2026-09-26.md for evidence and remaining
publication/provider-test/daily-capacity gates. Old consumed approvals stay used.

## Price-range investigation — 2026-09-26, not implemented

Free public-browser verification collected200/200 unique Wells listing URLs
using ten disjoint one-page price ranges; unfiltered counts before/after both200.
Each leaf count and every price bound matched. Earlier paid scan had199 unique
URLs; browser recovered missing760702704, a $49,900 peer at the failed boundary.
This is not all-town Firecrawl acceptance. No paid calls, code changes, allowance
edits, publication/import, email or activation in this investigation. Evidence:
docs/evidence/wells-price-bands-2026-09-26.json. Written design awaits review:
docs/superpowers/specs/2026-09-26-price-partition-discovery-design.md. Preserve
all safety gates; do not infer permission for another paid run from this evidence.

## Sparse release terminal result — 2026-09-26

Published af20ca7; controlled run36267949570 failed at20:08:09UTC on duplicate
pagination after eight complete towns. Old Orchard Beach sparse card now passes.
Saco recovered on its one retry; Wells failed both attempts; York not reached.
All44 summary responses retained and offline-replayed;220 request-reserved units
are not measured credits/cost. No source publication/import, app restart, email
or recurring activation. Protected source DB/helper/legacy hashes unchanged;
both consumer audit snapshots match exactly. Manual canary false, daily-enable
absent. Approval consumed; whole-run reservations15,000 exhaust normal rolling
capacity. Do not run another paid retry or weaken completeness. Next is an
architectural review of stable discovery (e.g. verified disjoint one-page price
ranges), not another identical scan. See docs/sparse-release-2026-09-26.md.

## Approved sparse release/test — 2026-09-26

Lucas said Go to publishing the correction and one bounded all-town test.
New exact approval `2026-09-26-sparse-discovery-test`: finalization only,
5,000 units maximum / at most 1,000 requests. Explicit daily/rolling ceilings
15,000 apply only to that dated single-use approval; normal constants and all
prior records remain unchanged. USD 5/month cap unchanged, no email or daily
activation during testing. Broad preflight: 569 tests + two subtests pass,
three unchanged slow Redfin tests omitted after passing in the prior full run.
See `docs/superpowers/plans/2026-09-26-sparse-release.md`. This supersedes the
no-further-test checkpoint below, not the requirement for complete feed proof.

## Sparse-card correction — locally verified 2026-09-26

Lucas approved fixing the importer. Addressless summary cards now count by
validated URL; queried town is provenance, not an invented home fact. Missing
address/city can be recovered by an incremental-only extractor from the visible
detail header. Truly hidden addresses remain unresolved. New unresolved URLs
are explicitly retained in append-only snapshot observations and the additive
manifest `unresolved` list, never inserted into the usable active feed. Existing
identity, completeness, two-attempt detail, atomic publication and budget gates
remain. Summary responses are bounded/redacted before parsing and retained by
an always-run artifact step; artifact access inherits the PUBLIC scraper repo.
Never call these private artifacts or put user data/provider envelopes in them.

Final-tree broad run: 556 tests + two subtests passed, three unchanged slow
Redfin tests deselected. Those three passed in this turn's earlier complete
553-test/two-subtest run (383.72s), before the final detail supplement. Scoped
Black/Ruff and diff checks pass; independent re-review found no blockers.
Protected helper/legacy code, source DB and both spend ledgers hash unchanged.
No paid run, commit, push, import, activation or email. This supersedes the
design-only gate below; all-town provider acceptance is still outstanding.
See `docs/sparse-discovery-verification-2026-09-26.md`.

## Price-sort provider result / coverage design gate — 2026-09-26

Release `928eb3c`; run `36264703193` failed at 19:07 UTC after20 summary requests
(100 request-reserved units, not measured charges). Price ordering completed
Biddeford, Kennebunk, Kennebunkport, Kittery and Ogunquit once each. Old Orchard
Beach page1 failed strict card parsing. Free browser inspection found a $1.3M
card without address/town at https://mainelistings.com/listings/759917817;
detail UI identifies MLS1662844, Property Type Land. Raw failed provider markdown
was not archived; this is a source-visible unsupported shape, not captured replay.
Next: review identity-first discovery/coverage separated from eligible-home facts,
explicit unresolved/ineligible handling and bounded diagnostic capture. Do not
silently drop cards or fabricate addresses; do not keep buying scans for regex fixes.
No source publication/import/email; source DB unchanged. Both launch gates OFF.
Whole-run reservations now total10,000 and exhaust the rolling cap, not just
today's window. Single-use approval consumed; no further paid test or schedule.
Sandbox and controlled audited rows/PDFs unchanged. See
`docs/price-sort-verification-2026-09-26.md` for terminal evidence.

## Immediate single-use price-sort test approved — 2026-09-26

Lucas declined waiting for the UTC reset and explicitly approved one additional
controlled test now: at most 5,000 reserved units / 1,000 requests, today's
internal ceiling 10,000, previous records retained, USD 5/month cap unchanged,
no email or daily refresh. Exact dated ID: `2026-09-26-price-sort-test`.
Use `finalization=true` AND that `one_time_approval` once. Only a matching dated
5,000-unit record can combine with finalization; old 500-unit exceptions cannot.
Each run still caps at 5,000 and the 10,000 rolling ceiling remains. This approval
supersedes the wait-for-capacity instruction below, not the immutable ledger.
Before publication: 204 focused tests passed; latest broad suite 540 passed,
two subtests, three unchanged slow Redfin tests deselected. Those three passed
in this turn's preceding full 535-test/two-subtest run (384.81 seconds).
No provider run has been dispatched at this checkpoint. Read release/run evidence
in `docs/price-sort-verification-2026-09-26.md` before any dispatch or retry.

## Price-sort correction locally verified — 2026-09-26

Lucas approved implementing and verifying the public Price-sort alternative.
Incremental Active summaries now request `list_price` descending on every page
and the one bounded whole-town retry. Weekly Closed/date ordering, frozen feed
helper, strict completeness/duplicate rejection and all budgets are unchanged.
Test-first assertions failed against date ordering before the correction.
Final full suite: 535 tests plus two subtests passed; focused suite: 182 passed;
independent read-only review found no issues. Scoped Black/Ruff and diff checks
pass. A fresh free public-browser scan at 18:51 UTC found all 118 Biddeford
listings exactly once across five pages (24/24/24/24/22), globally descending
prices, including both 350 and 365 Main Street. This is NOT all-town Firecrawl
acceptance or a stable-source guarantee. Protected code/source DB/budget ledger
hashes matched. Changes remain local and uncommitted; no push, paid run, feed
import, activation or email. Next: publication and separately authorized bounded
provider acceptance when capacity exists; today's 5,000 reservation stays used.
See `docs/price-sort-verification-2026-09-26.md`. This supersedes the candidate-
only implementation status below, not its historical paid-run failure evidence.

## Superseding finalization allowance — 2026-09-26

Lucas explicitly approved raising the temporary test ceiling to 5,000 units.
Manual `finalization=true` runs use up to 5,000/UTC day including prior reserved
units, still bounded by 10,000/trailing 30 days. Normal runs remain 500/day.
The old two 500-unit reservations stay immutable, leaving 4,000 today.
Run 36261585861 consumed that reservation and made 10 summary requests (50
request-level reserved units). It failed on duplicate source pagination after
the recovery retry; no complete source or consumer import. Full suite: 526
tests plus two subtests passed. A free browser price-sort scan found 118/118
unique Biddeford listings; this is a candidate investigation, not a shipped fix.
This does not raise the USD 5 provider cap, activate recurring refresh or send
email. The redundant 8:05 PM one-shot task is PAUSED. Stop using the manual
finalization input once testing is complete. See
`docs/finalization-allowance-2026-09-26.md` for verification and run evidence.
Older statements requiring another approval for today's testing are superseded.

## Recovery fix published — 2026-09-26

Lucas approved proceeding after offline verification. Commit 5fce0d0 is now
verified on GitHub main. Fresh focused suite: 170 passed. Manual canary remains
false and daily-enable variable absent. No new paid run was dispatched: today's
normal 500-unit allowance plus the previously consumed extra 500 are retained.
The next normal UTC-day window begins September 26 at 8:00 PM America/New_York.
This is conservative reserved test capacity, not actual spending or the USD 5
billing cap. No ledger reset, import, recurring activation or email. No delayed
test has been scheduled. The local-only checkpoint below describes the earlier
implementation phase; publication is now complete, live acceptance still pending.

## Bounded pagination recovery — local offline implementation 2026-09-26

Lucas approved one whole-town recovery retry and offline testing. Discovery now
discards an inconsistent town attempt and starts that town at page 1 once, while
keeping earlier complete towns. Only valid within-town duplicates, changed
page/result totals and terminal count mismatches qualify. Invalid cards/metadata,
cross-town duplicates, missing pages and transport/budget failures remain fatal.
No union of partial attempts; publication still requires complete discovery.
Existing callbacks reserve every retry request under unchanged limits. No paid
test, push, import, schedule activation or email is authorized by this approval.
Strict incremental parsing rejects malformed raw cards before they can masquerade
as retryable count drift. Final full suite: 521 tests and two subtests pass;
170 focused checks pass. Independent spec and quality re-reviews pass. Existing
deprecation warnings remain. Source DB/allowance/usage hashes unchanged.
See docs/superpowers/specs/2026-09-26-bounded-discovery-retry-design.md and
docs/bounded-discovery-retry-verification-2026-09-26.md for final local evidence.

## Additional approved canary — 2026-09-26

Lucas approved one extra capped test today. Implementation commit d88fc5c added
the dated approval2026-09-26-extra-canary, consumed by36257185586; previous
reservations were preserved. Five additional
actual credits, balance98,994. Source returned118 cards but117 unique URLs:
350 Main Street appears on pages4and5;365 Main Street missing versus first scan.
Saved exports in .firecrawl/canary-2026-09-26-extra reproduce rejection. This is
source pagination inconsistency, not dropped parser cards. No source publication,
consumer import or email. Both gates OFF; no further paid retry authorized.
Full suite457 tests plus two subtests passed; a subsequent captured-duplicate
rejection regression makes119 final focused checks. Independent review passed.
Approved Atlas card/registry update committed155a04e. Older extra-approval-needed notes are
historical; the new approval has been consumed and cannot be reused.

## Controlled canary result — 2026-09-26

Code published; daily activation remains OFF, manual canary flag FALSE.
Run36255865762 stopped before spend (checkout URL lacked optional.git); exact
origin fix published. Run36256050877 consumed five actual Firecrawl credits on
Biddeford summaries then rejected incomplete parsing before detail enrichment or
source publication. Whole500-unit allowance and five request reservations remain.

Saved response replay found legacy required bed/bath/sqft and lowercase `1 bed`
parsing gaps. New incremental_cards parser reads all118real cards, keeps omitted
facts None and leaves the weekly parser/frozen helper intact.111focused tests and
independent review pass. Malformed facts/terminators/towns still fail discovery.
Source database unchanged; no complete manifest or consumer import yet. A further
paid test today needs explicit revised allowance approval; otherwise next UTCday.
Never reset ledgers to force success. No daily schedule or email was activated.

## Superseding billing approval — 2026-09-26

Lucas explicitly approved retaining the existing USD 5/month Pay-as-you-go
limit and running the controlled refresh. No larger cap, plan upgrade, manual
credit purchase or email is authorized. Current matching-account UI readback
still shows USD 5 limit, zero spent and 99,004 credits. Dated matching-key proof
is in config/active-refresh-billing.json. The gate permits disabled Pay-as-you-go
or verified USD caps no greater than five; unknown/higher caps fail closed.
Whole-run/day/rolling limits are unchanged. See docs/approved-five-dollar-canary.md.
Older no-automatic-purchase and awaiting-approval notes below are superseded.

## Incremental Active lane — 2026-09-26 (daily activation OFF)

New `src/incremental_main.py` / `.github/workflows/incremental_active.yml` use
complete summary scans, new-only bounded detail enrichment, targeted status
checks and hash-bound complete manifests for outreach. Existing weekly closed
workflow and `src/maine_active.py` remain unchanged. No email/notification paths.
500 conservative reserved units/day,10,000/trailing30days; remote whole-run
reservation before paid calls, persistent attempts before new-detail calls.
Never reset ledgers to retry. See `docs/incremental-active-refresh.md`.

Matched-account UI on September26 showed99,004 remaining, Standard100k/month
billed yearly (credit resetOct23), Pay-as-you-go ON at approved USD5/month.
This supersedes the old annual5000-credit documentation below. Billing proof
must match the running key. No account setting changes were made. Publication
and a manually gated one-run canary are authorized; daily activation stays OFF.
Never reset usage/retry ledgers or claim a successful complete feed without
the finished run, matching database hash and consumer acceptance evidence.

## Current Status (2026-04-17)
**Phase: Maine MLS Phase 2 enrichment COMPLETE — leaderboard redesign shipped.**

### Pipelines Summary
| Source | Status | Records |
|--------|--------|---------|
| **Maine Listings (MREIS MLS)** | ✅ PRIMARY — weekly cron | **16,024 enriched** closed transactions (2011–2026); 2,253 listing agents + 2,634 buyer agents |
| Redfin (Playwright) | 🗄️ Archived | 2,398 transactions captured. Cron disabled; manual dispatch only. Strictly a subset of Maine MLS. |
| Zillow (Firecrawl directory + profile) | 🗄️ Archived | 740 agents, 683 enriched. Kept for profile richness (bios/photos/reviews). No cron. |

### Interactive Leaderboard (shipped 2026-04-17)
- **Leaderboard tab** at `data/index.html` → `/` on Pages
- 12 columns: `#`, Agent/Brokerage, Office/Agents, 12mo Δ, 12mo Vol, 12mo Sides, 3yr Vol, All-Time Vol, All-Time Sides, L/B, Avg 3yr, Most Recent, Primary Towns
- Agent / Brokerage toggle. Town filter (caps to top 50 when set). Period selector (12mo/3yr/All-time changes default sort). In-table name/office search.
- Biggest Movers banner: top 5 risers + top 5 fallers by 12mo rank vs prior-12mo rank. Auto-hides when < 10 qualifying entities (≥5 sides each).
- Row click + mover-card click both open detail modal with every period split.
- Data via `src/maine_kpis.py` (`query_agent_kpis`, `query_brokerage_kpis`, `compute_rank_movers`).

### Maine Listings Pipeline (Primary source)
- MaineListings.com is the Maine MLS public consumer portal (Maine Association of REALTORS). Every closed transaction shows both listing + buyer agent.
- Phase 1 (search page discovery): Complete. 16,029 listings across all 10 towns.
- Phase 2 (detail page enrichment): Complete. 16,024 enriched (5 Firecrawl 500 errors, 99.97% success). Concurrent with 25 Firecrawl workers, ~3h wall time.
- Weekly incremental (`--discover --recent-only --enrich`): ~50-100 credits/week, fits Hobby tier.
- Alerting: Pushover + Resend fire on circuit-breaker aborts and run summaries.
- DB backup before every mutating run (last 3 timestamped copies).

### Firecrawl Account Limits (verified live 2026-09-21)
Query these rather than trusting notes — both numbers below were previously
documented wrong, which cost a failed backfill run:

```bash
curl -s -H "Authorization: Bearer $FIRECRAWL_API_KEY" https://api.firecrawl.dev/v2/team/queue-status
curl -s -H "Authorization: Bearer $FIRECRAWL_API_KEY" https://api.firecrawl.dev/v2/team/credit-usage
```

- **`maxConcurrency` is 5.** Never pass `--workers` above 5. Higher values fail
  with `Request Timeout: ... timed out while waiting for a concurrency slot`,
  which trips the circuit breaker and aborts the batch. The `--workers 25` in
  older docs reflects a plan we are no longer on.
- **Credits are annual, not monthly:** 5,000 per billing period, currently
  2026-04-07 → 2027-04-07. This was previously documented as "Hobby plan
  (3K/mo)" — off by a factor of 12 in the wrong direction.
- Weekly incremental: ~50-150 credits/week. At ~28 weeks left in the period,
  routine operation is roughly 3,000-4,000 credits, so a large one-time
  backfill needs checking against `remainingCredits` first.

## Key Discoveries (Maine MLS)
1. **mainelistings.com is the official public MREIS portal** — operated by Maine Association of REALTORS. Data flows FROM MREIS TO Zillow/Realtor/Homes, not the reverse.
2. **Both agents visible on every closed transaction** — data no other scraped source provides (Redfin only shows listing agent, Zillow sold-rows paginate at 5).
3. **NUXT data blob has TWO `list_agent` objects** — first is `co_list_agent` (usually null), second has real data. Parser picks the one where `list_agent_email` is a quoted string.
4. **JSON-style escape sequences in string values** — NUXT double-encodes, so `"Better Homes\u002FMasiello"` arrives as literal `\u002F`. Decoded in Python after regex extraction.
5. **Town URL param requires human-readable spelling** — `?city=Old Orchard Beach` works; `?city=old_orchard_beach` silently returns zero results. Canonicalization layer in `maine_main._canonicalize_town`.
6. **Zillow numbers are inflated vs MLS truth** — cross-check on Troy Williams shows Zillow claims 1,680 local sales vs MLS reality of 592 sides over 15 years. Likely includes off-market or self-reported data. MLS is authoritative.
7. **Redfin CSV is capped at ~350 rows per town query** — undercounts every active agent. Strictly a subset of MLS.

## Key Discoveries (earlier sessions — Redfin + Zillow)
1. **Redfin CSV no longer includes agent columns** — confirmed for ALL MLS markets (2026).
2. **Redfin property pages DO show agent/brokerage** — must visit individual URL via Playwright. Two DOM structures: `.agent-card-wrapper` (Redfin-agent) and `.listing-agent-item` (non-Redfin).
3. **Redfin CloudFront blocks rapid sequential requests from the same browser session** — fresh browser context per page + residential proxy (IPRoyal) required.
4. **Zillow's PerimeterX blocks `interact()` and `browser()` modes** — only `Firecrawl.scrape()` bypasses it.
5. **Zillow sold-row pagination unreliable via Firecrawl actions** — React re-renders too fast. Only page 1 (5 most recent) reliably captured.
6. **Office branches must stay separate** — chain branches (RE/MAX, Sotheby's, Coldwell Banker) are competing entities within the same chain. No normalization across branches.

## Open Issues
- **Backfill COMPLETE (2026-09-21).** Repair queue drained to zero over three `backfill-enrichment` runs at `--workers 5`: 1,019 enriched, 431 delisted (`no_data`), 0 failed, ~1,477 credits. `Withdrawn` fell 879 → 455 (424 listings had been mislabelled); Closed rows missing a close date fell 602 → 8. Those last 8 hit `max_attempts=2` — their detail pages cannot be read; they are inert and will not be retried.
- **8 Closed rows still lack a close date** and 455 rows remain `Withdrawn`. Both sets are now evidence-based rather than guesses: each was checked against its detail page. Low priority.
- Monthly capture is restored (see session log). Any residual gap vs the prior year should close on its own as the weekly run pages the Closed search in close-date order.
- 5 Maine listings returned Firecrawl 500 errors during enrichment. Will auto-retry on next weekly run.
- 3 Maine listings have malformed `city` from search-card regex edge cases (e.g., "Kennebunkport, 04046"). Enrichment corrected most; 3 remain. Low-priority cleanup.
- "NON-MREIS AGENT" placeholder + brokerage-as-agent names filtered out at query time via `_AGENT_EXCLUSIONS` in `maine_report.py`. Add new pollutants to that set as they surface.

## Next Steps
1. Review and merge PR #11 to main. GitHub Pages auto-deploys on push.
2. Downgrade Firecrawl to Hobby tier ($99/mo → cheaper) after backfill lands on main. Weekly incremental fits 3K/mo easily.
3. If desired follow-ups: territorial map view, per-agent sparklines in detail modal, CSV export — all punted as out-of-scope per spec.

## Session Log (most recent first)
- **2026-09-21 (session 23):** Merged the session-22 fixes (PR #25) and ran the repair backfill; two further defects surfaced and were fixed to make it possible. **PR #26** — delisted listings still serve a full page whose NUXT blob has no agent data, and `_enrich_one` could not tell that from a malformed response, so ~33 dead listings tripped the 20-failure circuit breaker on every run and the backfill could never drain. Added `detail_response_error()` and a terminal `no_data` status; also logged two previously silent failure branches. **PR #27** — Firecrawl `maxConcurrency` is 5, not the 25 the docs claimed, and a `--workers 20` run died on concurrency-slot timeouts; `enrich_listings` now reads the account's real ceiling and clamps to it, failing open. Also corrected the credit documentation: the plan is **5,000/year** (period ending 2027-04-07), not "Hobby 3K/mo". **Backfill result:** queue drained to 0 — 1,019 enriched, 431 delisted, 0 failed. Monthly capture restored: Jul 2026 went 15 → 226 (vs 212 in Jul 2025), Jun 48 → 214, Aug 18 → 183, Sep 2 → 117. Erin Lamarche — the reported symptom — went from 1 to 4 closed sides in the trailing 12 months, including `108 Bradley Street, Saco` ($687,000), a sale nobody had flagged as missing. 300 tests passing. Credits after: 2,733 remaining.
- **2026-09-19 (session 22):** Fixed silent under-capture of closed transactions. Reported symptom: agent Erin Lamarche's recent sales missing from the tool. Four defects found, all confirmed against live data: (1) **Closed search sorted by list date** — mainelistings.com defaults to `sort_by=on_market_date`, so `--recent-only` paged the oldest-listed end of a 208-page result set; July 2026 captured 15 closings vs 212 in July 2025 (~93% shortfall). `build_search_url` now sends `sort_by=close_date&sort_order=desc` for Closed. (2) **No re-enrichment on status change** — a row enriched while Active stayed `success` forever, so flipping to Closed never filled `close_date`/`buyer_agent` (565 rows). `upsert_listing` now resets enrichment on status change, and `get_unenriched` treats a Closed row with no close date as unenriched. (3) **Withdrawn sweeper guessed** — any Active row unseen 7 days was marked `Withdrawn` without verification, silently converting real sales into withdrawn listings (924 rows) and dropping them from every closed-side report. Replaced with `queue_stale_for_verification()`; `mark_withdrawn_stale()` is now a 30-day fallback that only acts on rows we tried and failed to verify. (4) **`--db` ignored when `--workers 1`** — `_enrich_serial` opened the default DB, so serial runs silently wrote the wrong database. Added `--reverify-withdrawn` one-time repair and a `backfill-enrichment` workflow mode. Active cron moved from daily to weekly. 19 new tests (278 passing). Verified end-to-end: both screenshot properties now resolve correctly — 454 Ocean Avenue (Withdrawn → Closed 2026-07-31, $1,245,000) and 4 Micelan Road (Closed 2026-06-26, $800,000), both with buyer agent Erin Lamarche / Portside Real Estate Group.
- **2026-04-17 (session 21):** Shipped Phase 7 — Active Listings Pipeline. Added `status` column + 6 new attribute columns to `maine_transactions`, new `maine_listing_history` table for change-detected snapshots, daily cron for `mls_status=Active` scraping, withdrawn-sweeper (7-day stale threshold), anomaly detector (zero-delta days → failure alert), `--max-credits` budget cap, and `src/maine_active.py` read helpers (4 functions) for downstream tools. Closed-side queries now filter on `status='Closed'` so actives/pending/withdrawn don't pollute leaderboards. 291 tests passing (232 baseline + 59 new). Branch `feature/maine-active-listings` → PR.
- **2026-04-17 (session 20):** Maine MLS Leaderboard redesign shipped. New `src/maine_kpis.py` module with period queries + rank movers. `src/maine_dashboard.py` and `src/index_page.py` rewritten around KPI rollups. Biggest Movers banner + Agent/Brokerage toggle + period selector + in-table search. 24 new tests (232 total passing). Docs refreshed — Maine is now the primary source across README, AGENTS, PROJECT_PLAN, CLAUDE.md.
- **2026-04-16 (session 19):** Full Phase 2 enrichment. 16,024/16,029 closed MLS transactions enriched (99.97% success, 5 Firecrawl 500 errors). Concurrent refactor to ThreadPoolExecutor + circuit breaker + thread-safe SQLite writes. Pushover + Resend alerting wired. DB backup before mutating runs. Town canonicalization fix (`old_orchard_beach` → `Old Orchard Beach`). Redfin 4x/day cron disabled; Zillow already manual-only. Tabs reordered: Maine MLS default, then Leaderboard, Zillow (archive), Redfin (archive). PR #11 opened.
- **2026-04-15 (session 18):** Built Maine Listings (MREIS MLS) scraper. Phase 1 discovery: 10,587 closed listings across all 10 towns (~600 credits). Phase 2 enrichment deferred pending plan upgrade. Key insight: detail pages have TWO `list_agent` objects in NUXT — parser picks the one with a quoted email.
- **2026-04-10 to 2026-04-15 (sessions 13-17):** Zillow profile enrichment. 683/740 agents. Page-1-only sold rows due to React re-render timing. Tabbed dashboard (`data/index.html`) wrapping Redfin + Zillow. Added date-range chunking for county Redfin queries. Local Sales + Local % columns in master leaderboard.
- **2026-04-07 (sessions 10-12):** Zillow PerimeterX blocking → Firecrawl smoke test → Firecrawl SDK pipeline built (`zillow_firecrawl.py`, `zillow_directory_report.py`). 740 agents, 125 teams in 10 towns (~250 credits). Fixed eval injection in workflow, added pip-audit.
- **2026-04-06 (session 9):** Planned Zillow V1 as parallel dataset. Separate DB/state/artifacts, buyer/seller role-aware reporting, Zillow workflow + smoke diagnostics.
- **2026-03-31 (session 8):** 365-day rolling brokerage leaderboard. Office name normalization (15 variants). Anne Erwin Real Estate added to BROKERAGE_AS_AGENT exclusion.
- **2026-03-30 (session 7):** IPRoyal proxy outage diagnosed + renewed. Evaluated PrimeMLS (not viable — ToS + membership).
- **2026-03-22 to 2026-03-23 (sessions 4-6):** Pushed to GitHub. Residential proxy + resource blocking. HTML dashboard with trend badges. Property type filter (SFH + Condo). Brokerage-as-agent exclusion. GitHub Pages auto-deploy.
- **2026-03-21 (sessions 1-3):** Initial build. Redfin CSV collection (2,371 transactions). Playwright agent enrichment pipeline (two DOM structures, fresh context per page, CDN error detection). 97 tests passing.

## Workspace Atlas

This repo is one of ~30 projects in Lucas Knowles's SCMaine workspace. The
workspace atlas — `~/atlas` locally, https://github.com/PMTinkerer/atlas —
is the source of truth for what exists, where features live, and the
cross-project rules. Consult it before building anything that might already
exist elsewhere.

- Cold start: `~/atlas/ATLAS.md`
- This project's card: `~/atlas/projects/gw-re-agent-scraper.md`
- "Where does X already live": `~/atlas/capabilities.md`
- External-service patterns: `~/atlas/integrations.md`

Rules that matter most in this repo:

- Pipeline scraping: playwright==1.58.0 + playwright-stealth, fresh context per page, resource blocking, 10-30s jittered delays.
- Pin every dependency exactly; SHA-pin GitHub Actions; no emojis anywhere; secrets never in code (~/.env or project .env).

After shipping a change here, update the project's card and its
`last_verified` date in the atlas (see `~/atlas/protocol/UPDATE.md`).
