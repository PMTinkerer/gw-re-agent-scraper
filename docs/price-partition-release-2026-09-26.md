# Approved price-partition release and single test

## Subsequent local correction — 2026-09-27

The result-count stall described below has now been corrected locally, after
Lucas's explicit fix request. All 10 captured responses replay in 0.1266 seconds
total. Timeout and malformed-count regressions pass, and independent re-review
found no remaining issues. See `count-parser-performance-2026-09-27.md` for final
verification. No new paid run, publication, import, budget change, restart or
email occurred. The failed live test remains failed; complete-feed acceptance
still needs a separately authorized bounded test after publication.

## Terminal outcome: timed out, no complete feed

Run36277036335 completed cancelled at2026-09-27T00:10:45Z. GitHub's check
annotation explicitly states: "The job has exceeded the maximum execution time
of 1h30m0s". This was NOT a completed all-town scan. No restart or retry ran.

Only10 summary responses were captured, all Biddeford, status200 and untruncated.
They span22:40:52UTC through00:03:01UTC with roughly7–11 minutes between records.
No town-completion proof, new-detail checkpoint or complete manifest was published.
The earlier expectation that the run might be busy enriching new homes was not
borne out by terminal evidence: it was still in first-town discovery.

Offline replay of the first exact saved response, with transport replaced by
that local response and a3-second diagnostic watchdog, timed out in `re.findall`
at `src/refresh_transport.py:217`, the result-count extraction. No network call
was involved. This establishes a local parsing-performance defect; do not blame
the long delay solely on Firecrawl or just raise the workflow timeout. The next
step is a bounded count-parser fix and full captured-response performance
regressions, with progress visibility, before considering another paid test.
No source code was changed in this monitoring pass.

The first response contains a222,300-character line without `Results`. Running
the same regex on just the source lines containing `Results` returns two matching
119-count tokens in0.0006seconds. This diagnostic isolates the oversized-content
scan; it is not an implemented parser correction or complete coverage proof.

Artifact10919077871, `active-refresh-diagnostics-36277036335-1`, was downloaded
to `.firecrawl/partition-run-36277036335/` (directory0700/files0600). Artifact ZIP
digest reported by GitHub:4096ede03f7290b0c6dd230a36c2290d94bb392999b66ca9881b22724bbbb5bf.
Artifact access inherits the PUBLIC repository; it contains bounded public-source
markdown, not private application data. Its retention ends2026-10-27T00:10:42Z.

Remote main remainsc19c4c44b316d15e8ced723a25402d6985138f08: only the initial
whole-run allowance checkpoint differs from the release. Source DB/manifest,
frozen helper and legacy workflow are unchanged. The runner was killed before
its final per-request ledger checkpoint;10 retained responses are not proof of
exact request charges or final provider usage. The5,000-unit whole-run reservation
remains consumed and all prior reservations remain; total20,000 is not dollars.
Normal rolling capacity is exhausted even though the UTC date changed.

Private before/after audits match byte-for-byte for sandbox and controlled apps.
Saved quotes/PDFs/contact evidence are unchanged; real sandbox outbox/attempts/
ledger0/0/0, no-email marker1; both apps outreachfalse. Manual canary verifiedfalse,
daily-enable absent. No import, restart, email, budget change or paid retry.
Complete-feed acceptance and sustainable daily capacity remain unresolved.
The test-result heartbeat `verify-approved-realtor-feed-test` is now PAUSED.

Lucas explicitly approved publishing the reviewed fix and one controlled
all-town provider test capped at 1,000 requests (5,000 reserved units).
The exact dated approval is `2026-09-26-price-partition-test`. Its single-use
daily/rolling ceilings are 20,000 only to retain the prior 15,000 reservations
and permit the additional 5,000. These are not dollars or measured credits.
Normal limits, prior records and the approved USD5/month billing cap stay intact.
Email sending and recurring refresh stay disabled. No automatic retry.

## Release checklist

- [x] Test exact dated extension, single use, history preservation and invalid limits.
- [x] Run broad offline regression and verify protected files unchanged.
- [x] Publish reviewed fix plus exact approval; verify GitHub main.
- [x] Enable manual canary, dispatch exactly once, restore false after job starts.
- [x] Inspect terminal outcome and preserve diagnostics. Do not retry on failure.
- [ ] Verify complete manifest/database only if successful; assess consumer import
  and sustainable daily request capacity separately. No email activation.

Published release: `2b8cd7a7f2689fb539489dc88cea53574edd5669`, verified from
GitHub main. Controlled run [36277036335](https://github.com/PMTinkerer/gw-re-agent-scraper/actions/runs/36277036335)
dispatched exactly once; job108501685524 started `2026-09-26T22:40:27Z`.
Activation/dispatch/poll/rollback were combined with an EXIT trap; the manual
gate was reset false when the job entered in_progress. No recurring activation.
Terminal result recorded above. Do not duplicate or retry this run.

GitHub readback confirms manual canary false at22:40:31UTC, daily-enable absent.
Remote reservation `c19c4c44b316d15e8ced723a25402d6985138f08` records exactly
5,000 units for36277036335-1 at22:40:40UTC; retained total20,000 is not actual
provider spend. Three remaining slow legacy tests subsequently passed in297.38s;
all708 current tests are covered by the two runs. Consumer feed/history/backup/
source-trust focused checks:26 passed on isolated fixtures.

Historical monitoring setup: thread follow-up automation
`verify-approved-realtor-feed-test` checks every5 minutes, stays quiet without
actionable changes, verifies terminal evidence, reports outcome and pauses itself.
It explicitly forbids another dispatch, paid scrape, gate/budget change, email,
app import or restart. This is test-result monitoring, NOT daily feed activation.
No local scraper evidence commit/push while the producer is checkpointing main.

Preflight: 705 tests and two subtests passed (three unchanged slow Redfin tests
omitted after passing in the previous full run). Independent release review
repeated all 38 allowance tests, with no blocking findings. The new approval
was test-first: reservation failed under the previous 15,000-only implementation.
All prior approval/reservation objects are identical. Source DB, usage/retry
records, frozen helper, legacy source, workflows and billing proof unchanged.
Private read-only app baseline: `../production-launch/tmp/handoff-audit-partition-release-before.json`.
Real sandbox outbox/attempts/ledger remain 0/0/0; both apps have outreach false.
