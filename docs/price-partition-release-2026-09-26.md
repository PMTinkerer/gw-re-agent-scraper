# Approved price-partition release and single test

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
- [ ] Publish reviewed fix plus exact approval; verify GitHub main.
- [ ] Enable manual canary, dispatch exactly once, restore false after job starts.
- [ ] Inspect terminal outcome and preserve diagnostics. Do not retry on failure.
- [ ] Verify complete manifest/database only if successful; assess consumer import
  and sustainable daily request capacity separately. No email activation.

Provider-run evidence will be appended here after dispatch.

Preflight: 705 tests and two subtests passed (three unchanged slow Redfin tests
omitted after passing in the previous full run). Independent release review
repeated all 38 allowance tests, with no blocking findings. The new approval
was test-first: reservation failed under the previous 15,000-only implementation.
All prior approval/reservation objects are identical. Source DB, usage/retry
records, frozen helper, legacy source, workflows and billing proof unchanged.
Private read-only app baseline: `../production-launch/tmp/handoff-audit-partition-release-before.json`.
Real sandbox outbox/attempts/ledger remain 0/0/0; both apps have outreach false.
