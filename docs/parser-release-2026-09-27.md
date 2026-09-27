# Parser correction: approved publication and one controlled test

## Authority and limits

Lucas said **Go** after the verified local fix and the proposed next step of
publication followed by a separately approved controlled live test. This authorizes
one run, not a retry loop, daily activation, downstream import or email sending.

Exact one-time approval: `2026-09-27-parser-performance-test`, UTC September 27.
At most 5,000 new reserved units / 1,000 scrape requests. The explicit 25,000-unit
ceiling retains the existing 20,000 whole-run reservations; these are not dollars
or measured billed credits. Normal caps and the USD 5/month provider cap remain
unchanged. Prior approvals and reservations are preserved verbatim. The previous
timeout's final provider usage remains unverified, not treated as zero.

## Plan and acceptance checklist

- [x] Fetch main and retain the prior run's reservation (`c19c4c4`).
- [x] Record a fresh read-only application audit before publication.
- [x] Test the exact dated extension before implementing it: authorized reservation
  failed under the old code; wrong identity/date/mode/limits remain rejected.
- [x] Run offline checks and independent release review.
- [ ] Commit only reviewed source/tests/docs and the additive approval; publish
  without force to main and verify the exact remote hash.
- [ ] Enable only the manual canary, dispatch once, and restore it to false as soon
  as the job starts (with cleanup on failure). Keep daily-enable absent.
- [ ] Inspect that exact run's terminal outcome and diagnostic artifact. No retry.
- [ ] If successful, verify all ten configured towns, complete manifest and source
  database SHA-256; otherwise retain failure evidence without relaxing validation.
- [ ] Compare the post-test app audit with the baseline; no app import or restart.

Private baseline: `../production-launch/tmp/handoff-audit-parser-release-before.json`.
Real sandbox: four saved versions, two archived PDFs, zero outbox/provider attempts/
send ledger. Controlled: five saved versions, two archived PDFs, three outbox,
two previous provider attempts and two historical ledger rows. Both outreach flags
are false. No operational data was changed by the baseline audit.

The parser fix's full offline evidence is in
`count-parser-performance-2026-09-27.md`: 723 tests and two subtests passed before
this separately dated authorization. Captured-response replay is parser evidence,
not proof of complete feed acceptance. No progress or completion should be inferred
from elapsed time alone.

## Release verification

Final preflight: 727 tests plus two subtests passed (three unchanged slow mocked
Redfin tests deselected; those passed in the preceding full 723-test run).
Black/Ruff for changed Python files and `git diff --check` pass. Fresh replay of
the ten exact retained responses took 0.1442 seconds. All six historical
reservations and four historical approval objects are unchanged; the new approval
is additive. Protected source DB, manifest, usage/retry ledgers, billing config,
workflows and frozen helper have no differences from fetched main.

Independent release review: no findings; 111 allowance/CLI/price-transport checks
passed independently. The existing CLI refuses request 1,001 before transport,
and expanded allowances cannot run under a scheduled event. Publication is approved.
