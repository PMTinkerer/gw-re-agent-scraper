# Approved immediate price-sort test

Lucas explicitly approved one additional controlled test now: at most 5,000
reserved units / 1,000 requests, today's ceiling 10,000, prior records preserved,
provider USD 5/month unchanged, no email or recurring activation.

Design: retain the existing manual finalization switch and exact dated single-use
approval ID. Only a matching 5,000-unit approval can combine with finalization;
the old 500-unit exception cannot. Add the approved daily headroom but cap the
new run at 5,000 and preserve the 10,000 rolling ceiling. Propagate the same daily
ceiling to the request ledger only after reserve_run validates the approval.
Do not refund/reset reservations or permanently raise default limits.

- [ ] Add allowance tests for successful 5,000 extra reservation, immutable prior
  rows, reuse, wrong day/ID/amount, missing finalization, rolling limit and per-run
  ceiling. Add CLI test with prior request usage; request 1,001 must fail.
- [ ] Observe failures; minimally update refresh_allowance.py and incremental_main.py.
- [ ] Append exact dated approval to data/active_refresh_allowances.json without
  changing previous approvals or reservations. Run focused/full regression/review.
- [ ] Publish only reviewed files, read GitHub main back, dispatch once with
  finalization=true and one_time_approval=2026-09-26-price-sort-test. Restore
  canary=false once job starts; never set the daily-enable flag.
- [ ] Observe terminal run result; accept only complete all-town manifest and
  matching database checksum. No partial import. Record evidence and next gate.

Alternatives considered: waiting for UTC reset (user declined), deleting/refunding
prior reservations (rejected; loses the existing conservative audit boundary).
