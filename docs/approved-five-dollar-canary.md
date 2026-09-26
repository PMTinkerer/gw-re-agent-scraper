# Approved five-dollar billing policy and canary plan

## Explicit additional canary approval, September 26

Lucas approved one additional capped test today after the five-summary parser
failure, plus updating the shared Atlas index. Add a dated, single-use approval
to the durable ledger and require its exact ID on manual dispatch. Consume it
with the new reservation before paid work, preserving the original reservation.
Reject replay, other dates/IDs, schedule use and rolling-cap exhaustion. Normal
daily whole-run limits remain unchanged. Per-request daily accounting also
remains 500 units: the earlier 25 units leave at most 95 basic requests today.
No extra retries, automatic activation or email permission is implied.

Verification: new acceptance/replay/date/rolling/CLI tests failed first; implement
the narrow allowance, run focused regressions and independent review, publish,
dispatch once and close the manual gate. Verify provider usage and final source
coverage before any consumer import. Record failure honestly without another run.

September 26: Lucas explicitly approved leaving the existing account-wide
Pay-as-you-go limit at USD 5/month, then instructed us to run the controlled
refresh. This supersedes the previous no-automatic-purchases requirement.
No plan upgrade, limit increase, manual credit purchase or email is authorized.

Design: accept dated matching-key billing evidence for either disabled
Pay-as-you-go, or enabled with a numeric finite USD monthly limit between zero
and five. Reject missing, malformed, boolean or higher limits. Keep all existing
per-run/day/rolling credit reservations. Evidence is operator readback, not
continuous billing synchronization; retain its 30-day expiry.

Implementation checklist:

- [ ] Add acceptance test for enabled USD 5 and rejection tests for unknown,
  unlimited, wrong currency, nonnumeric/boolean/nonfinite/negative/higher caps.
- [ ] Run tests to see the new acceptance case fail; implement the bounded rule
  in `src/refresh_transport.py`; rerun transport and incremental suites.
- [ ] Independently review changes before release. Inspect existing no-email
  workflow, current remote state and staged files. Preserve all historical data.
- [ ] Record actual Billing UI readback and selected-key SHA-256, never the key.
- [ ] Publish reviewed source with daily flag absent/false. Enable manual-only
  canary, dispatch once, then disable its flag. Verify terminal run/artifacts,
  actual provider usage and immutable sandbox evidence before importing.
- [ ] If first-run parser/coverage/cap fails, preserve prior publication and all
  durable reservations. Do not reset limits or rerun paid work to force success.
- [ ] Document exact outcome; no daily activation or email activation in this test.
