# Approved five-dollar billing policy and canary plan

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
