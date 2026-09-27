# Approved alias release and bounded live verification batch

## Authority

Lucas: "Go ahead and do it and loop on it again cap 5 times if you run into
issues so that I don't have to keep re-approving."

This authorizes publishing the tested alias fix and a maximum of FIVE TOTAL
controlled live attempts, including the first attempt. Stop immediately upon
verified success; never run five merely to use the allowance. Diagnose failures
from retained evidence, fix locally with regression tests and review, publish
only relevant changes, then use the next unused approval. No blind reruns.

Each attempt retains the existing 5,000 reserved-unit / at-most-1,000-request
limit. Five exact single-use dated approvals permit at most 25,000 additional
reserved units / 5,000 requests total. Prior whole-run reservations total25,000;
per-slot cumulative ceilings are30,000/35,000/40,000/45,000/50,000. These are
internal units, NOT dollars or measured charges. The actual USD5/month provider
billing cap stays unchanged. Normal daily/rolling limits remain unchanged.
The batch approvals expire at the UTC date boundary; do not alter their dates
or recycle used records. No sixth dispatch, including skipped/failed attempts.

Email sending and daily refresh remain disabled. The manual canary gate may
be enabled only for an exact batch dispatch, then immediately returned false
after the run's job starts. No legacy notifications, app imports/restarts,
credential transfer or sending activation. Stop if a safety boundary cannot be
maintained. Routine implementation choices/review corrections need no reapproval.

## Release preparation

- Preserve and fast-forward remote checkpoint d30e3b9 before committing; its
  retries and all reservations must survive intact.
- Publish the prior reviewed alias repair, narrow batch allowance validation,
  five approval records and relevant tests/docs only.
- Validate exact-ID/date/mode/ceiling/single-use rejection, final broad suite,
  formatting and independent review before push.
- Capture private read-only application baseline before dispatch.
- Verify matching billing policy and existing credits through workflow preflight.

## Attempt register

Release preflight:850 tests and2subtests passed;3unchanged slow Redfin tests
excluded after passing in the previous full run. Scoped Black/Ruff/diff checks
pass. Independent batch review passed173focused tests with no blocking findings.
All prior reservations/approvals compare equal after five new approvals appended;
remote d30e3b9 checkpoint files were fast-forwarded intact. Source DB, frozen
helper, workflow and retry/request ledgers have no release diff.

Attempt1 is prepared for dispatch after remote release readback. Append every dispatch intention and actual GitHub
run ID here before proceeding to another attempt. Approval IDs in order:

1. `2026-09-27-alias-verification-1` — dispatch intended; no run ID yet
2. `2026-09-27-alias-verification-2` — unused
3. `2026-09-27-alias-verification-3` — unused
4. `2026-09-27-alias-verification-4` — unused
5. `2026-09-27-alias-verification-5` — unused

## Completion protocol

Inspect exact terminal run logs and diagnostic artifact. Success requires the
published complete manifest SHA-256 to match its source database, exact coverage
of all ten configured towns and agreement with the frozen active-listing reader.
Report unresolved listings honestly. Never import a temporary/simulated snapshot.
Use the existing read-only app audit and compare to the batch baseline; saved
quotes, PDF bytes, contact history, outbox/attempt/ledger and disabled flags must
remain unchanged. Record exact outcome/remaining gates; pause the follow-up on
verified success, five attempted cycles, expired authority or an unresolvable
safety/billing limit. Stay quiet on unchanged running states; notify Lucas on
success, final failure or genuinely required action, not routine retry permission.
