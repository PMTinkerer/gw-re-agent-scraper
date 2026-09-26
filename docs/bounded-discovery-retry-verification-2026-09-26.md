# Bounded discovery retry — local verification

## Subsequent publication checkpoint

Lucas approved proceeding after the offline handoff. On September 26 at about
17:40 UTC, the focused suite passed again (170 tests), and a fast-forward push
published `5fce0d0e4740102b23be22cdfc42c9a571d17391` to GitHub main. Remote SHA
readback matched. Repository variables still show canary false and no daily
enable variable. Latest incremental run is still the earlier failed
36257185586; no new paid run was dispatched.

The read-only allowance check found 1,000 reserved units today (normal 500 plus
the previously approved extra 500), 1,000 of 10,000 rolling units, and no remaining
normal run allowance. The normal daily window resets at September 26, 8:00 PM
America/New_York. Reservations were not reset or raised. No delayed test is
scheduled. Publication does not establish a successful live refresh or authorize
daily operation/email. The verification record below describes the prior offline
implementation phase.

Scope: user-approved implementation and offline testing only. No paid requests,
push, deployment, consumer import, schedule activation or email in this work.

## Implemented

- At most one fresh whole-town retry on valid duplicate cards, changed pagination
  totals or terminal result-count mismatch. Previously completed towns persist;
  incomplete attempt cards never contribute to accepted coverage.
- Fatal invalid cards/metadata, cross-town duplicates, missing pages, blocked or
  failed transport and budget exhaustion. Full-page card validation precedes
  recoverable-error classification.
- Strict incremental raw-card validation distinguishes malformed source content
  from genuine pagination drift. The existing weekly parser is unchanged.
- Original budgeted callbacks and new-only detail enrichment are unchanged.
  Publication remains downstream of complete all-town discovery.

## Test-first evidence

Implementer's initial new tests failed as intended: 23 failed / 27 passed in the
new discovery unit file, then 28 failed / 112 passed across the initial expanded
three-file suite. They failed at the previous immediate duplicate/count rejection.
The real BudgetLedger regression also failed before implementation and passed
after the retry change.

Independent quality review found that the previous regex silently dropped
malformed raw cards, allowing a count-mismatch retry. Four new transport tests
reproduced incorrect acceptance of a clean second attempt. Strict parsing plus
fatal transport translation fixed this, and broader malformed-price/status/URL,
missing-terminator, missing-price and unmatched-tail cases were added.

## Verified checks

Interpreter: adjacent `production-launch/.venv/bin/python`.

- Baseline before edits: 86 active-refresh/transport tests passed.
- Final focused discovery/active-refresh/transport/budget/allowance suite:
  **170 passed**, 941 existing `datetime.utcnow()` deprecation warnings.
- Final full suite on the corrected implementation: **521 passed, two subtests
  passed**, 1,628 existing deprecation warnings, 310.23 seconds. Command:
  `python -m pytest tests/ -q --disable-warnings --durations=5`.
  Three legacy Redfin tests accounted for 305.41 seconds of deliberate waits.
- Independent spec re-review: **149 passed**, no remaining findings.
- Independent final quality re-review: **149 passed**, no remaining findings.
  The original malformed-Baths reproduction now stops on page 1 with exactly
  one reservation and one mocked request, never a recovery retry.
- Real captured-card replay: inconsistent 118-card/117-unique scan followed by
  clean scan returns exactly 118 unique listings; two bad scans reject. Ten
  simulated requests have ten reservations, never free retries.
- Implementer and spec reviewer replayed all ten retained full raw response
  exports, including repeated images: both sets parse 24/24/24/24/22 cards.
  Original set has 118 unique URLs; extra canary set has 117 and remains rejected.
- Real local BudgetLedger integration preserves the prior reservation, denies
  the retry before another simulated fetch and preserves database/manifest bytes.
- Successful recovery enriches the final new home once, not known homes or
  discarded-attempt candidates. Subsequent refresh does not re-enrich it.
- Formatting/lint and `git diff --check` passed on the scoped implementation.

## Preserved operational evidence

Before/after SHA-256 checks match:

| File | SHA-256 |
| --- | --- |
| `src/maine_active.py` | `9dc31528ded1ed61dc036a6a7c74354433524725b703ce0b9f89ba3b69881b4a` |
| `.github/workflows/maine_listings.yml` | `12e79f4c318ca1ea3f9d294d81d2ce41d914eaec4dc9179d8d694021a6eaa40a` |
| `data/maine_listings.db` | `376cc6e38f4dcbe7fa84f5bc08977065cc9b53c052deb0e8ff3bfed05e46a66d` |
| `data/active_refresh_allowances.json` | `f24367f0bdbf4144ba2d9cfdf6f6aff8467ebbe73fc1c547c447ce0acf7b83e5` |
| `data/active_refresh_usage.db` | `eac2056787d80be5c7c3922e84a462e3841b687ee1ed12549596259234ba139d` |

No changes to the billing/allowance controls, workflows, frozen consumer helper,
legacy parser, tracked data, or outreach application. No live source or consumer
acceptance is claimed. Atlas remains at its previous published-state checkpoint;
this is a local bugfix, not an operational release.

## Remaining release gates

Publish the reviewed code separately, then obtain any additional paid-canary
authority needed before a new controlled run. Never reset consumed allowances.
Verify a complete all-town manifest and matching source hash, then consumer
acceptance with historical quotes/PDFs/contact evidence preserved. Daily refresh
and sending remain separate activation decisions; offline tests do not authorize
either. A one-retry policy can still reject persistently inconsistent source pages.
