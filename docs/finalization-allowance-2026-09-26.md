# Approved temporary finalization allowance

Lucas explicitly requested removing the test allowance or raising it to 5,000
while finalizing. Use the bounded option: manual finalization runs may reserve
up to 5,000 conservative units per UTC day, including earlier reservations.
The 10,000-unit trailing-30-day ceiling, five units per request, matching-account
billing proof and approved USD 5 monthly overage cap remain unchanged.

Normal scheduled/default runs retain their 500-unit limit. Expanded capacity
requires the manual workflow's explicit `finalization` input; it cannot combine
with a one-time exception. Remove/stop using this temporary input when finalizing
is complete. It never enables daily operation or email transport.

The next manual attempt can reserve 4,000 units because today's two previous
500-unit reservations are retained. These are conservative reserved units, not
4,000 actual billed credits. A failed run retains its full reservation.
The redundant 8:05 PM one-shot task was paused before any immediate dispatch.

Verification plan: test preservation, daily/rolling bounds, normal-limit fallback,
manual-only gate, and request/run enforcement; independent review before publishing.
Then dispatch the existing no-email lane once, restore its canary flag false,
and verify complete coverage and source checksum before any downstream import.

## Offline verification

- Five new regression tests failed before implementation, then passed.
- Focused incremental suite: 187 passed; fresh final allowance/CLI subset: 25 passed.
- Independent review: no critical, important or minor findings. An additional
  isolated CLI check allowed 800 five-unit requests under a returned 4,000-unit
  run reservation and rejected request 801.
- Frozen helper, legacy workflow, source DB and both existing ledgers retain
  their pre-change SHA-256 hashes. No historical reservations were rewritten.
- Fresh GitHub check: main at eacfc78, canary false, daily variable absent, no
  conflicting active run. Scheduled duplicate task readback: PAUSED.
- Full regression run is still in progress at this checkpoint.

## Immediate controlled attempt

Lucas's latest approval authorizes this controlled finalization test now.
An immediate manual dispatch is planned after publication with finalization=true,
no one_time_approval. This checkpoint must be reconciled with GitHub before any
retry; uncertain dispatch responses are never grounds to dispatch twice.
Run ID and terminal result will be appended below. Do not import partial data.
