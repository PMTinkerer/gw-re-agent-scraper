# Verified listing aliases — local repair and seven-pass verification

## Outcome and scope

The changed-address-link identity defect is repaired locally. Original source
row IDs, primary URLs, contact data and historical rows are retained. A verified
alias requires the same official URL path prefix and numeric source ID, exact
MLS number, and matching town. Genuine conflicts still reject publication.

Lucas approved continuous internal work and at least seven passes, superseding
routine design/plan clarification pauses. Ordinary implementation decisions,
test failures and review corrections were handled internally with focused
implementation, specification-review and quality-review agents. No new paid
request, publication, app import/restart, budget change, schedule activation or
email was performed. Changes remain in the existing isolated worktree.

## Findings and implementation

The failed run's final listing has current browser-visible MLS1672938 and
source ID782016957. Its URL changed from `willow-ridge-ridge` to
`willow-ridge-road`. The failed historical detail response was not retained;
this is browser-confirmed current evidence, not an exact replay of that payload.

Offline comparison of all 953 saved discovery URLs found five changed-link
identities. The source database also contains ten duplicated numeric-source-ID
groups with active rows, each pair agreeing on MLS and town. Thus a one-address
special case would leave other known failures unresolved.

`src/active_refresh.py` now:

- Stages additive `active_refresh_aliases` records with MLS/town, original URL,
  observed alias, verification time and run ID.
- Validates explicit detail identity before trusting a new alias or historical
  duplicate group, using the existing durable two-attempt limit and validated cache.
- Preserves the deterministic oldest row as the representative. All historical
  rows remain; verified secondary rows become Unverified with explicit alias
  evidence, never falsely Sold or deleted.
- Resolves summary membership, absence checks and manifest membership through
  that original identity. Known aliases require no repeated full enrichment.
- Honors freshly confirmed inactive status and corrects current address text
  while preserving original URL, contact data and prior history.
- Quarantines verified secondary rows even if the independent legacy importer
  reactivates them and the property is absent from current discovery.

The frozen `src/maine_active.py` interface, workflows, budgets and source data
are unchanged. Alias records live in the same staged snapshot as the source
rows and are covered by the existing atomic publication/hash contract.

## Verification passes

1. **Failure reproduction:** implementer observed five initial failing alias
   checks against the old behavior before implementing the fix.
2. **Positive identity repair:** verified correction, three repeated refreshes,
   original URL reappearance, original row/contact/history preservation.
3. **Adversarial identity checks:** wrong/missing MLS or town, wrong source ID,
   changed ZIP/board prefix, malformed URLs, conflicting historical groups,
   duplicate current source IDs and corrupted cached/persisted proof fail closed.
4. **Lifecycle and rollback:** historical duplicate groups, Pending/Sold,
   absence, later refresh failure, preserved retry evidence and cache reuse.
   Three invocations cannot exceed two detail attempts or publish failed proof.
5. **Saved-evidence integration:** all 154 captured, hash-checked summary
   responses replayed across all ten towns into a temporary source copy.
   See the explicit fixture qualifications below.
6. **Independent reviews and correction:** spec review passed. Quality review
   reproduced a secondary alias reactivated by the legacy importer surviving
   outside the manifest. Added a failing regression, fixed it, and obtained
   independent re-review approval. Frozen reader and manifest now agree.
7. **Final regression/preservation:** 86 focused tests; final broad run 750
   passed, two subtests passed, three unchanged slow legacy tests excluded.
   Scoped Black/Ruff and `git diff --check` pass. Protected-file hashes and
   before/after read-only application audits match exactly.

The earlier full-suite run passed 745 tests plus two subtests in 388.03 seconds,
including all three slow legacy Redfin tests. That run began before the final
review correction/eight additional regressions. The final-tree run above covers
all tests except those three unchanged Redfin tests; do not describe it as a
single final-tree full-suite run. Existing datetime deprecation warnings remain.

## Replay evidence and limitations

Reusable network-free command:

```sh
../production-launch/.venv/bin/python tools/replay_listing_aliases.py \
  --summary-dir .firecrawl/parser-run-36325959142/summaries \
  --source-db data/maine_listings.db \
  --checkpoint-state .firecrawl/parser-run-36325959142/checkpoint/data/active_refresh_retries.db
```

The harness never constructs a live transport session. It replays saved summary
bytes, reuses captured successful detail cache entries, and supplies **simulated
alias identity responses from historical rows**. Missing new details return
unresolved instead of being invented. Temporary publication is deleted afterward.

Observed rehearsal: 154 summaries, ten towns, 643 unique Active MLS rows, 310
unresolved new-detail fixture misses, 12 simulated alias proofs. All **18,214
original row IDs/URLs/MLS identities and 7,167 original history rows** survived.
Frozen helper output equals manifest active IDs; the manifest database hash
matches the temporary database. This is NOT fresh live feed acceptance, and the
643 rows span property types rather than constituting 643 eligible outreach homes.

A separate saved-response warm-hint replay returns exactly the same 953 URLs
using 97 summary requests versus 154 cold requests. That leaves only three
requests under the ordinary 100-request daily limit before any detail/status
work. It is not a future cost guarantee or proof of sustainable daily capacity.

## Protected operational state

`../production-launch/tmp/handoff-audit-listing-alias-before.json` and
`handoff-audit-listing-alias-after.json` are byte-identical. Real sandbox remains
at four saved projection versions, two PDF archives, 288 saved defaults,
325 pursuits, and outbox/provider-attempt/send-ledger counts **0/0/0**.
Controlled test evidence remains **3/2/2**, with five versions and two PDFs.
Both outreach flags remain false; no saved figures, PDF bytes or contact
evidence changed. Audits are private local evidence, not public artifacts.

Protected SHA-256 values before and after:

| File | SHA-256 |
| --- | --- |
| `data/maine_listings.db` | `376cc6e38f4dcbe7fa84f5bc08977065cc9b53c052deb0e8ff3bfed05e46a66d` |
| `data/active_refresh_usage.db` | `d1bd7998f34f71f0b239c27e5371619a0128606a3f8ef112817ed05caf424244` |
| `data/active_refresh_retries.db` | `703983aa958180a3ea4a281001dbc5fc79dfc061472751cb000734b664176a0e` |
| `data/active_refresh_allowances.json` | `44f71a356915f72ec3d16dbf92b535c2d113b9fd41201722436a0eba12950016` |
| `src/maine_active.py` | `9dc31528ded1ed61dc036a6a7c74354433524725b703ce0b9f89ba3b69881b4a` |
| `.github/workflows/incremental_active.yml` | `54e62ff64f00e9aa1c74da1e325e943908e95d50faa1addb9424fe69bdb932dd` |

## Remaining release boundaries

This local fix is not deployed. Any later publication must preserve remote
checkpoint-only changes from `d30e3b9`, including consumed approvals and retry
records; never overwrite them with this worktree's older data files. Fresh
provider acceptance, hash-matched complete feed validation and consumer import
remain separate from this offline repair. Daily capacity and broader outreach
launch gates remain open; sending and daily refresh stay disabled. The prior
monitor remains paused; no recurring replacement was created.
