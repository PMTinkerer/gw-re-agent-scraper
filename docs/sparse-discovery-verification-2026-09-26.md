# Sparse discovery correction — local verification

## Outcome

Implemented locally against `2667ac36ec26c709a46b7abf4744cbfadca9f3f9`.
No commit/push, paid provider request, publication/import, scheduling change or
email. Existing used allowances remain used. Live all-town acceptance is pending.

The importer no longer requires address/town on every result card in order to
count discovery coverage. URL, status, price and footer structure remain checked.
Present malformed facts are not treated as missing. Query town is stored as
`discovery_town`, not silently copied to a property's city. Duplicate URLs,
cross-town identities, unstable pagination and incomplete counts still fail.

Known sparse cards preserve verified fields and do not trigger detail or absence
checks. New details can supply absent summary facts. An incremental-only script
embeds the unchanged legacy extractor and supplements address/town/state/ZIP from
the page's unique visible h1 and adjacent location. It never extracts a withheld
address from hidden application state or from similar-listing cards.

Unresolved new listings retain URL, discovery town, reason and time in the
append-only `active_refresh_unresolved` table and current manifest `unresolved`
list. They stay outside usable active records; the CLI reports their count.
Two persistent detail attempts remain the limit. Resolution on a later run does
not erase the earlier audit. Schema version 1 and its required fields remain;
the downstream coverage reader permits this additive metadata. The frozen
consumer helper was not changed.

## Evidence

- RED: five of six initial sparse regressions failed before implementation.
- GREEN: sparse identity/audit tests, malformed-card guards, captured 118-card
  Biddeford replay and coverage/retry tests passed.
- RED: four diagnostics tests initially failed; actual transported detail-script
  recovery also failed before the incremental supplement was connected.
- GREEN: tests execute the actual transported JavaScript in Node against a
  synthetic public DOM, then pass its result through transport and DB publication.
- Free public-browser inspection confirmed the addressless land card at
  `https://mainelistings.com/listings/759917817` has no displayed address header.
  This remains synthetic parser reproduction, NOT replay of the unarchived failed
  Firecrawl response.
- Free public-browser verification of the supplemental selectors on
  `https://mainelistings.com/listings/ME/Kittery/03904/10376/22-trafton-lane-kittery-me-03904/775069982`
  produced `22 Trafton Lane`, `Kittery`, `ME`, `03904`. Temporary tab closed.
- Complete suite at the intermediate checkpoint: **553 tests + two subtests
  passed**, 383.72 seconds, existing deprecation warnings.
- Final-tree suite: **556 tests + two subtests passed**, three unchanged slow
  Redfin tests deselected (each passed in the earlier full run), 6.72 seconds.
- Final changed-script/sparse/transport selection: 55 passed. Independent
  re-review: no remaining blocking findings; 16 new regressions passed.
- Scoped Black, Ruff and `git diff --check` passed.

## Diagnostics

Only public summary markdown and allowlisted URL/time/status/hash metadata are
saved before parsing, so parser errors and later duplicate/count failures retain
their inputs. Exact configured credential occurrences are redacted; request
headers, provider envelopes, raw detail scripts and user application data are
excluded. Local files use 0600, exclusive unique names, no overwrite. Limits:
2 MB per JSON record, 100 MB per run directory, 1,000 records; truncation is
explicit and cannot be treated as full-response replay. Capacity is checked
before the next paid request. Provider HTTP failures without a returned source
document are not archived as source evidence.

GitHub workflow uploads only this ignored diagnostics directory on success or
failure, with 30-day retention. The upload action v4.6.2 SHA was verified via
upstream git tag: `ea165f8d65b6e75b540449e92b4886f43607fa02`.
GitHub readback verified this scraper repository is **public**. Artifacts inherit
that visibility; they are NOT private storage. No artifact was uploaded in this
turn. This retention is for reproducible public-source debugging, not indefinite
business history. The user's saved projections, send ledger and contacts remain
in the untouched outreach app's existing persistent storage.

## Protected files

Before/after SHA-256 values match:

| File | SHA-256 |
| --- | --- |
| `src/maine_active.py` | `9dc31528ded1ed61dc036a6a7c74354433524725b703ce0b9f89ba3b69881b4a` |
| `src/maine_firecrawl.py` | `fb1ef9c1ccd100a64b8475f36581073e7d4bb6aa5d91b9d127bdf352acfd2aae` |
| `.github/workflows/maine_listings.yml` | `12e79f4c318ca1ea3f9d294d81d2ce41d914eaec4dc9179d8d694021a6eaa40a` |
| `data/maine_listings.db` | `376cc6e38f4dcbe7fa84f5bc08977065cc9b53c052deb0e8ff3bfed05e46a66d` |
| `data/active_refresh_allowances.json` | `3dcc791f2a26729dcd220ca3b65af15f92f50b8217de68bd877d0b24882799ae` |
| `data/active_refresh_usage.db` | `8eb246dc3834571b594eca8625788e621e9a8af863c5df96e2893d452308d096` |

Next release step is publishing the reviewed correction, then separately bounded
all-town provider acceptance under valid available/approved capacity. Do not
reset the exhausted rolling ledger or reuse the consumed single-use approval.
