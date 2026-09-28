# Accepted feed isolation — September 28

Lucas approved isolating the accepted outreach feed from the legacy weekly
database. Content-addressed database/manifest bundles now publish together with
an atomic `data/accepted/current.json` pointer. Future incremental acceptance
checkpoints include the entire selected bundle. The weekly writer is unchanged.

Bootstrap preserves exact bytes from accepted commit
`9994ad5af38eefd55fd93f944b537dbca6f83d26`, not the subsequently modified legacy
database at `f9aa3e2a43b200294d614dfd57248dbae960ef94`.

- Database SHA256: `67eccc112e57af65104429357efc3deb5f381cb6196e06f6cb3a68c1124b5082`
- Manifest SHA256: `69e8f78e9cecce172a2675a60f03e279b409d71ac3aeb1bfe494991b4d4f6545`
- Original scan start: September 27, 17:48:59 UTC; completion 19:06:56 UTC.
- 946 unique active MLS IDs across all ten towns, with five disclosed unresolved
  listings. This is recovery of prior acceptance, not a fresh scrape.

Producer focused verification: 28 tests passed. Implementer broad verification:
854 tests plus two subtests passed, three known slow legacy cases omitted.
Independent quality review: producer acceptance 4, consumer trust/coverage 28,
pull/freshness 18 passed; no blocking findings. Spec finding about coordinated
dirty accepted files was fixed with a regression. Consumer requires selected
tracked clean files, exact hashes and unchanged freshness/coverage validation;
it never falls back to the legacy database.

Source database, legacy manifest, workflows, frozen helper and historical
reservations remain unchanged. No paid requests, budget changes, emails or
schedule activation. Consumer operational readback is recorded in its separate
isolation verification report; this producer report alone does not prove import.
