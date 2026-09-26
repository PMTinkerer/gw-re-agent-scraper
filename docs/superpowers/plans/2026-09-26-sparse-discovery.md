# Sparse discovery correction

Approved direction: Lucas requested fixing the importer after the price-sort
run exposed an addressless public result card. Implement locally and verify
offline; this is not a new paid-run, spending-cap, publishing or send approval.

1. Reproduce the missing address/town shape with explicitly synthetic tests.
   Keep the captured Biddeford pages as separate real-response regressions.
2. Count listings by validated source URL, with the queried town recorded only
   as discovery provenance. Validate any supplied town; never invent home facts.
3. Let validated detail data supply absent summary facts. Preserve known records.
   Record unresolved new URLs in a dedicated append-only observation table and
   additive manifest field. Never insert unresolved homes into the active feed.
4. Preserve strict count, pagination, duplicate, identity, status and publication
   guards. Retain the existing two-attempt detail limit and every spend ledger.
5. Archive bounded, credential-redacted summary responses before parsing. Retain
   them as workflow artifacts even on failure, enabling offline replay. Access
   inherits repository visibility: this scraper repository is public (verified
   via GitHub readback). Retain only public page markdown and allowlisted context,
   never credentials, request headers, provider envelopes or user/app records.
6. Run regression tests and independent review. Check protected file hashes and
   document exactly what was verified. Frozen helper and legacy lane unchanged.

Missing facts are not malformed facts: absent address/town is allowed during
discovery; a supplied blank/invalid fact must still be rejected or quarantined.
Complete coverage means all result identities were counted, not that every
result was eligible or fully enriched. Unresolved URLs remain visible in audit
evidence, including after their bounded detail attempts have been exhausted.
