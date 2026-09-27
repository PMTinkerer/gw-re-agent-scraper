# Result-count parser performance correction

## Scope and cause

Lucas approved fixing the offline-reproduced stall after the single price-range
test hit GitHub's 90-minute limit. No paid rerun, allowance extension, publishing,
schedule, email, app import or restart is part of this correction.

The result-count regex tried to start at every character of a non-whitespace
token. For a 222,300-character embedded-image line without `Results`, this
repeatedly scanned the rest of the token, causing quadratic work. A watchdog
on the exact first saved response stopped in `re.findall` at the count parser.

The correction scans each whole token once and checks for `Results` immediately
after it. After a match it resumes after `Results`, preserving the original
non-overlapping extraction, including attached malformed suffixes. It does not drop
long lines, truncate pages, relax result-count validation, guess counts, or
accept numeric suffixes of malformed tokens. Existing pagination/card/identity/
complete-coverage checks remain intact. Frozen feed helper and weekly lane stay
unchanged. No new dependencies or global timeout changes.

## Test-first evidence

A subprocess regression with a 250,000-character image token exceeded its 5-second
deadline against the old code. That process was killed by the test harness, so
it cannot hang the test suite. With the correction all 51 transport tests pass,
including long tokens before/after/on the same line as real counts, oversized
invalid count tokens, malformed count suffixes, markdown boundaries, conflicting
counts and contradictory pagination. Synthetic noise protects the failure class
without requiring provider access or committing large raw response payloads.

All 10 exact retained responses from run 36277036335 replayed offline through the
actual `summary_range` parser in 0.1266 seconds total (individual 0.0082–0.0174s),
with a 5-second per-response watchdog. Each response's hash/status/truncation
was checked. The HTTP boundary returned the existing local response, not a
new scrape. Counts for the unbounded/descending maximum-price probes were:
119, 109, 97, 86, 74, 62, 50, 39, 28, 14. Every response produced the required first-page
card count. This is parser validation, NOT complete all-town feed acceptance.

Independent review found an adjacent malformed-evidence regression in the first
boundary-only candidate (`1 Results-2 Results` and `1 Results! Results`). Both
were reproduced as failing tests before the final token-scanning correction;
both now reject as before. The final review has no remaining findings: 51 focused
tests pass, 200,000 seeded bounded old/new extraction comparisons match exactly,
and tokens/whitespace/many-short-token inputs scale linearly through one million
characters (slowest measured case 0.162s). A separate main-agent 100,000-case
comparison also matched. No network requests were made by those checks.

Final-tree full suite: **723 tests plus 2 subtests passed**, 2,634 warnings,
in 389.76 seconds, using `../production-launch/.venv/bin/python -m pytest -q
--disable-warnings`. The unchanged mocked Redfin tests retain real randomized
delays; the parser replay itself takes 0.1266 seconds. Scoped Black, Ruff,
`git diff --check` and protected-file checks pass. No differences under `data`,
`config`, `.github/workflows`, `src/maine_active.py` or `src/maine_firecrawl.py`.

The code remains local and unpublished;
all-town provider acceptance is still outstanding. A later paid test needs
separate explicit authorization and a valid bounded allowance; existing consumed
approvals/reservations must not be reused or reset. Email and daily refresh remain
disabled. This correction does not establish that every other live source edge
case has been resolved.
