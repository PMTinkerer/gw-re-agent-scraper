# Incremental refresh regression evidence

`biddeford-active-cards-2026-09-26.json` contains the public card text from all
five saved Firecrawl responses in controlled run36256050877. Exported through
the provider Activity Logs without re-scraping. Page counts24/24/24/24/22 total118.
Images/navigation were omitted from this fixture; card content was not rewritten.
Raw response copies are retained locally under `.firecrawl/canary-2026-09-26/`.

The legacy parser extracted only58 cards: it requires beds/baths/sqft and does not
accept lowercase `1 bed`. The incremental parser must preserve all118 identities
while leaving absent facts unknown. Inclusion in discovery is not outreach eligibility.
