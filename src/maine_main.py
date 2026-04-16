"""CLI entry point for the Maine Listings (MREIS MLS) pipeline."""
from __future__ import annotations

import argparse
import logging
import sys

from .maine_database import get_connection, init_db
from .maine_firecrawl import discover_listings, enrich_listings
from .maine_state import load_state, save_state

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


def _parse_towns(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [t.strip() for t in raw.split(',') if t.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Maine Listings (MREIS MLS) Transaction Scraper',
    )
    parser.add_argument('--discover', action='store_true',
                        help='Discover closed listings from search pages')
    parser.add_argument('--enrich', action='store_true',
                        help='Enrich listings with agent data from detail pages')
    parser.add_argument('--report', action='store_true',
                        help='Generate leaderboard report')
    parser.add_argument('--towns', type=str, default=None,
                        help='Comma-separated town names')
    parser.add_argument('--max-pages', type=int, default=90,
                        help='Max search result pages per town (default: 90)')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Detail pages to enrich per run (default: 50)')
    parser.add_argument('--recent-only', action='store_true',
                        help='Stop discovery when hitting known listings')
    parser.add_argument('--db', type=str, default=None,
                        help='Path to SQLite database')
    parser.add_argument('--state', type=str, default=None,
                        help='Path to state JSON file')
    args = parser.parse_args()

    if not (args.discover or args.enrich or args.report):
        parser.print_help()
        return 0

    towns = _parse_towns(args.towns)
    conn = get_connection(args.db)
    init_db(conn)
    state = load_state(args.state)

    if args.discover:
        logger.info('Starting Maine Listings discovery (max_pages=%d, recent_only=%s)...',
                     args.max_pages, args.recent_only)
        result = discover_listings(
            conn, state,
            towns=towns,
            max_pages=args.max_pages,
            recent_only=args.recent_only,
            state_path=args.state,
        )
        logger.info('Discovery: %d towns, %d listings found',
                     result['towns'], result['listings'])

    if args.enrich:
        logger.info('Starting detail page enrichment (batch=%d)...', args.batch_size)
        result = enrich_listings(conn, batch_size=args.batch_size)
        logger.info('Enrichment: %d enriched, %d failed, %d total',
                     result['enriched'], result['failed'], result['total'])

    if args.report:
        stats = conn.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN enrichment_status = 'success' THEN 1 ELSE 0 END) as enriched,
                   SUM(CASE WHEN listing_agent IS NOT NULL THEN 1 ELSE 0 END) as has_listing_agent,
                   SUM(CASE WHEN buyer_agent IS NOT NULL THEN 1 ELSE 0 END) as has_buyer_agent
            FROM maine_transactions
        ''').fetchone()
        logger.info('DB stats: %d total, %d enriched, %d with listing agent, %d with buyer agent',
                     stats['total'], stats['enriched'],
                     stats['has_listing_agent'], stats['has_buyer_agent'])

    save_state(state, args.state)
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
