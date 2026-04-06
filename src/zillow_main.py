"""CLI entry point for the Zillow leaderboard pipeline."""
from __future__ import annotations

import argparse
import logging
import os
import sys

from .database import get_team_gap_rows, get_zillow_connection, init_zillow_db
from .report import get_report_stats
from .zillow import (
    discover_zillow_profiles,
    generate_zillow_outputs,
    scrape_zillow_profiles,
)
from .zillow_state import load_state, reset_state, save_state

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


def _parse_towns(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [town.strip().replace(', ME', '') for town in raw.split(',') if town.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Zillow Leaderboard Pipeline — Southern Coastal Maine',
    )
    parser.add_argument('--discover', action='store_true', help='Discover Zillow profile URLs from town directories')
    parser.add_argument('--scrape-profiles', action='store_true', help='Scrape pending Zillow team and individual profiles')
    parser.add_argument('--report-only', action='store_true', help='Regenerate Zillow outputs from existing data')
    parser.add_argument('--reset-state', action='store_true', help='Reset Zillow discovery state to all pending')
    parser.add_argument('--batch-size', type=int, default=20, help='Profiles to scrape in one run (default: 20)')
    parser.add_argument('--towns', type=str, default=None, help='Comma-separated town names to limit discovery/scrape scope')
    parser.add_argument('--db', type=str, default=None, help='Path to the Zillow SQLite database')
    parser.add_argument('--state', type=str, default=None, help='Path to the Zillow state JSON file')
    args = parser.parse_args()

    towns = _parse_towns(args.towns)

    if args.reset_state:
        reset_state(args.state)
        logger.info('Zillow discovery state reset.')
        return 0

    state = load_state(args.state)
    conn = get_zillow_connection(args.db)
    init_zillow_db(conn)
    headless = os.environ.get('CI') == 'true'

    did_work = False

    if args.discover:
        logger.info('Starting Zillow directory discovery...')
        result = discover_zillow_profiles(
            conn,
            state,
            towns=towns,
            headless=headless,
            state_path=args.state,
        )
        logger.info('Discovery complete: %d towns processed, %d profiles found',
                    result['towns_processed'], result['profiles_found'])
        did_work = True

    if args.scrape_profiles:
        logger.info('Starting Zillow profile scrape (batch_size=%d, headless=%s)',
                    args.batch_size, headless)
        result = scrape_zillow_profiles(
            conn,
            batch_size=args.batch_size,
            headless=headless,
            towns=towns,
        )
        logger.info(
            'Profile scrape complete: %d profiles processed, %d individual rows, %d team rows, %d blocked',
            result['processed'], result['individual_rows'], result['team_rows'], result['blocked'],
        )
        did_work = True

    if args.report_only or did_work or not (args.discover or args.scrape_profiles):
        outputs = generate_zillow_outputs(conn)
        seller_stats = get_report_stats(conn, source='zillow', role='seller')
        buyer_stats = get_report_stats(conn, source='zillow', role='buyer')
        unresolved_team_rows = len(get_team_gap_rows(conn))
        logger.info(
            'Zillow outputs generated: seller=%d rows with %d seller agents, buyer=%d rows with %d buyer agents, unresolved team-only=%d',
            seller_stats['total'], seller_stats['unique_agents'],
            buyer_stats['total'], buyer_stats['unique_agents'],
            unresolved_team_rows,
        )
        logger.info('Artifacts: %s', outputs)

    save_state(state, args.state)
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
