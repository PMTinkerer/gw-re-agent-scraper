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
    run_zillow_smoke_check,
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
    parser.add_argument('--smoke-check', action='store_true', help='Run a fast Zillow proxy/access smoke check')
    parser.add_argument('--smoke-strict', action='store_true', help='Exit non-zero if Zillow smoke check does not get an ok response')
    parser.add_argument('--smoke-output', type=str, default=None, help='Path to write Zillow smoke diagnostics markdown')
    parser.add_argument('--batch-size', type=int, default=20, help='Profiles to scrape in one run (default: 20)')
    parser.add_argument('--towns', type=str, default=None, help='Comma-separated town names to limit discovery/scrape scope')
    parser.add_argument('--db', type=str, default=None, help='Path to the Zillow SQLite database')
    parser.add_argument('--state', type=str, default=None, help='Path to the Zillow state JSON file')
    parser.add_argument('--use-firecrawl', action='store_true', help='Use Firecrawl API instead of Playwright for directory discovery')
    parser.add_argument('--max-pages', type=int, default=25, help='Max directory pages per town when using Firecrawl (default: 25)')
    parser.add_argument('--directory-report', action='store_true', help='Generate directory-only leaderboard and dashboard')
    parser.add_argument('--enrich-profiles', action='store_true', help='Enrich Zillow profiles with career stats and sold data via Firecrawl')
    parser.add_argument('--enrich-batch', type=int, default=50, help='Profiles to enrich per batch (default: 50)')
    args = parser.parse_args()

    towns = _parse_towns(args.towns)
    headless = os.environ.get('CI') == 'true'
    pipeline_requested = args.discover or args.scrape_profiles or args.report_only or args.directory_report or args.enrich_profiles

    if args.smoke_check:
        logger.info('Starting Zillow smoke check...')
        smoke_result = run_zillow_smoke_check(
            towns=towns,
            headless=headless,
            output_path=args.smoke_output,
        )
        logger.info(
            'Smoke check complete: passed=%s, proxy_configured=%s, report=%s',
            smoke_result['passed'],
            smoke_result['proxy_configured'],
            smoke_result['report_path'],
        )
        if args.smoke_strict and not smoke_result['passed']:
            return 2

    if args.reset_state:
        reset_state(args.state)
        logger.info('Zillow discovery state reset.')
        return 0

    if not pipeline_requested:
        return 0

    state = load_state(args.state)
    conn = get_zillow_connection(args.db)
    init_zillow_db(conn)

    did_work = False

    if args.discover:
        if args.use_firecrawl:
            from .zillow_firecrawl import discover_zillow_profiles_firecrawl
            logger.info('Starting Zillow directory discovery via Firecrawl (max_pages=%d)...', args.max_pages)
            result = discover_zillow_profiles_firecrawl(
                conn,
                state,
                towns=towns,
                max_pages=args.max_pages,
                state_path=args.state,
            )
        else:
            logger.info('Starting Zillow directory discovery via Playwright...')
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

    if args.enrich_profiles:
        from .zillow_profile_scraper import enrich_zillow_profiles
        logger.info('Starting Zillow profile enrichment (batch=%d)...', args.enrich_batch)
        result = enrich_zillow_profiles(conn, batch_size=args.enrich_batch)
        logger.info(
            'Profile enrichment: %d enriched, %d failed, %d total',
            result['enriched'], result['failed'], result['total'],
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

    if args.directory_report or (args.use_firecrawl and did_work):
        from .zillow_directory_report import (
            generate_directory_dashboard,
            generate_directory_leaderboard,
            get_directory_stats,
        )
        report_path = generate_directory_leaderboard(conn)
        dashboard_path = generate_directory_dashboard(conn)
        dir_stats = get_directory_stats(conn)
        logger.info(
            'Directory outputs: %d agents, %d brokerages, %d teams, %d towns — %s, %s',
            dir_stats['agents'], dir_stats['brokerages'], dir_stats['teams'],
            dir_stats['towns_with_data'], report_path, dashboard_path,
        )

        from .database import get_connection
        from .index_page import generate_index_html
        from .maine_database import get_connection as get_maine_connection
        redfin_conn = get_connection()
        maine_conn = get_maine_connection()
        generate_index_html(
            redfin_conn=redfin_conn,
            zillow_conn=conn,
            maine_conn=maine_conn,
        )
        redfin_conn.close()
        maine_conn.close()

    save_state(state, args.state)
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
