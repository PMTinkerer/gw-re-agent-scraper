"""Main orchestrator for the agent listing scraper.

CLI entry point that coordinates scraping, database operations,
and report generation with resumable chunk-based processing.
"""
from __future__ import annotations

import argparse
import logging
import sys

from .database import (
    get_connection, init_db, fuzzy_merge_agents, rebuild_rankings, get_stats,
)
from .report import generate_leaderboard
from .scraper import discover_redfin_region_id, scrape_redfin, scrape_realtor
from .state import (
    TOWNS, load_state, save_state, get_next_chunks,
    mark_started, mark_complete, mark_failed,
    is_initial_complete, parse_chunk_key, slug_to_town,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


def _ensure_region_ids(state: dict) -> bool:
    """Discover and cache Redfin region IDs for all towns. Returns True if all found."""
    all_found = True
    for town in TOWNS:
        slug = town.lower().replace(' ', '_')
        if state.get('region_ids', {}).get(slug):
            continue
        logger.info('Discovering region ID for %s...', town)
        region_id = discover_redfin_region_id(town, state)
        if not region_id:
            logger.error('Could not find region ID for %s', town)
            all_found = False
    return all_found


def _process_chunk(chunk_key: str, conn, state: dict) -> int:
    """Process a single chunk. Returns row count, or raises on failure."""
    info = parse_chunk_key(chunk_key)
    source = info['source']
    town_slug = info['town_slug']
    year = info['year']

    town = slug_to_town(town_slug)
    if not town:
        raise ValueError(f'Unknown town slug: {town_slug}')

    if source == 'redfin':
        region_id = state.get('region_ids', {}).get(town_slug)
        if not region_id:
            raise ValueError(f'No region ID cached for {town}. Run --discover-regions first.')
        return scrape_redfin(town, region_id, conn, state)

    elif source == 'realtor':
        if year is None:
            raise ValueError(f'Realtor chunk missing year: {chunk_key}')
        result = scrape_realtor(town, year, conn, state)
        if result == -1:
            # Skipped (no API key or budget exhausted) — mark as pending, not failed
            logger.info('Skipping %s (no API key or budget exhausted)', chunk_key)
            return -1
        return result

    else:
        raise ValueError(f'Unknown source: {source}')


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Real Estate Agent Listing Scraper — Southern Coastal Maine',
    )
    parser.add_argument(
        '--mode', choices=['initial', 'incremental', 'auto'], default='auto',
        help='Scraping mode (default: auto)',
    )
    parser.add_argument(
        '--max-chunks', type=int, default=3,
        help='Max chunks to process this run (default: 3)',
    )
    parser.add_argument(
        '--towns', type=str, default=None,
        help='Comma-separated town names to process (default: all)',
    )
    parser.add_argument(
        '--source', choices=['redfin', 'realtor'], default=None,
        help='Only process this source',
    )
    parser.add_argument(
        '--report-only', action='store_true',
        help='Skip scraping, regenerate report from existing data',
    )
    parser.add_argument(
        '--discover-regions', action='store_true',
        help='Discover and cache Redfin region IDs for all towns',
    )
    parser.add_argument(
        '--merge-agents', action='store_true',
        help='Run fuzzy agent name merge on existing data',
    )
    parser.add_argument(
        '--reset-state', action='store_true',
        help='Reset scrape state (clear all progress)',
    )
    parser.add_argument(
        '--db', type=str, default=None,
        help='Path to SQLite database file',
    )
    args = parser.parse_args()

    # Load state
    state = load_state()

    # Handle utility modes
    if args.reset_state:
        logger.info('Resetting scrape state...')
        from .state import _default_state, generate_all_chunks
        state = _default_state()
        for key in generate_all_chunks():
            state['chunks'][key] = {'status': 'pending'}
        save_state(state)
        logger.info('State reset. All chunks are now pending.')
        return 0

    # Connect to database
    conn = get_connection(args.db)
    init_db(conn)

    if args.discover_regions:
        logger.info('Discovering Redfin region IDs...')
        success = _ensure_region_ids(state)
        save_state(state)
        if success:
            logger.info('All region IDs discovered: %s', state['region_ids'])
        else:
            logger.error('Some region IDs could not be found')
        conn.close()
        return 0 if success else 1

    if args.merge_agents:
        logger.info('Running fuzzy agent name merge...')
        merges = fuzzy_merge_agents(conn)
        logger.info('Merged %d agent name variants', len(merges))
        rebuild_rankings(conn)
        generate_leaderboard(conn)
        conn.close()
        return 0

    if args.report_only:
        logger.info('Regenerating report from existing data...')
        rebuild_rankings(conn)
        path = generate_leaderboard(conn)
        stats = get_stats(conn)
        logger.info('Report generated: %s (%d transactions)', path, stats['total_transactions'])
        conn.close()
        return 0

    # --- Main scraping flow ---

    # Ensure region IDs are available for Redfin
    if not args.source or args.source == 'redfin':
        _ensure_region_ids(state)
        save_state(state)

    # Determine mode
    mode = args.mode
    if mode == 'auto':
        mode = 'incremental' if is_initial_complete(state) else 'initial'
    logger.info('Mode: %s', mode)

    # Parse town filter
    town_filter = None
    if args.towns:
        # Support both "York, ME" and "York" formats
        town_filter = args.towns.split(',')[0].strip().replace(', ME', '')

    # Get chunks to process
    chunks = get_next_chunks(
        state,
        max_chunks=args.max_chunks,
        source_filter=args.source,
        town_filter=town_filter,
    )

    if not chunks:
        logger.info('No pending chunks to process.')
        # Still regenerate report
        rebuild_rankings(conn)
        generate_leaderboard(conn)
        conn.close()
        return 0

    logger.info('Processing %d chunks: %s', len(chunks), chunks)

    success_count = 0
    fail_count = 0

    for chunk_key in chunks:
        logger.info('--- Processing chunk: %s ---', chunk_key)
        mark_started(state, chunk_key)
        save_state(state)

        try:
            rows = _process_chunk(chunk_key, conn, state)

            if rows == -1:
                # Skipped (no API key or budget) — revert to pending
                state['chunks'][chunk_key] = {'status': 'pending'}
            else:
                mark_complete(state, chunk_key, rows)
                success_count += 1
                logger.info('Chunk %s complete: %d rows', chunk_key, rows)

        except Exception as e:
            logger.error('Chunk %s failed: %s', chunk_key, e)
            mark_failed(state, chunk_key, str(e))
            fail_count += 1

        save_state(state)

    # Post-processing
    if success_count > 0:
        logger.info('Running fuzzy agent merge...')
        merges = fuzzy_merge_agents(conn)
        if merges:
            logger.info('Merged %d agent name variants', len(merges))

    rebuild_rankings(conn)
    generate_leaderboard(conn)

    # Check if initial collection just completed
    if is_initial_complete(state) and state.get('mode') == 'initial':
        state['mode'] = 'incremental'
        logger.info('Initial collection complete! Switching to incremental mode.')
        save_state(state)

    stats = get_stats(conn)
    logger.info('Run summary: %d success, %d failed, %d total transactions in DB',
                success_count, fail_count, stats['total_transactions'])

    conn.close()

    if fail_count > 0 and success_count == 0:
        return 2  # Total failure
    elif fail_count > 0:
        return 1  # Partial failure
    return 0


if __name__ == '__main__':
    sys.exit(main())
