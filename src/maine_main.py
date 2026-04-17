"""CLI entry point for the Maine Listings (MREIS MLS) pipeline."""
from __future__ import annotations

import argparse
import logging
import sys

import os
import shutil
from datetime import datetime

from .maine_database import _DEFAULT_DB, get_connection, init_db
from .maine_firecrawl import discover_listings, enrich_listings
from .maine_notifier import notify_failure, notify_success
from .maine_state import load_state, save_state
from .state import TOWNS as _CANONICAL_TOWNS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


_CANON_LOOKUP = {t.lower().replace(' ', '_'): t for t in _CANONICAL_TOWNS}
_CANON_LOOKUP.update({t.lower(): t for t in _CANONICAL_TOWNS})


def _canonicalize_town(raw: str) -> str:
    """Map user input (any case/separator) to the canonical TOWNS spelling.

    mainelistings.com expects the human-readable name with spaces
    ("Old Orchard Beach"). Accept underscore/slug forms too.
    """
    key = raw.strip().lower().replace('-', ' ')
    if key in _CANON_LOOKUP:
        return _CANON_LOOKUP[key]
    key_us = key.replace(' ', '_')
    if key_us in _CANON_LOOKUP:
        return _CANON_LOOKUP[key_us]
    logger.warning("Unknown town '%s' — passing through as-is", raw)
    return raw


def _parse_towns(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [_canonicalize_town(t) for t in raw.split(',') if t.strip()]


def _backup_db(db_path: str | None) -> str | None:
    """Copy the maine_listings.db to a timestamped backup before a mutating run.

    Keeps the last 3 backups and deletes older ones. Silent no-op if the DB
    file doesn't exist yet.
    """
    path = db_path or _DEFAULT_DB
    if not os.path.exists(path):
        return None
    stamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    backup_path = f'{path}.bak_{stamp}'
    try:
        shutil.copy2(path, backup_path)
        logger.info('DB backed up to %s', backup_path)
    except Exception as exc:
        logger.warning('DB backup failed: %s', exc)
        return None

    # Keep the 3 most recent backups, remove older ones.
    directory = os.path.dirname(os.path.abspath(path))
    prefix = os.path.basename(path) + '.bak_'
    try:
        backups = sorted(
            (os.path.join(directory, f) for f in os.listdir(directory)
             if f.startswith(prefix)),
            reverse=True,
        )
        for old in backups[3:]:
            os.remove(old)
    except OSError:
        pass
    return backup_path


def _notify_enrichment_result(result: dict, run_id: str) -> None:
    """Send pushover + email summary or failure alert based on the result."""
    enriched = result['enriched']
    failed = result['failed']
    total = result['total']

    if result.get('aborted'):
        notify_failure(
            'Enrichment aborted by circuit breaker',
            f'After {enriched} successes and {failed} failures '
            f'(of {total}), the circuit breaker tripped and stopped the run. '
            f'The DB has been saved. Re-run with `--enrich` to resume.',
            run_id=run_id,
        )
        return

    if total == 0:
        # Nothing to do — no notification needed
        return

    success_rate = enriched / total * 100 if total else 0
    summary = (
        f'Enrichment complete: {enriched}/{total} '
        f'({success_rate:.1f}%) successful, {failed} failed'
    )
    details = (
        f'Run ID: {run_id}\n'
        f'Failures will be retried on the next run (up to max_attempts).'
    )
    if failed > enriched * 0.1 and total > 100:
        # More than 10% failure rate — treat as a warning
        notify_failure(
            'Enrichment completed with elevated failure rate',
            f'{summary}\n\n{details}',
            run_id=run_id,
        )
    else:
        notify_success(summary, details=details)


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Maine Listings (MREIS MLS) Transaction Scraper',
    )
    parser.add_argument('--discover', action='store_true',
                        help='Discover listings from search pages')
    parser.add_argument('--status', type=str, default='Closed',
                        choices=['Active', 'Closed'],
                        help='Listing status to discover (default: Closed)')
    parser.add_argument('--enrich', action='store_true',
                        help='Enrich listings with agent data from detail pages')
    parser.add_argument('--report', action='store_true',
                        help='Generate leaderboard markdown + HTML dashboard')
    parser.add_argument('--update-index', action='store_true',
                        help='Regenerate the unified index.html with all three sources')
    parser.add_argument('--towns', type=str, default=None,
                        help='Comma-separated town names')
    parser.add_argument('--max-pages', type=int, default=90,
                        help='Max search result pages per town (default: 90)')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='Detail pages to enrich per run (default: 50)')
    parser.add_argument('--recent-only', action='store_true',
                        help='Stop discovery when hitting known listings')
    parser.add_argument('--workers', type=int, default=1,
                        help='Concurrent workers for discovery/enrichment '
                             '(default: 1, max: 50). Discovery workers cap at num_towns.')
    parser.add_argument('--db', type=str, default=None,
                        help='Path to SQLite database')
    parser.add_argument('--state', type=str, default=None,
                        help='Path to state JSON file')
    args = parser.parse_args()

    if args.workers < 1 or args.workers > 50:
        parser.error('--workers must be between 1 and 50')

    if not (args.discover or args.enrich or args.report or args.update_index):
        parser.print_help()
        return 0

    towns = _parse_towns(args.towns)
    conn = get_connection(args.db)
    init_db(conn)
    state = load_state(args.state)

    if args.discover:
        logger.info('Starting Maine Listings discovery (status=%s, max_pages=%d, recent_only=%s, workers=%d)...',
                     args.status, args.max_pages, args.recent_only, args.workers)
        result = discover_listings(
            conn, state,
            towns=towns,
            max_pages=args.max_pages,
            recent_only=args.recent_only,
            state_path=args.state,
            workers=args.workers,
            status=args.status,
        )
        logger.info('Discovery: %d towns, %d listings found (new=%d, status_changes=%d)',
                     result['towns'], result['listings'],
                     result.get('new_listings', 0),
                     result.get('status_changes', 0))

    if args.enrich:
        _backup_db(args.db)
        run_id = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        logger.info('Starting detail page enrichment (batch=%d, workers=%d, run=%s)...',
                     args.batch_size, args.workers, run_id)
        try:
            result = enrich_listings(
                conn,
                batch_size=args.batch_size,
                workers=args.workers,
                db_path=args.db,
            )
        except Exception as exc:
            notify_failure(
                'Enrichment crashed unexpectedly',
                f'{type(exc).__name__}: {exc}',
                run_id=run_id,
            )
            raise
        logger.info('Enrichment: %d enriched, %d failed, %d total%s',
                     result['enriched'], result['failed'], result['total'],
                     ' (ABORTED by circuit breaker)' if result.get('aborted') else '')
        _notify_enrichment_result(result, run_id)

    if args.report:
        from .maine_dashboard import generate_maine_dashboard
        from .maine_report import generate_leaderboard
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
        generate_leaderboard(conn)
        generate_maine_dashboard(conn)

    if args.update_index:
        from .database import get_connection as get_redfin_conn
        from .database import get_zillow_connection
        from .index_page import generate_index_html
        generate_index_html(
            redfin_conn=get_redfin_conn(),
            zillow_conn=get_zillow_connection(),
            maine_conn=conn,
        )

    save_state(state, args.state)
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
