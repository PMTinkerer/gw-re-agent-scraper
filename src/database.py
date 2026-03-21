"""Database module for agent listing data.

Handles SQLite schema, transaction upserts, agent name normalization,
fuzzy deduplication, and ranking queries.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

# Default database path relative to project root
_DEFAULT_DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'agent_data.db')

# Agent designations to strip during normalization
_DESIGNATIONS = re.compile(
    r',?\s*\b('
    r'CRS|ABR|GRI|SRES|SRS|e-?PRO|CDPE|CNE|CLHMS|RSPS|PSA|MRP|AHWD|RENE'
    r'|C2EX|GREEN|SFR|BPOR|REALTOR|PA|BROKER|LIC|ASSOCIATE'
    r')\b\.?',
    re.IGNORECASE,
)

# Trailing junk after designation removal
_TRAILING_JUNK = re.compile(r'[,.\s]+$')


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Return a connection with Row factory enabled."""
    path = db_path or _DEFAULT_DB
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist."""
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mls_number TEXT UNIQUE NOT NULL,
            address TEXT,
            city TEXT,
            state TEXT DEFAULT 'ME',
            zip TEXT,
            sale_price INTEGER,
            list_price INTEGER,
            beds INTEGER,
            baths REAL,
            sqft INTEGER,
            year_built INTEGER,
            days_on_market INTEGER,
            sale_date TEXT,
            listing_agent TEXT,
            buyer_agent TEXT,
            listing_office TEXT,
            buyer_office TEXT,
            source_url TEXT,
            data_source TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            raw_listing_agent TEXT,
            raw_buyer_agent TEXT,
            enrichment_status TEXT,
            enrichment_attempts INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS agent_rankings (
            agent_name TEXT,
            listing_office TEXT,
            total_listing_sides INTEGER,
            listing_volume INTEGER,
            avg_listing_price INTEGER,
            high_value_listings INTEGER,
            primary_towns TEXT,
            most_recent_sale TEXT,
            last_updated TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_transactions_city ON transactions(city);
        CREATE INDEX IF NOT EXISTS idx_transactions_listing_agent ON transactions(listing_agent);
        CREATE INDEX IF NOT EXISTS idx_transactions_sale_date ON transactions(sale_date);
    ''')

    # Migrate existing databases: add enrichment columns (no-op for new databases)
    for col, typedef in [
        ('enrichment_status', 'TEXT'),
        ('enrichment_attempts', 'INTEGER DEFAULT 0'),
    ]:
        try:
            conn.execute(f'ALTER TABLE transactions ADD COLUMN {col} {typedef}')
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.execute('CREATE INDEX IF NOT EXISTS idx_transactions_enrichment ON transactions(enrichment_status)')
    conn.commit()


def normalize_agent_name(name: str | None) -> str | None:
    """Normalize an agent name for consistent ranking.

    - Strip whitespace
    - Remove designations (CRS, ABR, GRI, etc.)
    - Title case
    - Collapse multiple spaces
    """
    if not name or not name.strip():
        return None
    result = name.strip()
    # Remove designations
    result = _DESIGNATIONS.sub('', result)
    # Remove trailing junk left by designation removal
    result = _TRAILING_JUNK.sub('', result)
    # Remove parenthesized suffixes like "(Broker)"
    result = re.sub(r'\s*\(.*?\)\s*', ' ', result)
    # Collapse multiple spaces
    result = re.sub(r'\s+', ' ', result).strip()
    # Title case
    result = result.title()
    if not result:
        return None
    return result


def upsert_transaction(conn: sqlite3.Connection, record: dict) -> bool:
    """Insert or update a transaction, deduplicating on mls_number.

    Returns True if a row was inserted/updated, False if skipped.
    """
    mls = record.get('mls_number')
    if not mls or not str(mls).strip():
        return False

    # Preserve raw agent names before normalization
    raw_listing = record.get('listing_agent')
    raw_buyer = record.get('buyer_agent')
    normalized_listing = normalize_agent_name(raw_listing)
    normalized_buyer = normalize_agent_name(raw_buyer)

    try:
        conn.execute('''
            INSERT INTO transactions (
                mls_number, address, city, state, zip,
                sale_price, list_price, beds, baths, sqft,
                year_built, days_on_market, sale_date,
                listing_agent, buyer_agent, listing_office, buyer_office,
                source_url, data_source, scraped_at,
                raw_listing_agent, raw_buyer_agent
            ) VALUES (
                :mls_number, :address, :city, :state, :zip,
                :sale_price, :list_price, :beds, :baths, :sqft,
                :year_built, :days_on_market, :sale_date,
                :listing_agent, :buyer_agent, :listing_office, :buyer_office,
                :source_url, :data_source, :scraped_at,
                :raw_listing_agent, :raw_buyer_agent
            )
            ON CONFLICT(mls_number) DO UPDATE SET
                sale_price = COALESCE(excluded.sale_price, transactions.sale_price),
                list_price = COALESCE(excluded.list_price, transactions.list_price),
                listing_agent = COALESCE(excluded.listing_agent, transactions.listing_agent),
                buyer_agent = COALESCE(excluded.buyer_agent, transactions.buyer_agent),
                listing_office = COALESCE(excluded.listing_office, transactions.listing_office),
                buyer_office = COALESCE(excluded.buyer_office, transactions.buyer_office),
                sale_date = COALESCE(excluded.sale_date, transactions.sale_date),
                scraped_at = excluded.scraped_at
        ''', {
            'mls_number': str(mls).strip(),
            'address': record.get('address'),
            'city': record.get('city'),
            'state': record.get('state', 'ME'),
            'zip': record.get('zip'),
            'sale_price': _to_int(record.get('sale_price')),
            'list_price': _to_int(record.get('list_price')),
            'beds': _to_int(record.get('beds')),
            'baths': _to_float(record.get('baths')),
            'sqft': _to_int(record.get('sqft')),
            'year_built': _to_int(record.get('year_built')),
            'days_on_market': _to_int(record.get('days_on_market')),
            'sale_date': record.get('sale_date'),
            'listing_agent': normalized_listing,
            'buyer_agent': normalized_buyer,
            'listing_office': record.get('listing_office'),
            'buyer_office': record.get('buyer_office'),
            'source_url': record.get('source_url'),
            'data_source': record.get('data_source'),
            'scraped_at': record.get('scraped_at', datetime.utcnow().isoformat()),
            'raw_listing_agent': raw_listing,
            'raw_buyer_agent': raw_buyer,
        })
        return True
    except sqlite3.IntegrityError as e:
        logger.warning('Insert failed for MLS %s: %s', mls, e)
        return False


def fuzzy_merge_agents(conn: sqlite3.Connection, threshold: int = 90) -> list[tuple[str, str]]:
    """Merge near-duplicate agent names that share the same office.

    Returns list of (old_name, merged_into) tuples for audit.
    """
    try:
        from rapidfuzz import fuzz
    except ImportError:
        logger.warning('rapidfuzz not installed — skipping fuzzy merge')
        return []

    # Get agents sorted by frequency (most common first)
    rows = conn.execute('''
        SELECT listing_agent, listing_office, COUNT(*) as cnt
        FROM transactions
        WHERE listing_agent IS NOT NULL
        GROUP BY listing_agent, listing_office
        ORDER BY cnt DESC
    ''').fetchall()

    merges = []
    merged_away = set()  # Names already merged into something else

    for i, row_i in enumerate(rows):
        name_i = row_i['listing_agent']
        office_i = row_i['listing_office']
        if name_i in merged_away:
            continue

        for j in range(i + 1, len(rows)):
            name_j = rows[j]['listing_agent']
            office_j = rows[j]['listing_office']
            if name_j in merged_away:
                continue

            # Must share office (or both None)
            if office_i != office_j:
                continue

            score = fuzz.ratio(name_i, name_j)
            if score >= threshold:
                # Merge less common into more common
                logger.info(
                    'Merging agent "%s" (%d sales) into "%s" (%d sales) [score=%d]',
                    name_j, rows[j]['cnt'], name_i, row_i['cnt'], score,
                )
                conn.execute(
                    'UPDATE transactions SET listing_agent = ? WHERE listing_agent = ?',
                    (name_i, name_j),
                )
                merges.append((name_j, name_i))
                merged_away.add(name_j)

    if merges:
        conn.commit()
    return merges


def rebuild_rankings(conn: sqlite3.Connection) -> None:
    """Drop and rebuild the agent_rankings table from transactions."""
    now = datetime.utcnow().isoformat()

    conn.execute('DELETE FROM agent_rankings')

    conn.execute('''
        INSERT INTO agent_rankings (
            agent_name, listing_office, total_listing_sides, listing_volume,
            avg_listing_price, high_value_listings, primary_towns,
            most_recent_sale, last_updated
        )
        SELECT
            listing_agent,
            (
                SELECT listing_office FROM transactions t2
                WHERE t2.listing_agent = t.listing_agent
                    AND t2.listing_office IS NOT NULL
                GROUP BY listing_office
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) as primary_office,
            COUNT(*) as total_listing_sides,
            SUM(COALESCE(sale_price, list_price, 0)) as listing_volume,
            AVG(COALESCE(sale_price, list_price, 0)) as avg_listing_price,
            SUM(CASE WHEN COALESCE(sale_price, list_price, 0) >= 500000 THEN 1 ELSE 0 END) as high_value,
            (
                SELECT GROUP_CONCAT(city, ', ')
                FROM (
                    SELECT city, COUNT(*) as cnt
                    FROM transactions t3
                    WHERE t3.listing_agent = t.listing_agent
                        AND t3.city IS NOT NULL
                    GROUP BY city
                    ORDER BY cnt DESC
                    LIMIT 3
                )
            ) as primary_towns,
            MAX(sale_date) as most_recent_sale,
            :now as last_updated
        FROM transactions t
        WHERE listing_agent IS NOT NULL
        GROUP BY listing_agent
        ORDER BY listing_volume DESC
    ''', {'now': now})

    conn.commit()
    logger.info('Rankings rebuilt at %s', now)


def get_stats(conn: sqlite3.Connection) -> dict:
    """Return summary statistics about the database."""
    total = conn.execute('SELECT COUNT(*) FROM transactions').fetchone()[0]
    with_agent = conn.execute(
        'SELECT COUNT(*) FROM transactions WHERE listing_agent IS NOT NULL'
    ).fetchone()[0]

    date_range = conn.execute(
        'SELECT MIN(sale_date), MAX(sale_date) FROM transactions WHERE sale_date IS NOT NULL'
    ).fetchone()

    sources = conn.execute(
        'SELECT data_source, COUNT(*) FROM transactions GROUP BY data_source'
    ).fetchall()

    towns = conn.execute(
        'SELECT city, COUNT(*) FROM transactions WHERE city IS NOT NULL GROUP BY city ORDER BY COUNT(*) DESC'
    ).fetchall()

    return {
        'total_transactions': total,
        'with_listing_agent': with_agent,
        'date_range': (date_range[0], date_range[1]) if date_range else (None, None),
        'sources': {r[0]: r[1] for r in sources},
        'towns': {r[0]: r[1] for r in towns},
    }


# --- Enrichment helpers ---

def get_enrichment_queue(conn: sqlite3.Connection, batch_size: int = 200) -> list[dict]:
    """Return transactions needing agent enrichment.

    Includes records never attempted (NULL) and failed records with < 3 attempts.
    """
    rows = conn.execute('''
        SELECT id, mls_number, source_url
        FROM transactions
        WHERE (enrichment_status IS NULL
               OR (enrichment_status = 'error' AND enrichment_attempts < 3))
          AND source_url IS NOT NULL
        ORDER BY id
        LIMIT ?
    ''', (batch_size,)).fetchall()
    return [dict(r) for r in rows]


def set_enrichment_status(
    conn: sqlite3.Connection,
    mls_number: str,
    status: str,
    agent_data: dict | None = None,
) -> None:
    """Update the enrichment status for a transaction.

    Args:
        status: 'success', 'no_agent', or 'error'
        agent_data: If provided, dict with listing_agent/listing_office/buyer_agent/buyer_office
                    keys — merged into the record via upsert_transaction.
    """
    if status == 'success' and agent_data:
        # Use upsert to merge agent data (COALESCE preserves existing values)
        record = {
            'mls_number': mls_number,
            'data_source': 'redfin',
            **agent_data,
        }
        upsert_transaction(conn, record)

    conn.execute('''
        UPDATE transactions
        SET enrichment_status = ?,
            enrichment_attempts = COALESCE(enrichment_attempts, 0) + 1
        WHERE mls_number = ?
    ''', (status, mls_number))
    conn.commit()


def get_enrichment_stats(conn: sqlite3.Connection) -> dict:
    """Return enrichment progress counts."""
    total = conn.execute(
        'SELECT COUNT(*) FROM transactions WHERE source_url IS NOT NULL'
    ).fetchone()[0]
    pending = conn.execute(
        'SELECT COUNT(*) FROM transactions WHERE source_url IS NOT NULL AND enrichment_status IS NULL'
    ).fetchone()[0]
    success = conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE enrichment_status = 'success'"
    ).fetchone()[0]
    no_agent = conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE enrichment_status = 'no_agent'"
    ).fetchone()[0]
    error = conn.execute(
        "SELECT COUNT(*) FROM transactions WHERE enrichment_status = 'error'"
    ).fetchone()[0]
    return {
        'total': total,
        'pending': pending,
        'success': success,
        'no_agent': no_agent,
        'error': error,
    }


# --- Helpers ---

def _to_int(val) -> int | None:
    """Convert a value to int, returning None on failure."""
    if val is None:
        return None
    try:
        # Handle strings like "$1,234,567" or "1234567"
        cleaned = str(val).replace('$', '').replace(',', '').strip()
        if not cleaned:
            return None
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def _to_float(val) -> float | None:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        cleaned = str(val).replace(',', '').strip()
        if not cleaned:
            return None
        return float(cleaned)
    except (ValueError, TypeError):
        return None
