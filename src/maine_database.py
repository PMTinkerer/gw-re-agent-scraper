"""Database functions for Maine Listings (MREIS MLS) pipeline.

Manages the maine_listings.db SQLite database with transaction data
including both listing and buyer agent information from the MLS.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

_DEFAULT_DB = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'maine_listings.db',
)


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Return a connection with Row factory enabled."""
    path = db_path or _DEFAULT_DB
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create the maine_transactions table and indexes."""
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS maine_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mls_number TEXT,
            address TEXT,
            city TEXT,
            state TEXT DEFAULT 'ME',
            zip TEXT,
            sale_price INTEGER,
            list_price INTEGER,
            beds INTEGER,
            baths REAL,
            sqft INTEGER,
            property_type TEXT,
            days_on_market INTEGER,
            close_date TEXT,
            listing_agent TEXT,
            listing_agent_id TEXT,
            listing_agent_email TEXT,
            listing_office TEXT,
            buyer_agent TEXT,
            buyer_agent_id TEXT,
            buyer_agent_email TEXT,
            buyer_office TEXT,
            detail_url TEXT UNIQUE,
            listing_key TEXT,
            enrichment_status TEXT,
            enrichment_attempts INTEGER DEFAULT 0,
            discovered_at TEXT NOT NULL,
            enriched_at TEXT,
            scraped_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_maine_city
            ON maine_transactions(city);
        CREATE INDEX IF NOT EXISTS idx_maine_close_date
            ON maine_transactions(close_date);
        CREATE INDEX IF NOT EXISTS idx_maine_listing_agent
            ON maine_transactions(listing_agent);
        CREATE INDEX IF NOT EXISTS idx_maine_buyer_agent
            ON maine_transactions(buyer_agent);
        CREATE INDEX IF NOT EXISTS idx_maine_enrichment
            ON maine_transactions(enrichment_status);
        CREATE INDEX IF NOT EXISTS idx_maine_mls
            ON maine_transactions(mls_number);
    ''')
    conn.commit()


def upsert_listing(conn: sqlite3.Connection, record: dict) -> bool:
    """Insert or update a listing discovered from search pages."""
    now = datetime.utcnow().isoformat()
    try:
        conn.execute('''
            INSERT INTO maine_transactions (
                address, city, state, zip, sale_price,
                beds, baths, sqft, listing_office,
                detail_url, discovered_at, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(detail_url) DO UPDATE SET
                sale_price = COALESCE(excluded.sale_price, maine_transactions.sale_price),
                listing_office = COALESCE(excluded.listing_office, maine_transactions.listing_office)
        ''', (
            record.get('address'), record.get('city'), record.get('state', 'ME'),
            record.get('zip'), record.get('sale_price'),
            record.get('beds'), record.get('baths'), record.get('sqft'),
            record.get('listing_office'), record.get('detail_url'),
            now, now,
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError as e:
        logger.debug('Insert failed for %s: %s', record.get('detail_url'), e)
        return False


def enrich_listing(conn: sqlite3.Connection, detail_url: str, data: dict) -> bool:
    """Update a listing with agent data from detail page scraping."""
    now = datetime.utcnow().isoformat()
    conn.execute('''
        UPDATE maine_transactions SET
            mls_number = COALESCE(?, mls_number),
            listing_agent = ?,
            listing_agent_id = ?,
            listing_agent_email = ?,
            listing_office = COALESCE(?, listing_office),
            buyer_agent = ?,
            buyer_agent_id = ?,
            buyer_agent_email = ?,
            buyer_office = ?,
            close_date = ?,
            sale_price = COALESCE(?, sale_price),
            list_price = ?,
            property_type = ?,
            days_on_market = ?,
            enrichment_status = 'success',
            enrichment_attempts = enrichment_attempts + 1,
            enriched_at = ?
        WHERE detail_url = ?
    ''', (
        data.get('mls_number'),
        data.get('listing_agent'), data.get('listing_agent_id'),
        data.get('listing_agent_email'), data.get('listing_office'),
        data.get('buyer_agent'), data.get('buyer_agent_id'),
        data.get('buyer_agent_email'), data.get('buyer_office'),
        data.get('close_date'), data.get('sale_price'),
        data.get('list_price'), data.get('property_type'),
        data.get('days_on_market'),
        now, detail_url,
    ))
    conn.commit()
    return True


def mark_enrichment_failed(
    conn: sqlite3.Connection, detail_url: str, error: str,
) -> None:
    """Mark a listing enrichment as failed."""
    now = datetime.utcnow().isoformat()
    conn.execute('''
        UPDATE maine_transactions SET
            enrichment_status = 'error',
            enrichment_attempts = enrichment_attempts + 1,
            enriched_at = ?
        WHERE detail_url = ?
    ''', (now, detail_url))
    conn.commit()


def get_unenriched(
    conn: sqlite3.Connection, batch_size: int = 50, max_attempts: int = 2,
) -> list[dict]:
    """Return listings needing detail page enrichment."""
    rows = conn.execute('''
        SELECT detail_url, city FROM maine_transactions
        WHERE (enrichment_status IS NULL OR enrichment_status = 'error')
          AND enrichment_attempts < ?
        ORDER BY discovered_at DESC
        LIMIT ?
    ''', (max_attempts, batch_size)).fetchall()
    return [dict(r) for r in rows]


def url_exists(conn: sqlite3.Connection, detail_url: str) -> bool:
    """Check if a listing URL already exists in the database."""
    row = conn.execute(
        'SELECT 1 FROM maine_transactions WHERE detail_url = ? LIMIT 1',
        (detail_url,),
    ).fetchone()
    return row is not None
