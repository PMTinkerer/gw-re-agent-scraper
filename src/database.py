"""Database module for agent listing data.

Handles SQLite schema, transaction upserts, agent name normalization,
fuzzy deduplication, and ranking queries.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

# Default database path relative to project root
_DEFAULT_DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'agent_data.db')
_DEFAULT_ZILLOW_DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'zillow_agent_data.db')

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

# Office name normalization: map variant spellings to canonical name.
# Applied at upsert time so the DB stays clean.
OFFICE_NORMALIZATION = {
    "anne erwin sothebys international rlty": "Anne Erwin Sotheby's International Realty",
    "the aland realty group": "The Aland Realty Group, LLC",
    "kw coastal and lakes & mountains realty": "Keller Williams Coastal and Lakes & Mountains Realty",
    "kw coastal and lakes & mountains realty/portsmouth": "Keller Williams Coastal and Lakes & Mountains Realty",
    "century 21 atlantic realty": "CENTURY 21 Atlantic Realty",
    "century 21 barbara patterson": "CENTURY 21 Barbara Patterson",
    "better homes and gardens real estate - the masiello group": "Better Homes & Gardens Real Estate/The Masiello Group",
    "signature homes real estate group": "Signature Homes Real Estate Group, LLC",
    "great island realty llc": "Great Island Realty, LLC",
    "great island realty, llc": "Great Island Realty, LLC",
    "landvest, inc.": "LandVest, Inc.",
    "abigail douris real estatellc": "Abigail Douris Real Estate LLC",
    "red post realty": "Red Post Realty, LLC",
    "cameron prestige llc": "Cameron Prestige, LLC",
    "samonas realty": "Samonas Realty, LLC",
    "carey giampa, llc/rye": "Carey & Giampa, LLC",
}

# Agents that are actually brokerages — excluded from agent rankings.
# These are cases where listing_agent is a company name, not a person.
# Checked case-insensitively against the normalized listing_agent field.
BROKERAGE_AS_AGENT = {
    "anne erwin real estate",
    "anchor real estate",
}

_ADDRESS_PUNCTUATION_RE = re.compile(r'[^A-Z0-9\s]')
_ADDRESS_WHITESPACE_RE = re.compile(r'\s+')
_ZIP_RE = re.compile(r'(\d{5})')
_UNIT_TOKEN_RE = re.compile(
    r'\b(?:APARTMENT|APT|UNIT|SUITE|STE|#)\s*([A-Z0-9-]+)\b',
    re.IGNORECASE,
)
_HASH_UNIT_RE = re.compile(r'#\s*([A-Z0-9-]+)\b', re.IGNORECASE)
_ADDRESS_TOKEN_MAP = {
    'AVENUE': 'AVE',
    'BOULEVARD': 'BLVD',
    'CIRCLE': 'CIR',
    'COURT': 'CT',
    'DRIVE': 'DR',
    'HIGHWAY': 'HWY',
    'LANE': 'LN',
    'PLACE': 'PL',
    'ROAD': 'RD',
    'SQUARE': 'SQ',
    'STREET': 'ST',
    'TERRACE': 'TER',
}
_ROLE_TO_COLUMNS = {
    'seller': ('listing_agent', 'listing_office'),
    'buyer': ('buyer_agent', 'buyer_office'),
}


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Return a connection with Row factory enabled."""
    path = db_path or _DEFAULT_DB
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def get_zillow_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Return a Zillow connection with Row factory enabled."""
    return get_connection(db_path or _DEFAULT_ZILLOW_DB)


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
    # Safety: SQLite doesn't support parameterized DDL, so col/typedef are
    # validated below. These values come ONLY from this hardcoded list.
    for col, typedef in [
        ('enrichment_status', 'TEXT'),
        ('enrichment_attempts', 'INTEGER DEFAULT 0'),
        ('property_type', 'TEXT'),
    ]:
        assert col.isidentifier(), f'Invalid column name for migration: {col}'
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


def normalize_office_name(name: str | None) -> str | None:
    """Normalize an office name using the canonical map."""
    if not name or not name.strip():
        return None
    stripped = name.strip()
    canonical = OFFICE_NORMALIZATION.get(stripped.lower())
    return canonical if canonical else stripped


def normalize_address(
    address: str | None,
    city: str | None = None,
    state: str | None = None,
    zip_code: str | None = None,
) -> str | None:
    """Normalize address components into a stable uppercase dedup string."""
    if not any([address, city, state, zip_code]):
        return None

    street = (address or '').upper().strip()
    street = street.replace('&', ' AND ')
    street = _HASH_UNIT_RE.sub(r' UNIT \1', street)
    street = _UNIT_TOKEN_RE.sub(r' UNIT \1', street)
    street = _ADDRESS_PUNCTUATION_RE.sub(' ', street)
    street = _ADDRESS_WHITESPACE_RE.sub(' ', street).strip()
    if street:
        tokens = [_ADDRESS_TOKEN_MAP.get(token, token) for token in street.split()]
        street = ' '.join(tokens)

    normalized_city = _ADDRESS_PUNCTUATION_RE.sub(' ', (city or '').upper())
    normalized_city = _ADDRESS_WHITESPACE_RE.sub(' ', normalized_city).strip()
    normalized_state = _ADDRESS_PUNCTUATION_RE.sub(' ', (state or '').upper())
    normalized_state = _ADDRESS_WHITESPACE_RE.sub(' ', normalized_state).strip()

    zip_match = _ZIP_RE.search(str(zip_code or ''))
    normalized_zip = zip_match.group(1) if zip_match else ''

    parts = [part for part in [street, normalized_city, normalized_state, normalized_zip] if part]
    return ' | '.join(parts) if parts else None


def sha256_text(value: str | None) -> str | None:
    """Return a SHA-256 hex digest for the given string."""
    if value is None:
        return None
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def build_transaction_match_key(
    normalized_address_hash: str | None,
    sale_date: str | None,
    sale_price,
) -> str | None:
    """Build the future cross-source transaction dedup key."""
    if not normalized_address_hash or not sale_date or sale_price in (None, ''):
        return None
    return sha256_text(f'{normalized_address_hash}|{sale_date}|{_to_int(sale_price)}')


def build_observation_id(
    agent_profile_url: str | None,
    represented_side: str | None,
    normalized_address_hash: str | None,
    sale_date: str | None,
    sale_price,
) -> str | None:
    """Build a stable Zillow row identifier."""
    if not all([agent_profile_url, represented_side, normalized_address_hash, sale_date]):
        return None
    price = _to_int(sale_price)
    if price is None:
        return None
    return sha256_text(
        f'{agent_profile_url}|{represented_side.lower()}|{normalized_address_hash}|{sale_date}|{price}'
    )


def get_role_columns(role: str) -> tuple[str, str]:
    """Return the agent/office columns for a logical role."""
    columns = _ROLE_TO_COLUMNS.get(role.lower())
    if not columns:
        raise ValueError(f'Unknown role: {role}')
    return columns


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

    # Normalize office names to canonical forms
    listing_office = normalize_office_name(record.get('listing_office'))
    buyer_office = normalize_office_name(record.get('buyer_office'))

    try:
        conn.execute('''
            INSERT INTO transactions (
                mls_number, address, city, state, zip,
                sale_price, list_price, beds, baths, sqft,
                year_built, days_on_market, sale_date,
                listing_agent, buyer_agent, listing_office, buyer_office,
                source_url, data_source, scraped_at,
                raw_listing_agent, raw_buyer_agent, property_type
            ) VALUES (
                :mls_number, :address, :city, :state, :zip,
                :sale_price, :list_price, :beds, :baths, :sqft,
                :year_built, :days_on_market, :sale_date,
                :listing_agent, :buyer_agent, :listing_office, :buyer_office,
                :source_url, :data_source, :scraped_at,
                :raw_listing_agent, :raw_buyer_agent, :property_type
            )
            ON CONFLICT(mls_number) DO UPDATE SET
                sale_price = COALESCE(excluded.sale_price, transactions.sale_price),
                list_price = COALESCE(excluded.list_price, transactions.list_price),
                listing_agent = COALESCE(excluded.listing_agent, transactions.listing_agent),
                buyer_agent = COALESCE(excluded.buyer_agent, transactions.buyer_agent),
                listing_office = COALESCE(excluded.listing_office, transactions.listing_office),
                buyer_office = COALESCE(excluded.buyer_office, transactions.buyer_office),
                sale_date = COALESCE(excluded.sale_date, transactions.sale_date),
                scraped_at = excluded.scraped_at,
                raw_listing_agent = COALESCE(excluded.raw_listing_agent, transactions.raw_listing_agent),
                raw_buyer_agent = COALESCE(excluded.raw_buyer_agent, transactions.raw_buyer_agent),
                property_type = COALESCE(excluded.property_type, transactions.property_type)
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
            'listing_office': listing_office,
            'buyer_office': buyer_office,
            'source_url': record.get('source_url'),
            'data_source': record.get('data_source'),
            'scraped_at': record.get('scraped_at', datetime.utcnow().isoformat()),
            'raw_listing_agent': raw_listing,
            'raw_buyer_agent': raw_buyer,
            'property_type': record.get('property_type'),
        })
        return True
    except sqlite3.IntegrityError as e:
        logger.warning('Insert failed for MLS %s: %s', mls, e)
        return False


def init_zillow_db(conn: sqlite3.Connection) -> None:
    """Create Zillow-specific schema on top of the shared transactions table."""
    init_db(conn)

    for col, typedef in [
        ('represented_side', 'TEXT'),
        ('agent_profile_url', 'TEXT'),
        ('profile_type', 'TEXT'),
        ('normalized_address', 'TEXT'),
        ('normalized_address_hash', 'TEXT'),
        ('transaction_match_key', 'TEXT'),
        ('observation_id', 'TEXT'),
        ('local_directory_town', 'TEXT'),
        ('attribution_confidence', 'TEXT'),
    ]:
        assert col.isidentifier(), f'Invalid Zillow column name: {col}'
        try:
            conn.execute(f'ALTER TABLE transactions ADD COLUMN {col} {typedef}')
        except sqlite3.OperationalError:
            pass

    conn.executescript('''
        CREATE TABLE IF NOT EXISTS zillow_profiles (
            profile_url TEXT PRIMARY KEY,
            profile_type TEXT NOT NULL,
            profile_name TEXT,
            office_name TEXT,
            raw_card_text TEXT,
            sales_last_12_months INTEGER,
            total_sales INTEGER,
            average_price INTEGER,
            price_range TEXT,
            scrape_status TEXT,
            scrape_attempts INTEGER DEFAULT 0,
            last_error TEXT,
            discovered_at TEXT NOT NULL,
            last_scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS zillow_profile_towns (
            profile_url TEXT NOT NULL,
            town TEXT NOT NULL,
            local_sales_count INTEGER DEFAULT 0,
            discovered_at TEXT NOT NULL,
            PRIMARY KEY (profile_url, town)
        );

        CREATE TABLE IF NOT EXISTS zillow_team_members (
            team_profile_url TEXT NOT NULL,
            member_profile_url TEXT NOT NULL,
            member_name TEXT,
            discovered_at TEXT NOT NULL,
            PRIMARY KEY (team_profile_url, member_profile_url)
        );

        CREATE TABLE IF NOT EXISTS team_only_sales_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_profile_url TEXT NOT NULL,
            team_name TEXT,
            property_url TEXT,
            represented_side TEXT NOT NULL,
            sale_date TEXT,
            sale_price INTEGER,
            normalized_address TEXT,
            normalized_address_hash TEXT,
            transaction_match_key TEXT,
            local_town TEXT,
            resolved_by_member INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(team_profile_url, represented_side, transaction_match_key)
        );

        CREATE INDEX IF NOT EXISTS idx_transactions_match_key ON transactions(transaction_match_key);
        CREATE INDEX IF NOT EXISTS idx_transactions_profile_url ON transactions(agent_profile_url);
        CREATE INDEX IF NOT EXISTS idx_transactions_represented_side ON transactions(represented_side);
        CREATE INDEX IF NOT EXISTS idx_zillow_profiles_status ON zillow_profiles(scrape_status, profile_type);
        CREATE INDEX IF NOT EXISTS idx_zillow_profile_towns_town ON zillow_profile_towns(town);
        CREATE INDEX IF NOT EXISTS idx_team_only_sales_log_resolved ON team_only_sales_log(resolved_by_member);
        CREATE INDEX IF NOT EXISTS idx_team_only_sales_log_match_key
            ON team_only_sales_log(transaction_match_key, represented_side);
    ''')
    conn.commit()


def upsert_zillow_transaction(conn: sqlite3.Connection, record: dict) -> bool:
    """Insert or update a Zillow observation in the Zillow transactions table."""
    normalized_address = record.get('normalized_address') or normalize_address(
        record.get('address'),
        record.get('city'),
        record.get('state'),
        record.get('zip'),
    )
    normalized_address_hash = (
        record.get('normalized_address_hash') or sha256_text(normalized_address)
    )
    transaction_match_key = record.get('transaction_match_key') or build_transaction_match_key(
        normalized_address_hash,
        record.get('sale_date'),
        record.get('sale_price'),
    )
    observation_id = record.get('observation_id') or build_observation_id(
        record.get('agent_profile_url'),
        record.get('represented_side'),
        normalized_address_hash,
        record.get('sale_date'),
        record.get('sale_price'),
    )
    if not observation_id:
        return False

    raw_listing = record.get('listing_agent')
    raw_buyer = record.get('buyer_agent')
    normalized_listing = normalize_agent_name(raw_listing)
    normalized_buyer = normalize_agent_name(raw_buyer)
    listing_office = normalize_office_name(record.get('listing_office'))
    buyer_office = normalize_office_name(record.get('buyer_office'))

    try:
        conn.execute('''
            INSERT INTO transactions (
                mls_number, address, city, state, zip,
                sale_price, list_price, beds, baths, sqft,
                year_built, days_on_market, sale_date,
                listing_agent, buyer_agent, listing_office, buyer_office,
                source_url, data_source, scraped_at,
                raw_listing_agent, raw_buyer_agent, property_type,
                represented_side, agent_profile_url, profile_type,
                normalized_address, normalized_address_hash, transaction_match_key,
                observation_id, local_directory_town, attribution_confidence
            ) VALUES (
                :mls_number, :address, :city, :state, :zip,
                :sale_price, :list_price, :beds, :baths, :sqft,
                :year_built, :days_on_market, :sale_date,
                :listing_agent, :buyer_agent, :listing_office, :buyer_office,
                :source_url, :data_source, :scraped_at,
                :raw_listing_agent, :raw_buyer_agent, :property_type,
                :represented_side, :agent_profile_url, :profile_type,
                :normalized_address, :normalized_address_hash, :transaction_match_key,
                :observation_id, :local_directory_town, :attribution_confidence
            )
            ON CONFLICT(mls_number) DO UPDATE SET
                sale_price = COALESCE(excluded.sale_price, transactions.sale_price),
                list_price = COALESCE(excluded.list_price, transactions.list_price),
                sale_date = COALESCE(excluded.sale_date, transactions.sale_date),
                listing_agent = COALESCE(excluded.listing_agent, transactions.listing_agent),
                buyer_agent = COALESCE(excluded.buyer_agent, transactions.buyer_agent),
                listing_office = COALESCE(excluded.listing_office, transactions.listing_office),
                buyer_office = COALESCE(excluded.buyer_office, transactions.buyer_office),
                source_url = COALESCE(excluded.source_url, transactions.source_url),
                scraped_at = excluded.scraped_at,
                raw_listing_agent = COALESCE(excluded.raw_listing_agent, transactions.raw_listing_agent),
                raw_buyer_agent = COALESCE(excluded.raw_buyer_agent, transactions.raw_buyer_agent),
                represented_side = COALESCE(excluded.represented_side, transactions.represented_side),
                agent_profile_url = COALESCE(excluded.agent_profile_url, transactions.agent_profile_url),
                profile_type = COALESCE(excluded.profile_type, transactions.profile_type),
                normalized_address = COALESCE(excluded.normalized_address, transactions.normalized_address),
                normalized_address_hash = COALESCE(excluded.normalized_address_hash, transactions.normalized_address_hash),
                transaction_match_key = COALESCE(excluded.transaction_match_key, transactions.transaction_match_key),
                observation_id = COALESCE(excluded.observation_id, transactions.observation_id),
                local_directory_town = COALESCE(excluded.local_directory_town, transactions.local_directory_town),
                attribution_confidence = COALESCE(excluded.attribution_confidence, transactions.attribution_confidence)
        ''', {
            'mls_number': observation_id,
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
            'listing_office': listing_office,
            'buyer_office': buyer_office,
            'source_url': record.get('source_url'),
            'data_source': record.get('data_source', 'zillow'),
            'scraped_at': record.get('scraped_at', datetime.utcnow().isoformat()),
            'raw_listing_agent': raw_listing,
            'raw_buyer_agent': raw_buyer,
            'property_type': record.get('property_type'),
            'represented_side': record.get('represented_side'),
            'agent_profile_url': record.get('agent_profile_url'),
            'profile_type': record.get('profile_type', 'individual'),
            'normalized_address': normalized_address,
            'normalized_address_hash': normalized_address_hash,
            'transaction_match_key': transaction_match_key,
            'observation_id': observation_id,
            'local_directory_town': record.get('local_directory_town'),
            'attribution_confidence': record.get('attribution_confidence'),
        })
        return True
    except sqlite3.IntegrityError as e:
        logger.warning('Insert failed for Zillow observation %s: %s', observation_id, e)
        return False


def record_zillow_directory_profile(
    conn: sqlite3.Connection,
    town: str,
    profile_url: str,
    profile_type: str,
    local_sales_count: int,
    raw_card_text: str | None = None,
    profile_name: str | None = None,
    office_name: str | None = None,
    sales_last_12_months: int | None = None,
    price_range: str | None = None,
) -> None:
    """Upsert a discovered Zillow profile and its town appearance."""
    now = datetime.utcnow().isoformat()
    conn.execute('''
        INSERT INTO zillow_profiles (
            profile_url, profile_type, raw_card_text,
            profile_name, office_name, sales_last_12_months, price_range,
            discovered_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(profile_url) DO UPDATE SET
            profile_type = excluded.profile_type,
            raw_card_text = COALESCE(excluded.raw_card_text, zillow_profiles.raw_card_text),
            profile_name = COALESCE(excluded.profile_name, zillow_profiles.profile_name),
            office_name = COALESCE(excluded.office_name, zillow_profiles.office_name),
            sales_last_12_months = COALESCE(excluded.sales_last_12_months, zillow_profiles.sales_last_12_months),
            price_range = COALESCE(excluded.price_range, zillow_profiles.price_range)
    ''', (profile_url, profile_type, raw_card_text,
          profile_name, office_name, sales_last_12_months, price_range, now))
    conn.execute('''
        INSERT INTO zillow_profile_towns (
            profile_url, town, local_sales_count, discovered_at
        ) VALUES (
            ?, ?, ?, ?
        )
        ON CONFLICT(profile_url, town) DO UPDATE SET
            local_sales_count = MAX(zillow_profile_towns.local_sales_count, excluded.local_sales_count)
    ''', (profile_url, town, local_sales_count, now))
    conn.commit()


def record_zillow_team_member(
    conn: sqlite3.Connection,
    team_profile_url: str,
    member_profile_url: str,
    member_name: str | None = None,
) -> None:
    """Record a team-to-member relationship and enqueue the member profile."""
    now = datetime.utcnow().isoformat()
    conn.execute('''
        INSERT INTO zillow_team_members (
            team_profile_url, member_profile_url, member_name, discovered_at
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT(team_profile_url, member_profile_url) DO UPDATE SET
            member_name = COALESCE(excluded.member_name, zillow_team_members.member_name)
    ''', (team_profile_url, member_profile_url, member_name, now))
    conn.execute('''
        INSERT INTO zillow_profiles (
            profile_url, profile_type, profile_name, discovered_at
        ) VALUES (?, 'individual', ?, ?)
        ON CONFLICT(profile_url) DO UPDATE SET
            profile_name = COALESCE(excluded.profile_name, zillow_profiles.profile_name),
            profile_type = 'individual'
    ''', (member_profile_url, member_name, now))
    conn.commit()


def get_pending_zillow_profiles(
    conn: sqlite3.Connection,
    batch_size: int = 25,
) -> list[dict]:
    """Return pending Zillow profiles, prioritizing teams for member discovery."""
    rows = conn.execute('''
        SELECT profile_url, profile_type, profile_name
        FROM zillow_profiles
        WHERE scrape_status IS NULL
           OR (scrape_status IN ('failed', 'blocked', 'captcha') AND scrape_attempts < 3)
        ORDER BY CASE profile_type WHEN 'team' THEN 0 ELSE 1 END, profile_url
        LIMIT ?
    ''', (batch_size,)).fetchall()
    return [dict(r) for r in rows]


def mark_zillow_profile_status(
    conn: sqlite3.Connection,
    profile_url: str,
    status: str,
    error: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Update Zillow profile scrape status and optional profile metadata."""
    metadata = metadata or {}
    try:
        conn.execute('''
            UPDATE zillow_profiles
            SET scrape_status = ?,
                scrape_attempts = COALESCE(scrape_attempts, 0) + 1,
                last_error = ?,
                last_scraped_at = ?,
                profile_name = COALESCE(?, profile_name),
                office_name = COALESCE(?, office_name),
                sales_last_12_months = COALESCE(?, sales_last_12_months),
                total_sales = COALESCE(?, total_sales),
                average_price = COALESCE(?, average_price),
                price_range = COALESCE(?, price_range)
            WHERE profile_url = ?
        ''', (
            status,
            error,
            datetime.utcnow().isoformat(),
            metadata.get('profile_name'),
            metadata.get('office_name'),
            _to_int(metadata.get('sales_last_12_months')),
            _to_int(metadata.get('total_sales')),
            _to_int(metadata.get('average_price')),
            metadata.get('price_range'),
            profile_url,
        ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def log_team_only_sale(conn: sqlite3.Connection, record: dict) -> None:
    """Upsert a team-only sale for later gap analysis."""
    conn.execute('''
        INSERT INTO team_only_sales_log (
            team_profile_url, team_name, property_url, represented_side,
            sale_date, sale_price, normalized_address, normalized_address_hash,
            transaction_match_key, local_town, resolved_by_member, created_at
        ) VALUES (
            :team_profile_url, :team_name, :property_url, :represented_side,
            :sale_date, :sale_price, :normalized_address, :normalized_address_hash,
            :transaction_match_key, :local_town, :resolved_by_member, :created_at
        )
        ON CONFLICT(team_profile_url, represented_side, transaction_match_key) DO UPDATE SET
            team_name = COALESCE(excluded.team_name, team_only_sales_log.team_name),
            property_url = COALESCE(excluded.property_url, team_only_sales_log.property_url),
            local_town = COALESCE(excluded.local_town, team_only_sales_log.local_town)
    ''', {
        'team_profile_url': record.get('team_profile_url'),
        'team_name': record.get('team_name'),
        'property_url': record.get('property_url'),
        'represented_side': record.get('represented_side'),
        'sale_date': record.get('sale_date'),
        'sale_price': _to_int(record.get('sale_price')),
        'normalized_address': record.get('normalized_address'),
        'normalized_address_hash': record.get('normalized_address_hash'),
        'transaction_match_key': record.get('transaction_match_key'),
        'local_town': record.get('local_town'),
        'resolved_by_member': 1 if record.get('resolved_by_member') else 0,
        'created_at': record.get('created_at', datetime.utcnow().isoformat()),
    })
    conn.commit()


def resolve_team_only_sales(
    conn: sqlite3.Connection,
    transaction_match_key: str | None,
    represented_side: str | None,
) -> int:
    """Mark team-only rows as resolved when matched by an individual observation."""
    if not transaction_match_key or not represented_side:
        return 0
    cursor = conn.execute('''
        UPDATE team_only_sales_log
        SET resolved_by_member = 1
        WHERE transaction_match_key = ?
          AND LOWER(represented_side) = LOWER(?)
    ''', (transaction_match_key, represented_side))
    conn.commit()
    return cursor.rowcount


def get_team_gap_rows(conn: sqlite3.Connection) -> list[dict]:
    """Return unresolved team-only rows for reporting."""
    rows = conn.execute('''
        SELECT team_name, team_profile_url, local_town, represented_side,
               sale_date, sale_price, property_url
        FROM team_only_sales_log
        WHERE resolved_by_member = 0
        ORDER BY team_name, sale_date DESC
    ''').fetchall()
    return [dict(r) for r in rows]


def fuzzy_merge_agents(conn: sqlite3.Connection, threshold: int = 90) -> list[tuple[str, str]]:
    """Merge near-duplicate agent names that share the same office.

    Returns list of (old_name, merged_into) tuples for audit.
    """
    try:
        from rapidfuzz import fuzz
    except ImportError:
        logger.warning('rapidfuzz not installed — skipping fuzzy merge')
        return []

    # Convergence loop: re-run until no new merges (handles transitive chains)
    all_merges = []
    while True:
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

        all_merges.extend(merges)
        if not merges:
            break  # Convergence: no new merges found

    if all_merges:
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
    try:
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
    except Exception:
        conn.rollback()
        raise


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
