"""Read-only helpers for downstream tools.

The downstream mailer and agent-outreach tools (in a separate repo) read
from data/maine_listings.db via these four functions. The interface is
frozen here so the downstream repo has a stable contract.
"""
from __future__ import annotations

import sqlite3
from typing import Optional


_ACTIVE_STATUSES = ('Active',)
_ACTIVE_OR_PENDING = ('Active', 'Pending')


def _dict(row) -> dict:
    return dict(row) if row is not None else {}


def query_active_listings(
    conn: sqlite3.Connection,
    *,
    towns: Optional[list[str]] = None,
    min_days_on_market: Optional[int] = None,
    include_pending: bool = False,
) -> list[dict]:
    """Currently-active listings with agent contact + property details.

    Only rows where status='Active' (and optionally 'Pending'). `Closed` and
    `Withdrawn` are never returned.
    """
    statuses = _ACTIVE_OR_PENDING if include_pending else _ACTIVE_STATUSES
    placeholders = ','.join(['?'] * len(statuses))
    params: list = list(statuses)

    where_extra = ''
    if towns:
        town_placeholders = ','.join(['?'] * len(towns))
        where_extra += f' AND LOWER(city) IN ({town_placeholders})'
        params += [t.lower() for t in towns]
    if min_days_on_market is not None:
        where_extra += ' AND days_on_market >= ?'
        params.append(min_days_on_market)

    rows = conn.execute(f'''
        SELECT
            detail_url, mls_number, status,
            address, city, state, zip,
            list_price, list_date, days_on_market,
            beds, baths, sqft, year_built, lot_sqft,
            property_type, description, photo_url,
            listing_agent, listing_agent_email, listing_agent_id,
            listing_office, last_seen_at
        FROM maine_transactions
        WHERE status IN ({placeholders})
        {where_extra}
        ORDER BY list_date DESC
    ''', params).fetchall()
    return [_dict(r) for r in rows]


def query_listing_history(
    conn: sqlite3.Connection, detail_url: str,
) -> list[dict]:
    """Full price/status timeline for one listing, oldest first."""
    rows = conn.execute('''
        SELECT snapshot_date, status, list_price
        FROM maine_listing_history
        WHERE detail_url = ?
        ORDER BY id ASC
    ''', (detail_url,)).fetchall()
    return [_dict(r) for r in rows]


def query_new_since(conn: sqlite3.Connection, *, since_iso: str) -> list[dict]:
    """Listings whose first history row is on or after since_iso.

    The *first* history row represents the listing's debut in the pipeline.
    Downstream: the daily mailer tool calls this with yesterday's timestamp
    to get "new listings since my last run".
    """
    rows = conn.execute('''
        WITH first_seen AS (
            SELECT detail_url, MIN(snapshot_date) AS first_snap
            FROM maine_listing_history
            GROUP BY detail_url
        )
        SELECT
            t.detail_url, t.address, t.city, t.status,
            t.list_price, t.list_date, t.beds, t.baths, t.sqft,
            t.listing_agent, t.listing_agent_email, t.listing_office,
            fs.first_snap
        FROM maine_transactions t
        JOIN first_seen fs ON fs.detail_url = t.detail_url
        WHERE fs.first_snap >= ?
          AND t.status IN ('Active', 'Pending')
        ORDER BY fs.first_snap ASC
    ''', (since_iso,)).fetchall()
    return [_dict(r) for r in rows]


def query_stale_listings(
    conn: sqlite3.Connection, *, min_dom: int = 60,
) -> list[dict]:
    """Active listings on market at least min_dom days.

    Downstream: the agent-outreach tool uses this to target motivated
    sellers ("your listing's been up 60 days — want an STR projection?").
    """
    rows = conn.execute('''
        SELECT
            detail_url, address, city, status,
            list_price, list_date, days_on_market,
            beds, baths, sqft, property_type,
            listing_agent, listing_agent_email, listing_office
        FROM maine_transactions
        WHERE status = 'Active'
          AND days_on_market IS NOT NULL
          AND days_on_market >= ?
        ORDER BY days_on_market DESC
    ''', (min_dom,)).fetchall()
    return [_dict(r) for r in rows]
