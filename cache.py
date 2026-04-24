#!/usr/bin/env python3
"""
Domain-level scrape cache.

Stores scraped leads keyed by domain in SQLite so that repeated runs
against the same city/keyword don't re-scrape sites we've already
processed. TTL defaults to 30 days — businesses don't change phone
numbers or email addresses very often.
"""

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from urllib.parse import urlparse


CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/data/cache"))
DEFAULT_TTL_SECONDS = 30 * 24 * 3600  # 30 days

_lock = threading.Lock()
_conn: sqlite3.Connection | None = None


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            db_path = CACHE_DIR / "leads.db"
        except (OSError, PermissionError):
            # Fall back to local dir if /data isn't writable (dev machine)
            db_path = Path(".cache") / "leads.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                domain TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                payload TEXT NOT NULL,
                scraped_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS email_mx (
                domain TEXT PRIMARY KEY,
                valid INTEGER NOT NULL,
                checked_at REAL NOT NULL
            )
        """)
        conn.commit()
        _conn = conn
    return _conn


def _domain_of(url: str) -> str | None:
    try:
        d = urlparse(url).netloc.lower()
        if d.startswith("www."):
            d = d[4:]
        return d or None
    except Exception:
        return None


def get_lead(url: str, ttl: float = DEFAULT_TTL_SECONDS) -> dict | None:
    """Return cached lead for this URL's domain, or None if missing/stale."""
    domain = _domain_of(url)
    if not domain:
        return None
    cutoff = time.time() - ttl
    with _lock:
        row = _get_conn().execute(
            "SELECT payload, scraped_at FROM leads WHERE domain = ? AND scraped_at >= ?",
            (domain, cutoff),
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return None


def put_lead(lead: dict):
    """Store a scraped lead keyed by its URL's domain. Skips errors."""
    if not lead or "error" in lead:
        return
    url = lead.get("url", "")
    domain = _domain_of(url)
    if not domain:
        return
    payload = json.dumps(lead, default=str)
    with _lock:
        _get_conn().execute(
            "INSERT OR REPLACE INTO leads (domain, url, payload, scraped_at) VALUES (?, ?, ?, ?)",
            (domain, url, payload, time.time()),
        )
        _get_conn().commit()


def get_mx_valid(domain: str, ttl: float = 7 * 24 * 3600) -> bool | None:
    """Return cached MX-validity for a domain. None if unknown/stale."""
    cutoff = time.time() - ttl
    with _lock:
        row = _get_conn().execute(
            "SELECT valid FROM email_mx WHERE domain = ? AND checked_at >= ?",
            (domain.lower(), cutoff),
        ).fetchone()
    return bool(row[0]) if row else None


def put_mx_valid(domain: str, valid: bool):
    with _lock:
        _get_conn().execute(
            "INSERT OR REPLACE INTO email_mx (domain, valid, checked_at) VALUES (?, ?, ?)",
            (domain.lower(), int(valid), time.time()),
        )
        _get_conn().commit()


def stats() -> dict:
    with _lock:
        leads_count = _get_conn().execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        mx_count = _get_conn().execute("SELECT COUNT(*) FROM email_mx").fetchone()[0]
    return {"cached_leads": leads_count, "cached_mx": mx_count}
