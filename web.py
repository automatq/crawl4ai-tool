#!/usr/bin/env python3
"""
Crawl4AI Lead Scraper — Web Interface

Run:  python web.py
Then open http://localhost:5000
"""

import asyncio
import json
import os
import re
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

from flask import Flask, Response, jsonify, redirect, render_template, request, stream_with_context
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash

from scrape import search_leads, scrape_all, normalize_url, ScrapeConfig
from gmaps import scrape_google_maps, scrape_google_maps_area
from homestars import scrape_homestars_multi_city, scrape_homestars
from outreach import run_outreach
import scoring
import cache

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB max upload
def _get_secret_key():
    """Get a stable secret key that persists across redeploys."""
    if os.environ.get("SECRET_KEY"):
        return os.environ["SECRET_KEY"]
    # Store on persistent volume so it survives redeploys
    key_file = Path(os.environ.get("RUNS_DB", "/data/scraper.db")).parent / ".secret_key"
    try:
        if key_file.exists():
            return key_file.read_text().strip()
        key = uuid4().hex + uuid4().hex
        key_file.parent.mkdir(parents=True, exist_ok=True)
        key_file.write_text(key)
        return key
    except Exception:
        return uuid4().hex + uuid4().hex

app.secret_key = _get_secret_key()

# ── Authentication ───────────────────────────────────────────────────

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login_page"


class User(UserMixin):
    def __init__(self, id, username):
        self.id = id
        self.username = username


@login_manager.user_loader
def load_user(user_id):
    conn = _get_runs_db()
    row = conn.execute("SELECT id, username FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if row:
        return User(row["id"], row["username"])
    return None


@login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith("/api/"):
        return jsonify(error="Not authenticated"), 401
    return redirect("/login")


@app.before_request
def require_login():
    # Allow public endpoints by name
    public_endpoints = {"login_page", "api_login", "api_signup", "health", "static"}
    if request.endpoint and request.endpoint in public_endpoints:
        return None
    # Allow static files and login page by path
    if request.path.startswith("/static/") or request.path == "/login":
        return None
    if not current_user.is_authenticated:
        if request.path.startswith("/api/"):
            return jsonify(error="Not authenticated"), 401
        return redirect("/login")


# ── Canadian cities database ──────────────────────────────────────────

CITIES_DB = Path(__file__).parent / "canada_cities.db"


def _get_cities_db():
    """Return a sqlite3 connection to the cities database."""
    conn = sqlite3.connect(str(CITIES_DB))
    conn.row_factory = sqlite3.Row
    return conn


# ── Run history database ──────────────────────────────────────────────

RUNS_DB = Path(os.environ.get("RUNS_DB", "/data/scraper.db"))
RUNS_DB.parent.mkdir(parents=True, exist_ok=True)


def _get_runs_db():
    """Return a sqlite3 connection to the run history database."""
    conn = sqlite3.connect(str(RUNS_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        user_id TEXT,
        mode TEXT,
        keyword TEXT,
        cities TEXT,
        status TEXT DEFAULT 'running',
        lead_count INTEGER DEFAULT 0,
        duration_seconds REAL,
        created_at TEXT,
        finished_at TEXT,
        grid_spacing_km REAL,
        max_results INTEGER,
        error TEXT,
        backup_filename TEXT
    )""")
    conn.commit()
    # Migrations for existing tables
    for col, spec in [("backup_filename", "TEXT"), ("user_id", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE runs ADD COLUMN {col} {spec}")
            conn.commit()
        except Exception:
            pass
    return conn


def _log_run_start(job: "Job"):
    """Insert a new run record when a job starts."""
    ic = job.input_config
    city_ids = ic.get("city_ids")
    cities = json.dumps(city_ids) if city_ids else ic.get("city", "")
    try:
        conn = _get_runs_db()
        conn.execute(
            "INSERT INTO runs (run_id, user_id, mode, keyword, cities, status, created_at, grid_spacing_km, max_results) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (job.id, job.user_id, ic.get("mode", ""), ic.get("keyword", ""), cities, "running",
             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(job.created_at)),
             ic.get("grid_spacing_km"), ic.get("max_results")),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[RUN LOG] start error: {e}", flush=True)


def _log_run_finish(job: "Job"):
    """Update the run record when a job finishes."""
    try:
        conn = _get_runs_db()
        conn.execute(
            "UPDATE runs SET status=?, lead_count=?, duration_seconds=?, finished_at=?, error=? WHERE run_id=?",
            (job.status, len(job.results), round(job.duration, 1),
             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(job.finished_at)),
             job.error or None, job.id),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[RUN LOG] finish error: {e}", flush=True)


# ── Persistent backup to Railway volume ──────────────────────────────

BACKUP_DIR = Path(os.environ.get("BACKUP_DIR", "/data/backups"))
BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def _save_backup(job: "Job"):
    """Save completed job results to the persistent volume."""
    if not job.results:
        return
    try:
        ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime(job.created_at))
        mode = job.input_config.get("mode", "unknown")
        label = job.input_config.get("keyword", "") or job.input_config.get("url", "") or "import"
        # Sanitize label for filesystem
        label = re.sub(r'[^\w\-]', '_', label)[:40]
        filename = f"{ts}_{job.id}_{mode}_{label}.json"

        payload = {
            "job_id": job.id,
            "user_id": job.user_id,
            "created_at": job.created_at,
            "finished_at": job.finished_at,
            "duration": round(job.duration, 1),
            "status": job.status,
            "input_config": job.input_config,
            "lead_count": len(job.results),
            "leads": job.results,
        }
        backup_path = BACKUP_DIR / filename
        backup_path.write_text(json.dumps(payload, indent=2, default=str))
        job.log(f"Backup saved: {filename}")
        # Link backup file to run record
        try:
            db = _get_runs_db()
            db.execute("UPDATE runs SET backup_filename=? WHERE run_id=?", (filename, job.id))
            db.commit()
            db.close()
        except Exception:
            pass
    except Exception as e:
        job.log(f"Backup failed: {e}", "error")


# ── Job management ────────────────────────────────────────────────────

@dataclass
class Job:
    id: str
    status: str = "pending"  # pending | searching | scraping | done | error | cancelled
    progress_pct: int = 0
    progress_msg: str = ""
    results: list = field(default_factory=list)
    logs: list = field(default_factory=list)
    error: str = ""
    cancel_flag: bool = False
    created_at: float = field(default_factory=time.time)
    finished_at: float = 0
    input_config: dict = field(default_factory=dict)
    user_id: str = ""
    lock: threading.Lock = field(default_factory=threading.Lock)

    def log(self, msg: str, level: str = "info"):
        with self.lock:
            self.logs.append({
                "time": time.time(),
                "level": level,
                "msg": msg,
            })

    @property
    def duration(self) -> float:
        end = self.finished_at or time.time()
        return end - self.created_at


jobs: dict[str, Job] = {}

# Worker pool — fixed number of threads pulling from a shared queue
import queue

_work_queue: queue.Queue = queue.Queue()
_NUM_WORKERS = 2  # Start conservative, scale up once stable


def _worker():
    """Worker thread — pulls jobs from queue and runs them."""
    import traceback
    while True:
        func, args = _work_queue.get()
        try:
            func(*args)
        except Exception as e:
            print(f"[WORKER ERROR] {e}", flush=True)
            traceback.print_exc()
        _work_queue.task_done()


# Start worker threads once at import time
for _i in range(_NUM_WORKERS):
    _t = threading.Thread(target=_worker, daemon=True)
    _t.start()


def _cleanup_old_jobs():
    cutoff = time.time() - 3600
    stale = [jid for jid, j in jobs.items() if j.created_at < cutoff]
    for jid in stale:
        del jobs[jid]


def _create_job(**input_config) -> Job:
    _cleanup_old_jobs()
    user_id = current_user.id if current_user.is_authenticated else ""
    job = Job(id=uuid4().hex[:8], input_config=input_config, user_id=user_id)
    jobs[job.id] = job
    return job


# ── Background workers ────────────────────────────────────────────────

def _make_config(input_config: dict) -> ScrapeConfig:
    """Build a ScrapeConfig from the job's input config."""
    proxies_raw = input_config.get("proxies", "").strip()
    proxies = [p.strip() for p in proxies_raw.split("\n") if p.strip()] if proxies_raw else []
    return ScrapeConfig(
        proxies=proxies,
        stealth=input_config.get("stealth", True),
        use_google_maps=input_config.get("google_maps", False),
        crawl_depth=1 if input_config.get("deep_crawl", False) else 0,
        concurrency=int(input_config.get("concurrency", 8)),
        use_cache=input_config.get("use_cache", True),
        enrich=input_config.get("enrich", True),
    )


def _run_outreach_phase(job: Job, leads: list[dict], cfg: ScrapeConfig) -> list[dict]:
    """Run outreach on leads without emails, if enabled. Returns updated leads list."""
    ic = job.input_config
    if not ic.get("outreach_enabled"):
        return leads

    sender = {
        "email": ic.get("sender_email", ""),
        "phone": ic.get("sender_phone", ""),
        "company": ic.get("sender_company", ""),
    }
    template = ic.get("message_template", "")
    if not sender.get("email") or not template:
        job.log("Outreach skipped: sender email and message template required", "warn")
        return leads

    no_email = [l for l in leads if not l.get("emails") and l.get("url") and "error" not in l]
    if not no_email:
        job.log("Outreach skipped: all leads already have emails")
        return leads

    job.log(f"Starting outreach for {len(no_email)} leads without emails...")
    with job.lock:
        job.status = "outreach"
        job.progress_msg = f"Submitting contact forms for {len(no_email)} leads..."

    def outreach_progress(i, total, msg):
        if job.cancel_flag:
            raise InterruptedError("Cancelled")
        with job.lock:
            job.progress_pct = 80 + int((i / max(total, 1)) * 20)
            job.progress_msg = msg
        job.log(msg)

    outreach_results = asyncio.run(
        run_outreach(no_email, sender, template, cfg, on_progress=outreach_progress)
    )

    # Merge outreach results back by URL
    outreach_by_url = {r["url"]: r for r in outreach_results}
    merged = []
    for lead in leads:
        url = lead.get("url", "")
        if url in outreach_by_url:
            merged.append(outreach_by_url[url])
        else:
            merged.append(lead)

    # Log summary
    statuses = {}
    for r in outreach_results:
        s = r.get("outreach_status", "unknown")
        statuses[s] = statuses.get(s, 0) + 1
    summary = ", ".join(f"{v} {k}" for k, v in sorted(statuses.items()))
    job.log(f"Outreach complete: {summary}")

    return merged


def _run_keyword_job(job: Job, keyword: str, cities: list[str], num: int):
    _log_run_start(job)
    try:
        cfg = _make_config(job.input_config)

        with job.lock:
            job.status = "searching"
            job.progress_msg = f"Searching for '{keyword}'..."
        job.log(f"Starting keyword search: '{keyword}' in {', '.join(cities)}")
        job.log(f"Target: {num} leads")
        if cfg.stealth:
            job.log("Stealth mode: enabled")
        if cfg.proxies:
            job.log(f"Proxy rotation: {len(cfg.proxies)} proxies")
        if cfg.use_google_maps:
            job.log("Google Maps search: enabled")
        if cfg.crawl_depth >= 1:
            job.log("Deep crawl: enabled (following internal links)")

        def search_progress(found, target, msg):
            if job.cancel_flag:
                raise InterruptedError("Cancelled")
            with job.lock:
                job.progress_pct = min(45, int((found / max(target, 1)) * 45))
                job.progress_msg = msg
            job.log(msg)

        urls = asyncio.run(search_leads(keyword, cities, num, on_progress=search_progress, config=cfg))

        if job.cancel_flag:
            with job.lock:
                job.status = "cancelled"
                job.progress_msg = "Cancelled"
                job.finished_at = time.time()
            job.log("Cancelled by user", "warn")
            _log_run_finish(job)
            return

        if not urls:
            with job.lock:
                job.status = "done"
                job.progress_pct = 100
                job.progress_msg = "No business sites found."
                job.finished_at = time.time()
            job.log("No business sites found", "warn")
            _log_run_finish(job)
            return

        job.log(f"Found {len(urls)} unique business URLs")

        with job.lock:
            job.status = "scraping"
            job.progress_msg = f"Scraping {len(urls)} sites..."

        def scrape_progress(i, total, msg):
            if job.cancel_flag:
                raise InterruptedError("Cancelled")
            with job.lock:
                pct = 45 + int((i / max(total, 1)) * 35) if job.input_config.get("outreach_enabled") else 45 + int((i / max(total, 1)) * 55)
                job.progress_pct = pct
                job.progress_msg = msg
            job.log(msg)

        leads = asyncio.run(scrape_all(urls, on_progress=scrape_progress, config=cfg))
        job.log(f"Scraping complete: {len(leads)} leads in {job.duration:.1f}s")

        # Outreach phase (if enabled)
        leads = _run_outreach_phase(job, leads, cfg)

        leads = scoring.sort_by_score(scoring.annotate(leads))
        _log_tier_summary(job, leads)

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = leads
            job.progress_msg = f"Done — {len(leads)} leads scraped."
            job.finished_at = time.time()
        job.log(f"Completed: {len(leads)} leads in {job.duration:.1f}s")
        _save_backup(job)
        _log_run_finish(job)

    except InterruptedError:
        with job.lock:
            job.status = "cancelled"
            job.progress_msg = "Cancelled by user."
            job.finished_at = time.time()
        job.log("Cancelled by user", "warn")
        _log_run_finish(job)
    except Exception as e:
        with job.lock:
            job.status = "error"
            job.error = str(e)
            job.progress_msg = f"Error: {e}"
            job.finished_at = time.time()
        job.log(f"Error: {e}", "error")
        _log_run_finish(job)


def _log_tier_summary(job: "Job", leads: list[dict]):
    tiers: dict[str, int] = {}
    for l in leads:
        t = l.get("tier", "unknown")
        tiers[t] = tiers.get(t, 0) + 1
    if tiers:
        summary = ", ".join(f"{v} {k}" for k, v in sorted(tiers.items()))
        job.log(f"Tier breakdown: {summary}")


def _map_source_record(rec: dict) -> dict:
    """Normalize an imported record to our lead schema, preserving all source data."""
    url = (rec.get("website") or rec.get("url") or "").strip()
    if url and not url.startswith("https://www.google.com/maps"):
        url = normalize_url(url)
    else:
        url = ""

    phone = rec.get("phone") or rec.get("phoneUnformatted") or ""
    parts = [rec.get("street", ""), rec.get("city", ""), rec.get("postalCode", "")]
    addr = ", ".join(p for p in parts if p) or rec.get("address", "")

    return {
        "url": url,
        "company": rec.get("title") or rec.get("name") or "",
        "description": rec.get("description", ""),
        "emails": [],
        "phones": [phone] if phone else [],
        "address": addr,
        "socials": {},
        "hours": rec.get("openingHours", "") or "",
        "category": rec.get("categoryName", ""),
        "rating": rec.get("totalScore"),
        "google_rating": rec.get("totalScore"),
        "google_reviews": rec.get("reviewsCount"),
        "neighborhood": rec.get("neighborhood", ""),
    }


def _merge_enrichment(result: dict, scraped: dict):
    """Overlay scraped website data onto a source record."""
    if scraped.get("emails"):
        result["emails"] = sorted(set(result.get("emails", []) + scraped["emails"]))
    if scraped.get("socials"):
        merged_socials = dict(result.get("socials", {}))
        merged_socials.update(scraped["socials"])
        result["socials"] = merged_socials
    if scraped.get("hours") and not result.get("hours"):
        result["hours"] = scraped["hours"]
    if scraped.get("description") and not result.get("description"):
        result["description"] = scraped["description"]
    if scraped.get("company") and not result.get("company"):
        result["company"] = scraped["company"]
    if scraped.get("phones") and not result.get("phones"):
        result["phones"] = scraped["phones"]
    if scraped.get("address") and not result.get("address"):
        result["address"] = scraped["address"]


def _run_import_job(job: Job, records: list[dict]):
    """Scrape URLs from imported data (e.g. Google Maps export) and merge results."""
    try:
        cfg = _make_config(job.input_config)

        # Collect unique website URLs for scraping
        urls_to_scrape: list[str] = []
        seen_urls: set[str] = set()
        for rec in records:
            url = (rec.get("website") or rec.get("url") or "").strip()
            if not url or url.startswith("https://www.google.com/maps"):
                continue
            url = normalize_url(url)
            if url not in seen_urls:
                seen_urls.add(url)
                urls_to_scrape.append(url)

        job.log(f"Imported {len(records)} records, {len(urls_to_scrape)} have scrapable websites")

        # Scrape websites for enrichment (if any)
        scraped_by_url: dict[str, dict] = {}
        if urls_to_scrape:
            with job.lock:
                job.status = "scraping"
                job.progress_msg = f"Scraping {len(urls_to_scrape)} websites for enrichment..."
            if cfg.stealth:
                job.log("Stealth mode: enabled")
            if cfg.crawl_depth >= 1:
                job.log("Deep crawl: enabled")

            def scrape_progress(i, total, msg):
                if job.cancel_flag:
                    raise InterruptedError("Cancelled")
                with job.lock:
                    job.progress_pct = int((i / max(total, 1)) * 100)
                    job.progress_msg = msg
                job.log(msg)

            leads = asyncio.run(scrape_all(urls_to_scrape, on_progress=scrape_progress, config=cfg))
            for lead in leads:
                if "error" not in lead:
                    scraped_by_url[lead.get("url", "")] = lead

        # Build final results from ALL original records
        merged = []
        for rec in records:
            result = _map_source_record(rec)
            if result["url"] and result["url"] in scraped_by_url:
                _merge_enrichment(result, scraped_by_url[result["url"]])
            merged.append(result)

        # Outreach phase (if enabled)
        merged = _run_outreach_phase(job, merged, cfg)

        merged = scoring.sort_by_score(scoring.annotate(merged))
        _log_tier_summary(job, merged)

        enriched_count = sum(1 for r in merged if r.get("emails") or r.get("socials"))
        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = merged
            job.progress_msg = f"Done — {len(merged)} leads ({enriched_count} enriched with emails/socials)."
            job.finished_at = time.time()
        job.log(f"Completed: {len(merged)} leads ({enriched_count} enriched) in {job.duration:.1f}s")
        _save_backup(job)

    except InterruptedError:
        with job.lock:
            job.status = "cancelled"
            job.progress_msg = "Cancelled by user."
            job.finished_at = time.time()
        job.log("Cancelled by user", "warn")
    except Exception as e:
        with job.lock:
            job.status = "error"
            job.error = str(e)
            job.progress_msg = f"Error: {e}"
            job.finished_at = time.time()
        job.log(f"Error: {e}", "error")


def _run_maps_job(job: Job, query: str, max_results: int, enrich: bool):
    """Run a Google Maps scraping job."""
    _log_run_start(job)
    try:
        cfg = _make_config(job.input_config)
        ic = job.input_config
        area_search = ic.get("area_search", False)
        polygon = ic.get("polygon")

        with job.lock:
            job.status = "searching"
            job.progress_msg = f"Searching Google Maps for '{query}'..."
        if area_search and polygon:
            job.log(f"Starting polygon area search: '{ic.get('keyword', query)}' (max {max_results})")
            job.log(f"Grid spacing: {ic.get('grid_spacing_km', 1.0)} km")
        else:
            job.log(f"Starting Google Maps scrape: '{query}' (max {max_results})")
        if cfg.stealth:
            job.log("Stealth mode: enabled")
        if cfg.proxies:
            job.log(f"Proxy rotation: {len(cfg.proxies)} proxies")
        if enrich:
            job.log("Website enrichment: enabled (emails/socials)")

        def maps_progress(current, total, msg):
            if job.cancel_flag:
                raise InterruptedError("Cancelled")
            with job.lock:
                if enrich:
                    job.progress_pct = min(70, int((current / max(total, 1)) * 70))
                else:
                    job.progress_pct = int((current / max(total, 1)) * 100)
                job.progress_msg = msg
            job.log(msg)

        if area_search and polygon:
            leads = asyncio.run(scrape_google_maps_area(
                keyword=ic.get("keyword", query),
                polygon=polygon,
                grid_spacing_km=float(ic.get("grid_spacing_km", 1.0)),
                max_results=max_results,
                enrich_websites=enrich,
                config=cfg,
                on_progress=maps_progress,
            ))
        else:
            leads = asyncio.run(scrape_google_maps(
                query=query,
                max_results=max_results,
                enrich_websites=enrich,
                config=cfg,
                on_progress=maps_progress,
            ))

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = leads
            job.progress_msg = f"Done — {len(leads)} leads from Google Maps."
            job.finished_at = time.time()

        # Outreach phase (if enabled)
        leads = _run_outreach_phase(job, leads, cfg)

        leads = scoring.sort_by_score(scoring.annotate(leads))
        _log_tier_summary(job, leads)

        with job.lock:
            job.results = leads
            job.finished_at = time.time()

        job.log(f"Completed: {len(leads)} leads in {job.duration:.1f}s")
        _save_backup(job)
        _log_run_finish(job)

    except InterruptedError:
        with job.lock:
            job.status = "cancelled"
            job.progress_msg = "Cancelled by user."
            job.finished_at = time.time()
        job.log("Cancelled by user", "warn")
        _log_run_finish(job)
    except Exception as e:
        with job.lock:
            job.status = "error"
            job.error = str(e)
            job.progress_msg = f"Error: {e}"
            job.finished_at = time.time()
        job.log(f"Error: {e}", "error")
        _log_run_finish(job)


def _run_multi_city_maps_job(job: Job, keyword: str, city_ids: list[int],
                              max_per_city: int, grid_spacing_km: float, enrich: bool):
    """Run polygon area search across multiple cities from the database."""
    _log_run_start(job)
    try:
        cfg = _make_config(job.input_config)

        # Fetch city metadata + polygons from DB
        conn = _get_cities_db()
        placeholders = ",".join("?" * len(city_ids))
        cities = conn.execute(
            f"SELECT id, city, province_name, polygon_geojson FROM cities "
            f"WHERE id IN ({placeholders}) AND polygon_geojson IS NOT NULL",
            city_ids,
        ).fetchall()
        conn.close()

        if not cities:
            raise ValueError("No valid cities found for the given IDs")

        total_cities = len(cities)
        all_leads = []
        seen_place_ids = set()

        with job.lock:
            job.status = "searching"
            job.progress_msg = f"Searching {total_cities} cities for '{keyword}'..."
        job.log(f"Starting multi-city search: '{keyword}' across {total_cities} cities")
        if cfg.stealth:
            job.log("Stealth mode: enabled")
        if enrich:
            job.log("Website enrichment: enabled")

        for city_i, city_row in enumerate(cities):
            if job.cancel_flag:
                raise InterruptedError("Cancelled")

            city_name = city_row["city"]
            province = city_row["province_name"]
            polygon = json.loads(city_row["polygon_geojson"])
            base_pct = int((city_i / total_cities) * 90)

            with job.lock:
                job.progress_pct = base_pct
                job.progress_msg = f"Searching {city_name}, {province} ({city_i + 1}/{total_cities})..."
            job.log(f"City {city_i + 1}/{total_cities}: {city_name}, {province}")

            def maps_progress(current, total, msg, _cn=city_name, _bp=base_pct):
                if job.cancel_flag:
                    raise InterruptedError("Cancelled")
                city_pct = int((current / max(total, 1)) * (90 / total_cities))
                with job.lock:
                    job.progress_pct = min(90, _bp + city_pct)
                    job.progress_msg = f"[{_cn}] {msg}"
                job.log(f"[{_cn}] {msg}")

            city_leads = asyncio.run(scrape_google_maps_area(
                keyword=keyword,
                polygon=polygon,
                grid_spacing_km=grid_spacing_km,
                max_results=max_per_city,
                enrich_websites=enrich,
                config=cfg,
                on_progress=maps_progress,
            ))

            # Deduplicate across cities by place_id
            for lead in city_leads:
                pid = lead.get("place_id", "")
                if pid and pid in seen_place_ids:
                    continue
                if pid:
                    seen_place_ids.add(pid)
                lead["source_city"] = f"{city_name}, {province}"
                all_leads.append(lead)

            job.log(f"Found {len(city_leads)} leads in {city_name} "
                    f"({len(all_leads)} total unique)")

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = all_leads
            job.progress_msg = f"Done — {len(all_leads)} leads across {total_cities} cities."
            job.finished_at = time.time()

        # Outreach phase (if enabled)
        all_leads = _run_outreach_phase(job, all_leads, cfg)
        with job.lock:
            job.results = all_leads
            job.finished_at = time.time()

        job.log(f"Completed: {len(all_leads)} leads in {job.duration:.1f}s")
        _save_backup(job)
        _log_run_finish(job)

    except InterruptedError:
        with job.lock:
            job.status = "cancelled"
            job.progress_msg = "Cancelled by user."
            job.finished_at = time.time()
        job.log("Cancelled by user", "warn")
        _log_run_finish(job)
    except Exception as e:
        with job.lock:
            job.status = "error"
            job.error = str(e)
            job.progress_msg = f"Error: {e}"
            job.finished_at = time.time()
        job.log(f"Error: {e}", "error")
        _log_run_finish(job)


def _run_homestars_job(job: Job, keyword: str, city_ids: list[int],
                        max_per_city: int, enrich: bool):
    """Run HomeStars scraping across selected cities."""
    _log_run_start(job)
    try:
        cfg = _make_config(job.input_config)

        # Fetch city metadata from DB
        conn = _get_cities_db()
        placeholders = ",".join("?" * len(city_ids))
        cities = conn.execute(
            f"SELECT id, city, province_name FROM cities "
            f"WHERE id IN ({placeholders})",
            city_ids,
        ).fetchall()
        conn.close()

        if not cities:
            raise ValueError("No valid cities found for the given IDs")

        city_list = [{"city": c["city"], "province_name": c["province_name"]} for c in cities]
        total_cities = len(city_list)

        with job.lock:
            job.status = "searching"
            job.progress_msg = f"Searching HomeStars in {total_cities} cities for '{keyword}'..."
        job.log(f"Starting HomeStars search: '{keyword}' across {total_cities} cities")
        if enrich:
            job.log("Website enrichment: enabled")

        def hs_progress(current, total, msg):
            if job.cancel_flag:
                raise InterruptedError("Cancelled")
            with job.lock:
                pct = int((current / max(total, 1)) * 90)
                job.progress_pct = min(90, pct)
                job.progress_msg = msg
            job.log(msg)

        leads = asyncio.run(scrape_homestars_multi_city(
            keyword=keyword,
            cities=city_list,
            max_per_city=max_per_city,
            enrich_websites=enrich,
            config=cfg,
            on_progress=hs_progress,
        ))

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = leads
            job.progress_msg = f"Done — {len(leads)} leads from HomeStars."
            job.finished_at = time.time()

        # Outreach phase (if enabled)
        leads = _run_outreach_phase(job, leads, cfg)

        leads = scoring.sort_by_score(scoring.annotate(leads))
        _log_tier_summary(job, leads)

        with job.lock:
            job.results = leads
            job.finished_at = time.time()

        job.log(f"Completed: {len(leads)} leads in {job.duration:.1f}s")
        _save_backup(job)
        _log_run_finish(job)

    except InterruptedError:
        with job.lock:
            job.status = "cancelled"
            job.progress_msg = "Cancelled by user."
            job.finished_at = time.time()
        job.log("Cancelled by user", "warn")
        _log_run_finish(job)
    except Exception as e:
        with job.lock:
            job.status = "error"
            job.error = str(e)
            job.progress_msg = f"Error: {e}"
            job.finished_at = time.time()
        job.log(f"Error: {e}", "error")
        _log_run_finish(job)


def _run_url_job(job: Job, urls: list[str]):
    _log_run_start(job)
    try:
        cfg = _make_config(job.input_config)

        with job.lock:
            job.status = "scraping"
            job.progress_msg = f"Scraping {len(urls)} URL(s)..."
        job.log(f"Starting direct scrape of {len(urls)} URL(s)")
        if cfg.stealth:
            job.log("Stealth mode: enabled")
        if cfg.crawl_depth >= 1:
            job.log("Deep crawl: enabled")

        def scrape_progress(i, total, msg):
            if job.cancel_flag:
                raise InterruptedError("Cancelled")
            with job.lock:
                job.progress_pct = int((i / max(total, 1)) * 100)
                job.progress_msg = msg
            job.log(msg)

        leads = asyncio.run(scrape_all(urls, on_progress=scrape_progress, config=cfg))

        # Outreach phase (if enabled)
        leads = _run_outreach_phase(job, leads, cfg)

        leads = scoring.sort_by_score(scoring.annotate(leads))
        _log_tier_summary(job, leads)

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = leads
            job.progress_msg = f"Done — {len(leads)} leads found."
            job.finished_at = time.time()
        job.log(f"Completed: {len(leads)} leads in {job.duration:.1f}s")
        _save_backup(job)
        _log_run_finish(job)

    except InterruptedError:
        with job.lock:
            job.status = "cancelled"
            job.progress_msg = "Cancelled by user."
            job.finished_at = time.time()
        job.log("Cancelled by user", "warn")
        _log_run_finish(job)
    except Exception as e:
        with job.lock:
            job.status = "error"
            job.error = str(e)
            job.progress_msg = f"Error: {e}"
            job.finished_at = time.time()
        job.log(f"Error: {e}", "error")
        _log_run_finish(job)


# ── Routes ────────────────────────────────────────────────────────────

@app.route("/login")
def login_page():
    if current_user.is_authenticated:
        return redirect("/")
    return render_template("login.html")


@app.post("/api/login")
def api_login():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify(error="Username and password required"), 400

    conn = _get_runs_db()
    row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    conn.close()

    if not row or not check_password_hash(row["password_hash"], password):
        return jsonify(error="Invalid username or password"), 401

    login_user(User(row["id"], row["username"]), remember=True)
    return jsonify(ok=True, username=row["username"])


@app.post("/api/signup")
def api_signup():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""
    if not username or len(username) < 3:
        return jsonify(error="Username must be at least 3 characters"), 400
    if len(password) < 6:
        return jsonify(error="Password must be at least 6 characters"), 400

    user_id = uuid4().hex
    pw_hash = generate_password_hash(password)
    conn = _get_runs_db()
    try:
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) VALUES (?,?,?,?)",
            (user_id, username, pw_hash, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify(error="Username already taken"), 409
    conn.close()

    login_user(User(user_id, username), remember=True)
    return jsonify(ok=True, username=username)


@app.post("/api/logout")
def api_logout():
    logout_user()
    return jsonify(ok=True)


@app.route("/")
def index():
    return render_template("index.html", username=current_user.username)


@app.get("/health")
def health():
    backup_count = len(list(BACKUP_DIR.glob("*.json")))
    try:
        usage = shutil.disk_usage("/data")
        disk_used_mb = round(usage.used / 1024 / 1024)
        disk_free_mb = round(usage.free / 1024 / 1024)
    except Exception:
        disk_used_mb = disk_free_mb = None
    try:
        cache_stats = cache.stats()
    except Exception:
        cache_stats = {}
    return jsonify(
        status="ok",
        queue_size=_work_queue.qsize(),
        active_jobs=sum(1 for j in jobs.values() if j.status in ("searching", "scraping")),
        total_jobs=len(jobs),
        backups_on_disk=backup_count,
        disk_used_mb=disk_used_mb,
        disk_free_mb=disk_free_mb,
        **cache_stats,
    )


@app.get("/api/provinces")
def api_provinces():
    """Return list of provinces that have cities with polygons."""
    conn = _get_cities_db()
    rows = conn.execute(
        "SELECT DISTINCT province_name FROM cities "
        "WHERE status='success' AND polygon_geojson IS NOT NULL "
        "ORDER BY province_name"
    ).fetchall()
    conn.close()
    return jsonify(provinces=[r["province_name"] for r in rows])


@app.get("/api/cities")
def api_cities():
    """Return filterable city list (without polygon data)."""
    province = request.args.get("province", "").strip()
    min_pop = int(request.args.get("min_population", 0))

    conn = _get_cities_db()
    query = (
        "SELECT id, city, province_id, province_name, population "
        "FROM cities WHERE status='success' AND polygon_geojson IS NOT NULL"
    )
    params = []
    if province:
        query += " AND province_name = ?"
        params.append(province)
    if min_pop > 0:
        query += " AND population >= ?"
        params.append(min_pop)
    query += " ORDER BY population DESC"

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify(cities=[dict(r) for r in rows])


@app.post("/api/search")
def api_search():
    data = request.get_json(force=True)
    keyword = data.get("keyword", "").strip()
    cities_raw = data.get("cities", "").strip()
    num = int(data.get("num", 50))

    if not keyword:
        return jsonify(error="Keyword is required"), 400
    if not cities_raw:
        return jsonify(error="At least one city is required"), 400

    cities = [c.strip() for c in cities_raw.split(",") if c.strip()]
    job = _create_job(
        mode="keyword", keyword=keyword, cities=cities, num=num,
        stealth=data.get("stealth", True),
        google_maps=data.get("google_maps", False),
        deep_crawl=data.get("deep_crawl", False),
        proxies=data.get("proxies", ""),
        concurrency=int(data.get("concurrency", 8)),
        use_cache=bool(data.get("use_cache", True)),
        enrich=bool(data.get("enrich", True)),
    )

    _work_queue.put((_run_keyword_job, (job, keyword, cities, num)))
    return jsonify(job_id=job.id)


@app.post("/api/maps")
def api_maps():
    data = request.get_json(force=True)
    keyword = data.get("keyword", "").strip()
    if not keyword:
        return jsonify(error="Keyword is required"), 400

    city_ids = data.get("city_ids", [])
    enrich = data.get("enrich_websites", False)
    grid_spacing_km = float(data.get("grid_spacing_km", 1.0))
    outreach_keys = ("outreach_enabled", "sender_email", "sender_phone",
                     "sender_company", "message_template")

    # ── Multi-city mode (city_ids from database) ──
    if city_ids:
        max_results = min(int(data.get("max_results", 500)), 2000)
        job = _create_job(
            mode="maps_multi_city", keyword=keyword, city_ids=city_ids,
            max_results=max_results, enrich_websites=enrich,
            grid_spacing_km=grid_spacing_km,
            stealth=data.get("stealth", True),
            proxies=data.get("proxies", ""),
            concurrency=int(data.get("concurrency", 2)),
            **{k: data[k] for k in outreach_keys if k in data},
        )
        _work_queue.put((_run_multi_city_maps_job,
                         (job, keyword, city_ids, max_results, grid_spacing_km, enrich)))
        return jsonify(job_id=job.id)

    # ── Single polygon or city text mode (legacy) ──
    city = data.get("city", "").strip()
    query = data.get("query", "").strip()
    area_search = data.get("area_search", False)
    polygon = data.get("polygon")

    max_cap = 2000 if (area_search and polygon) else 120
    max_results = min(int(data.get("max_results", 100)), max_cap)

    if not query:
        if keyword and city:
            query = f"{keyword} in {city}"
        elif keyword:
            query = keyword

    if area_search and not polygon:
        return jsonify(error="Polygon GeoJSON is required for area search"), 400

    job = _create_job(
        mode="maps", query=query, keyword=keyword, max_results=max_results,
        enrich_websites=enrich, area_search=area_search, polygon=polygon,
        grid_spacing_km=grid_spacing_km,
        stealth=data.get("stealth", True),
        proxies=data.get("proxies", ""),
        concurrency=int(data.get("concurrency", 4)),
        use_cache=bool(data.get("use_cache", True)),
        enrich=bool(data.get("enrich", True)),
        **{k: data[k] for k in outreach_keys if k in data},
    )

    _work_queue.put((_run_maps_job, (job, query, max_results, enrich)))
    return jsonify(job_id=job.id)


@app.post("/api/homestars")
def api_homestars():
    data = request.get_json(force=True)
    keyword = data.get("keyword", "").strip()
    if not keyword:
        return jsonify(error="Keyword is required"), 400

    city_ids = data.get("city_ids", [])
    if not city_ids:
        return jsonify(error="Select at least one city"), 400

    max_results = min(int(data.get("max_results", 100)), 500)
    enrich = data.get("enrich_websites", False)
    outreach_keys = ("outreach_enabled", "sender_email", "sender_phone",
                     "sender_company", "message_template")

    job = _create_job(
        mode="homestars", keyword=keyword, city_ids=city_ids,
        max_results=max_results, enrich_websites=enrich,
        stealth=data.get("stealth", True),
        proxies=data.get("proxies", ""),
        concurrency=int(data.get("concurrency", 3)),
        use_cache=bool(data.get("use_cache", True)),
        enrich=bool(data.get("enrich", True)),
        **{k: data[k] for k in outreach_keys if k in data},
    )
    _work_queue.put((_run_homestars_job,
                     (job, keyword, city_ids, max_results, enrich)))
    return jsonify(job_id=job.id)


@app.post("/api/scrape")
def api_scrape():
    data = request.get_json(force=True)
    url = data.get("url", "").strip()
    if not url:
        return jsonify(error="URL is required"), 400

    urls = [normalize_url(url)]
    job = _create_job(
        mode="url", url=urls[0],
        stealth=data.get("stealth", True),
        deep_crawl=data.get("deep_crawl", False),
        proxies=data.get("proxies", ""),
        concurrency=int(data.get("concurrency", 8)),
        use_cache=bool(data.get("use_cache", True)),
        enrich=bool(data.get("enrich", True)),
    )

    _work_queue.put((_run_url_job, (job, urls)))
    return jsonify(job_id=job.id)


@app.post("/api/import")
def api_import():
    data = request.get_json(force=True)

    # Accept {"records": [...]}, a raw array [...], or a single record {...}
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict) and "records" in data:
        records = data.get("records", [])
    elif isinstance(data, dict) and ("website" in data or "url" in data):
        # Single record from n8n (sends one item per request)
        records = [data]
    else:
        records = []

    if not records:
        return jsonify(error="No records provided"), 400

    # Default to higher concurrency for bulk imports
    default_concurrency = min(12, max(4, len(records) // 10))

    # If raw array was sent, use defaults for config
    opts = {} if isinstance(data, list) else data

    job = _create_job(
        mode="import", record_count=len(records),
        stealth=opts.get("stealth", True),
        deep_crawl=opts.get("deep_crawl", True),
        proxies=opts.get("proxies", ""),
        concurrency=int(opts.get("concurrency", default_concurrency)),
        use_cache=bool(opts.get("use_cache", True)),
        enrich=bool(opts.get("enrich", True)),
    )

    _work_queue.put((_run_import_job, (job, records)))
    return jsonify(job_id=job.id)


@app.get("/api/progress/<job_id>")
def api_progress(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error="Job not found"), 404

    def generate():
        last_log_idx = 0
        while True:
            with job.lock:
                new_logs = job.logs[last_log_idx:]
                last_log_idx = len(job.logs)
                data = {
                    "status": job.status,
                    "progress_pct": job.progress_pct,
                    "progress_msg": job.progress_msg,
                    "count": len(job.results),
                    "duration": round(job.duration, 1),
                    "logs": new_logs,
                }
                done = job.status in ("done", "error", "cancelled")
            yield f"data: {json.dumps(data)}\n\n"
            if done:
                break
            time.sleep(0.5)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/results/<job_id>")
def api_results(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error="Job not found"), 404
    return jsonify(
        leads=job.results,
        status=job.status,
        duration=round(job.duration, 1),
        input_config=job.input_config,
    )


def _export_leads(leads, fmt="csv"):
    """Export a list of leads as CSV or JSON Response."""
    if fmt == "json":
        return Response(
            json.dumps(leads, indent=2),
            mimetype="application/json",
            headers={"Content-Disposition": "attachment; filename=leads.json"},
        )

    import csv
    import io
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow([
        "tier", "score",
        "url", "company", "description", "emails", "phones",
        "address", "city", "state", "zip_code", "hours", "socials",
        "category", "rating", "neighborhood",
        "google_reviews", "google_rating", "review_distribution",
        "homestars_rating", "homestars_reviews", "homestars_url", "homestars_verified",
        "price_level", "maps_url", "latitude", "longitude",
        "place_id", "plus_code", "is_closed", "source_city",
    ])
    for lead in leads:
        if "error" in lead:
            continue
        is_closed = lead.get("is_temporarily_closed") or lead.get("is_permanently_closed")
        w.writerow([
            lead.get("tier", ""),
            lead.get("score", ""),
            lead.get("url", ""),
            lead.get("company", ""),
            lead.get("description", ""),
            "; ".join(lead.get("emails", [])),
            "; ".join(lead.get("phones", [])),
            lead.get("address", "") or "",
            lead.get("city", "") or "",
            lead.get("state", "") or "",
            lead.get("zip_code", "") or "",
            lead.get("hours", "") or "",
            json.dumps(lead.get("socials", {})),
            lead.get("category", ""),
            lead.get("rating", ""),
            lead.get("neighborhood", ""),
            lead.get("google_reviews", ""),
            lead.get("google_rating", ""),
            json.dumps(lead.get("review_distribution", {})) if lead.get("review_distribution") else "",
            lead.get("homestars_rating", ""),
            lead.get("homestars_reviews", ""),
            lead.get("homestars_url", ""),
            "Yes" if lead.get("homestars_verified") else "",
            lead.get("price_level", ""),
            lead.get("maps_url", ""),
            lead.get("latitude", ""),
            lead.get("longitude", ""),
            lead.get("place_id", ""),
            lead.get("plus_code", ""),
            "Yes" if is_closed else "",
            lead.get("source_city", ""),
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@app.get("/api/export/<job_id>")
def api_export(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error="Job not found"), 404
    return _export_leads(job.results, request.args.get("format", "csv"))


def _get_run_leads(run_id):
    """Look up leads for a run from in-memory jobs or backup files."""
    # Try in-memory job first
    job = jobs.get(run_id)
    if job and job.results:
        return job.results

    # Try backup file linked in DB
    conn = _get_runs_db()
    row = conn.execute("SELECT backup_filename FROM runs WHERE run_id=?", (run_id,)).fetchone()
    conn.close()

    if row and row["backup_filename"]:
        path = BACKUP_DIR / row["backup_filename"]
        if path.exists():
            try:
                data = json.loads(path.read_text())
                return data.get("leads", [])
            except Exception:
                pass

    # Fallback: scan backups for matching job_id
    for f in BACKUP_DIR.glob(f"*_{run_id}_*.json"):
        try:
            data = json.loads(f.read_text())
            return data.get("leads", [])
        except Exception:
            continue

    return None


@app.get("/api/runs")
def api_runs():
    """List run history for the current user."""
    conn = _get_runs_db()
    rows = conn.execute(
        "SELECT * FROM runs WHERE user_id=? OR user_id IS NULL OR user_id='' ORDER BY created_at DESC LIMIT 100",
        (current_user.id,),
    ).fetchall()
    conn.close()
    return jsonify(runs=[dict(r) for r in rows])


@app.get("/api/runs/<run_id>")
def api_run_detail(run_id):
    """Return run metadata + leads (if backup still available)."""
    conn = _get_runs_db()
    row = conn.execute(
        "SELECT * FROM runs WHERE run_id=? AND (user_id=? OR user_id IS NULL OR user_id='')",
        (run_id, current_user.id),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify(error="Run not found"), 404

    result = dict(row)
    leads = _get_run_leads(run_id)
    result["leads"] = leads
    result["backup_expired"] = leads is None
    return jsonify(result)


@app.get("/api/export/run/<run_id>")
def api_export_run(run_id):
    """Export leads from a historical run."""
    leads = _get_run_leads(run_id)
    if leads is None:
        return jsonify(error="Lead data expired or not found"), 410
    return _export_leads(leads, request.args.get("format", "csv"))


@app.get("/api/backups")
def api_backups():
    """List all backed-up scrape results from the persistent volume."""
    backups = []
    for f in sorted(BACKUP_DIR.glob("*.json"), reverse=True):
        try:
            meta = json.loads(f.read_text())
            backups.append({
                "filename": f.name,
                "job_id": meta.get("job_id"),
                "created_at": meta.get("created_at"),
                "finished_at": meta.get("finished_at"),
                "duration": meta.get("duration"),
                "status": meta.get("status"),
                "lead_count": meta.get("lead_count", 0),
                "input_config": meta.get("input_config", {}),
            })
        except Exception:
            continue
    return jsonify(backups=backups)


@app.get("/api/backups/<filename>")
def api_backup_detail(filename):
    """Retrieve a specific backup file's full data."""
    # Prevent path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        return jsonify(error="Invalid filename"), 400
    path = BACKUP_DIR / filename
    if not path.exists():
        return jsonify(error="Backup not found"), 404
    return Response(
        path.read_text(),
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/api/maps-debug")
def api_maps_debug():
    """Return the last Maps HTML dump for debugging parsing issues."""
    for p in [Path("/data/debug/maps_debug.html"), Path("/tmp/maps_debug.html")]:
        if p.exists():
            html = p.read_text()
            # Return a summary: first 2000 chars + all href patterns
            import re
            hrefs = re.findall(r'href="([^"]*maps/place[^"]*)"', html)
            aria_labels = re.findall(r'aria-label="([^"]{5,80})"', html)
            return jsonify(
                html_length=len(html),
                first_2000=html[:2000],
                place_hrefs=hrefs[:20],
                aria_labels=aria_labels[:30],
                has_feed=('role="feed"' in html),
                has_consent=('consent' in html.lower()),
            )
    return jsonify(error="No debug HTML found — run a Maps scrape first"), 404


@app.post("/api/cancel/<job_id>")
def api_cancel(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error="Job not found"), 404
    job.cancel_flag = True
    return jsonify(ok=True)


# ── Main ──────────────────────────────────────────────────────────────

def open_browser(port: int):
    """Open the browser after a short delay to let Flask start."""
    import webbrowser
    time.sleep(1.5)
    webbrowser.open(f"http://localhost:{port}")


if __name__ == "__main__":
    import os
    import sys

    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5000))
    debug = "--debug" in sys.argv
    is_railway = "RAILWAY_ENVIRONMENT" in os.environ

    print(f"Starting Lead Scraper on http://localhost:{port}")

    if not debug and not is_railway:
        # Auto-open browser in local mode
        threading.Thread(target=open_browser, args=(port,), daemon=True).start()

    app.run(
        host="0.0.0.0" if is_railway else "127.0.0.1",
        debug=debug,
        port=port,
        threaded=True,
    )
