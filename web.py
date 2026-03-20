#!/usr/bin/env python3
"""
Crawl4AI Lead Scraper — Web Interface

Run:  python web.py
Then open http://localhost:5000
"""

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse
from uuid import uuid4

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

from scrape import search_leads, scrape_all, normalize_url, ScrapeConfig

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB max upload


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


def _cleanup_old_jobs():
    cutoff = time.time() - 3600
    stale = [jid for jid, j in jobs.items() if j.created_at < cutoff]
    for jid in stale:
        del jobs[jid]


def _create_job(**input_config) -> Job:
    _cleanup_old_jobs()
    job = Job(id=uuid4().hex[:8], input_config=input_config)
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
        concurrency=int(input_config.get("concurrency", 3)),
    )


def _run_keyword_job(job: Job, keyword: str, cities: list[str], num: int):
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
            return

        if not urls:
            with job.lock:
                job.status = "done"
                job.progress_pct = 100
                job.progress_msg = "No business sites found."
                job.finished_at = time.time()
            job.log("No business sites found", "warn")
            return

        job.log(f"Found {len(urls)} unique business URLs")

        with job.lock:
            job.status = "scraping"
            job.progress_msg = f"Scraping {len(urls)} sites..."

        def scrape_progress(i, total, msg):
            if job.cancel_flag:
                raise InterruptedError("Cancelled")
            with job.lock:
                job.progress_pct = 45 + int((i / max(total, 1)) * 55)
                job.progress_msg = msg
            job.log(msg)

        leads = asyncio.run(scrape_all(urls, on_progress=scrape_progress, config=cfg))

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = leads
            job.progress_msg = f"Done — {len(leads)} leads scraped."
            job.finished_at = time.time()
        job.log(f"Completed: {len(leads)} leads scraped in {job.duration:.1f}s")

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


def _run_import_job(job: Job, records: list[dict]):
    """Scrape URLs from imported data (e.g. Google Maps export) and merge results."""
    try:
        cfg = _make_config(job.input_config)

        # Extract website URLs from records, keeping the source data for merging
        url_to_record: dict[str, dict] = {}
        for rec in records:
            url = rec.get("website") or rec.get("url") or ""
            url = url.strip()
            if not url or url.startswith("https://www.google.com/maps"):
                continue
            url = normalize_url(url)
            if url not in url_to_record:
                url_to_record[url] = rec

        urls = list(url_to_record.keys())
        if not urls:
            with job.lock:
                job.status = "done"
                job.progress_pct = 100
                job.progress_msg = "No website URLs found in imported data."
                job.finished_at = time.time()
            job.log("No website URLs found in imported data", "warn")
            return

        with job.lock:
            job.status = "scraping"
            job.progress_msg = f"Scraping {len(urls)} URLs from imported data..."
        job.log(f"Found {len(urls)} website URLs from {len(records)} imported records")
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

        # Merge: use imported data as base, overlay scraped data on top
        merged = []
        for lead in leads:
            url = lead.get("url", "")
            source = url_to_record.get(url, {})

            # Start with scraped data
            result = dict(lead)

            # Fill in gaps from source data (Google Maps fields)
            if not result.get("company") or result["company"] == urlparse(url).netloc:
                result["company"] = source.get("title") or result.get("company", "")

            if not result.get("address"):
                # Build address from source fields
                parts = [source.get("street", ""), source.get("city", ""), source.get("postalCode", "")]
                addr = ", ".join(p for p in parts if p)
                result["address"] = addr or source.get("address", "")

            if not result.get("phones"):
                phone = source.get("phone") or source.get("phoneUnformatted", "")
                if phone:
                    result["phones"] = [phone]

            # Add Maps-specific fields
            if source.get("categoryName"):
                result["category"] = source["categoryName"]
            if source.get("totalScore"):
                result["rating"] = source["totalScore"]
            if source.get("neighborhood"):
                result["neighborhood"] = source["neighborhood"]

            merged.append(result)

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = merged
            job.progress_msg = f"Done — {len(merged)} leads enriched."
            job.finished_at = time.time()
        job.log(f"Completed: {len(merged)} leads enriched in {job.duration:.1f}s")

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


def _run_url_job(job: Job, urls: list[str]):
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

        with job.lock:
            job.status = "done"
            job.progress_pct = 100
            job.results = leads
            job.progress_msg = f"Done — {len(leads)} leads found."
            job.finished_at = time.time()
        job.log(f"Completed: {len(leads)} leads in {job.duration:.1f}s")

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


# ── Routes ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


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
        concurrency=int(data.get("concurrency", 3)),
    )

    thread = threading.Thread(
        target=_run_keyword_job, args=(job, keyword, cities, num), daemon=True
    )
    thread.start()
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
        concurrency=int(data.get("concurrency", 3)),
    )

    thread = threading.Thread(
        target=_run_url_job, args=(job, urls), daemon=True
    )
    thread.start()
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
    default_concurrency = min(10, max(3, len(records) // 10))

    # If raw array was sent, use defaults for config
    opts = {} if isinstance(data, list) else data

    job = _create_job(
        mode="import", record_count=len(records),
        stealth=opts.get("stealth", True),
        deep_crawl=opts.get("deep_crawl", True),
        proxies=opts.get("proxies", ""),
        concurrency=int(opts.get("concurrency", default_concurrency)),
    )

    thread = threading.Thread(
        target=_run_import_job, args=(job, records), daemon=True
    )
    thread.start()
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


@app.get("/api/export/<job_id>")
def api_export(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error="Job not found"), 404

    fmt = request.args.get("format", "csv")

    if fmt == "json":
        return Response(
            json.dumps(job.results, indent=2),
            mimetype="application/json",
            headers={"Content-Disposition": "attachment; filename=leads.json"},
        )

    import csv
    import io
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow([
        "url", "company", "description", "emails", "phones",
        "address", "hours", "socials", "category", "rating", "neighborhood",
    ])
    for lead in job.results:
        if "error" in lead:
            continue
        w.writerow([
            lead.get("url", ""),
            lead.get("company", ""),
            lead.get("description", ""),
            "; ".join(lead.get("emails", [])),
            "; ".join(lead.get("phones", [])),
            lead.get("address", "") or "",
            lead.get("hours", "") or "",
            json.dumps(lead.get("socials", {})),
            lead.get("category", ""),
            lead.get("rating", ""),
            lead.get("neighborhood", ""),
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


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
