#!/usr/bin/env python3
"""
HomeStars Scraper Module

Scrapes business listings from HomeStars.com (Canadian home services platform).
Uses Google search (site:homestars.com) to discover company profile URLs,
then scrapes each profile page for detailed business data.

Extracts: company name, rating (0-10), review count, phone, website,
address, categories, verified status, years in business.
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Callable
from urllib.parse import quote_plus

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from scrape import (
    ScrapeConfig,
    DomainRateLimiter,
    _crawl_with_retry,
    normalize_url,
    extract_emails,
    extract_phones,
    extract_social_links,
    extract_description,
)

log = logging.getLogger("homestars")
logging.basicConfig(level=logging.INFO)

# ── Debug dump directory ──────────────────────────────────────────────

DEBUG_DIR = Path(os.environ.get("DEBUG_DIR", "/data/debug"))
DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def _dump_debug(name: str, content: str):
    """Save debug content to a file."""
    try:
        path = DEBUG_DIR / f"hs_{name}_{int(time.time())}.html"
        path.write_text(content[:500_000])  # Cap at 500KB
        log.info(f"Debug dump saved: {path}")
    except Exception as e:
        log.warning(f"Debug dump failed: {e}")


def _dump_json_debug(name: str, data):
    """Save debug JSON to a file."""
    try:
        path = DEBUG_DIR / f"hs_{name}_{int(time.time())}.json"
        path.write_text(json.dumps(data, indent=2, default=str)[:500_000])
        log.info(f"Debug JSON dump saved: {path}")
    except Exception as e:
        log.warning(f"Debug JSON dump failed: {e}")


# ── Google consent JS (reused from gmaps) ─────────────────────────────

CONSENT_JS = """
const btns = document.querySelectorAll('button');
for (const btn of btns) {
    const text = btn.textContent.trim().toLowerCase();
    if (text === 'accept all' || text === 'i agree' || text === 'consent'
        || text === 'accept' || text === 'ok' || text === 'got it') {
        btn.click();
        break;
    }
}
const form = document.querySelector('form[action*="consent"]');
if (form) {
    const submit = form.querySelector('button, input[type="submit"]');
    if (submit) submit.click();
}
"""

# ── JS to extract __NEXT_DATA__ from HomeStars pages ──────────────────

EXTRACT_NEXT_DATA_JS = """
(function() {
    const el = document.getElementById('__NEXT_DATA__');
    if (!el) return;
    const div = document.createElement('div');
    div.id = '__hs_next_data__';
    div.style.display = 'none';
    div.textContent = el.textContent;
    document.body.appendChild(div);
})();
"""


# ── Browser config ────────────────────────────────────────────────────

def _make_hs_browser_config(config: ScrapeConfig) -> BrowserConfig:
    """Browser config — needs full rendering for both Google and HomeStars."""
    from crawl4ai.async_configs import ProxyConfig
    from crawl4ai.proxy_strategy import RoundRobinProxyStrategy

    kwargs = dict(
        headless=True,
        enable_stealth=config.stealth,
        user_agent_mode="random" if config.stealth else "",
        text_mode=False,
        light_mode=False,
        extra_args=[
            "--disable-gpu",
            "--disable-dev-shm-usage",
        ],
    )
    if config.proxies:
        proxy_configs = [ProxyConfig(server=p) for p in config.proxies]
        kwargs["proxy_config"] = ProxyConfig(
            server=config.proxies[0],
            strategy=RoundRobinProxyStrategy(proxy_configs),
        )
    return BrowserConfig(**kwargs)


# ── Phase 1: Search engines for HomeStars URLs ──────────────────────

_HS_COMPANY_URL_RE = re.compile(
    r'https?://(?:www\.)?homestars\.com/companies/([\w][\w-]*)',
    re.IGNORECASE,
)


def _extract_hs_urls(html: str, seen: set) -> list[str]:
    """Extract unique HomeStars company URLs from search result HTML."""
    new_urls = []
    for slug in _HS_COMPANY_URL_RE.findall(html):
        hs_url = f"https://homestars.com/companies/{slug}"
        if hs_url not in seen:
            seen.add(hs_url)
            new_urls.append(hs_url)
    return new_urls


async def _ddg_search_homestars(
    crawler: AsyncWebCrawler,
    keyword: str,
    city: str,
    rate_limiter: DomainRateLimiter,
    max_results: int = 100,
    on_log: Callable | None = None,
) -> list[str]:
    """Search DuckDuckGo for HomeStars company pages."""
    found_urls = []
    seen = set()

    # DuckDuckGo doesn't have clean pagination — fetch first page with more results
    query = f"site:homestars.com/companies {keyword} {city}"
    url = f"https://duckduckgo.com/?q={quote_plus(query)}&t=h_&ia=web"
    log.info(f"DuckDuckGo search URL: {url}")

    if on_log:
        on_log(f"DuckDuckGo search: '{keyword} {city}'...")

    run_config = CrawlerRunConfig(
        js_code="""
            // Scroll down to trigger lazy-loading of more results
            for (let i = 0; i < 5; i++) {
                window.scrollTo(0, document.body.scrollHeight);
                await new Promise(r => setTimeout(r, 1500));
            }
        """,
        wait_for="css:body",
        page_timeout=25000,
    )

    result, err = await _crawl_with_retry(
        crawler, url, run_config,
        max_retries=3, timeout=40,
        rate_limiter=rate_limiter,
    )

    if err or not result.success:
        log.warning(f"DuckDuckGo search failed: {err}")
        if result and result.html:
            _dump_debug("ddg_fail", result.html)
        return []

    html = result.html or ""
    _dump_debug("ddg_results", html)
    log.info(f"DuckDuckGo HTML length: {len(html)} chars")

    new_urls = _extract_hs_urls(html, seen)
    found_urls.extend(new_urls)
    log.info(f"DuckDuckGo: {len(new_urls)} HomeStars URLs found")
    if new_urls:
        log.info(f"DuckDuckGo matches: {new_urls[:5]}")

    return found_urls[:max_results]


async def _bing_search_homestars(
    crawler: AsyncWebCrawler,
    keyword: str,
    city: str,
    rate_limiter: DomainRateLimiter,
    max_results: int = 100,
    on_log: Callable | None = None,
) -> list[str]:
    """Search Bing for HomeStars company pages."""
    found_urls = []
    seen = set()
    page = 0
    max_pages = min(10, (max_results // 10) + 1)

    while len(found_urls) < max_results and page < max_pages:
        first = page * 10 + 1
        query = f"site:homestars.com/companies {keyword} {city}"
        url = f"https://www.bing.com/search?q={quote_plus(query)}&first={first}"
        log.info(f"Bing search URL: {url}")

        if on_log:
            on_log(f"Bing search page {page + 1}: '{keyword} {city}'...")

        run_config = CrawlerRunConfig(
            wait_for="css:#b_results",
            page_timeout=20000,
        )

        result, err = await _crawl_with_retry(
            crawler, url, run_config,
            max_retries=3, timeout=30,
            rate_limiter=rate_limiter,
        )

        if err or not result.success:
            log.warning(f"Bing search failed (page {page}): {err}")
            if result and result.html:
                _dump_debug(f"bing_fail_p{page}", result.html)
            break

        html = result.html or ""
        _dump_debug(f"bing_p{page}", html)
        log.info(f"Bing page {page} HTML length: {len(html)} chars")

        new_urls = _extract_hs_urls(html, seen)
        found_urls.extend(new_urls)
        log.info(f"Bing page {page + 1}: {len(new_urls)} new URLs ({len(found_urls)} total)")
        if new_urls:
            log.info(f"Bing matches: {new_urls[:5]}")

        if not new_urls:
            break

        page += 1
        await asyncio.sleep(random.uniform(1.5, 3.0))

    return found_urls[:max_results]


async def _search_homestars(
    crawler: AsyncWebCrawler,
    keyword: str,
    city: str,
    rate_limiter: DomainRateLimiter,
    max_results: int = 100,
    on_log: Callable | None = None,
) -> list[str]:
    """Search for HomeStars profiles using DuckDuckGo first, then Bing as fallback."""

    # Try DuckDuckGo first — no CAPTCHAs
    if on_log:
        on_log(f"Searching DuckDuckGo for '{keyword}' in {city}...")
    urls = await _ddg_search_homestars(
        crawler, keyword, city, rate_limiter,
        max_results=max_results, on_log=on_log,
    )

    if urls:
        log.info(f"DuckDuckGo found {len(urls)} profiles — skipping Bing")
        return urls

    # Fallback to Bing
    log.info("DuckDuckGo returned 0 results — trying Bing...")
    if on_log:
        on_log(f"Trying Bing for '{keyword}' in {city}...")
    urls = await _bing_search_homestars(
        crawler, keyword, city, rate_limiter,
        max_results=max_results, on_log=on_log,
    )

    if urls:
        log.info(f"Bing found {len(urls)} profiles")
    else:
        log.warning(f"Both DuckDuckGo and Bing returned 0 results for '{keyword}' in {city}")

    return urls


# ── Phase 2: Scrape HomeStars profile pages ───────────────────────────

def _extract_next_data(html: str) -> dict | None:
    """Extract __NEXT_DATA__ JSON from HomeStars page HTML."""
    # Try the injected div first
    m = re.search(
        r'<div\s+id="__hs_next_data__"[^>]*>(.*?)</div>',
        html, re.DOTALL,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # Fallback: parse directly from the script tag
    m = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    return None


def _parse_profile_from_next_data(data: dict, homestars_url: str) -> dict | None:
    """Extract company data from a profile page's __NEXT_DATA__."""
    props = data.get("props", {}).get("pageProps", {})

    # Try various keys where company data might live
    company = None
    for key in ("company", "professional", "profile", "companyData", "pro"):
        if key in props and isinstance(props[key], dict):
            company = props[key]
            break

    # If no specific key, try pageProps itself if it has a name
    if not company:
        if any(k in props for k in ("name", "companyName", "title")):
            company = props

    if not company or not isinstance(company, dict):
        return None

    return _normalize_company(company, homestars_url)


def _normalize_company(item: dict, homestars_url: str = "") -> dict | None:
    """Normalize a company data dict into our lead schema."""
    if not isinstance(item, dict):
        return None

    # Company name — try many possible keys
    name = ""
    for key in ("name", "companyName", "company_name", "title", "displayName", "businessName"):
        val = item.get(key)
        if val and isinstance(val, str):
            name = val.strip()
            break
    if not name:
        return None

    # Rating (HomeStars uses 0-10 scale)
    rating = None
    for key in ("starScore", "star_score", "rating", "averageRating", "score", "overallRating"):
        val = item.get(key)
        if val is not None:
            try:
                rating = round(float(val), 1)
                break
            except (ValueError, TypeError):
                pass

    # Review count
    reviews = None
    for key in ("reviewCount", "review_count", "numReviews", "reviewsCount", "totalReviews"):
        val = item.get(key)
        if val is not None:
            try:
                reviews = int(val)
                break
            except (ValueError, TypeError):
                pass

    # Phone
    phone = ""
    for key in ("phone", "phoneNumber", "phone_number", "contactPhone"):
        val = item.get(key)
        if val and isinstance(val, str):
            phone = val.strip()
            break

    # Website
    website = ""
    for key in ("website", "websiteUrl", "website_url", "websiteLink"):
        val = item.get(key)
        if val and isinstance(val, str):
            website = val.strip()
            break
    if website and not website.startswith("http"):
        website = "https://" + website

    # Address
    address_parts = []
    for key in ("address", "streetAddress", "street_address", "street"):
        val = item.get(key)
        if val and isinstance(val, str):
            address_parts.append(val)
            break
    city = ""
    for key in ("city", "cityName", "city_name"):
        val = item.get(key)
        if val and isinstance(val, str):
            city = val
            break
    province = ""
    for key in ("province", "provinceName", "state"):
        val = item.get(key)
        if val and isinstance(val, str):
            province = val
            break
    if city:
        address_parts.append(city)
    if province:
        address_parts.append(province)
    address = ", ".join(address_parts)

    # Category
    category = ""
    cats = item.get("categories") or item.get("services") or item.get("trades") or []
    if isinstance(cats, list) and cats:
        if isinstance(cats[0], dict):
            category = cats[0].get("name") or cats[0].get("title") or ""
        elif isinstance(cats[0], str):
            category = cats[0]
    if not category:
        for key in ("category", "primaryCategory", "trade", "profession"):
            val = item.get(key)
            if val and isinstance(val, str):
                category = val
                break

    # Verified status
    verified = False
    for key in ("verified", "isVerified", "is_verified"):
        val = item.get(key)
        if val:
            if isinstance(val, dict):
                verified = any(val.values())
            else:
                verified = bool(val)
            break

    # Years in business
    years = None
    for key in ("yearsInBusiness", "years_in_business", "experience"):
        val = item.get(key)
        if val is not None:
            try:
                years = int(val)
                break
            except (ValueError, TypeError):
                pass

    # Description
    desc = ""
    for key in ("description", "about", "bio", "companyDescription"):
        val = item.get(key)
        if val and isinstance(val, str):
            desc = val.strip()
            break

    return {
        "company": name,
        "category": category if isinstance(category, str) else "",
        "url": normalize_url(website) if website else "",
        "emails": [],
        "phones": [phone] if phone else [],
        "address": address,
        "hours": "",
        "google_rating": None,
        "google_reviews": None,
        "homestars_rating": rating,
        "homestars_reviews": reviews,
        "homestars_url": homestars_url,
        "homestars_verified": verified,
        "homestars_years": years,
        "socials": {},
        "price_level": "",
        "description": desc,
    }


def _parse_profile_from_html(html: str, md: str, homestars_url: str) -> dict:
    """Fallback: extract company data from rendered HTML/markdown."""
    result = {
        "company": "",
        "category": "",
        "url": "",
        "emails": [],
        "phones": [],
        "address": "",
        "hours": "",
        "google_rating": None,
        "google_reviews": None,
        "homestars_rating": None,
        "homestars_reviews": None,
        "homestars_url": homestars_url,
        "homestars_verified": False,
        "homestars_years": None,
        "socials": {},
        "price_level": "",
        "description": "",
    }

    # Company name from h1
    m = re.search(r'<h1[^>]*>([^<]+)</h1>', html)
    if m:
        result["company"] = m.group(1).strip()

    # Try name from URL slug as last resort
    if not result["company"]:
        m = re.search(r'/companies/\d+-(.+?)(?:\?|$)', homestars_url)
        if m:
            result["company"] = m.group(1).replace('-', ' ').title()

    # Phone from HTML/markdown
    phones = extract_phones(md or html)
    if phones:
        result["phones"] = phones

    # Rating — "X.X / 10" or "Star Score: X.X"
    m = re.search(r'(\d+\.?\d*)\s*/\s*10', html)
    if not m:
        m = re.search(r'Star\s*Score[:\s]*(\d+\.?\d*)', html, re.I)
    if m:
        try:
            result["homestars_rating"] = round(float(m.group(1)), 1)
        except ValueError:
            pass

    # Review count
    m = re.search(r'(\d+)\s*reviews?', html, re.I)
    if m:
        try:
            result["homestars_reviews"] = int(m.group(1))
        except ValueError:
            pass

    # Emails
    emails = extract_emails(md or html)
    if emails:
        result["emails"] = emails

    # Description from markdown (first substantial paragraph)
    if md:
        desc = extract_description(md)
        if desc:
            result["description"] = desc

    return result


async def _scrape_profile(
    crawler: AsyncWebCrawler,
    homestars_url: str,
    rate_limiter: DomainRateLimiter,
    dump_first: bool = False,
) -> dict | None:
    """Scrape a single HomeStars company profile page."""
    run_config = CrawlerRunConfig(
        js_code=CONSENT_JS + "\n" + EXTRACT_NEXT_DATA_JS,
        wait_for="css:body",
        page_timeout=30000,
    )

    result, err = await _crawl_with_retry(
        crawler, homestars_url, run_config,
        max_retries=2, timeout=30,
        rate_limiter=rate_limiter,
    )

    if err or not result.success:
        log.warning(f"Failed to load profile: {homestars_url} (error: {err})")
        if result and result.html:
            _dump_debug("profile_fail", result.html)
        return None

    html = result.html or ""
    md = ""
    try:
        md = result.markdown_v2.raw_markdown if result.markdown_v2 else ""
    except Exception:
        pass

    # Dump first profile for debugging
    if dump_first:
        _dump_debug("profile_html", html)
        if md:
            _dump_debug("profile_md", md)

    # Try __NEXT_DATA__ extraction
    next_data = _extract_next_data(html)
    if next_data:
        if dump_first:
            _dump_json_debug("profile_next_data", next_data)

        lead = _parse_profile_from_next_data(next_data, homestars_url)
        if lead and lead.get("company"):
            log.info(f"Extracted from __NEXT_DATA__: {lead['company']}")
            return lead
        else:
            log.info("__NEXT_DATA__ found but couldn't parse company data")

    # Fallback to HTML/markdown parsing
    lead = _parse_profile_from_html(html, md, homestars_url)
    if lead.get("company"):
        log.info(f"Extracted from HTML fallback: {lead['company']}")
    else:
        log.warning(f"Could not extract company data from {homestars_url}")
        _dump_debug("profile_empty", html)

    return lead if lead.get("company") else None


# ── Main entry points ─────────────────────────────────────────────────

async def scrape_homestars(
    keyword: str,
    city: str,
    province_name: str,
    max_results: int = 100,
    enrich_websites: bool = False,
    config: ScrapeConfig | None = None,
    on_progress: Callable | None = None,
) -> list[dict]:
    """
    Scrape HomeStars listings via Google search + profile scraping.

    Args:
        keyword: Search keyword (e.g. "plumber", "electrician")
        city: City name (e.g. "Toronto")
        province_name: Full province name (e.g. "Ontario")
        max_results: Maximum listings to return
        enrich_websites: If True, visit business websites for emails/socials
        config: Scraper configuration
        on_progress: Callback(current, total, message)
    """
    config = config or ScrapeConfig()
    search_limiter = DomainRateLimiter(min_delay=2.0)
    hs_limiter = DomainRateLimiter(min_delay=2.0)
    browser_config = _make_hs_browser_config(config)

    log.info(f"=== HomeStars scrape start: keyword='{keyword}', city='{city}', province='{province_name}', max={max_results} ===")

    def _progress(current, total, msg):
        if on_progress:
            on_progress(current, total, msg)

    _progress(0, max_results, f"Searching for HomeStars '{keyword}' in {city}...")

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Phase 1: Find HomeStars profile URLs via DuckDuckGo / Bing
        profile_urls = await _search_homestars(
            crawler, keyword, city, search_limiter,
            max_results=max_results,
            on_log=lambda msg: _progress(0, max_results, msg),
        )

        log.info(f"Google search returned {len(profile_urls)} profile URLs: {profile_urls[:5]}")

        if not profile_urls:
            _progress(0, 1, f"No HomeStars results found for '{keyword}' in {city}")
            return []

        log.info(f"Found {len(profile_urls)} HomeStars profiles to scrape")
        _progress(0, len(profile_urls), f"Found {len(profile_urls)} profiles, scraping details...")

        # Phase 2: Scrape each profile page
        sem = asyncio.Semaphore(max(config.concurrency, 3))
        scraped = 0
        all_leads = []

        async def scrape_one(i: int, url: str) -> dict | None:
            nonlocal scraped
            async with sem:
                lead = await _scrape_profile(
                    crawler, url, hs_limiter,
                    dump_first=(i == 0),  # Dump first profile for debugging
                )
                scraped += 1
                _progress(scraped, len(profile_urls),
                          f"Scraping profiles ({scraped}/{len(profile_urls)})...")
                return lead

        tasks = [scrape_one(i, url) for i, url in enumerate(profile_urls)]
        results = await asyncio.gather(*tasks)

        for lead in results:
            if lead:
                all_leads.append(lead)

        log.info(f"Scraped {len(all_leads)} leads from {len(profile_urls)} profiles")

        # Phase 3: Optional website enrichment
        if enrich_websites and all_leads:
            enrichable = [l for l in all_leads if l.get("url")]
            enrich_total = len(enrichable)
            enriched = 0

            if enrich_total > 0:
                _progress(0, enrich_total, "Enriching with website data...")

                async def enrich_one(listing: dict) -> dict:
                    nonlocal enriched
                    website = listing.get("url", "")
                    if not website:
                        return listing

                    async with sem:
                        try:
                            run_config = CrawlerRunConfig(
                                wait_for="css:body",
                                page_timeout=20000,
                            )
                            res, err = await _crawl_with_retry(
                                crawler, website, run_config,
                                max_retries=2, timeout=20,
                                rate_limiter=hs_limiter,
                            )
                            if not err and res.success:
                                text = ""
                                try:
                                    text = res.markdown_v2.raw_markdown if res.markdown_v2 else ""
                                except Exception:
                                    pass
                                page_html = res.html or ""

                                emails = extract_emails(text or page_html)
                                if emails:
                                    listing["emails"] = list(set(listing.get("emails", []) + emails))

                                phones_found = extract_phones(text or page_html)
                                if phones_found:
                                    listing["phones"] = list(set(listing.get("phones", []) + phones_found))

                                socials = extract_social_links(page_html)
                                if socials:
                                    listing["socials"] = {**listing.get("socials", {}), **socials}

                                desc = extract_description(text)
                                if desc and not listing.get("description"):
                                    listing["description"] = desc
                        except Exception as e:
                            log.warning(f"Enrich error for {website}: {e}")

                        enriched += 1
                        _progress(enriched, enrich_total,
                                  f"Enriching websites ({enriched}/{enrich_total})...")

                    return listing

                tasks = [enrich_one(l) for l in all_leads]
                all_leads = list(await asyncio.gather(*tasks))

    _progress(len(all_leads), len(all_leads),
              f"Done — {len(all_leads)} leads from HomeStars")

    return all_leads


async def scrape_homestars_multi_city(
    keyword: str,
    cities: list[dict],
    max_per_city: int = 100,
    enrich_websites: bool = False,
    config: ScrapeConfig | None = None,
    on_progress: Callable | None = None,
) -> list[dict]:
    """
    Scrape HomeStars across multiple cities via Google search.

    Args:
        keyword: Search keyword
        cities: List of dicts with 'city' and 'province_name' keys
        max_per_city: Max listings per city
        enrich_websites: Visit business websites
        config: Scraper configuration
        on_progress: Callback(current, total, message)
    """
    config = config or ScrapeConfig()
    all_leads = []
    seen_urls = set()
    total_cities = len(cities)

    for i, city_info in enumerate(cities):
        city_name = city_info["city"]
        province = city_info["province_name"]
        base_pct = int((i / total_cities) * 100)

        def city_progress(current, total, msg, _cn=city_name, _bp=base_pct):
            if on_progress:
                city_pct = int((current / max(total, 1)) * (100 / total_cities))
                on_progress(
                    min(100, _bp + city_pct),
                    100,
                    f"[{_cn}] {msg}",
                )

        city_progress(0, 1, f"Starting {city_name}, {province}...")

        city_leads = await scrape_homestars(
            keyword=keyword,
            city=city_name,
            province_name=province,
            max_results=max_per_city,
            enrich_websites=enrich_websites,
            config=config,
            on_progress=city_progress,
        )

        # Deduplicate across cities
        new_count = 0
        for lead in city_leads:
            hs_url = lead.get("homestars_url", "")
            if hs_url and hs_url in seen_urls:
                continue
            if hs_url:
                seen_urls.add(hs_url)
            lead["source_city"] = f"{city_name}, {province}"
            all_leads.append(lead)
            new_count += 1

        log.info(f"{city_name}: {new_count} new leads ({len(all_leads)} total)")

    if on_progress:
        on_progress(100, 100, f"Done — {len(all_leads)} leads across {total_cities} cities")

    return all_leads
