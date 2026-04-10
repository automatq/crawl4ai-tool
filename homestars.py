#!/usr/bin/env python3
"""
HomeStars Scraper Module

Scrapes business listings from HomeStars.com (Canadian home services platform).
Extracts: company name, rating (0-10), review count, phone, website,
address, categories, verified status, years in business.

Uses crawl4ai headless browser + __NEXT_DATA__ JSON extraction (Next.js site).
"""

import asyncio
import json
import logging
import random
import re
import time
from typing import Callable
from urllib.parse import quote

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


# ── Province name → HomeStars URL code mapping ───────────────────────

PROVINCE_CODES = {
    "Alberta": "ab",
    "British Columbia": "bc",
    "Manitoba": "mb",
    "New Brunswick": "nb",
    "Newfoundland and Labrador": "nl",
    "Northwest Territories": "nt",
    "Nova Scotia": "ns",
    "Nunavut": "nu",
    "Ontario": "on",
    "Prince Edward Island": "pe",
    "Quebec": "qc",
    "Saskatchewan": "sk",
    "Yukon": "yt",
}


# ── JS to extract __NEXT_DATA__ from the page ────────────────────────

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

DISMISS_COOKIE_JS = """
(function() {
    // Dismiss cookie consent banners
    const btns = document.querySelectorAll('button');
    for (const b of btns) {
        const t = b.textContent.trim().toLowerCase();
        if (t === 'accept' || t === 'accept all' || t === 'ok' || t === 'got it') {
            b.click();
            break;
        }
    }
})();
"""


# ── Browser config ────────────────────────────────────────────────────

def _make_hs_browser_config(config: ScrapeConfig) -> BrowserConfig:
    """HomeStars needs full rendering (Next.js SPA)."""
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


# ── URL helpers ───────────────────────────────────────────────────────

def _build_search_url(keyword: str, city: str, province_code: str, page: int = 1) -> str:
    """Build HomeStars search URL using legacy format with pagination."""
    # Slugify city name: "St. Catharines" -> "st-catharines"
    city_slug = re.sub(r'[^a-z0-9]+', '-', city.lower()).strip('-')
    # Slugify keyword
    kw_slug = re.sub(r'[^a-z0-9]+', '-', keyword.lower()).strip('-')
    url = f"https://homestars.com/{province_code}/{city_slug}/{kw_slug}"
    if page > 1:
        url += f"?page={page}"
    return url


# ── Parsing ───────────────────────────────────────────────────────────

def _extract_next_data(html: str) -> dict | None:
    """Extract __NEXT_DATA__ JSON from the page HTML."""
    # Try the injected div first
    m = re.search(
        r'<div\s+id="__hs_next_data__"[^>]*>(.*?)</div>',
        html, re.DOTALL
    )
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # Fallback: parse directly from the script tag
    m = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    return None


def _parse_listings_from_next_data(data: dict) -> list[dict]:
    """Extract listing URLs and basic info from search results __NEXT_DATA__."""
    listings = []

    # Navigate the Next.js data structure — try common paths
    props = data.get("props", {}).get("pageProps", {})

    # Try various keys where listings might be stored
    for key in ("companies", "professionals", "results", "listings", "pros", "searchResults"):
        items = props.get(key)
        if items and isinstance(items, list):
            for item in items:
                listing = _normalize_listing(item)
                if listing:
                    listings.append(listing)
            if listings:
                return listings

    # Try nested structures
    search_data = props.get("searchData") or props.get("data") or props.get("initialData") or {}
    if isinstance(search_data, dict):
        for key in ("companies", "professionals", "results", "listings", "pros"):
            items = search_data.get(key)
            if items and isinstance(items, list):
                for item in items:
                    listing = _normalize_listing(item)
                    if listing:
                        listings.append(listing)
                if listings:
                    return listings

    # Deep scan: look for any array of objects with company-like fields
    listings = _deep_scan_for_listings(props)
    return listings


def _deep_scan_for_listings(obj, depth=0) -> list[dict]:
    """Recursively scan for arrays that look like company listings."""
    if depth > 5:
        return []

    if isinstance(obj, list) and len(obj) > 0:
        # Check if items look like company listings
        if isinstance(obj[0], dict):
            has_name = any(k in obj[0] for k in ("name", "companyName", "company_name", "title"))
            has_id = any(k in obj[0] for k in ("id", "companyId", "company_id", "slug"))
            if has_name and has_id:
                results = []
                for item in obj:
                    listing = _normalize_listing(item)
                    if listing:
                        results.append(listing)
                if results:
                    return results

    if isinstance(obj, dict):
        for v in obj.values():
            results = _deep_scan_for_listings(v, depth + 1)
            if results:
                return results

    return []


def _normalize_listing(item: dict) -> dict | None:
    """Normalize a single listing from the Next.js data into our lead schema."""
    if not isinstance(item, dict):
        return None

    # Extract company name
    name = (
        item.get("name") or item.get("companyName") or item.get("company_name")
        or item.get("title") or item.get("displayName") or ""
    )
    if not name:
        return None

    # Extract ID and slug for building profile URL
    company_id = item.get("id") or item.get("companyId") or item.get("company_id") or ""
    slug = item.get("slug") or item.get("urlSlug") or item.get("url_slug") or ""

    # Build profile URL
    homestars_url = ""
    if company_id:
        if slug:
            homestars_url = f"https://homestars.com/companies/{company_id}-{slug}"
        else:
            homestars_url = f"https://homestars.com/companies/{company_id}"
    elif item.get("url") or item.get("profileUrl"):
        homestars_url = item.get("url") or item.get("profileUrl")
        if homestars_url and not homestars_url.startswith("http"):
            homestars_url = f"https://homestars.com{homestars_url}"

    # Rating (HomeStars uses 0-10 scale)
    rating = item.get("starScore") or item.get("star_score") or item.get("rating") or item.get("averageRating")
    if rating is not None:
        try:
            rating = round(float(rating), 1)
        except (ValueError, TypeError):
            rating = None

    # Review count
    reviews = item.get("reviewCount") or item.get("review_count") or item.get("numReviews") or item.get("reviewsCount")
    if reviews is not None:
        try:
            reviews = int(reviews)
        except (ValueError, TypeError):
            reviews = None

    # Phone
    phone = item.get("phone") or item.get("phoneNumber") or item.get("phone_number") or ""

    # Website
    website = item.get("website") or item.get("websiteUrl") or item.get("website_url") or ""
    if website and not website.startswith("http"):
        website = "https://" + website

    # Address
    address_parts = []
    for key in ("address", "streetAddress", "street_address", "street"):
        if item.get(key):
            address_parts.append(str(item[key]))
            break
    city = item.get("city") or item.get("cityName") or ""
    province = item.get("province") or item.get("provinceName") or item.get("state") or ""
    if city:
        address_parts.append(str(city))
    if province:
        address_parts.append(str(province))
    address = ", ".join(address_parts)

    # Category
    category = ""
    cats = item.get("categories") or item.get("services") or item.get("trades") or []
    if isinstance(cats, list) and cats:
        if isinstance(cats[0], dict):
            category = cats[0].get("name") or cats[0].get("title") or ""
        elif isinstance(cats[0], str):
            category = cats[0]
    elif isinstance(cats, str):
        category = cats
    if not category:
        category = item.get("category") or item.get("primaryCategory") or item.get("trade") or ""

    # Verified status
    verified = item.get("verified") or item.get("isVerified") or item.get("is_verified") or False
    if isinstance(verified, dict):
        verified = any(verified.values())

    # Years in business
    years = item.get("yearsInBusiness") or item.get("years_in_business") or item.get("experience") or None

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
        "homestars_verified": bool(verified),
        "homestars_years": years,
        "socials": {},
        "price_level": "",
        "description": item.get("description") or item.get("about") or "",
    }


def _parse_detail_from_next_data(data: dict) -> dict:
    """Extract detailed company info from a profile page's __NEXT_DATA__."""
    props = data.get("props", {}).get("pageProps", {})

    # The company data might be under various keys
    company = (
        props.get("company") or props.get("professional") or props.get("profile")
        or props.get("companyData") or props
    )

    if not isinstance(company, dict):
        return {}

    result = _normalize_listing(company)
    if not result:
        return {}

    # Override with more detailed fields from the profile page
    # Description might be longer on detail page
    desc = company.get("description") or company.get("about") or company.get("bio") or ""
    if desc:
        result["description"] = desc

    # Service areas
    areas = company.get("serviceAreas") or company.get("service_areas") or []
    if areas and isinstance(areas, list):
        if isinstance(areas[0], dict):
            result["service_areas"] = [a.get("name", "") for a in areas]
        elif isinstance(areas[0], str):
            result["service_areas"] = areas

    # All categories/services
    all_cats = company.get("categories") or company.get("services") or company.get("trades") or []
    if all_cats and isinstance(all_cats, list):
        if isinstance(all_cats[0], dict):
            result["all_categories"] = [c.get("name") or c.get("title") or "" for c in all_cats]
        elif isinstance(all_cats[0], str):
            result["all_categories"] = all_cats

    return result


def _extract_total_results(data: dict) -> int | None:
    """Try to extract total result count from search __NEXT_DATA__."""
    props = data.get("props", {}).get("pageProps", {})

    for key in ("totalCount", "total", "totalResults", "count", "resultCount"):
        val = props.get(key)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass

    # Check nested
    for container_key in ("searchData", "data", "pagination", "meta"):
        container = props.get(container_key, {})
        if isinstance(container, dict):
            for key in ("totalCount", "total", "totalResults", "count"):
                val = container.get(key)
                if val is not None:
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        pass
    return None


# ── Fallback HTML parsing (if __NEXT_DATA__ doesn't work) ────────────

def _parse_listings_from_html(html: str) -> list[dict]:
    """Fallback: extract listings from rendered HTML using regex."""
    listings = []

    # Find company profile links
    urls = re.findall(r'href="(/companies/\d+[^"]*)"', html)
    seen = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        full_url = f"https://homestars.com{url}"
        # Try to extract name from URL slug
        m = re.match(r'/companies/(\d+)-(.+)', url)
        name = ""
        if m:
            name = m.group(2).replace('-', ' ').title()
        listings.append({
            "company": name,
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
            "homestars_url": full_url,
            "homestars_verified": False,
            "homestars_years": None,
            "socials": {},
            "price_level": "",
            "description": "",
        })

    return listings


def _parse_detail_from_html(html: str, md: str) -> dict:
    """Fallback: extract company details from rendered HTML."""
    result = {}

    # Company name from h1
    m = re.search(r'<h1[^>]*>([^<]+)</h1>', html)
    if m:
        result["company"] = m.group(1).strip()

    # Phone
    phones = extract_phones(md or html)
    if phones:
        result["phones"] = phones

    # Rating
    m = re.search(r'(\d+\.?\d*)\s*/\s*10', html) or re.search(r'Star\s*Score[:\s]*(\d+\.?\d*)', html, re.I)
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

    return result


# ── Main scraper functions ────────────────────────────────────────────

async def _scrape_search_page(
    crawler: AsyncWebCrawler,
    url: str,
    rate_limiter: DomainRateLimiter,
) -> tuple[list[dict], int | None]:
    """Scrape a single search results page. Returns (listings, total_count)."""
    run_config = CrawlerRunConfig(
        js_code=DISMISS_COOKIE_JS + "\n" + EXTRACT_NEXT_DATA_JS,
        wait_for="css:body",
        page_timeout=30000,
    )

    result, err = await _crawl_with_retry(
        crawler, url, run_config,
        max_retries=3, timeout=45,
        rate_limiter=rate_limiter,
    )

    if err or not result.success:
        log.warning(f"Failed to load search page: {url} (error: {err})")
        return [], None

    html = result.html or ""
    next_data = _extract_next_data(html)

    if next_data:
        listings = _parse_listings_from_next_data(next_data)
        total = _extract_total_results(next_data)
        if listings:
            log.info(f"Extracted {len(listings)} listings from __NEXT_DATA__ (total: {total})")
            return listings, total

    # Fallback to HTML parsing
    listings = _parse_listings_from_html(html)
    log.info(f"Extracted {len(listings)} listings from HTML fallback")
    return listings, None


async def _scrape_detail_page(
    crawler: AsyncWebCrawler,
    homestars_url: str,
    rate_limiter: DomainRateLimiter,
) -> dict:
    """Scrape a single company profile page for full details."""
    run_config = CrawlerRunConfig(
        js_code=DISMISS_COOKIE_JS + "\n" + EXTRACT_NEXT_DATA_JS,
        wait_for="css:body",
        page_timeout=30000,
    )

    result, err = await _crawl_with_retry(
        crawler, homestars_url, run_config,
        max_retries=2, timeout=30,
        rate_limiter=rate_limiter,
    )

    if err or not result.success:
        log.warning(f"Failed to load detail page: {homestars_url} (error: {err})")
        return {}

    html = result.html or ""
    md = result.markdown_v2.raw_markdown if hasattr(result, 'markdown_v2') and result.markdown_v2 else ""
    next_data = _extract_next_data(html)

    if next_data:
        detail = _parse_detail_from_next_data(next_data)
        if detail:
            return detail

    # Fallback
    return _parse_detail_from_html(html, md)


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
    Scrape HomeStars listings for a keyword in a single city.

    Args:
        keyword: Service category (e.g. "plumbing", "electrical")
        city: City name (e.g. "Toronto")
        province_name: Full province name (e.g. "Ontario")
        max_results: Maximum listings to return
        enrich_websites: If True, visit business websites for emails/socials
        config: Scraper configuration
        on_progress: Callback(current, total, message)
    """
    config = config or ScrapeConfig()
    province_code = PROVINCE_CODES.get(province_name, "on")
    rate_limiter = DomainRateLimiter(min_delay=2.5)
    browser_config = _make_hs_browser_config(config)

    all_listings = []
    seen_urls = set()
    page = 1
    max_pages = 20  # Safety limit
    total_estimate = None

    def _progress(current, total, msg):
        if on_progress:
            on_progress(current, total, msg)

    _progress(0, max_results, f"Searching HomeStars for '{keyword}' in {city}, {province_name}...")

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Phase 1: Collect listings from search pages
        while len(all_listings) < max_results and page <= max_pages:
            url = _build_search_url(keyword, city, province_code, page)
            _progress(len(all_listings), max_results,
                      f"Loading search page {page}...")

            page_listings, total = await _scrape_search_page(crawler, url, rate_limiter)

            if total is not None and total_estimate is None:
                total_estimate = min(total, max_results)
                log.info(f"Total results available: {total}")

            if not page_listings:
                log.info(f"No more listings on page {page}, stopping pagination")
                break

            new_count = 0
            for listing in page_listings:
                hs_url = listing.get("homestars_url", "")
                if hs_url and hs_url in seen_urls:
                    continue
                if hs_url:
                    seen_urls.add(hs_url)
                all_listings.append(listing)
                new_count += 1
                if len(all_listings) >= max_results:
                    break

            log.info(f"Page {page}: {new_count} new listings ({len(all_listings)} total)")
            _progress(len(all_listings), total_estimate or max_results,
                      f"Found {len(all_listings)} listings (page {page})...")

            if new_count == 0:
                break

            page += 1
            # Random delay between pages
            await asyncio.sleep(random.uniform(1.5, 3.0))

        if not all_listings:
            _progress(0, 1, "No listings found on HomeStars")
            return []

        log.info(f"Collected {len(all_listings)} listings, now fetching details...")

        # Phase 2: Fetch detail pages for richer data
        sem = asyncio.Semaphore(max(config.concurrency, 3))
        detail_count = 0
        total_details = len(all_listings)

        async def fetch_detail(i: int, listing: dict) -> dict:
            nonlocal detail_count
            hs_url = listing.get("homestars_url", "")
            if not hs_url:
                return listing

            async with sem:
                detail = await _scrape_detail_page(crawler, hs_url, rate_limiter)
                detail_count += 1
                _progress(
                    detail_count, total_details,
                    f"Fetching details ({detail_count}/{total_details})..."
                )

            if detail:
                # Merge detail data into listing (detail wins for non-empty fields)
                for k, v in detail.items():
                    if v and (not listing.get(k) or k in ("description", "phones", "homestars_rating",
                                                           "homestars_reviews", "homestars_verified")):
                        listing[k] = v

            return listing

        tasks = [fetch_detail(i, l) for i, l in enumerate(all_listings)]
        all_listings = await asyncio.gather(*tasks)

        # Phase 3: Optional website enrichment for emails/socials
        if enrich_websites:
            enriched = 0
            enrich_total = sum(1 for l in all_listings if l.get("url"))
            _progress(0, enrich_total or 1, "Enriching with website data...")

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
                            rate_limiter=rate_limiter,
                        )
                        if not err and res.success:
                            text = res.markdown_v2.raw_markdown if hasattr(res, 'markdown_v2') and res.markdown_v2 else ""
                            html = res.html or ""

                            emails = extract_emails(text or html)
                            if emails:
                                listing["emails"] = list(set(listing.get("emails", []) + emails))

                            phones_found = extract_phones(text or html)
                            if phones_found:
                                listing["phones"] = list(set(listing.get("phones", []) + phones_found))

                            socials = extract_social_links(html)
                            if socials:
                                listing["socials"] = {**listing.get("socials", {}), **socials}

                            desc = extract_description(text)
                            if desc and not listing.get("description"):
                                listing["description"] = desc
                    except Exception as e:
                        log.warning(f"Enrich error for {website}: {e}")

                    enriched += 1
                    _progress(enriched, enrich_total, f"Enriching websites ({enriched}/{enrich_total})...")

                return listing

            tasks = [enrich_one(l) for l in all_listings]
            all_listings = await asyncio.gather(*tasks)

    _progress(len(all_listings), len(all_listings),
              f"Done — {len(all_listings)} listings from HomeStars")

    return list(all_listings)


async def scrape_homestars_multi_city(
    keyword: str,
    cities: list[dict],
    max_per_city: int = 100,
    enrich_websites: bool = False,
    config: ScrapeConfig | None = None,
    on_progress: Callable | None = None,
) -> list[dict]:
    """
    Scrape HomeStars across multiple cities.

    Args:
        keyword: Service category
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
