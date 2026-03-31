#!/usr/bin/env python3
"""
Google Maps Scraper Module

Scrapes business listings directly from Google Maps search results,
extracting: name, category, address, phone, website, rating, reviews,
review distribution, hours, price level, coordinates, and place ID.

Optionally enriches results by visiting business websites for emails/socials.
"""

import asyncio
import json
import random
import re
import time
from typing import Callable
from urllib.parse import quote_plus, urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from scrape import (
    ScrapeConfig,
    DomainRateLimiter,
    _crawl_with_retry,
    normalize_url,
    extract_emails,
    extract_phones,
    extract_social_links,
    extract_structured_data,
    extract_description,
    guess_company_name,
)


# ── Browser config for Maps (needs full DOM, not text-only) ──────────

def _make_maps_browser_config(config: ScrapeConfig) -> BrowserConfig:
    """Maps needs full rendering — no text_mode or light_mode."""
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
        kwargs["proxy"] = config.proxies[0]
    return BrowserConfig(**kwargs)


# ── JavaScript for scrolling the results panel ───────────────────────

SCROLL_JS = """
(async () => {
    // Find the scrollable results container
    const feed = document.querySelector('div[role="feed"]');
    if (!feed) return 0;

    let prevCount = 0;
    let sameCountRounds = 0;
    const maxScrolls = %d;

    for (let i = 0; i < maxScrolls; i++) {
        feed.scrollTop = feed.scrollHeight;
        await new Promise(r => setTimeout(r, 1500 + Math.random() * 1000));

        const items = feed.querySelectorAll('div[jsaction*="mouseover"]');
        const count = items.length;

        // Check if we've reached the end marker
        const endMarker = feed.querySelector('span.HlvSq');
        if (endMarker) break;

        if (count === prevCount) {
            sameCountRounds++;
            if (sameCountRounds >= 3) break;
        } else {
            sameCountRounds = 0;
        }
        prevCount = count;
    }
    return prevCount;
})()
"""

# ── Parsing helpers ──────────────────────────────────────────────────

_COORD_RE = re.compile(r'@(-?\d+\.\d+),(-?\d+\.\d+)')
_PLACE_ID_RE = re.compile(r'ChIJ[\w-]+')
_RATING_RE = re.compile(r'(\d\.?\d?)\s*stars?', re.I)
_REVIEW_COUNT_RE = re.compile(r'\((\d[\d,]*)\)', re.I)
_PRICE_RE = re.compile(r'(\${1,4})\s*·')
_PHONE_RE = re.compile(
    r'(?<!\d)(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?[2-9]\d{2}[-.\s]?\d{4}(?!\d)'
)
_HOURS_ARIA_RE = re.compile(
    r'aria-label="([^"]*(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)[^"]*)"',
    re.I,
)
_REVIEW_DIST_RE = re.compile(
    r'(\d)\s*stars?,\s*(\d[\d,]*)\s*reviews?', re.I
)
_PLUS_CODE_RE = re.compile(r'[23456789CFGHJMPQRVWX]{4}\+[23456789CFGHJMPQRVWX]{2,3}\s+\w+', re.I)


def _parse_listings_from_html(html: str) -> list[dict]:
    """Extract listing cards from the Maps search results page HTML.

    Uses aria-labels and href patterns for stability across Google's
    frequent class name changes.
    """
    listings = []
    seen_urls = set()

    # Find all place links: <a href="/maps/place/..." aria-label="Business Name">
    link_re = re.compile(
        r'<a[^>]+href="(/maps/place/[^"]+)"[^>]*aria-label="([^"]*)"',
        re.I,
    )

    for match in link_re.finditer(html):
        path, name = match.group(1), match.group(2)
        maps_url = "https://www.google.com" + path

        if maps_url in seen_urls:
            continue
        seen_urls.add(maps_url)

        # Extract coordinates from URL
        lat, lng = None, None
        coord_match = _COORD_RE.search(path)
        if coord_match:
            lat = float(coord_match.group(1))
            lng = float(coord_match.group(2))

        # Extract place ID from URL
        place_id = ""
        pid_match = _PLACE_ID_RE.search(path)
        if pid_match:
            place_id = pid_match.group(0)

        listing = {
            "name": name.strip(),
            "maps_url": maps_url,
            "latitude": lat,
            "longitude": lng,
            "place_id": place_id,
        }
        listings.append(listing)

    return listings


def _parse_listing_detail(html: str, maps_url: str) -> dict:
    """Parse full details from an individual listing's Maps page."""
    detail = {}

    # ── Rating & review count ──
    # aria-label="4.5 stars 238 Reviews"
    stars_label = re.search(
        r'aria-label="(\d\.?\d?)\s*stars?\s*(\d[\d,]*)\s*[Rr]eviews?"', html
    )
    if stars_label:
        detail["rating"] = float(stars_label.group(1))
        detail["review_count"] = int(stars_label.group(2).replace(",", ""))
    else:
        # Fallback: separate patterns
        r = _RATING_RE.search(html)
        if r:
            detail["rating"] = float(r.group(1))
        rc = _REVIEW_COUNT_RE.search(html)
        if rc:
            detail["review_count"] = int(rc.group(1).replace(",", ""))

    # ── Category ──
    # Category appears in a button with jsaction containing "category"
    cat_re = re.compile(r'<button[^>]*jsaction="[^"]*category[^"]*"[^>]*>([^<]+)</button>', re.I)
    cat_match = cat_re.search(html)
    if cat_match:
        detail["category"] = cat_match.group(1).strip()
    else:
        # Fallback: look for category-like span near the rating
        cat_re2 = re.compile(r'<span[^>]*>([^<]+)</span>\s*·\s*(?:\$|€|£)', re.I)
        cat_match2 = cat_re2.search(html)
        if cat_match2:
            detail["category"] = cat_match2.group(1).strip()

    # ── Address ──
    # aria-label="Address: 123 Main St, Denver, CO 80202"
    addr_re = re.compile(r'aria-label="Address[:\s]*([^"]+)"', re.I)
    addr_match = addr_re.search(html)
    if addr_match:
        full_addr = addr_match.group(1).strip()
        detail["address"] = full_addr
        _parse_address_components(full_addr, detail)

    # ── Phone ──
    phone_re = re.compile(r'aria-label="Phone[:\s]*([^"]+)"', re.I)
    phone_match = phone_re.search(html)
    if phone_match:
        detail["phone"] = phone_match.group(1).strip()
    else:
        # Fallback: data-tooltip="Copy phone number" nearby
        phone_tooltip = re.search(
            r'data-tooltip="Copy phone number"[^>]*>.*?<span[^>]*>([^<]+)</span>',
            html, re.S
        )
        if phone_tooltip:
            detail["phone"] = phone_tooltip.group(1).strip()

    # ── Website ──
    web_re = re.compile(r'aria-label="Website[:\s]*([^"]+)"', re.I)
    web_match = web_re.search(html)
    if web_match:
        detail["website"] = web_match.group(1).strip()
    else:
        # Look for the website link directly
        web_link = re.search(
            r'<a[^>]+data-item-id="authority"[^>]+href="([^"]+)"', html
        )
        if web_link:
            detail["website"] = web_link.group(1).strip()

    # ── Price level ──
    price_match = _PRICE_RE.search(html)
    if price_match:
        detail["price_level"] = price_match.group(1)
    else:
        # Check aria-label
        price_aria = re.search(r'aria-label="[^"]*(\${1,4})\s*·', html)
        if price_aria:
            detail["price_level"] = price_aria.group(1)

    # ── Hours ──
    hours_match = _HOURS_ARIA_RE.search(html)
    if hours_match:
        detail["hours"] = hours_match.group(1).strip()
    else:
        # Try table-based hours
        hours_rows = re.findall(
            r'<tr[^>]*>.*?<td[^>]*>(\w+day)</td>.*?<td[^>]*>([^<]+)</td>.*?</tr>',
            html, re.S | re.I
        )
        if hours_rows:
            detail["hours"] = "; ".join(f"{day}: {hrs.strip()}" for day, hrs in hours_rows)

    # ── Review distribution ──
    dist = {}
    for star, count in _REVIEW_DIST_RE.findall(html):
        dist[star] = int(count.replace(",", ""))
    if not dist:
        # Fallback: percentage-based distribution
        pct_re = re.compile(r'(\d)\s*stars?\s*(\d+)%', re.I)
        total = detail.get("review_count", 0)
        for star, pct in pct_re.findall(html):
            if total:
                dist[star] = round(total * int(pct) / 100)
    if dist:
        detail["review_distribution"] = dist

    # ── Coordinates from URL ──
    coord_match = _COORD_RE.search(maps_url)
    if coord_match:
        detail["latitude"] = float(coord_match.group(1))
        detail["longitude"] = float(coord_match.group(2))

    # ── Place ID ──
    pid_match = _PLACE_ID_RE.search(maps_url)
    if pid_match:
        detail["place_id"] = pid_match.group(0)

    # ── Plus code ──
    plus_match = _PLUS_CODE_RE.search(html)
    if plus_match:
        detail["plus_code"] = plus_match.group(0).strip()

    # ── Closed status ──
    detail["is_temporarily_closed"] = bool(
        re.search(r'temporarily closed', html, re.I)
    )
    detail["is_permanently_closed"] = bool(
        re.search(r'permanently closed', html, re.I)
    )

    # ── Description ──
    desc_re = re.compile(
        r'<div[^>]*class="[^"]*PYvSYb[^"]*"[^>]*>([^<]+)</div>', re.I
    )
    desc_match = desc_re.search(html)
    if desc_match:
        detail["description"] = desc_match.group(1).strip()

    # ── Thumbnail ──
    thumb_re = re.compile(r'<img[^>]+class="[^"]*(?:p6VGsd|tactile-hero-image)[^"]*"[^>]+src="([^"]+)"', re.I)
    thumb_match = thumb_re.search(html)
    if thumb_match:
        detail["thumbnail_url"] = thumb_match.group(1)

    return detail


def _parse_address_components(address: str, detail: dict):
    """Break a full address like '123 Main St, Denver, CO 80202' into components."""
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 3:
        detail["street"] = parts[0]
        detail["city"] = parts[-2].strip() if len(parts) >= 3 else ""
        # Last part often has "STATE ZIP"
        last = parts[-1].strip()
        state_zip = re.match(r'([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?', last)
        if state_zip:
            detail["state"] = state_zip.group(1)
            detail["zip_code"] = state_zip.group(2) or ""
        else:
            detail["state"] = last
    elif len(parts) == 2:
        detail["street"] = parts[0]
        detail["city"] = parts[1]


# ── Main scraping functions ──────────────────────────────────────────

async def _scroll_and_collect(
    crawler: AsyncWebCrawler,
    search_url: str,
    max_results: int,
    rate_limiter: DomainRateLimiter,
) -> str:
    """Navigate to Maps search URL and scroll to load all results.

    Returns the full HTML of the page after scrolling.
    """
    max_scrolls = (max_results // 10) + 5

    config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        wait_until="networkidle",
        js_code=SCROLL_JS % max_scrolls,
        delay_before_return_html=2.0,
    )

    result, err = await _crawl_with_retry(
        crawler, search_url, config,
        max_retries=2, timeout=60,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        return ""

    return str(result.html) if result.html else ""


async def _scrape_detail_page(
    crawler: AsyncWebCrawler,
    maps_url: str,
    rate_limiter: DomainRateLimiter,
) -> dict:
    """Visit an individual Maps listing and extract full details."""
    config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        wait_until="networkidle",
        delay_before_return_html=1.5,
    )

    # Random delay to avoid detection
    await asyncio.sleep(random.uniform(2.0, 5.0))

    result, err = await _crawl_with_retry(
        crawler, maps_url, config,
        max_retries=2, timeout=30,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        return {}

    html = str(result.html) if result.html else ""
    return _parse_listing_detail(html, maps_url)


async def _enrich_with_website(
    lead: dict,
    crawler: AsyncWebCrawler,
    rate_limiter: DomainRateLimiter,
    config: ScrapeConfig,
) -> dict:
    """Visit the business website to extract emails, socials, description."""
    website = lead.get("website", "")
    if not website:
        return lead

    website = normalize_url(website)
    crawl_config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        wait_until="domcontentloaded",
    )

    result, err = await _crawl_with_retry(
        crawler, website, crawl_config,
        max_retries=2, timeout=20,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        return lead

    html = str(result.html) if result.html else ""
    md = str(result.markdown) if result.markdown else ""

    emails = extract_emails(md + " " + html)
    if emails:
        lead["emails"] = emails

    phones = extract_phones(md)
    if phones and not lead.get("phone"):
        lead["phone"] = phones[0]

    socials = extract_social_links(md + " " + html)
    if socials:
        lead["socials"] = socials

    if not lead.get("description"):
        lead["description"] = extract_description(md)

    return lead


async def scrape_google_maps(
    query: str,
    max_results: int = 100,
    enrich_websites: bool = False,
    config: ScrapeConfig | None = None,
    on_progress: Callable | None = None,
) -> list[dict]:
    """Main entry point: search Google Maps and extract business data.

    Args:
        query: Search query, e.g. "plumber in Denver, CO"
        max_results: Maximum listings to collect (Google caps at ~120)
        enrich_websites: If True, visit each business website for emails/socials
        config: Scraping configuration (proxies, stealth, etc.)
        on_progress: Callback(current, total, message)

    Returns:
        List of lead dicts with all extracted fields.
    """
    config = config or ScrapeConfig()
    browser_config = _make_maps_browser_config(config)
    rate_limiter = DomainRateLimiter(min_delay=3.0)

    max_results = min(max_results, 120)  # Google Maps hard limit

    def progress(current, total, msg):
        if on_progress:
            on_progress(current, total, msg)

    progress(0, max_results, f"Searching Google Maps for '{query}'...")

    search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Phase 1: Scroll search results to load listings
        html = await _scroll_and_collect(crawler, search_url, max_results, rate_limiter)

        if not html:
            progress(0, max_results, "No results found on Google Maps")
            return []

        # Phase 2: Parse listing cards from results page
        listings = _parse_listings_from_html(html)
        listings = listings[:max_results]

        if not listings:
            progress(0, max_results, "Could not parse any listings from Google Maps")
            return []

        progress(0, len(listings), f"Found {len(listings)} listings, scraping details...")

        # Phase 3: Visit each listing's detail page
        leads = []
        sem = asyncio.Semaphore(config.concurrency)

        async def scrape_one(i: int, listing: dict) -> dict:
            async with sem:
                progress(i, len(listings), f"Scraping {listing['name']}...")

                detail = await _scrape_detail_page(
                    crawler, listing["maps_url"], rate_limiter
                )

                # Merge listing data with detail data
                lead = {**listing, **detail}

                # Normalize the lead structure
                lead.setdefault("category", "")
                lead.setdefault("address", "")
                lead.setdefault("phone", "")
                lead.setdefault("website", "")
                lead.setdefault("rating", None)
                lead.setdefault("review_count", None)
                lead.setdefault("review_distribution", {})
                lead.setdefault("hours", "")
                lead.setdefault("price_level", "")
                lead.setdefault("latitude", None)
                lead.setdefault("longitude", None)
                lead.setdefault("place_id", "")
                lead.setdefault("plus_code", "")
                lead.setdefault("is_temporarily_closed", False)
                lead.setdefault("is_permanently_closed", False)
                lead.setdefault("description", "")
                lead.setdefault("thumbnail_url", "")
                lead.setdefault("emails", [])
                lead.setdefault("socials", {})

                return lead

        tasks = [scrape_one(i, l) for i, l in enumerate(listings)]
        leads = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions
        good_leads = []
        for i, lead in enumerate(leads):
            if isinstance(lead, Exception):
                progress(i + 1, len(listings), f"Error scraping listing: {lead}")
                continue
            good_leads.append(lead)

        progress(len(good_leads), len(listings),
                 f"Scraped {len(good_leads)} listings from Maps")

        # Phase 4: Optional website enrichment
        if enrich_websites:
            with_websites = [l for l in good_leads if l.get("website")]
            if with_websites:
                progress(0, len(with_websites), "Enriching with website data...")

                async def enrich_one(i: int, lead: dict) -> dict:
                    async with sem:
                        progress(i, len(with_websites),
                                 f"Enriching {lead['name']} from website...")
                        return await _enrich_with_website(
                            lead, crawler, rate_limiter, config
                        )

                enrich_tasks = [
                    enrich_one(i, l) for i, l in enumerate(with_websites)
                ]
                enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)

                # Update leads with enriched data
                enriched_map = {}
                for i, result in enumerate(enriched):
                    if not isinstance(result, Exception):
                        enriched_map[with_websites[i]["maps_url"]] = result

                for j, lead in enumerate(good_leads):
                    if lead["maps_url"] in enriched_map:
                        good_leads[j] = enriched_map[lead["maps_url"]]

                progress(len(with_websites), len(with_websites),
                         f"Enriched {len(enriched_map)} leads with website data")

    # Final output — rename fields to match the app's convention
    output = []
    for lead in good_leads:
        out = {
            "company": lead.get("name", ""),
            "url": lead.get("website", ""),
            "maps_url": lead.get("maps_url", ""),
            "category": lead.get("category", ""),
            "description": lead.get("description", ""),
            "emails": lead.get("emails", []),
            "phones": [lead["phone"]] if lead.get("phone") else [],
            "address": lead.get("address", ""),
            "city": lead.get("city", ""),
            "state": lead.get("state", ""),
            "zip_code": lead.get("zip_code", ""),
            "hours": lead.get("hours", ""),
            "google_rating": lead.get("rating"),
            "google_reviews": lead.get("review_count"),
            "review_distribution": lead.get("review_distribution", {}),
            "price_level": lead.get("price_level", ""),
            "latitude": lead.get("latitude"),
            "longitude": lead.get("longitude"),
            "place_id": lead.get("place_id", ""),
            "plus_code": lead.get("plus_code", ""),
            "is_temporarily_closed": lead.get("is_temporarily_closed", False),
            "is_permanently_closed": lead.get("is_permanently_closed", False),
            "thumbnail_url": lead.get("thumbnail_url", ""),
            "socials": lead.get("socials", {}),
        }
        output.append(out)

    progress(len(output), len(output),
             f"Done — {len(output)} leads from Google Maps")
    return output
