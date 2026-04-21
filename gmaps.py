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
import logging
import math
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
    extract_description,
)

log = logging.getLogger("gmaps")
logging.basicConfig(level=logging.INFO)


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
            "--lang=en-US",
        ],
    )
    if config.proxies:
        kwargs["proxy"] = config.proxies[0]
    return BrowserConfig(**kwargs)


# ── JavaScript snippets ──────────────────────────────────────────────

# Dismiss Google consent dialog if present
CONSENT_JS = """
// Click "Accept all" on consent dialog
const btns = document.querySelectorAll('button');
for (const btn of btns) {
    const text = btn.textContent.trim().toLowerCase();
    if (text === 'accept all' || text === 'i agree' || text === 'consent') {
        btn.click();
        break;
    }
}
// Also try the form-based consent
const form = document.querySelector('form[action*="consent"]');
if (form) {
    const submit = form.querySelector('button, input[type="submit"]');
    if (submit) submit.click();
}
"""

# Extract listing data from live DOM into a hidden div for Python to parse
EXTRACT_LISTINGS_JS = """
(function() {
  var feed = document.querySelector('div[role="feed"]');
  var container = feed || document;
  var anchors = container.querySelectorAll('a[href*="/maps/place/"]');
  var listings = [];
  var seen = {};
  for (var i = 0; i < anchors.length; i++) {
    var a = anchors[i];
    var href = a.href;
    if (seen[href]) continue;
    seen[href] = true;
    listings.push({ href: href, name: a.getAttribute('aria-label') || '' });
  }
  var el = document.getElementById('__gmaps_listings__');
  if (!el) {
    el = document.createElement('div');
    el.id = '__gmaps_listings__';
    el.style.display = 'none';
    document.body.appendChild(el);
  }
  el.textContent = JSON.stringify(listings);
})();
"""

# Scroll the results panel and extract listings
SCROLL_JS = """
const feed = document.querySelector('div[role="feed"]');
if (feed) { feed.scrollTop = feed.scrollHeight; }
""" + EXTRACT_LISTINGS_JS

# Extract detail fields from a Maps listing page into a hidden div
EXTRACT_DETAIL_JS = """
(function() {
  var d = {};

  // Title
  var h1 = document.querySelector('h1');
  if (h1) d.title = h1.textContent.trim();

  // Rating + review count from aria-labels
  var ariaEls = document.querySelectorAll('[aria-label]');
  for (var i = 0; i < ariaEls.length; i++) {
    var lbl = ariaEls[i].getAttribute('aria-label') || '';
    if (!d.rating) {
      var starMatch = lbl.match(/([\d.]+)\s*star/i);
      if (starMatch) d.rating = parseFloat(starMatch[1]);
    }
    if (!d.reviewCount) {
      var revMatch = lbl.match(/([\d,]+)\s+review/i);
      if (revMatch) d.reviewCount = parseInt(revMatch[1].replace(/,/g, ''), 10);
    }
  }
  if (!d.reviewCount) {
    var bodyText = document.body.innerText || '';
    var revFb = bodyText.match(/\\(([\d,]+)\\)/);
    if (revFb) d.reviewCount = parseInt(revFb[1].replace(/,/g, ''), 10);
  }

  // Category
  var catBtn = document.querySelector('button[jsaction*="category"]');
  if (catBtn) {
    d.category = catBtn.textContent.trim();
  } else {
    var mainBtns = document.querySelectorAll('div[role="main"] button');
    for (var i = 0; i < mainBtns.length; i++) {
      var t = mainBtns[i].textContent.trim();
      if (t && t.length < 40 && !/close|open|claim|share|save|send|direction|review|photo|call|menu/i.test(t) && !/^\\d/.test(t)) {
        d.category = t;
        break;
      }
    }
  }

  // Address
  var addrEl = document.querySelector('button[data-item-id="address"]');
  if (!addrEl) addrEl = document.querySelector('[data-tooltip="Copy address"]');
  if (!addrEl) addrEl = document.querySelector('button[aria-label*="Address"]');
  if (addrEl) {
    var addrLabel = addrEl.getAttribute('aria-label') || '';
    d.address = addrLabel.replace(/^Address:\\s*/i, '').trim() || addrEl.textContent.trim();
  }

  // Phone
  var phoneEl = document.querySelector('button[data-item-id^="phone:"]');
  if (!phoneEl) phoneEl = document.querySelector('[data-tooltip="Copy phone number"]');
  if (phoneEl) {
    var phoneLbl = phoneEl.getAttribute('aria-label') || '';
    d.phone = phoneLbl.replace(/^Phone:\\s*/i, '').trim() || phoneEl.textContent.trim();
  }

  // Website
  var webEl = document.querySelector('a[data-item-id="authority"]');
  if (!webEl) webEl = document.querySelector('a[aria-label*="website" i]');
  if (webEl) d.website = webEl.href;

  // Hours from aria-labels containing day names
  var days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
  var hoursArr = [];
  for (var i = 0; i < ariaEls.length; i++) {
    var lbl = ariaEls[i].getAttribute('aria-label') || '';
    for (var j = 0; j < days.length; j++) {
      if (lbl.indexOf(days[j]) !== -1 && lbl.length < 200) {
        hoursArr.push(lbl.trim());
        break;
      }
    }
  }
  if (hoursArr.length > 0) d.hours = hoursArr.join('; ');

  // Price level
  var mainEl = document.querySelector('div[role="main"]');
  if (mainEl) {
    var mainText = mainEl.innerText || '';
    var priceMatch = mainText.match(/(?:^|\\s)(\\${1,4})(?:\\s|\\xB7|$)/m);
    if (priceMatch) d.priceLevel = priceMatch[1];
  }

  // Review distribution
  var dist = {};
  for (var i = 0; i < ariaEls.length; i++) {
    var lbl = ariaEls[i].getAttribute('aria-label') || '';
    var distMatch = lbl.match(/(\\d)\\s*stars?,\\s*([\\d,]+)\\s*review/i);
    if (distMatch) {
      dist[distMatch[1]] = parseInt(distMatch[2].replace(/,/g, ''), 10);
    }
  }
  if (Object.keys(dist).length === 0 && d.reviewCount) {
    for (var i = 0; i < ariaEls.length; i++) {
      var lbl = ariaEls[i].getAttribute('aria-label') || '';
      var pctMatch = lbl.match(/(\\d)\\s*stars?,?\\s*(\\d+)%/i);
      if (pctMatch) {
        dist[pctMatch[1]] = Math.round(d.reviewCount * parseInt(pctMatch[2]) / 100);
      }
    }
  }
  if (Object.keys(dist).length > 0) d.reviewDistribution = dist;

  // Coordinates from URL
  var url = window.location.href;
  var coordMatch = url.match(/!3d(-?\\d+\\.\\d+)!4d(-?\\d+\\.\\d+)/);
  if (!coordMatch) coordMatch = url.match(/@(-?\\d+\\.\\d+),(-?\\d+\\.\\d+)/);
  if (coordMatch) {
    d.latitude = parseFloat(coordMatch[1]);
    d.longitude = parseFloat(coordMatch[2]);
  }

  // Place ID from URL
  var pidMatch = url.match(/!1s(ChIJ[^!&?]+)/);
  if (pidMatch) d.placeId = decodeURIComponent(pidMatch[1]);

  // Closed status
  var bt = document.body.innerText || '';
  d.permanentlyClosed = /permanently closed/i.test(bt);
  d.temporarilyClosed = /temporarily closed/i.test(bt);

  // Plus code
  var plusMatch = bt.match(/[23456789CFGHJMPQRVWX]{4}\\+[23456789CFGHJMPQRVWX]{2,3}\\s+\\w+/i);
  if (plusMatch) d.plusCode = plusMatch[0].trim();

  // Description
  var descEl = document.querySelector('div.PYvSYb');
  if (descEl) d.description = descEl.textContent.trim();

  // Write to hidden div
  var el = document.getElementById('__gmaps_detail__');
  if (!el) {
    el = document.createElement('div');
    el.id = '__gmaps_detail__';
    el.style.display = 'none';
    document.body.appendChild(el);
  }
  el.textContent = JSON.stringify(d);
})();
"""


# ── Parsing helpers ──────────────────────────────────────────────────

_COORD_RE = re.compile(r'@(-?\d+\.\d+),(-?\d+\.\d+)')
_PLACE_ID_RE = re.compile(r'ChIJ[\w-]+')
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
_PLUS_CODE_RE = re.compile(
    r'[23456789CFGHJMPQRVWX]{4}\+[23456789CFGHJMPQRVWX]{2,3}\s+\w+', re.I
)


def _parse_listings_from_js_data(html: str) -> list[dict] | None:
    """Try to extract listing data from the __gmaps_listings__ hidden div.

    Returns a list of listing dicts if found, or None to signal fallback to regex.
    """
    marker = 'id="__gmaps_listings__"'
    idx = html.find(marker)
    if idx == -1:
        return None

    # Find content between > and </div>
    start = html.find(">", idx + len(marker))
    if start == -1:
        return None
    start += 1
    end = html.find("</div>", start)
    if end == -1:
        return None

    json_str = html[start:end].strip()
    if not json_str:
        return None

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        # crawl4ai may HTML-entity-encode the JSON
        import html as html_mod
        try:
            data = json.loads(html_mod.unescape(json_str))
        except (json.JSONDecodeError, Exception):
            log.warning("Found __gmaps_listings__ div but could not parse JSON")
            return None

    if not isinstance(data, list):
        return None

    listings = []
    seen_urls = set()
    for item in data:
        href = item.get("href", "")
        name = item.get("name", "")
        if not href or "/maps/place/" not in href:
            continue
        _add_listing(listings, seen_urls, href, name)

    return listings if listings else None


def _parse_detail_from_js_data(html: str) -> dict | None:
    """Extract detail data from the __gmaps_detail__ hidden div.

    Returns a dict of detail fields if found, or None to signal fallback to regex.
    """
    marker = 'id="__gmaps_detail__"'
    idx = html.find(marker)
    if idx == -1:
        return None

    start = html.find(">", idx + len(marker))
    if start == -1:
        return None
    start += 1
    end = html.find("</div>", start)
    if end == -1:
        return None

    json_str = html[start:end].strip()
    if not json_str:
        return None

    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        import html as html_mod
        try:
            data = json.loads(html_mod.unescape(json_str))
        except (json.JSONDecodeError, Exception):
            log.warning("Found __gmaps_detail__ div but could not parse JSON")
            return None

    if not isinstance(data, dict):
        return None

    detail = {}
    if data.get("title"):
        detail["name"] = data["title"]
    if data.get("rating") is not None:
        detail["rating"] = data["rating"]
    if data.get("reviewCount") is not None:
        detail["review_count"] = data["reviewCount"]
    if data.get("category"):
        detail["category"] = data["category"]
    if data.get("address"):
        detail["address"] = data["address"]
        _parse_address_components(data["address"], detail)
    if data.get("phone"):
        detail["phone"] = data["phone"]
    if data.get("website"):
        detail["website"] = data["website"]
    if data.get("hours"):
        detail["hours"] = data["hours"]
    if data.get("priceLevel"):
        detail["price_level"] = data["priceLevel"]
    if data.get("reviewDistribution"):
        detail["review_distribution"] = data["reviewDistribution"]
    if data.get("latitude") is not None:
        detail["latitude"] = data["latitude"]
    if data.get("longitude") is not None:
        detail["longitude"] = data["longitude"]
    if data.get("placeId"):
        detail["place_id"] = data["placeId"]
    detail["is_permanently_closed"] = bool(data.get("permanentlyClosed", False))
    detail["is_temporarily_closed"] = bool(data.get("temporarilyClosed", False))
    if data.get("plusCode"):
        detail["plus_code"] = data["plusCode"]
    if data.get("description"):
        detail["description"] = data["description"]

    return detail if detail else None


def _parse_listings_from_html(html: str) -> list[dict]:
    """Extract listing cards from the Maps search results page HTML."""
    listings = []
    seen_urls = set()

    # Save debug HTML on first call for diagnostics
    try:
        from pathlib import Path
        debug_dir = Path("/data/debug") if Path("/data").exists() else Path("/tmp")
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_file = debug_dir / "maps_debug.html"
        debug_file.write_text(html[:500000])
        log.info(f"Debug HTML saved to {debug_file}")
    except Exception as e:
        log.warning(f"Could not save debug HTML: {e}")

    # Primary strategy: extract from JS-injected JSON data in hidden div
    js_listings = _parse_listings_from_js_data(html)
    if js_listings:
        log.info(f"Parsed {len(js_listings)} listings from JS extraction ({len(html)} chars)")
        return js_listings

    # Fallback: regex strategies on raw HTML
    # Strategy 1: aria-label on anchor tags
    link_re = re.compile(
        r'<a\s[^>]*?href="(/maps/place/[^"]+)"[^>]*?aria-label="([^"]*)"',
        re.I | re.S,
    )
    for match in link_re.finditer(html):
        path, name = match.group(1), match.group(2)
        _add_listing(listings, seen_urls, path, name)

    # Strategy 1b: aria-label before href (attribute order varies)
    if not listings:
        link_re2 = re.compile(
            r'<a\s[^>]*?aria-label="([^"]*)"[^>]*?href="(/maps/place/[^"]+)"',
            re.I | re.S,
        )
        for match in link_re2.finditer(html):
            name, path = match.group(1), match.group(2)
            _add_listing(listings, seen_urls, path, name)

    # Strategy 2: Any href to /maps/place/ — extract name from URL
    if not listings:
        href_re = re.compile(r'href="(/maps/place/([^/]+)/[^"]*)"', re.I)
        for match in href_re.finditer(html):
            path = match.group(1)
            name = match.group(2).replace("+", " ").replace("%20", " ")
            _add_listing(listings, seen_urls, path, name)

    # Strategy 3: Look for place URLs anywhere in the HTML (not just href)
    if not listings:
        url_re = re.compile(r'/maps/place/([^/"]+)/[^"\s<>]+', re.I)
        for match in url_re.finditer(html):
            path = match.group(0)
            name = match.group(1).replace("+", " ").replace("%20", " ")
            _add_listing(listings, seen_urls, path, name)

    log.info(f"Parsed {len(listings)} listings from HTML ({len(html)} chars)")
    return listings


def _add_listing(listings: list, seen_urls: set, path: str, name: str):
    """Helper to add a listing, avoiding duplicates."""
    if not path.startswith("http"):
        maps_url = "https://www.google.com" + path
    else:
        maps_url = path

    # Deduplicate by place name (URLs may differ slightly)
    dedup_key = maps_url.split("?")[0].split("!")[0]
    if dedup_key in seen_urls:
        return
    seen_urls.add(dedup_key)

    lat, lng = None, None
    coord_match = _COORD_RE.search(path)
    if coord_match:
        lat = float(coord_match.group(1))
        lng = float(coord_match.group(2))

    place_id = ""
    pid_match = _PLACE_ID_RE.search(path)
    if pid_match:
        place_id = pid_match.group(0)

    # URL-decode the name
    from urllib.parse import unquote
    name = unquote(name).strip()

    listings.append({
        "name": name,
        "maps_url": maps_url,
        "latitude": lat,
        "longitude": lng,
        "place_id": place_id,
    })


def _parse_listing_detail(html: str, md: str, maps_url: str) -> dict:
    """Parse full details from an individual listing's Maps page."""
    # Primary: try JS-injected data from hidden div
    js_detail = _parse_detail_from_js_data(html)
    if js_detail:
        log.info(f"Parsed detail from JS extraction: {len(js_detail)} fields")
        # Fill URL-derived fields that JS might have missed
        if "latitude" not in js_detail:
            coord_match = _COORD_RE.search(maps_url)
            if coord_match:
                js_detail["latitude"] = float(coord_match.group(1))
                js_detail["longitude"] = float(coord_match.group(2))
        if "place_id" not in js_detail:
            pid_match = _PLACE_ID_RE.search(maps_url)
            if pid_match:
                js_detail["place_id"] = pid_match.group(0)

        # Fill missing phone/address/website from HTML regex fallback
        if "phone" not in js_detail:
            phone_match = re.search(r'aria-label="Phone[:\s]*([^"]+)"', html, re.I)
            if phone_match:
                js_detail["phone"] = phone_match.group(1).strip()
            else:
                phones = _PHONE_RE.findall(md)
                if phones:
                    js_detail["phone"] = phones[0].strip()
        if "address" not in js_detail:
            addr_match = re.search(r'aria-label="Address[:\s]*([^"]+)"', html, re.I)
            if addr_match:
                js_detail["address"] = addr_match.group(1).strip()
                _parse_address_components(js_detail["address"], js_detail)
        if "website" not in js_detail:
            web_match = re.search(r'aria-label="Website[:\s]*([^"]+)"', html, re.I)
            if web_match:
                js_detail["website"] = web_match.group(1).strip()

        return js_detail

    # Fallback: regex on HTML + markdown
    detail = {}
    # Combine HTML and markdown for searching
    text = html + "\n" + md

    # ── Rating & review count ──
    stars_label = re.search(
        r'aria-label="(\d\.?\d?)\s*stars?\s*(\d[\d,]*)\s*[Rr]eviews?"', html
    )
    if stars_label:
        detail["rating"] = float(stars_label.group(1))
        detail["review_count"] = int(stars_label.group(2).replace(",", ""))
    else:
        # Try markdown patterns like "4.5(238)"
        md_rating = re.search(r'(\d\.\d)\s*\((\d[\d,]*)\)', md)
        if md_rating:
            detail["rating"] = float(md_rating.group(1))
            detail["review_count"] = int(md_rating.group(2).replace(",", ""))
        else:
            r = re.search(r'(\d\.?\d?)\s*stars?', html, re.I)
            if r:
                detail["rating"] = float(r.group(1))
            rc = re.search(r'\((\d[\d,]*)\s*(?:reviews?)?\)', html)
            if rc:
                detail["review_count"] = int(rc.group(1).replace(",", ""))

    # ── Category ──
    # In markdown, category often appears near the top
    cat_match = re.search(r'(?:^|\n)\s*([A-Z][a-z]+(?:\s+[a-z]+)*)\s*\n.*?(?:stars?|\d\.\d)', md)
    if not cat_match:
        cat_match = re.search(
            r'<button[^>]*jsaction="[^"]*category[^"]*"[^>]*>([^<]+)</button>', html, re.I
        )
    if cat_match:
        detail["category"] = cat_match.group(1).strip()

    # ── Address ──
    addr_re = re.compile(r'aria-label="Address[:\s]*([^"]+)"', re.I)
    addr_match = addr_re.search(html)
    if addr_match:
        full_addr = addr_match.group(1).strip()
        detail["address"] = full_addr
        _parse_address_components(full_addr, detail)
    else:
        # Try markdown — address often has a street number
        addr_md = re.search(
            r'(\d{1,5}\s+[\w\s.]+(?:St|Street|Ave|Avenue|Blvd|Dr|Drive|Rd|Road|Ln|Way|Ct|Pl)'
            r'[.,]?\s*[\w\s]*,?\s*[A-Z]{2}\s*\d{5})',
            md, re.I
        )
        if addr_md:
            detail["address"] = addr_md.group(1).strip()
            _parse_address_components(detail["address"], detail)

    # ── Phone ──
    phone_re = re.compile(r'aria-label="Phone[:\s]*([^"]+)"', re.I)
    phone_match = phone_re.search(html)
    if phone_match:
        detail["phone"] = phone_match.group(1).strip()
    else:
        # Try markdown for phone patterns
        phones = _PHONE_RE.findall(md)
        if phones:
            detail["phone"] = phones[0].strip()

    # ── Website ──
    web_re = re.compile(r'aria-label="Website[:\s]*([^"]+)"', re.I)
    web_match = web_re.search(html)
    if web_match:
        detail["website"] = web_match.group(1).strip()
    else:
        web_link = re.search(
            r'<a[^>]+data-item-id="authority"[^>]+href="([^"]+)"', html
        )
        if web_link:
            detail["website"] = web_link.group(1).strip()
        else:
            # Look in markdown for website-like URLs
            web_md = re.search(
                r'\]\((https?://(?!www\.google|maps\.google|play\.google)[^\s)]+)\)',
                md
            )
            if web_md:
                url = web_md.group(1)
                parsed = urlparse(url)
                if parsed.netloc and '.' in parsed.netloc:
                    skip = {'google.com', 'gstatic.com', 'googleapis.com', 'ggpht.com'}
                    if not any(parsed.netloc.endswith(s) for s in skip):
                        detail["website"] = url

    # ── Price level ──
    price_match = re.search(r'(\${1,4})\s*·', text)
    if price_match:
        detail["price_level"] = price_match.group(1)
    else:
        price_aria = re.search(r'aria-label="[^"]*(\${1,4})\s*·', html)
        if price_aria:
            detail["price_level"] = price_aria.group(1)

    # ── Hours ──
    hours_match = _HOURS_ARIA_RE.search(html)
    if hours_match:
        detail["hours"] = hours_match.group(1).strip()
    else:
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
        re.search(r'temporarily closed', text, re.I)
    )
    detail["is_permanently_closed"] = bool(
        re.search(r'permanently closed', text, re.I)
    )

    # ── Description ──
    desc_re = re.compile(
        r'<div[^>]*class="[^"]*PYvSYb[^"]*"[^>]*>([^<]+)</div>', re.I
    )
    desc_match = desc_re.search(html)
    if desc_match:
        detail["description"] = desc_match.group(1).strip()

    # ── Thumbnail ──
    thumb_re = re.compile(
        r'<img[^>]+class="[^"]*(?:p6VGsd|tactile-hero-image)[^"]*"[^>]+src="([^"]+)"',
        re.I,
    )
    thumb_match = thumb_re.search(html)
    if thumb_match:
        detail["thumbnail_url"] = thumb_match.group(1)

    return detail


def _parse_address_components(address: str, detail: dict):
    """Break a full address like '123 Main St, Denver, CO 80202' into components."""
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 3:
        detail["street"] = parts[0]
        detail["city"] = parts[-2].strip()
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


# ── Polygon geometry helpers (ported from gmaps-scraper/geometry.js) ──

def _km_to_deg_lat(km: float) -> float:
    """Convert km to degrees latitude (constant everywhere)."""
    return km / 111.32


def _km_to_deg_lng(km: float, lat: float) -> float:
    """Convert km to degrees longitude (varies with latitude)."""
    return km / (111.32 * math.cos(math.radians(lat)))


def _point_in_polygon(lng: float, lat: float, ring: list) -> bool:
    """Ray-casting: is (lng, lat) inside a polygon ring? Ring is [[lng, lat], ...]."""
    x, y = lng, lat
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y) and x < ((xj - xi) * (y - yi)) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def _get_polygon_rings(geojson: dict) -> list:
    """Extract outer rings from GeoJSON Polygon or MultiPolygon."""
    if not geojson:
        return []
    gtype = geojson.get("type", "")
    if gtype == "Polygon":
        return [geojson["coordinates"][0]]
    if gtype == "MultiPolygon":
        return [poly[0] for poly in geojson["coordinates"]]
    # Handle FeatureCollection or Feature wrapper
    if gtype == "Feature" and geojson.get("geometry"):
        return _get_polygon_rings(geojson["geometry"])
    if gtype == "FeatureCollection":
        for feature in geojson.get("features", []):
            rings = _get_polygon_rings(feature.get("geometry", {}))
            if rings:
                return rings
    return []


def _generate_grid_points(geojson: dict, spacing_km: float = 1.0) -> list[dict]:
    """Generate {lat, lng} grid points inside a GeoJSON polygon."""
    rings = _get_polygon_rings(geojson)
    if not rings:
        raise ValueError("No valid polygon rings found in GeoJSON")

    points = []
    for ring in rings:
        lngs = [p[0] for p in ring]
        lats = [p[1] for p in ring]
        min_lng, max_lng = min(lngs), max(lngs)
        min_lat, max_lat = min(lats), max(lats)
        mid_lat = (min_lat + max_lat) / 2

        lat_step = _km_to_deg_lat(spacing_km)
        lng_step = _km_to_deg_lng(spacing_km, mid_lat)

        lat = min_lat
        while lat <= max_lat:
            lng = min_lng
            while lng <= max_lng:
                if _point_in_polygon(lng, lat, ring):
                    points.append({"lat": round(lat, 7), "lng": round(lng, 7)})
                lng += lng_step
            lat += lat_step

    return points


# ── Main scraping functions ──────────────────────────────────────────

async def _scroll_and_collect(
    crawler: AsyncWebCrawler,
    search_url: str,
    max_results: int,
    rate_limiter: DomainRateLimiter,
    on_log: Callable | None = None,
) -> str:
    """Navigate to Maps search URL and scroll to load all results."""

    def _log(msg):
        log.info(msg)
        if on_log:
            on_log(msg)

    # Step 1: Initial page load with consent dismissal
    initial_config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        wait_until="domcontentloaded",
        js_code=CONSENT_JS + "\n" + EXTRACT_LISTINGS_JS,
        delay_before_return_html=3.0,
    )

    _log("Loading Google Maps search page...")
    result, err = await _crawl_with_retry(
        crawler, search_url, initial_config,
        max_retries=3, timeout=45,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        _log(f"Failed to load Maps: {getattr(result, 'error_message', err)}")
        return ""

    html = str(result.html) if result.html else ""
    _log(f"Page loaded ({len(html)} chars HTML)")

    # Check if we got results already
    listings = _parse_listings_from_html(html)
    _log(f"Initial parse: {len(listings)} listings found")

    if len(listings) >= max_results:
        return html

    # No results at all — skip scrolling (saves ~5-10s per empty cell)
    if not listings:
        _log("No results on this page, skipping scrolls")
        return html

    # Step 2: Scroll to load more results
    prev_count = len(listings)
    stale_rounds = 0
    max_scroll_attempts = (max_results // 5) + 5

    for scroll_i in range(max_scroll_attempts):
        if len(listings) >= max_results:
            break

        scroll_config = CrawlerRunConfig(
            word_count_threshold=0,
            remove_overlay_elements=True,
            wait_until="domcontentloaded",
            js_code=SCROLL_JS,
            delay_before_return_html=2.0,
        )

        result, err = await _crawl_with_retry(
            crawler, search_url, scroll_config,
            max_retries=1, timeout=20,
            rate_limiter=rate_limiter,
        )

        if not result.success:
            _log(f"Scroll attempt {scroll_i + 1} failed, stopping")
            break

        html = str(result.html) if result.html else html
        listings = _parse_listings_from_html(html)

        if len(listings) == prev_count:
            stale_rounds += 1
            if stale_rounds >= 3:
                _log(f"No new listings after 3 scrolls, stopping at {len(listings)}")
                break
        else:
            stale_rounds = 0
            _log(f"Scroll {scroll_i + 1}: {len(listings)} listings loaded")

        prev_count = len(listings)
        await asyncio.sleep(random.uniform(1.0, 2.0))

    _log(f"Finished scrolling: {len(listings)} total listings")
    return html


async def _scrape_detail_page(
    crawler: AsyncWebCrawler,
    maps_url: str,
    rate_limiter: DomainRateLimiter,
) -> dict:
    """Visit an individual Maps listing and extract full details."""
    config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        wait_until="domcontentloaded",
        js_code=CONSENT_JS + "\n" + EXTRACT_DETAIL_JS,
        delay_before_return_html=3.5,
    )

    await asyncio.sleep(random.uniform(2.0, 4.0))

    result, err = await _crawl_with_retry(
        crawler, maps_url, config,
        max_retries=3, timeout=30,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        log.warning(f"Detail page failed for {maps_url}: {err or 'unknown error'}")
        return {}

    html = str(result.html) if result.html else ""
    md = str(result.markdown) if result.markdown else ""
    detail = _parse_listing_detail(html, md, maps_url)
    if not detail.get("phone") and not detail.get("address"):
        log.info(f"Detail page loaded but no phone/address extracted: {maps_url}")
    return detail


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
    rate_limiter = DomainRateLimiter(min_delay=5.0)

    max_results = min(max_results, 120)

    def progress(current, total, msg):
        if on_progress:
            on_progress(current, total, msg)

    progress(0, max_results, f"Searching Google Maps for '{query}'...")

    search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Phase 1: Scroll search results to load listings
        html = await _scroll_and_collect(
            crawler, search_url, max_results, rate_limiter,
            on_log=lambda msg: progress(0, max_results, msg),
        )

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
        sem = asyncio.Semaphore(config.concurrency)

        async def scrape_one(i: int, listing: dict) -> dict:
            async with sem:
                progress(i, len(listings), f"[{i+1}/{len(listings)}] {listing['name']}")

                detail = await _scrape_detail_page(
                    crawler, listing["maps_url"], rate_limiter
                )

                lead = {**listing, **detail}

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
        results = await asyncio.gather(*tasks, return_exceptions=True)

        good_leads = []
        for i, lead in enumerate(results):
            if isinstance(lead, Exception):
                progress(i + 1, len(listings), f"Error: {lead}")
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
                                 f"[{i+1}/{len(with_websites)}] Enriching {lead['name']}...")
                        return await _enrich_with_website(
                            lead, crawler, rate_limiter, config
                        )

                enrich_tasks = [
                    enrich_one(i, l) for i, l in enumerate(with_websites)
                ]
                enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)

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


# ── Shared detail/enrichment pipeline ────────────────────────────────

async def _scrape_details_and_enrich(
    crawler: AsyncWebCrawler,
    listings: list[dict],
    max_results: int,
    enrich_websites: bool,
    config: ScrapeConfig,
    rate_limiter: DomainRateLimiter,
    progress: Callable,
) -> list[dict]:
    """Scrape detail pages and optionally enrich with website data.

    Shared between scrape_google_maps() and scrape_google_maps_area().
    """
    sem = asyncio.Semaphore(max(config.concurrency, 5))

    async def scrape_one(i: int, listing: dict) -> dict:
        async with sem:
            progress(i, len(listings), f"[{i+1}/{len(listings)}] {listing['name']}")
            detail = await _scrape_detail_page(crawler, listing["maps_url"], rate_limiter)
            lead = {**listing, **detail}
            for key, default in [
                ("category", ""), ("address", ""), ("phone", ""), ("website", ""),
                ("rating", None), ("review_count", None), ("review_distribution", {}),
                ("hours", ""), ("price_level", ""), ("latitude", None), ("longitude", None),
                ("place_id", ""), ("plus_code", ""), ("is_temporarily_closed", False),
                ("is_permanently_closed", False), ("description", ""), ("thumbnail_url", ""),
                ("emails", []), ("socials", {}),
            ]:
                lead.setdefault(key, default)
            return lead

    tasks = [scrape_one(i, l) for i, l in enumerate(listings)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    good_leads = []
    for i, lead in enumerate(results):
        if isinstance(lead, Exception):
            progress(i + 1, len(listings), f"Error: {lead}")
            continue
        good_leads.append(lead)

    progress(len(good_leads), len(listings),
             f"Scraped {len(good_leads)} listings from Maps")

    # Website enrichment
    if enrich_websites:
        with_websites = [l for l in good_leads if l.get("website")]
        if with_websites:
            progress(0, len(with_websites), "Enriching with website data...")

            async def enrich_one(i: int, lead: dict) -> dict:
                async with sem:
                    progress(i, len(with_websites),
                             f"[{i+1}/{len(with_websites)}] Enriching {lead['name']}...")
                    return await _enrich_with_website(lead, crawler, rate_limiter, config)

            enrich_tasks = [enrich_one(i, l) for i, l in enumerate(with_websites)]
            enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)

            enriched_map = {}
            for i, result in enumerate(enriched):
                if not isinstance(result, Exception):
                    enriched_map[with_websites[i]["maps_url"]] = result

            for j, lead in enumerate(good_leads):
                if lead["maps_url"] in enriched_map:
                    good_leads[j] = enriched_map[lead["maps_url"]]

            progress(len(with_websites), len(with_websites),
                     f"Enriched {len(enriched_map)} leads with website data")

    return good_leads


def _format_leads(leads: list[dict]) -> list[dict]:
    """Rename fields to match the app's convention."""
    output = []
    for lead in leads:
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
    return output


# ── Polygon area search ──────────────────────────────────────────────

async def scrape_google_maps_area(
    keyword: str,
    polygon: dict,
    grid_spacing_km: float = 1.0,
    max_results: int = 500,
    enrich_websites: bool = False,
    config: ScrapeConfig | None = None,
    on_progress: Callable | None = None,
) -> list[dict]:
    """Search Google Maps across a polygon grid to overcome the 120-result cap.

    Args:
        keyword: Business type, e.g. "plumber"
        polygon: GeoJSON Polygon, MultiPolygon, Feature, or FeatureCollection
        grid_spacing_km: Distance between grid search points in km
        max_results: Maximum total results (cap across all grid cells)
        enrich_websites: Visit business websites for emails/socials
        config: Scraping configuration
        on_progress: Callback(current, total, message)

    Returns:
        List of lead dicts (same format as scrape_google_maps).
    """
    config = config or ScrapeConfig()
    browser_config = _make_maps_browser_config(config)
    rate_limiter = DomainRateLimiter(min_delay=5.0)

    def progress(current, total, msg):
        if on_progress:
            on_progress(current, total, msg)

    # Generate grid points
    grid_points = _generate_grid_points(polygon, grid_spacing_km)
    if not grid_points:
        progress(0, 0, "No grid points generated — check your polygon")
        return []

    polygon_rings = _get_polygon_rings(polygon)
    total_cells = len(grid_points)
    progress(0, total_cells, f"Grid: {total_cells} search cells for '{keyword}'")

    if total_cells > 500:
        progress(0, total_cells, f"Warning: {total_cells} cells is very large — consider increasing grid spacing")

    # Phase 1: Parallel grid cell search with multiple browser workers
    # Railway Hobby: 8GB RAM, 8 vCPUs — 3 concurrent browsers for stability
    NUM_WORKERS = min(3, total_cells)
    seen_place_ids: set[str] = set()
    seen_urls: set[str] = set()
    all_listings: list[dict] = []
    cells_done = [0]  # mutable counter for progress across workers

    async def grid_worker(worker_id: int, points: list[dict]):
        """Process a subset of grid cells in its own browser instance."""
        # Stagger startup so browsers don't all launch at once
        if worker_id > 0:
            await asyncio.sleep(worker_id * 3)

        worker_browser = _make_maps_browser_config(config)
        worker_rate_limiter = DomainRateLimiter(min_delay=2.0)
        crawler = None

        try:
            for local_i, point in enumerate(points):
                if len(all_listings) >= max_results:
                    break

                search_url = (
                    f"https://www.google.com/maps/search/"
                    f"{quote_plus(keyword)}/@{point['lat']},{point['lng']},15z"
                )

                cells_done[0] += 1
                progress(cells_done[0], total_cells,
                         f"[W{worker_id+1}] Cell {cells_done[0]}/{total_cells}: "
                         f"@ {point['lat']:.4f},{point['lng']:.4f}")

                # (Re)create browser if needed (first run or after crash)
                if crawler is None:
                    crawler = AsyncWebCrawler(config=worker_browser)
                    await crawler.__aenter__()
                    log.info(f"[W{worker_id+1}] Browser started")

                try:
                    html = await _scroll_and_collect(
                        crawler, search_url, max_results=120,
                        rate_limiter=worker_rate_limiter,
                    )
                except Exception as e:
                    log.warning(f"[W{worker_id+1}] Browser error: {e}, restarting...")
                    try:
                        await crawler.__aexit__(None, None, None)
                    except Exception:
                        pass
                    crawler = None
                    await asyncio.sleep(5)  # cooldown before restart
                    continue  # skip this cell, move to next

                if html:
                    cell_listings = _parse_listings_from_html(html)
                    new_count = 0
                    for listing in cell_listings:
                        pid = listing.get("place_id", "")
                        url_key = listing["maps_url"].split("?")[0].split("!")[0]

                        if pid and pid in seen_place_ids:
                            continue
                        if url_key in seen_urls:
                            continue

                        if pid:
                            seen_place_ids.add(pid)
                        seen_urls.add(url_key)
                        all_listings.append(listing)
                        new_count += 1

                        if len(all_listings) >= max_results:
                            break

                    if new_count:
                        progress(cells_done[0], total_cells,
                                 f"[W{worker_id+1}] +{new_count} new ({len(all_listings)} unique total)")

                # Delay between cells within this worker
                if local_i < len(points) - 1:
                    await asyncio.sleep(random.uniform(2, 4))
        finally:
            if crawler:
                try:
                    await crawler.__aexit__(None, None, None)
                except Exception:
                    pass

    # Interleave grid points across workers (round-robin)
    # so nearby cells go to different workers
    worker_chunks = [grid_points[i::NUM_WORKERS] for i in range(NUM_WORKERS)]

    progress(0, total_cells,
             f"Starting {NUM_WORKERS} parallel workers across {total_cells} cells")

    await asyncio.gather(*[
        grid_worker(i, chunk) for i, chunk in enumerate(worker_chunks)
    ])

    progress(total_cells, total_cells,
             f"Grid search complete: {len(all_listings)} unique listings from {total_cells} cells")

    if not all_listings:
        progress(0, 0, "No listings found in polygon area")
        return []

    # Phase 2: Scrape detail pages + enrichment (with own browser)
    async with AsyncWebCrawler(config=browser_config) as detail_crawler:
        good_leads = await _scrape_details_and_enrich(
            detail_crawler, all_listings, max_results, enrich_websites,
            config, rate_limiter, progress,
        )

    # Phase 3: Post-filter by polygon boundary
    if polygon_rings:
        filtered = []
        for lead in good_leads:
            lat = lead.get("latitude")
            lng = lead.get("longitude")
            if lat is not None and lng is not None:
                inside = any(_point_in_polygon(lng, lat, ring) for ring in polygon_rings)
                if not inside:
                    log.debug(f"Outside polygon, dropping: {lead.get('name')} ({lat},{lng})")
                    continue
            filtered.append(lead)

        dropped = len(good_leads) - len(filtered)
        if dropped:
            progress(len(filtered), len(filtered),
                     f"Polygon filter: dropped {dropped} results outside boundary")
        good_leads = filtered

    output = _format_leads(good_leads)
    progress(len(output), len(output),
             f"Done — {len(output)} leads from polygon area search")
    return output
