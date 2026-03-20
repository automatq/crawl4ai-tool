#!/usr/bin/env python3
"""
Crawl4AI Lead Generation Scraper

Scrapes websites to extract business lead data:
  - Company name & description
  - Email addresses
  - Phone numbers
  - Social media links
  - Physical addresses

Usage:
  # Scrape a single URL
  python scrape.py https://example.com

  # Search by keyword + cities
  python scrape.py -k "hvac" -c "Denver" "Phoenix" "Dallas"

  # Search with more results per city
  python scrape.py -k "plumbing" -c "Miami" -n 20

  # Export to CSV
  python scrape.py -k "hvac" -c "Denver" -o leads.csv

  # Scrape URLs from a file
  python scrape.py urls.txt -o leads.csv
"""

import argparse
import asyncio
import csv
import json
import re
import sys
import time
from urllib.parse import quote_plus, unquote, urlparse

import random

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.async_configs import ProxyConfig
from crawl4ai.proxy_strategy import RoundRobinProxyStrategy


# ── Scraper configuration ────────────────────────────────────────────

class ScrapeConfig:
    """Global configuration passed through the scraping pipeline."""

    def __init__(
        self,
        proxies: list[str] | None = None,
        stealth: bool = True,
        use_google_maps: bool = False,
        crawl_depth: int = 0,
        concurrency: int = 3,
        timeout: float = 30,
    ):
        self.proxies = proxies or []
        self.stealth = stealth
        self.use_google_maps = use_google_maps
        self.crawl_depth = crawl_depth  # 0 = main page only, 1 = follow internal links
        self.concurrency = concurrency
        self.timeout = timeout

    def make_browser_config(self) -> BrowserConfig:
        """Create a BrowserConfig with stealth + proxy settings."""
        kwargs = dict(
            headless=True,
            enable_stealth=self.stealth,
            user_agent_mode="random" if self.stealth else "",
        )
        if self.proxies:
            proxy_configs = [ProxyConfig(server=p) for p in self.proxies]
            # Use first proxy for browser-level config
            kwargs["proxy"] = self.proxies[0]
        return BrowserConfig(**kwargs)

    def make_proxy_rotation(self) -> RoundRobinProxyStrategy | None:
        if len(self.proxies) > 1:
            proxy_configs = [ProxyConfig(server=p) for p in self.proxies]
            return RoundRobinProxyStrategy(proxies=proxy_configs)
        return None


# ── Extraction patterns ──────────────────────────────────────────────

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?[2-9]\d{2}[-.\s]?\d{4}(?!\d)"
)
SOCIAL_DOMAINS = {
    "linkedin.com", "twitter.com", "x.com", "facebook.com",
    "instagram.com", "github.com", "youtube.com",
}


def extract_emails(text: str) -> list[str]:
    """Pull unique email addresses, filtering out common false positives."""
    ignore_domains = {
        "example.com", "domain.com", "email.com", "yoursite.com",
        "sentry.io", "googleapis.com", "schema.org", "w3.org",
        "wixpress.com", "squarespace.com", "wordpress.com",
        "cloudflare.com", "jsdelivr.net", "googleusercontent.com",
    }
    ignore_prefixes = {
        "noreply", "no-reply", "mailer-daemon", "postmaster",
        "webmaster", "hostmaster", "abuse",
    }
    emails = set()
    for m in EMAIL_RE.findall(text):
        local, domain = m.split("@", 1)
        domain = domain.lower()
        local_lower = local.lower()
        if domain in ignore_domains:
            continue
        if local_lower in ignore_prefixes:
            continue
        # Skip image-like patterns (logo@2x, icon@3x)
        if re.match(r".*@\dx$", m, re.I):
            continue
        if re.match(r".+\.(png|jpg|jpeg|gif|svg|webp)$", local_lower):
            continue
        emails.add(m.lower())
    return sorted(emails)


def extract_phones(text: str) -> list[str]:
    phones = []
    seen_digits = set()
    for m in PHONE_RE.findall(text):
        digits = re.sub(r"\D", "", m)
        # Normalize to 10 digits (strip leading 1)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10 and digits not in seen_digits:
            seen_digits.add(digits)
            # Normalize to (XXX) XXX-XXXX format
            formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            phones.append(formatted)
    return phones


def extract_social_links(text: str) -> dict[str, str]:
    """Return {platform: url} for known social media links found in markdown."""
    url_re = re.compile(r'https?://[^\s)\]>"\']+')
    socials: dict[str, str] = {}
    for url in url_re.findall(text):
        try:
            domain = urlparse(url).netloc.lower().lstrip("www.")
        except Exception:
            continue
        for sd in SOCIAL_DOMAINS:
            if domain.endswith(sd):
                platform = sd.split(".")[0]
                if platform == "x":
                    platform = "twitter"
                socials.setdefault(platform, url)
    return socials


def extract_address(text: str) -> str | None:
    """Try to pull a US-style street address."""
    addr_re = re.compile(
        r"\d{1,5}\s[\w\s.]+(?:St|Street|Ave|Avenue|Blvd|Boulevard|Dr|Drive"
        r"|Rd|Road|Ln|Lane|Way|Ct|Court|Pl|Place)"
        r"[.,]?\s*[\w\s]*,?\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?",
        re.I,
    )
    m = addr_re.search(text)
    return m.group(0).strip() if m else None


def guess_company_name(
    html: str, markdown: str, url: str, structured: dict | None = None
) -> str:
    """Best-effort company name from meta tags, structured data, or markdown."""
    # 1. Structured data name (JSON-LD)
    if structured and structured.get("name"):
        return structured["name"][:120]

    # 2. og:site_name meta tag
    og_match = re.search(
        r'<meta[^>]+property=["\']og:site_name["\'][^>]+content=["\']([^"\']+)["\']',
        html, re.I,
    )
    if not og_match:
        og_match = re.search(
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:site_name["\']',
            html, re.I,
        )
    if og_match:
        name = og_match.group(1).strip()
        if len(name) >= 2:
            return name[:120]

    # 3. <title> tag
    title_match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
    if title_match:
        title = title_match.group(1).strip()
        for sep in [" | ", " - ", " — ", " · ", " :: ", " » "]:
            if sep in title:
                title = title.split(sep)[0].strip()
                break
        if len(title) >= 3:
            return title[:120]

    # 4. Markdown heuristic (original approach)
    skip_re = re.compile(r"^(\d+\.\d|!\[|skip to|cookie|menu|nav)", re.I)
    for line in markdown.splitlines():
        cleaned = line.strip().lstrip("# ").strip()
        if not cleaned or len(cleaned) < 3:
            continue
        if cleaned.startswith(("[", "*", "!", "<", "{")):
            continue
        if skip_re.match(cleaned):
            continue
        for sep in [" | ", " - ", " — ", " \\ "]:
            if sep in cleaned:
                cleaned = cleaned.split(sep)[0].strip()
                break
        if len(cleaned) >= 3:
            return cleaned[:120]

    return urlparse(url).netloc


def extract_description(markdown: str) -> str:
    """Grab the first meaningful paragraph as a description."""
    for line in markdown.splitlines():
        line = line.strip()
        if len(line) > 60 and not line.startswith("#"):
            return line[:300]
    return ""


def extract_structured_data(html: str) -> dict:
    """Extract business info from JSON-LD structured data blocks."""
    result: dict = {}
    ld_pattern = re.compile(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        re.S | re.I,
    )
    target_types = {
        "localbusiness", "organization", "store", "restaurant",
        "medicalclinic", "dentist", "attorney", "autobody",
        "autodealer", "autorepair", "professionalservice",
    }

    for block in ld_pattern.findall(html):
        try:
            data = json.loads(block)
        except (json.JSONDecodeError, ValueError):
            continue

        # Handle @graph arrays
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and "@graph" in data:
            items = data["@graph"] if isinstance(data["@graph"], list) else [data["@graph"]]
        elif isinstance(data, dict):
            items = [data]

        for item in items:
            if not isinstance(item, dict):
                continue
            item_type = item.get("@type", "")
            if isinstance(item_type, list):
                item_type = item_type[0] if item_type else ""
            if item_type.lower() not in target_types:
                continue

            if not result.get("name") and item.get("name"):
                result["name"] = str(item["name"]).strip()
            if not result.get("phone") and item.get("telephone"):
                result["phone"] = str(item["telephone"]).strip()
            if not result.get("email") and item.get("email"):
                result["email"] = str(item["email"]).strip()
            if not result.get("hours") and item.get("openingHours"):
                hours = item["openingHours"]
                if isinstance(hours, list):
                    result["hours"] = ", ".join(str(h) for h in hours)
                else:
                    result["hours"] = str(hours)
            if not result.get("hours") and item.get("openingHoursSpecification"):
                specs = item["openingHoursSpecification"]
                if isinstance(specs, list):
                    parts = []
                    for spec in specs:
                        days = spec.get("dayOfWeek", "")
                        if isinstance(days, list):
                            days = ", ".join(str(d).split("/")[-1] for d in days)
                        opens = spec.get("opens", "")
                        closes = spec.get("closes", "")
                        if days and opens:
                            parts.append(f"{days}: {opens}-{closes}")
                    if parts:
                        result["hours"] = "; ".join(parts)

            # Address
            addr = item.get("address")
            if not result.get("address") and isinstance(addr, dict):
                parts = [
                    addr.get("streetAddress", ""),
                    addr.get("addressLocality", ""),
                    addr.get("addressRegion", ""),
                    addr.get("postalCode", ""),
                ]
                formatted = ", ".join(p for p in parts if p)
                if formatted:
                    result["address"] = formatted

            # Social links from sameAs
            if not result.get("sameAs") and item.get("sameAs"):
                same_as = item["sameAs"]
                if isinstance(same_as, str):
                    same_as = [same_as]
                result["sameAs"] = same_as

    return result


def extract_business_hours(html: str, markdown: str) -> str | None:
    """Extract business hours from HTML or markdown text."""
    # Try regex patterns in markdown
    hours_patterns = [
        re.compile(
            r"(?:hours|open|schedule)[:\s]*"
            r"((?:mon|tue|wed|thu|fri|sat|sun|weekday|weekend)[\w\s,\-:;aApPmM./]+)",
            re.I,
        ),
        re.compile(
            r"((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*[\s\-–]+\w+day\s*:?\s*\d{1,2}[:\d]*\s*[aApP][mM]"
            r"\s*[-–]\s*\d{1,2}[:\d]*\s*[aApP][mM])",
            re.I,
        ),
    ]
    for pat in hours_patterns:
        m = pat.search(markdown)
        if m:
            hours_text = m.group(1).strip() if pat.groups else m.group(0).strip()
            # Clean up and truncate
            hours_text = re.sub(r"\s+", " ", hours_text)
            return hours_text[:200]
    return None


# ── Resilience helpers ────────────────────────────────────────────────

class DomainRateLimiter:
    """Enforce a minimum delay between requests to the same domain."""

    def __init__(self, min_delay: float = 2.0):
        self.min_delay = min_delay
        self._last_access: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def wait(self, url: str):
        try:
            domain = urlparse(url).netloc.lower()
        except Exception:
            return
        async with self._lock:
            now = time.monotonic()
            last = self._last_access.get(domain, 0)
            wait_time = max(0, self.min_delay - (now - last))
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_access[domain] = time.monotonic()


async def _crawl_with_retry(
    crawler: AsyncWebCrawler,
    url: str,
    config: CrawlerRunConfig,
    max_retries: int = 3,
    timeout: float = 30,
    rate_limiter: DomainRateLimiter | None = None,
):
    """Crawl a URL with retries, timeout, and optional rate limiting.

    Returns (result, error_type) where error_type is None on success,
    or one of 'timeout', 'blocked', 'dns_failure', 'unknown'.
    """
    for attempt in range(max_retries):
        if rate_limiter:
            await rate_limiter.wait(url)
        try:
            result = await asyncio.wait_for(
                crawler.arun(url=url, config=config),
                timeout=timeout,
            )
            if result.success:
                return result, None

            err = (result.error_message or "").lower()
            if "name resolution" in err or "err_name_not_resolved" in err:
                return result, "dns_failure"
            if any(code in err for code in ["403", "429", "blocked"]):
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** (attempt + 2))  # 4s, 8s
                    continue
                return result, "blocked"
            # Other failure — retry
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))  # 2s, 4s
                continue
            return result, "unknown"

        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))
                continue
            from types import SimpleNamespace
            return SimpleNamespace(
                success=False, error_message="Timed out",
                html="", markdown=""
            ), "timeout"
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))
                continue
            from types import SimpleNamespace
            return SimpleNamespace(
                success=False, error_message=str(e),
                html="", markdown=""
            ), "unknown"

    # Should not reach here, but just in case
    from types import SimpleNamespace
    return SimpleNamespace(
        success=False, error_message="Max retries exceeded",
        html="", markdown=""
    ), "unknown"


# ── URL normalization ─────────────────────────────────────────────────

def normalize_url(url: str) -> str:
    """Ensure URL has a scheme prefix."""
    url = url.strip()
    if not url.startswith(("http://", "https://", "file://", "raw:")):
        url = "https://" + url
    return url


# ── DuckDuckGo search for leads ───────────────────────────────────────

SKIP_DOMAINS = {
    "duckduckgo.com", "google.com", "googleapis.com", "gstatic.com",
    "youtube.com", "schema.org", "w3.org", "wikipedia.org",
    "yelp.com", "yelp.ca", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "pinterest.com", "tiktok.com", "reddit.com",
    "mapquest.com", "yellowpages.com", "yellowpages.ca", "bbb.org", "angi.com",
    "angieslist.com", "homeadvisor.com", "thumbtack.com",
    "nextdoor.com", "manta.com", "chamberofcommerce.com",
    "forbes.com", "usnews.com", "nytimes.com", "cnn.com",
    "bobvila.com", "thisoldhouse.com", "architecturaldigest.com",
    "expertise.com", "porch.com", "bark.com", "networx.com",
    "airconditioningup.com", "fixr.com", "buildzoom.com",
    "todayshomeowner.com", "superpages.com", "citysearch.com",
    "bing.com", "microsoft.com", "msn.com", "brave.com",
    "search.brave.com", "amazon.com", "ebay.com",
}


def _filter_urls(found: list[str], seen: set[str], limit: int) -> list[str]:
    """Filter out aggregator sites and deduplicate."""
    urls = []
    for u in found:
        try:
            domain = urlparse(u).netloc.lower().lstrip("www.")
        except Exception:
            continue
        if domain in seen:
            continue
        if any(domain.endswith(sd) for sd in SKIP_DOMAINS):
            continue
        seen.add(domain)
        urls.append(u)
        if len(urls) >= limit:
            break
    return urls


# ── Search engine: DuckDuckGo ─────────────────────────────────────────

def _extract_ddg_urls(html: str, md: str) -> list[str]:
    url_pattern = re.compile(r'uddg=([^&"\'>\s]+)')
    found = [unquote(u) for u in url_pattern.findall(html)]
    if not found:
        md_url_re = re.compile(r'uddg=([^&)\s]+)')
        for m in md_url_re.findall(md):
            found.append(unquote(m))
    return found


async def _search_ddg(
    query: str, crawler: AsyncWebCrawler, num_results: int = 10,
) -> list[str]:
    """Search DuckDuckGo, paginating as needed."""
    config = CrawlerRunConfig(word_count_threshold=0, remove_overlay_elements=True)
    all_found: list[str] = []
    offset = 0
    max_pages = (num_results // 10) + 2

    for page in range(max_pages):
        search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        if offset > 0:
            search_url += f"&s={offset}&dc={offset + 1}&o=json&v=l"

        result, err = await _crawl_with_retry(crawler, search_url, config, max_retries=2, timeout=20)
        if not result.success:
            break

        html = str(result.html) if result.html else ""
        md = str(result.markdown) if result.markdown else ""
        found = _extract_ddg_urls(html, md)
        if not found:
            break

        all_found.extend(found)
        if len(all_found) >= num_results * 2:  # raw count before filtering
            break
        offset += 10

    return all_found


# ── Search engine: Bing ───────────────────────────────────────────────

def _extract_bing_urls(html: str) -> list[str]:
    # Bing result links are in <li class="b_algo"><h2><a href="...">
    href_re = re.compile(r'<li[^>]*class="b_algo"[^>]*>.*?<a\s+href="(https?://[^"]+)"', re.S)
    found = href_re.findall(html)
    if not found:
        # Fallback: grab any http links that aren't bing/microsoft
        fallback_re = re.compile(r'href="(https?://(?!www\.bing|www\.microsoft)[^"]+)"')
        found = fallback_re.findall(html)
    return found


async def _search_bing(
    query: str, crawler: AsyncWebCrawler, num_results: int = 10,
) -> list[str]:
    """Search Bing, paginating as needed."""
    config = CrawlerRunConfig(word_count_threshold=0, remove_overlay_elements=True)
    all_found: list[str] = []
    offset = 0
    max_pages = (num_results // 10) + 2

    for page in range(max_pages):
        search_url = f"https://www.bing.com/search?q={quote_plus(query)}"
        if offset > 0:
            search_url += f"&first={offset + 1}"

        result, err = await _crawl_with_retry(crawler, search_url, config, max_retries=2, timeout=20)
        if not result.success:
            break

        html = str(result.html) if result.html else ""
        found = _extract_bing_urls(html)
        if not found:
            break

        all_found.extend(found)
        if len(all_found) >= num_results * 2:
            break
        offset += 10

    return all_found


# ── Search engine: Brave ──────────────────────────────────────────────

def _extract_brave_urls(html: str, md: str) -> list[str]:
    # Brave results are in <a class="result-header" href="...">
    href_re = re.compile(r'class="result-header"[^>]*href="(https?://[^"]+)"', re.S)
    found = href_re.findall(html)
    if not found:
        # Fallback: extract URLs from markdown links
        md_re = re.compile(r'\]\((https?://[^\s)]+)\)')
        found = md_re.findall(md)
    return found


async def _search_brave(
    query: str, crawler: AsyncWebCrawler, num_results: int = 10,
) -> list[str]:
    """Search Brave, paginating as needed."""
    config = CrawlerRunConfig(word_count_threshold=0, remove_overlay_elements=True)
    all_found: list[str] = []
    offset = 0
    max_pages = (num_results // 10) + 2

    for page in range(max_pages):
        search_url = f"https://search.brave.com/search?q={quote_plus(query)}"
        if offset > 0:
            search_url += f"&offset={offset}"

        result, err = await _crawl_with_retry(crawler, search_url, config, max_retries=2, timeout=20)
        if not result.success:
            break

        html = str(result.html) if result.html else ""
        md = str(result.markdown) if result.markdown else ""
        found = _extract_brave_urls(html, md)
        if not found:
            break

        all_found.extend(found)
        if len(all_found) >= num_results * 2:
            break
        offset += 10

    return all_found


# ── Search engine: Google Maps ────────────────────────────────────────

def _extract_maps_urls(html: str, md: str) -> list[str]:
    """Extract business website URLs from Google Maps results."""
    urls = []
    # Google Maps embeds website links in various formats
    # Look for links that are actual business websites
    url_re = re.compile(r'https?://(?!www\.google|maps\.google|play\.google)[^\s"\'<>]+')
    for u in url_re.findall(html):
        parsed = urlparse(u)
        if parsed.netloc and '.' in parsed.netloc:
            urls.append(u)
    # Also try markdown links
    md_re = re.compile(r'\]\((https?://(?!www\.google|maps\.google)[^\s)]+)\)')
    for u in md_re.findall(md):
        urls.append(u)
    return urls


def _extract_maps_leads(html: str, md: str) -> list[dict]:
    """Extract structured lead data directly from Google Maps results page."""
    leads = []
    # Google Maps shows business cards with name, address, phone, rating
    # We extract what we can from the rendered page
    # Phone numbers and addresses are often directly visible
    phones = PHONE_RE.findall(md)
    emails = EMAIL_RE.findall(md)

    return leads  # Main value is the URLs we extract, leads from direct scraping


async def _search_google_maps(
    query: str, crawler: AsyncWebCrawler, num_results: int = 10,
) -> list[str]:
    """Search Google Maps for business listings."""
    config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        wait_until="networkidle",
    )
    search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"

    result, err = await _crawl_with_retry(crawler, search_url, config, max_retries=2, timeout=25)
    if not result.success:
        return []

    html = str(result.html) if result.html else ""
    md = str(result.markdown) if result.markdown else ""

    # Extract business website URLs from maps results
    found = _extract_maps_urls(html, md)
    return found


# ── Unified multi-engine search ───────────────────────────────────────

SEARCH_ENGINES = [
    ("DuckDuckGo", _search_ddg),
    ("Bing", _search_bing),
    ("Brave", _search_brave),
]


async def search_multi(
    keyword: str,
    city: str,
    crawler: AsyncWebCrawler,
    seen: set[str],
    num_results: int = 10,
    use_google_maps: bool = False,
) -> list[str]:
    """Search across all engines for a keyword+city, return filtered URLs."""
    query = f"{keyword} {city}"
    all_urls: list[str] = []

    engines = list(SEARCH_ENGINES)
    if use_google_maps:
        engines.append(("Google Maps", _search_google_maps))

    for engine_name, engine_fn in engines:
        if len(all_urls) >= num_results:
            break

        remaining = num_results - len(all_urls)
        try:
            raw = await engine_fn(query, crawler, remaining)
        except Exception as e:
            print(f"  {engine_name} failed: {e}, trying next engine...")
            continue
        new = _filter_urls(raw, seen, remaining)
        all_urls.extend(new)

    return all_urls


# ── Industry synonyms ─────────────────────────────────────────────────
# Map common keywords to related search terms in the same industry.

INDUSTRY_SYNONYMS: dict[str, list[str]] = {
    "dentist": [
        "dental clinic", "dental office", "dental practice", "dental group",
        "dental care", "family dentist", "cosmetic dentist", "pediatric dentist",
        "orthodontist", "oral surgeon", "endodontist", "periodontist",
        "dental implants", "dental hygienist", "teeth whitening",
        "emergency dentist", "denture clinic", "prosthodontist",
    ],
    "hvac": [
        "heating and cooling", "air conditioning", "furnace repair",
        "ac repair", "heat pump", "ductwork", "boiler repair",
        "heating contractor", "cooling contractor", "refrigeration",
        "ventilation", "indoor air quality", "duct cleaning",
    ],
    "plumber": [
        "plumbing", "plumbing company", "drain cleaning", "sewer repair",
        "water heater", "pipe repair", "emergency plumber", "plumbing contractor",
        "backflow testing", "sump pump", "gas line repair",
    ],
    "electrician": [
        "electrical contractor", "electrical service", "wiring",
        "electrical repair", "panel upgrade", "lighting installation",
        "emergency electrician", "commercial electrician", "residential electrician",
    ],
    "roofer": [
        "roofing", "roofing company", "roof repair", "roof replacement",
        "roofing contractor", "shingle repair", "flat roof", "metal roofing",
        "gutter installation", "roof inspection",
    ],
    "lawyer": [
        "attorney", "law firm", "legal services", "law office",
        "family lawyer", "criminal lawyer", "personal injury lawyer",
        "immigration lawyer", "real estate lawyer", "corporate lawyer",
        "divorce attorney", "estate planning attorney",
    ],
    "accountant": [
        "accounting firm", "cpa", "bookkeeper", "bookkeeping",
        "tax preparer", "tax accountant", "payroll services",
        "financial advisor", "auditing services",
    ],
    "realtor": [
        "real estate agent", "real estate broker", "real estate office",
        "property management", "realty", "home sales", "commercial real estate",
    ],
    "auto": [
        "auto repair", "mechanic", "auto body shop", "car repair",
        "transmission repair", "brake repair", "oil change", "tire shop",
        "auto detailing", "collision repair",
    ],
    "restaurant": [
        "catering", "cafe", "diner", "bistro", "eatery",
        "food delivery", "takeout", "banquet hall",
    ],
}

# ── Neighborhood / area expansion ─────────────────────────────────────
# Major cities mapped to their neighborhoods and nearby areas.
# This turns 1 city into many sub-searches.

CITY_NEIGHBORHOODS: dict[str, list[str]] = {
    "toronto": [
        "Toronto", "Scarborough", "North York", "Etobicoke", "East York",
        "York", "Mississauga", "Brampton", "Markham", "Vaughan",
        "Richmond Hill", "Oakville", "Burlington", "Ajax", "Pickering",
        "Whitby", "Oshawa", "Newmarket", "Aurora", "Milton",
        "Downtown Toronto", "Midtown Toronto", "Yorkville Toronto",
        "Liberty Village Toronto", "Leslieville Toronto", "The Beaches Toronto",
        "Bloor West Village Toronto", "Danforth Toronto", "Kensington Toronto",
        "Queen West Toronto", "Parkdale Toronto", "Roncesvalles Toronto",
        "High Park Toronto", "Leaside Toronto", "Riverdale Toronto",
        "Willowdale Toronto", "Thornhill", "Woodbridge",
    ],
    "new york": [
        "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island",
        "Harlem", "Upper East Side", "Upper West Side", "Midtown Manhattan",
        "Lower East Side", "Chelsea Manhattan", "SoHo Manhattan",
        "Tribeca Manhattan", "Greenwich Village", "East Village Manhattan",
        "Williamsburg Brooklyn", "Park Slope Brooklyn", "DUMBO Brooklyn",
        "Astoria Queens", "Flushing Queens", "Long Island City",
        "Jersey City", "Hoboken", "Yonkers", "White Plains",
        "New Rochelle", "Mount Vernon", "Stamford",
    ],
    "los angeles": [
        "Los Angeles", "Hollywood", "Santa Monica", "Beverly Hills",
        "Pasadena", "Glendale", "Burbank", "Long Beach", "Torrance",
        "Inglewood", "Culver City", "West Hollywood", "Koreatown LA",
        "Silver Lake LA", "Echo Park LA", "Venice LA", "Westwood LA",
        "Brentwood LA", "Encino", "Sherman Oaks", "Studio City",
        "Woodland Hills", "Northridge", "Van Nuys", "Downey",
        "Whittier", "Pomona", "Arcadia", "Alhambra",
    ],
    "chicago": [
        "Chicago", "Evanston", "Oak Park", "Cicero", "Schaumburg",
        "Naperville", "Arlington Heights", "Skokie", "Des Plaines",
        "Lincoln Park Chicago", "Wicker Park Chicago", "Logan Square Chicago",
        "Lakeview Chicago", "Hyde Park Chicago", "Loop Chicago",
        "River North Chicago", "Gold Coast Chicago", "Bucktown Chicago",
        "Pilsen Chicago", "Bridgeport Chicago", "Oak Lawn", "Orland Park",
    ],
    "houston": [
        "Houston", "Sugar Land", "Katy", "Pearland", "Pasadena TX",
        "The Woodlands", "Spring TX", "Cypress TX", "Humble TX",
        "League City", "Missouri City", "Baytown", "Conroe",
        "Montrose Houston", "Heights Houston", "Midtown Houston",
        "Galleria Houston", "Memorial Houston", "Rice Village Houston",
    ],
    "phoenix": [
        "Phoenix", "Scottsdale", "Tempe", "Mesa", "Chandler",
        "Gilbert", "Glendale AZ", "Peoria AZ", "Surprise AZ",
        "Goodyear AZ", "Avondale AZ", "Buckeye AZ", "Cave Creek",
        "Fountain Hills", "Paradise Valley",
    ],
    "dallas": [
        "Dallas", "Fort Worth", "Plano", "Irving", "Arlington TX",
        "Frisco", "McKinney", "Denton TX", "Richardson TX", "Garland TX",
        "Grand Prairie", "Mesquite TX", "Carrollton TX", "Lewisville TX",
        "Allen TX", "Flower Mound", "Southlake TX", "Grapevine TX",
        "Uptown Dallas", "Deep Ellum Dallas", "Oak Lawn Dallas",
    ],
    "denver": [
        "Denver", "Aurora CO", "Lakewood CO", "Arvada", "Westminster CO",
        "Thornton CO", "Centennial CO", "Boulder", "Littleton CO",
        "Broomfield", "Parker CO", "Castle Rock CO", "Englewood CO",
        "Highlands Ranch", "Lone Tree CO", "Golden CO",
        "Cherry Creek Denver", "LoDo Denver", "Capitol Hill Denver",
        "Wash Park Denver", "RiNo Denver",
    ],
    "miami": [
        "Miami", "Miami Beach", "Coral Gables", "Hialeah",
        "Fort Lauderdale", "Hollywood FL", "Pembroke Pines",
        "Boca Raton", "Doral", "Aventura", "Kendall FL",
        "Coconut Grove Miami", "Wynwood Miami", "Brickell Miami",
        "Little Havana Miami", "Homestead FL", "Key Biscayne",
    ],
    "atlanta": [
        "Atlanta", "Marietta GA", "Roswell GA", "Sandy Springs",
        "Alpharetta", "Decatur GA", "Kennesaw", "Duluth GA",
        "Johns Creek", "Lawrenceville GA", "Smyrna GA", "Brookhaven GA",
        "Buckhead Atlanta", "Midtown Atlanta", "Virginia Highland Atlanta",
    ],
    "seattle": [
        "Seattle", "Bellevue WA", "Redmond WA", "Kirkland WA",
        "Renton WA", "Kent WA", "Tacoma", "Everett WA", "Bothell WA",
        "Shoreline WA", "Burien WA", "Federal Way",
        "Capitol Hill Seattle", "Fremont Seattle", "Ballard Seattle",
        "Queen Anne Seattle", "West Seattle", "University District Seattle",
    ],
    "san francisco": [
        "San Francisco", "Oakland", "Berkeley", "San Jose",
        "Daly City", "South San Francisco", "San Mateo", "Palo Alto",
        "Mountain View CA", "Sunnyvale", "Santa Clara", "Fremont CA",
        "Walnut Creek", "Concord CA", "Hayward CA", "Redwood City",
        "Mission District SF", "Castro SF", "Marina District SF",
        "SOMA SF", "Nob Hill SF", "North Beach SF",
    ],
    "london": [
        "London", "Westminster", "Camden", "Islington", "Hackney",
        "Tower Hamlets", "Southwark", "Lambeth", "Wandsworth",
        "Hammersmith", "Kensington", "Chelsea London", "Greenwich",
        "Lewisham", "Croydon", "Bromley", "Barnet", "Enfield",
        "Ealing", "Hounslow", "Richmond London", "Kingston London",
        "Stratford London", "Shoreditch", "Brixton", "Clapham",
        "Notting Hill", "Mayfair", "Soho London",
    ],
    "vancouver": [
        "Vancouver", "Burnaby", "Surrey BC", "Richmond BC",
        "Coquitlam", "North Vancouver", "West Vancouver",
        "New Westminster", "Langley BC", "Delta BC", "Port Moody",
        "Kitsilano Vancouver", "Mount Pleasant Vancouver",
        "Yaletown Vancouver", "Gastown Vancouver", "Commercial Drive Vancouver",
    ],
}


def _expand_cities(cities: list[str]) -> list[str]:
    """Expand city names into neighborhoods if we have them mapped."""
    expanded = []
    seen = set()
    for city in cities:
        key = city.lower().strip()
        if key in CITY_NEIGHBORHOODS:
            for area in CITY_NEIGHBORHOODS[key]:
                if area.lower() not in seen:
                    seen.add(area.lower())
                    expanded.append(area)
        else:
            if key not in seen:
                seen.add(key)
                expanded.append(city)
    return expanded


def _build_query_variations(keyword: str) -> list[str]:
    """Generate search query variations including industry synonyms."""
    kw = keyword.lower().strip()
    base_terms = [kw]

    # Add industry synonyms
    for industry_kw, synonyms in INDUSTRY_SYNONYMS.items():
        if industry_kw in kw or kw in industry_kw:
            for syn in synonyms:
                if syn.lower() != kw:
                    base_terms.append(syn)
            break  # Only match one industry

    # Business suffixes to append to each base term
    suffixes = [
        "", "near me", "company", "services", "reviews",
        "best", "top rated", "affordable", "emergency",
        "licensed", "local", "certified", "professionals",
    ]

    variations = []
    for term in base_terms:
        for suffix in suffixes:
            if suffix and suffix not in term:
                variations.append(f"{term} {suffix}")
            elif not suffix:
                variations.append(term)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for v in variations:
        if v not in seen:
            seen.add(v)
            unique.append(v)

    return unique


# Type alias for progress callback: (found_so_far, target, status_message)
ProgressCallback = None  # typing: Callable[[int, int, str], None] | None


async def search_leads(
    keyword: str,
    cities: list[str],
    num_results: int = 10,
    on_progress: ProgressCallback = None,
    config: ScrapeConfig | None = None,
) -> list[str]:
    """Search multiple engines across cities/neighborhoods with query variations."""
    config = config or ScrapeConfig()
    browser_cfg = config.make_browser_config()
    all_urls = []
    seen = set()

    # Expand cities into neighborhoods for large targets
    if num_results > 50:
        search_areas = _expand_cities(cities)
    else:
        search_areas = list(cities)

    variations = _build_query_variations(keyword)
    per_query = min(30, max(10, num_results // max(1, len(search_areas) * 2)))

    def _progress(msg: str):
        if on_progress:
            on_progress(len(all_urls), num_results, msg)
        print(msg)

    engines = list(SEARCH_ENGINES)
    if config.use_google_maps:
        engines.append(("Google Maps", _search_google_maps))
    engine_names = ", ".join(name for name, _ in engines)
    _progress(
        f"  {len(variations)} queries x {len(search_areas)} areas x "
        f"{len(engines)} engines ({engine_names})\n"
        f"  Target: {num_results} leads"
    )
    if config.stealth:
        _progress("  Stealth mode: enabled")
    if config.proxies:
        _progress(f"  Proxies: {len(config.proxies)} configured")

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        for query_kw in variations:
            for area in search_areas:
                if len(all_urls) >= num_results:
                    break

                remaining = num_results - len(all_urls)
                batch_size = min(per_query, remaining)

                _progress(f"  [{len(all_urls)}/{num_results}] \"{query_kw}\" in {area}")

                new = await search_multi(
                    query_kw, area, crawler, seen, batch_size,
                    use_google_maps=config.use_google_maps,
                )
                all_urls.extend(new)

            if len(all_urls) >= num_results:
                break

    _progress(f"  Total unique business sites found: {len(all_urls)}")
    return all_urls


async def scrape_all(
    urls: list[str],
    on_progress: ProgressCallback = None,
    config: ScrapeConfig | None = None,
) -> list[dict]:
    """Scrape all URLs with no limit. Processes in batches to manage memory."""
    config = config or ScrapeConfig()
    browser_cfg = config.make_browser_config()
    total = len(urls)
    all_results: list[dict] = []
    rate_limiter = DomainRateLimiter(min_delay=2.0)
    counter = {"done": 0}
    lock = asyncio.Lock()

    # Process in batches to avoid memory blowup on large imports
    batch_size = max(50, config.concurrency * 10)

    for batch_start in range(0, total, batch_size):
        batch_urls = urls[batch_start:batch_start + batch_size]
        batch_results: list[dict | None] = [None] * len(batch_urls)
        sem = asyncio.Semaphore(config.concurrency)

        async def _task(idx: int, url: str):
            async with sem:
                lead = await scrape_lead(
                    url, crawler,
                    rate_limiter=rate_limiter,
                    timeout=config.timeout,
                    crawl_depth=config.crawl_depth,
                )
                batch_results[idx] = lead
                async with lock:
                    counter["done"] += 1
                    if on_progress:
                        on_progress(counter["done"], total,
                                    f"Scraping {counter['done']}/{total}: {url[:60]}...")

        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            tasks = [_task(i, url) for i, url in enumerate(batch_urls)]
            await asyncio.gather(*tasks)

        all_results.extend(r for r in batch_results if r is not None)

    return all_results


# ── Sub-page & deep crawling ──────────────────────────────────────────

SUB_PATHS = ["/contact", "/contact-us", "/about", "/about-us"]


async def _scrape_sub_pages(
    crawler: AsyncWebCrawler,
    base_url: str,
    rate_limiter: DomainRateLimiter | None = None,
    timeout: float = 10,
) -> str:
    """Scrape contact/about sub-pages and return combined markdown."""
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    config = CrawlerRunConfig(
        word_count_threshold=10,
        exclude_external_links=False,
        remove_overlay_elements=True,
    )
    parts = []
    for path in SUB_PATHS:
        sub_url = base + path
        result, err = await _crawl_with_retry(
            crawler, sub_url, config, max_retries=1, timeout=timeout,
            rate_limiter=rate_limiter,
        )
        if result.success:
            md = str(result.markdown) if result.markdown else ""
            if md.strip():
                parts.append(md)
    return "\n".join(parts)


def _extract_internal_links(html: str, base_url: str, max_links: int = 10) -> list[str]:
    """Extract internal page links from HTML, prioritizing contact/service pages."""
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower()
    base = f"{parsed_base.scheme}://{parsed_base.netloc}"

    # Find all href links
    href_re = re.compile(r'href=["\']([^"\'#]+)["\']', re.I)
    links = set()

    # Priority keywords — pages most likely to have contact info
    priority_keywords = [
        "contact", "about", "team", "staff", "location", "service",
        "our-team", "meet", "office", "reach", "connect", "directions",
    ]

    priority_links = []
    other_links = []

    for href in href_re.findall(html):
        href = href.strip()
        if href.startswith("mailto:") or href.startswith("tel:") or href.startswith("javascript:"):
            continue

        # Resolve relative URLs
        if href.startswith("/"):
            full_url = base + href
        elif href.startswith("http"):
            full_url = href
        else:
            continue

        # Only same-domain links
        try:
            link_domain = urlparse(full_url).netloc.lower()
        except Exception:
            continue
        if link_domain != base_domain:
            continue

        # Skip asset/file links
        path = urlparse(full_url).path.lower()
        if any(path.endswith(ext) for ext in [".pdf", ".jpg", ".png", ".gif", ".css", ".js", ".svg"]):
            continue

        if full_url in links or full_url == base_url:
            continue
        links.add(full_url)

        if any(kw in path.lower() for kw in priority_keywords):
            priority_links.append(full_url)
        else:
            other_links.append(full_url)

    # Return priority links first, then fill with others
    result = priority_links[:max_links]
    remaining = max_links - len(result)
    if remaining > 0:
        result.extend(other_links[:remaining])
    return result


async def _deep_crawl(
    crawler: AsyncWebCrawler,
    base_url: str,
    html: str,
    rate_limiter: DomainRateLimiter | None = None,
    timeout: float = 10,
    max_pages: int = 5,
) -> str:
    """Follow internal links and return combined markdown from linked pages."""
    internal_links = _extract_internal_links(html, base_url, max_links=max_pages)
    if not internal_links:
        return ""

    config = CrawlerRunConfig(
        word_count_threshold=10,
        exclude_external_links=False,
        remove_overlay_elements=True,
    )
    parts = []
    for link_url in internal_links:
        result, err = await _crawl_with_retry(
            crawler, link_url, config, max_retries=1, timeout=timeout,
            rate_limiter=rate_limiter,
        )
        if result.success:
            md = str(result.markdown) if result.markdown else ""
            if md.strip():
                parts.append(md)
    return "\n".join(parts)


# ── Core scraping ─────────────────────────────────────────────────────

async def scrape_lead(
    url: str,
    crawler: AsyncWebCrawler,
    rate_limiter: DomainRateLimiter | None = None,
    timeout: float = 30,
    crawl_depth: int = 0,
) -> dict:
    """Scrape a single URL and return structured lead data.

    crawl_depth:
        0 = main page + sub-pages (/contact, /about) if no emails found
        1 = also follow internal links to find more contact data
    """
    url = normalize_url(url)

    config = CrawlerRunConfig(
        word_count_threshold=10,
        exclude_external_links=False,
        remove_overlay_elements=True,
    )

    result, err = await _crawl_with_retry(
        crawler, url, config, max_retries=3, timeout=timeout,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        error_msg = result.error_message or "Failed to crawl"
        if err:
            error_msg = f"{error_msg} ({err})"
        return {"url": url, "error": error_msg}

    html = str(result.html) if result.html else ""
    md = str(result.markdown) if result.markdown else ""

    emails = extract_emails(md)
    extra_md = ""

    # Try sub-pages if no emails found on main page
    if not emails:
        extra_md = await _scrape_sub_pages(crawler, url, rate_limiter, timeout=10)

    # Deep crawl: follow internal links for more data
    if crawl_depth >= 1:
        deep_md = await _deep_crawl(
            crawler, url, html, rate_limiter, timeout=10, max_pages=5
        )
        if deep_md:
            extra_md = extra_md + "\n" + deep_md if extra_md else deep_md

    combined_md = md + "\n" + extra_md if extra_md else md

    if extra_md:
        emails = extract_emails(combined_md)

    structured = extract_structured_data(html)

    return {
        "url": url,
        "company": guess_company_name(html, md, url, structured),
        "description": extract_description(md),
        "emails": emails,
        "phones": extract_phones(combined_md),
        "address": structured.get("address") or extract_address(combined_md),
        "socials": extract_social_links(combined_md),
        "hours": structured.get("hours") or extract_business_hours(html, combined_md),
    }


# ── Output formatters ────────────────────────────────────────────────

def format_table(leads: list[dict]) -> str:
    """Human-readable table for the terminal."""
    lines = []
    for i, lead in enumerate(leads, 1):
        if "error" in lead:
            lines.append(f"\n{'='*60}\n#{i}  {lead['url']}\n  ERROR: {lead['error']}")
            continue
        lines.append(f"\n{'='*60}")
        lines.append(f"#{i}  {lead['url']}")
        lines.append(f"  Company:     {lead['company']}")
        if lead["description"]:
            lines.append(f"  Description: {lead['description']}")
        if lead["emails"]:
            lines.append(f"  Emails:      {', '.join(lead['emails'])}")
        if lead["phones"]:
            lines.append(f"  Phones:      {', '.join(lead['phones'])}")
        if lead["address"]:
            lines.append(f"  Address:     {lead['address']}")
        if lead.get("hours"):
            lines.append(f"  Hours:       {lead['hours']}")
        if lead["socials"]:
            for platform, url in lead["socials"].items():
                lines.append(f"  {platform:12s} {url}")
    return "\n".join(lines)


def write_csv(leads: list[dict], path: str):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["url", "company", "description", "emails", "phones", "address", "hours", "socials"])
        for lead in leads:
            w.writerow([
                lead.get("url", ""),
                lead.get("company", ""),
                lead.get("description", ""),
                "; ".join(lead.get("emails", [])),
                "; ".join(lead.get("phones", [])),
                lead.get("address", ""),
                lead.get("hours", ""),
                json.dumps(lead.get("socials", {})),
            ])
    print(f"\nSaved {len(leads)} leads to {path}")


def write_json(leads: list[dict], path: str):
    with open(path, "w") as f:
        json.dump(leads, f, indent=2)
    print(f"\nSaved {len(leads)} leads to {path}")


# ── CLI ───────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Scrape websites for lead generation data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for HVAC companies in specific cities
  python scrape.py -k "hvac" -c "Denver" "Phoenix" "Dallas"

  # More results per city, export to CSV
  python scrape.py -k "plumbing contractor" -c "Miami" "Tampa" -n 20 -o leads.csv

  # Scrape a single URL
  python scrape.py https://example.com

  # Scrape from a file of URLs
  python scrape.py urls.txt -o leads.csv
        """,
    )

    # Keyword search mode
    p.add_argument(
        "-k", "--keyword",
        help="Business keyword to search for (e.g. 'hvac', 'plumbing contractor').",
    )
    p.add_argument(
        "-c", "--cities",
        nargs="+",
        help="Cities to search in (e.g. 'Denver' 'Phoenix' 'Dallas').",
    )
    p.add_argument(
        "-n", "--num-results",
        type=int,
        default=50,
        help="Max leads to find (default: 50). Set higher for more results.",
    )

    # Direct URL mode
    p.add_argument(
        "target",
        nargs="?",
        help="A URL to scrape, or a .txt file with one URL per line.",
    )

    # Output options
    p.add_argument(
        "-o", "--output",
        help="Output file path (e.g. leads.csv or leads.json).",
    )
    p.add_argument(
        "-f", "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format (default: table).",
    )

    # Advanced options
    p.add_argument(
        "--proxy",
        nargs="+",
        help="Proxy server(s) for rotation (e.g. http://proxy1:8080 http://proxy2:8080).",
    )
    p.add_argument(
        "--no-stealth",
        action="store_true",
        help="Disable stealth mode (browser fingerprint randomization).",
    )
    p.add_argument(
        "--google-maps",
        action="store_true",
        help="Also search Google Maps for business listings.",
    )
    p.add_argument(
        "--deep",
        action="store_true",
        help="Deep crawl: follow internal links to find more contact data.",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of concurrent scrapers (default: 3).",
    )

    return p.parse_args()


def load_urls(target: str) -> list[str]:
    if target.endswith(".txt"):
        with open(target) as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return [normalize_url(target)]


def main():
    args = parse_args()

    cfg = ScrapeConfig(
        proxies=args.proxy or [],
        stealth=not args.no_stealth,
        use_google_maps=args.google_maps,
        crawl_depth=1 if args.deep else 0,
        concurrency=args.concurrency,
    )

    # Keyword search mode
    if args.keyword:
        if not args.cities:
            print("Error: --cities is required with --keyword.", file=sys.stderr)
            sys.exit(1)

        print(f"Searching for '{args.keyword}' in {len(args.cities)} city/cities...\n")
        urls = asyncio.run(search_leads(args.keyword, args.cities, args.num_results, config=cfg))

        if not urls:
            print("No business websites found.", file=sys.stderr)
            sys.exit(1)

        print(f"\nScraping {len(urls)} business sites...\n")
        leads = asyncio.run(scrape_all(urls, config=cfg))

    # Direct URL / file mode
    elif args.target:
        urls = load_urls(args.target)
        if not urls:
            print("No URLs provided.", file=sys.stderr)
            sys.exit(1)

        print(f"Scraping {len(urls)} URL(s)...\n")
        leads = asyncio.run(scrape_all(urls, config=cfg))

    else:
        print("Error: provide a URL/file or use -k/-c for keyword search.", file=sys.stderr)
        print("Run with --help for usage.", file=sys.stderr)
        sys.exit(1)

    # Determine output format
    fmt = args.format
    out = args.output

    if out and fmt == "table":
        if out.endswith(".csv"):
            fmt = "csv"
        elif out.endswith(".json"):
            fmt = "json"

    if fmt == "csv":
        write_csv(leads, out or "leads.csv")
    elif fmt == "json":
        write_json(leads, out or "leads.json")
    else:
        print(format_table(leads))


if __name__ == "__main__":
    main()
