#!/usr/bin/env python3
"""
Contact Form Outreach — auto-submit contact forms for leads without emails.

Detects contact forms on websites, classifies fields, fills them with
templated messages, and submits. Skips forms with CAPTCHAs.
"""

import asyncio
import json
import logging
import random
import re
from typing import Callable
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from scrape import (
    ScrapeConfig,
    DomainRateLimiter,
    _crawl_with_retry,
    _extract_internal_links,
    SUB_PATHS,
)

log = logging.getLogger(__name__)

# ── Contact page discovery paths ──────────────────────────────────────

CONTACT_PATHS = ["/contact", "/contact-us", "/get-in-touch", "/get-a-quote", "/reach-out"]

# ── JavaScript: detect forms on a page ────────────────────────────────

DETECT_FORMS_JS = """
(function() {
    const results = { forms: [], has_captcha: false };

    // Check for CAPTCHAs
    const captchaSelectors = [
        'iframe[src*="recaptcha"]', 'iframe[src*="hcaptcha"]',
        '.g-recaptcha', '.h-captcha', '.cf-turnstile',
        'iframe[src*="turnstile"]',
    ];
    for (const sel of captchaSelectors) {
        if (document.querySelector(sel)) {
            results.has_captcha = true;
            break;
        }
    }

    // Find all forms
    document.querySelectorAll('form').forEach((form, fi) => {
        const fields = [];
        const formRect = form.getBoundingClientRect();

        // Skip invisible forms
        const style = window.getComputedStyle(form);
        if (style.display === 'none' || style.visibility === 'hidden') return;

        form.querySelectorAll('input, textarea, select').forEach((el) => {
            const elStyle = window.getComputedStyle(el);
            const isHidden = (
                elStyle.display === 'none' ||
                elStyle.visibility === 'hidden' ||
                elStyle.opacity === '0' ||
                el.type === 'hidden' ||
                (el.offsetWidth === 0 && el.offsetHeight === 0)
            );

            // Find associated label
            let labelText = '';
            if (el.id) {
                const label = document.querySelector('label[for="' + el.id + '"]');
                if (label) labelText = label.textContent.trim();
            }
            if (!labelText) {
                const parent = el.closest('label');
                if (parent) labelText = parent.textContent.trim();
            }

            // Build unique selector for this field
            let selector = '';
            if (el.id) selector = '#' + CSS.escape(el.id);
            else if (el.name) selector = `form:nth-of-type(${fi+1}) [name="${CSS.escape(el.name)}"]`;
            else selector = `form:nth-of-type(${fi+1}) ${el.tagName.toLowerCase()}:nth-of-type(${Array.from(form.querySelectorAll(el.tagName)).indexOf(el)+1})`;

            fields.push({
                tag: el.tagName.toLowerCase(),
                type: el.type || '',
                name: el.name || '',
                id: el.id || '',
                placeholder: el.placeholder || '',
                label: labelText.slice(0, 100),
                required: el.required,
                hidden: isHidden,
                selector: selector,
            });
        });

        // Find submit button
        let submitSelector = '';
        const submitBtn = form.querySelector('button[type="submit"], input[type="submit"]')
            || form.querySelector('button:not([type="button"]):not([type="reset"])')
            || form.querySelector('[role="button"]');
        if (submitBtn) {
            if (submitBtn.id) submitSelector = '#' + CSS.escape(submitBtn.id);
            else submitSelector = `form:nth-of-type(${fi+1}) button`;
        }

        if (fields.length > 0) {
            results.forms.push({
                index: fi,
                fields: fields,
                submit_selector: submitSelector,
                action: form.action || '',
                method: form.method || 'GET',
            });
        }
    });

    // Write results to a hidden div for Python to read
    let div = document.getElementById('__outreach_detect');
    if (!div) { div = document.createElement('div'); div.id = '__outreach_detect'; div.style.display='none'; document.body.appendChild(div); }
    div.textContent = JSON.stringify(results);
})();
"""


def _build_fill_js(field_map: dict[str, tuple[str, str]], submit_selector: str) -> str:
    """Build JavaScript to fill form fields and submit.

    field_map: { selector: (value, tag) }
    """
    steps = []
    delay = 0
    for selector, (value, tag) in field_map.items():
        escaped_val = json.dumps(value)
        delay += random.randint(150, 300)
        if tag == "textarea":
            steps.append(f"""
setTimeout(() => {{
    const el = document.querySelector({json.dumps(selector)});
    if (el) {{
        const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set;
        setter.call(el, {escaped_val});
        el.dispatchEvent(new Event('input', {{bubbles: true}}));
        el.dispatchEvent(new Event('change', {{bubbles: true}}));
    }}
}}, {delay});""")
        else:
            steps.append(f"""
setTimeout(() => {{
    const el = document.querySelector({json.dumps(selector)});
    if (el) {{
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
        setter.call(el, {escaped_val});
        el.dispatchEvent(new Event('input', {{bubbles: true}}));
        el.dispatchEvent(new Event('change', {{bubbles: true}}));
    }}
}}, {delay});""")

    # Submit after all fields filled
    delay += 500
    if submit_selector:
        steps.append(f"""
setTimeout(() => {{
    const btn = document.querySelector({json.dumps(submit_selector)});
    if (btn) btn.click();
    else {{
        const form = document.querySelector('form');
        if (form) form.submit();
    }}
}}, {delay});""")

    # Check for confirmation after submit
    delay += 3000
    steps.append(f"""
setTimeout(() => {{
    const body = document.body.innerText.toLowerCase();
    const confirmed = (
        body.includes('thank you') || body.includes('thanks') ||
        body.includes('message sent') || body.includes('message received') ||
        body.includes("we'll get back") || body.includes('successfully') ||
        body.includes('submitted') || body.includes('received your')
    );
    let div = document.getElementById('__outreach_result');
    if (!div) {{ div = document.createElement('div'); div.id = '__outreach_result'; div.style.display='none'; document.body.appendChild(div); }}
    div.textContent = JSON.stringify({{confirmed: confirmed, url: window.location.href}});
}}, {delay});""")

    return "\n".join(steps)


# ── Field classification ──────────────────────────────────────────────

_FIELD_PATTERNS = {
    "email": re.compile(r"e[\-_]?mail", re.I),
    "name": re.compile(r"\b(name|full[\s_-]?name|your[\s_-]?name|first[\s_-]?name)\b", re.I),
    "phone": re.compile(r"\b(phone|tel|mobile|cell)\b", re.I),
    "company": re.compile(r"\b(company|organization|business|firm)\b", re.I),
    "subject": re.compile(r"\bsubject\b", re.I),
    "message": re.compile(r"\b(message|comment|inquiry|details|question|how can we help)\b", re.I),
}


def _classify_field(field: dict) -> str | None:
    """Classify a form field into a known type, or None."""
    # Type-based detection (highest priority)
    if field["type"] == "email":
        return "email"
    if field["type"] == "tel":
        return "phone"
    if field["tag"] == "textarea":
        return "message"

    # Check name, placeholder, label against patterns
    text = f"{field['name']} {field['placeholder']} {field['label']}"
    for field_type, pattern in _FIELD_PATTERNS.items():
        if pattern.search(text):
            # Make sure "name" doesn't match "company name"
            if field_type == "name" and _FIELD_PATTERNS["company"].search(text):
                return "company"
            return field_type

    return None


def _pick_best_form(forms: list[dict]) -> dict | None:
    """Pick the most likely contact form from detected forms."""
    best = None
    best_score = -1

    for form in forms:
        visible_fields = [f for f in form["fields"] if not f["hidden"]]
        classified = {}
        for f in visible_fields:
            ftype = _classify_field(f)
            if ftype and ftype not in classified:
                classified[ftype] = f

        # Must have at least email + message to be a contact form
        if "email" not in classified or "message" not in classified:
            continue

        # Score: more recognized fields = better
        score = len(classified)
        if score > best_score:
            best_score = score
            best = {"form": form, "classified": classified}

    return best


def render_template(template: str, lead: dict, sender: dict) -> str:
    """Render a message template with lead and sender variables."""
    try:
        return template.format(
            company_name=lead.get("company", "your company"),
            city=lead.get("city", lead.get("address", "")),
            sender_email=sender.get("email", ""),
            sender_phone=sender.get("phone", ""),
            sender_company=sender.get("company", ""),
        )
    except (KeyError, IndexError):
        # Fall back to raw template if variables are malformed
        return template


# ── Contact page finder ───────────────────────────────────────────────

async def _find_contact_page(
    crawler: AsyncWebCrawler,
    base_url: str,
    rate_limiter: DomainRateLimiter | None,
    timeout: float = 15,
) -> str | None:
    """Find a contact page URL for a given website. Returns URL or None."""
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    # If the URL itself looks like a contact page, use it
    if any(kw in parsed.path.lower() for kw in ["contact", "reach", "get-in-touch"]):
        return base_url

    # Try hardcoded contact paths first
    config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
    )
    for path in CONTACT_PATHS:
        url = base + path
        result, err = await _crawl_with_retry(
            crawler, url, config, max_retries=1, timeout=timeout,
            rate_limiter=rate_limiter,
        )
        if result.success:
            html = str(result.html) if result.html else ""
            if "<form" in html.lower():
                return url

    # Scan homepage for contact links
    result, err = await _crawl_with_retry(
        crawler, base_url, config, max_retries=1, timeout=timeout,
        rate_limiter=rate_limiter,
    )
    if result.success:
        html = str(result.html) if result.html else ""
        # Check if the homepage itself has a contact form
        if "<form" in html.lower():
            return base_url
        # Look for contact page links
        links = _extract_internal_links(html, base_url, max_links=5)
        for link in links:
            link_path = urlparse(link).path.lower()
            if any(kw in link_path for kw in ["contact", "reach", "connect", "message", "get-in-touch", "quote"]):
                return link

    return None


# ── Main outreach function ────────────────────────────────────────────

async def run_outreach(
    leads: list[dict],
    sender: dict,
    message_template: str,
    config: ScrapeConfig,
    on_progress: Callable | None = None,
) -> list[dict]:
    """
    Submit contact forms for leads without emails.

    Returns the leads list with outreach_status and outreach_detail added.
    """
    # Build browser config — need full rendering for forms
    browser_cfg = BrowserConfig(
        headless=True,
        enable_stealth=config.stealth,
        user_agent_mode="random" if config.stealth else "",
        text_mode=False,   # Need full rendering
        light_mode=False,  # Need CSS for form visibility detection
        extra_args=["--disable-gpu", "--disable-dev-shm-usage"],
    )
    if config.proxies:
        browser_cfg.proxy = config.proxies[0]

    rate_limiter = DomainRateLimiter(min_delay=5.0)
    results = []
    total = len(leads)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        for i, lead in enumerate(leads):
            if on_progress:
                on_progress(i, total, f"Outreach {i+1}/{total}: {lead.get('company', lead.get('url', ''))}")

            result = dict(lead)

            # Skip leads that already have emails
            if lead.get("emails"):
                result["outreach_status"] = "skipped"
                result["outreach_detail"] = "Has email — skipped"
                results.append(result)
                continue

            url = lead.get("url", "")
            if not url:
                result["outreach_status"] = "no_form_found"
                result["outreach_detail"] = "No website URL"
                results.append(result)
                continue

            try:
                status, detail, contact_url = await _submit_one(
                    crawler, lead, sender, message_template, rate_limiter,
                )
                result["outreach_status"] = status
                result["outreach_detail"] = detail
                if contact_url:
                    result["contact_page_url"] = contact_url
            except Exception as e:
                log.error(f"Outreach error for {url}: {e}")
                result["outreach_status"] = "error"
                result["outreach_detail"] = str(e)

            results.append(result)

            # Random delay between submissions
            if i < total - 1:
                delay = random.uniform(3, 7)
                await asyncio.sleep(delay)

    if on_progress:
        on_progress(total, total, f"Outreach complete: {total} leads processed")

    return results


async def _submit_one(
    crawler: AsyncWebCrawler,
    lead: dict,
    sender: dict,
    message_template: str,
    rate_limiter: DomainRateLimiter,
) -> tuple[str, str, str | None]:
    """Submit a single contact form. Returns (status, detail, contact_page_url)."""
    url = lead["url"]

    # Phase 1: Find contact page
    contact_url = await _find_contact_page(crawler, url, rate_limiter)
    if not contact_url:
        return "no_form_found", "No contact page found", None

    # Phase 2: Detect forms
    detect_config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        js_code=DETECT_FORMS_JS,
        delay_before_return_html=2.0,
    )

    result, err = await _crawl_with_retry(
        crawler, contact_url, detect_config,
        max_retries=2, timeout=20,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        return "error", f"Failed to load {contact_url}", contact_url

    html = str(result.html) if result.html else ""

    # Parse detection results
    detect_match = re.search(r'id="__outreach_detect"[^>]*>([^<]+)<', html)
    if not detect_match:
        return "no_form_found", "Could not detect forms on page", contact_url

    try:
        detect_data = json.loads(detect_match.group(1))
    except json.JSONDecodeError:
        return "no_form_found", "Form detection failed (invalid JSON)", contact_url

    if detect_data.get("has_captcha"):
        return "captcha_blocked", "CAPTCHA detected — skipped", contact_url

    forms = detect_data.get("forms", [])
    if not forms:
        return "no_form_found", "No forms found on contact page", contact_url

    # Pick best form
    best = _pick_best_form(forms)
    if not best:
        return "no_form_found", "No suitable contact form (needs email + message fields)", contact_url

    classified = best["classified"]
    form_data = best["form"]

    # Phase 3: Fill and submit
    message = render_template(message_template, lead, sender)

    field_map = {}
    for ftype, field in classified.items():
        if field["hidden"]:
            continue  # Skip honeypots
        selector = field["selector"]
        tag = field["tag"]
        if ftype == "email":
            field_map[selector] = (sender.get("email", ""), tag)
        elif ftype == "name":
            field_map[selector] = (sender.get("company", ""), tag)
        elif ftype == "phone":
            field_map[selector] = (sender.get("phone", ""), tag)
        elif ftype == "company":
            field_map[selector] = (sender.get("company", ""), tag)
        elif ftype == "subject":
            subject = f"AI Automation Inquiry — {sender.get('company', 'AxiomFlow')}"
            field_map[selector] = (subject, tag)
        elif ftype == "message":
            field_map[selector] = (message, tag)

    fill_js = _build_fill_js(field_map, form_data.get("submit_selector", ""))

    submit_config = CrawlerRunConfig(
        word_count_threshold=0,
        remove_overlay_elements=True,
        js_code=fill_js,
        delay_before_return_html=5.0,  # Wait for fill + submit + confirmation
    )

    result, err = await _crawl_with_retry(
        crawler, contact_url, submit_config,
        max_retries=1, timeout=30,
        rate_limiter=rate_limiter,
    )

    if not result.success:
        return "failed", f"Form submission failed: {err}", contact_url

    # Check for confirmation
    html = str(result.html) if result.html else ""
    result_match = re.search(r'id="__outreach_result"[^>]*>([^<]+)<', html)
    if result_match:
        try:
            submit_data = json.loads(result_match.group(1))
            if submit_data.get("confirmed"):
                return "submitted", f"Form submitted on {urlparse(contact_url).path}", contact_url
        except json.JSONDecodeError:
            pass

    # If no confirmation detected, assume it went through (some sites don't show confirmation)
    return "submitted", f"Form filled and submitted on {urlparse(contact_url).path} (no confirmation detected)", contact_url
