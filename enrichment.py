#!/usr/bin/env python3
"""
Lead enrichment: email MX validation.

Check MX records so dead emails don't land in your outreach list.
Results are cached per-domain in SQLite (see cache.py) with a 7-day TTL.

Enrichment failures are silent — they just leave the lead unchanged.
"""

import asyncio

from cache import get_mx_valid, put_mx_valid


try:
    import dns.resolver  # type: ignore
    _DNS_AVAILABLE = True
    _resolver = dns.resolver.Resolver()
    _resolver.lifetime = 3.0
    _resolver.timeout = 3.0
except ImportError:
    _DNS_AVAILABLE = False
    _resolver = None


def _domain_of_email(email: str) -> str | None:
    if "@" not in email:
        return None
    return email.rsplit("@", 1)[1].lower().strip()


def _check_mx_sync(domain: str) -> bool:
    """Synchronous MX lookup — returns True if domain has mail servers."""
    if not _DNS_AVAILABLE or not _resolver:
        return True  # No DNS lib — fall back to accepting everything
    try:
        answers = _resolver.resolve(domain, "MX")
        return len(answers) > 0
    except Exception:
        # Fall back to A record — RFC 5321 implies a mail server if A exists
        try:
            answers = _resolver.resolve(domain, "A")
            return len(answers) > 0
        except Exception:
            return False


async def validate_emails(emails: list[str]) -> list[str]:
    """Return only emails whose domains have valid MX records.

    Deduplicates MX checks per-domain; results cached to SQLite.
    """
    if not emails or not _DNS_AVAILABLE:
        return list(emails)

    # Group by domain
    by_domain: dict[str, list[str]] = {}
    for e in emails:
        d = _domain_of_email(e)
        if d:
            by_domain.setdefault(d, []).append(e)

    # Check each unique domain (cache → DNS)
    domain_validity: dict[str, bool] = {}
    to_check: list[str] = []
    for d in by_domain:
        cached = get_mx_valid(d)
        if cached is not None:
            domain_validity[d] = cached
        else:
            to_check.append(d)

    if to_check:
        loop = asyncio.get_event_loop()
        results = await asyncio.gather(
            *(loop.run_in_executor(None, _check_mx_sync, d) for d in to_check),
            return_exceptions=True,
        )
        for d, ok in zip(to_check, results):
            valid = bool(ok) if not isinstance(ok, Exception) else False
            domain_validity[d] = valid
            put_mx_valid(d, valid)

    return [e for e in emails if domain_validity.get(_domain_of_email(e) or "", True)]


async def enrich_lead(lead: dict) -> dict:
    """Apply email MX validation to a lead. Mutates and returns it."""
    if "error" in lead:
        return lead

    emails = lead.get("emails") or []
    if emails:
        validated = await validate_emails(emails)
        lead["emails"] = validated
        if len(validated) < len(emails):
            lead["invalid_emails"] = [e for e in emails if e not in validated]

    return lead
