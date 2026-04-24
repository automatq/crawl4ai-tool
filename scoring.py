#!/usr/bin/env python3
"""
Lead scoring.

Assign a 0–10 score to each lead so callers can sort/filter the CSV
and prioritize outreach. Higher = better target.

Scoring rubric:
  +3  has at least one valid email
  +2  has phone number
  +1  has physical address
  +1  has at least one social profile
  +2  underserved (0-50 Google reviews) — likely small business
  +1  mid-size (51-200 Google reviews)
  -1  enterprise (>1000 reviews) — hard to reach decision-maker
  -2  explicit error / failed scrape

The tier label groups the numeric score for quick filtering in the UI.
"""


def score_lead(lead: dict) -> tuple[int, str]:
    """Return (score, tier) for a lead dict.

    Tiers: 'hot' (7+), 'warm' (4-6), 'cold' (1-3), 'dead' (≤0).
    """
    if "error" in lead:
        return -2, "dead"

    score = 0

    emails = lead.get("emails") or []
    if emails:
        # Prefer personal-looking emails over generic info@/contact@
        has_personal = any(
            "." in e.split("@", 1)[0] or
            not e.split("@", 1)[0].lower() in {
                "info", "contact", "hello", "admin", "office",
                "support", "sales", "enquiries", "inquiries",
            }
            for e in emails
        )
        score += 3 if has_personal else 2

    if lead.get("phones"):
        score += 2

    if lead.get("address"):
        score += 1

    socials = lead.get("socials") or {}
    if socials:
        score += 1

    reviews = lead.get("google_reviews")
    if reviews is not None:
        if reviews <= 50:
            score += 2
        elif reviews <= 200:
            score += 1
        elif reviews > 1000:
            score -= 1

    # Rating floor — avoid very-low-rated businesses
    rating = lead.get("google_rating")
    if rating is not None and rating < 3.0:
        score -= 1

    tier = _tier_for(score)
    return score, tier


def _tier_for(score: int) -> str:
    if score >= 7:
        return "hot"
    if score >= 4:
        return "warm"
    if score >= 1:
        return "cold"
    return "dead"


def annotate(leads: list[dict]) -> list[dict]:
    """Add 'score' and 'tier' fields to each lead in-place; return list."""
    for lead in leads:
        s, t = score_lead(lead)
        lead["score"] = s
        lead["tier"] = t
    return leads


def sort_by_score(leads: list[dict]) -> list[dict]:
    """Return a new list sorted best-first by score, then by review count."""
    return sorted(
        leads,
        key=lambda l: (
            l.get("score", 0),
            l.get("google_reviews") or 0,
        ),
        reverse=True,
    )
