"""
Knowledge Base manager.
Stores .md uploads and URL-fetched content in memory (keyed by UUID).
KB text is injected into Claude's system prompt at score time so scoring
is grounded in Cority's product context, ICP, and positioning.
"""

import os
import uuid
import httpx
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

# In-memory KB store: { id: { title, content, type, url, added_at } }
_kb_store: Dict[str, Dict[str, Any]] = {}

# Seed with Cority ICP context so it works before any uploads
_CORITY_SEED_ID = "cority-icp-seed"
_kb_store[_CORITY_SEED_ID] = {
    "id": _CORITY_SEED_ID,
    "title": "Cority ICP & Product Context",
    "type": "seed",
    "url": None,
    "content": """
Cority is an enterprise EHS (Environment, Health & Safety), quality management, and ESG software company.
It serves large, regulated organizations — primarily in healthcare, manufacturing, energy, chemicals, and government.

Cority's core product capabilities:
- Incident management and near-miss tracking
- OSHA recordkeeping and regulatory compliance (OSHA 300/300A logs)
- Corrective and Preventive Action (CAPA) workflows
- Audit management and inspection scheduling
- Document control and policy management
- Environmental monitoring and permit tracking
- Occupational health and exposure monitoring
- ESG reporting and carbon accounting
- Quality management (ISO 9001, 14001, 45001)

Ideal Customer Profile (ICP) in healthcare:
- Large hospital systems (10+ facilities) or multi-site health networks
- Organizations under active CMS scrutiny, OSHA enforcement, or accreditation pressure
- Facilities with recurring deficiency citations (especially F-tags severity G and above)
- Health systems with recent OSHA inspections involving willful/repeat violations or penalties > $10K
- Organizations investing in safety/quality technology modernization
- Upcoming Joint Commission, DNV, or CMS accreditation surveys

Buying triggers (strongest signals to act on):
1. CMS enforcement fine or serious deficiency citation → immediate compliance gap to address
2. OSHA repeat violation or penalty > $5K → regulatory pressure creating urgency
3. RFP or sources-sought for quality/safety/compliance software → active procurement cycle
4. New hire in EHS, quality, or compliance leadership → new stakeholder evaluating tools
5. Budget line item for compliance or technology modernization → approved funding
6. Upcoming accreditation survey (Joint Commission, DNV, CARF) → audit readiness urgency

Competitors: Intelex, VelocityEHS, Enablon (Wolters Kluwer), Benchmark Gensuite, Sphera.
""",
    "added_at": datetime.now(timezone.utc).isoformat(),
    "char_count": 0,
}
_kb_store[_CORITY_SEED_ID]["char_count"] = len(_kb_store[_CORITY_SEED_ID]["content"])


# ─── Public API ──────────────────────────────────────────────────────────────

def add_markdown(title: str, content: str) -> Dict[str, Any]:
    """Store an uploaded .md file."""
    entry_id = str(uuid.uuid4())
    entry = {
        "id": entry_id,
        "title": title,
        "type": "upload",
        "url": None,
        "content": content,
        "char_count": len(content),
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    _kb_store[entry_id] = entry
    return entry


async def fetch_url(url: str, title: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch a URL, extract readable text, and store as a KB entry.
    Strips HTML tags and collapses whitespace.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        html = resp.text

    # Strip HTML tags
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    # Truncate to 8000 chars to keep token cost reasonable
    if len(text) > 8000:
        text = text[:8000] + "… [truncated]"

    entry_id = str(uuid.uuid4())
    entry = {
        "id": entry_id,
        "title": title or url,
        "type": "url",
        "url": url,
        "content": text,
        "char_count": len(text),
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    _kb_store[entry_id] = entry
    return entry


def remove_entry(entry_id: str) -> bool:
    """Remove a KB entry. Cannot remove the seed entry."""
    if entry_id == _CORITY_SEED_ID:
        return False
    if entry_id in _kb_store:
        del _kb_store[entry_id]
        return True
    return False


def list_entries() -> List[Dict[str, Any]]:
    """Return all KB entries (without full content — use get_context() for that)."""
    return [
        {k: v for k, v in entry.items() if k != "content"}
        for entry in _kb_store.values()
    ]


def get_context(max_chars: int = 12000) -> str:
    """
    Return combined KB text for injection into Claude system prompts.
    Seed entry always included first. Others appended up to max_chars.
    """
    parts = []
    total = 0

    # Seed first
    seed = _kb_store.get(_CORITY_SEED_ID)
    if seed:
        parts.append(f"=== {seed['title']} ===\n{seed['content']}")
        total += seed["char_count"]

    # Then uploads/URLs
    for entry in _kb_store.values():
        if entry["id"] == _CORITY_SEED_ID:
            continue
        chunk = f"=== {entry['title']} ===\n{entry['content']}"
        if total + len(chunk) > max_chars:
            break
        parts.append(chunk)
        total += len(chunk)

    return "\n\n".join(parts)


def get_entry(entry_id: str) -> Optional[Dict[str, Any]]:
    return _kb_store.get(entry_id)
