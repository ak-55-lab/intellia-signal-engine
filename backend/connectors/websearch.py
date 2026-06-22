"""
Web search connector — multi-source, per-column configurable.

Search provider priority:
  1. Brave Search API  — if BRAVE_API_KEY is set (works from cloud IPs)
  2. DuckDuckGo        — fallback for local dev (blocked by many cloud providers)

Each column carries a `sources` list. Each source has:
  type        : web_general | site_search | domain_list | news | api_osha | api_cms
  label       : display name shown in UI and signal attribution
  target      : domain(s) for site_search / domain_list (comma-separated)
  query_hint  : overrides the default keyword strategy for this source

api_osha / api_cms sources are flagged here but fetched in main.py.
Returns (combined_text, sources_checked, queries_trace).
"""

import asyncio
import httpx
from typing import List, Dict, Optional, Tuple, Any
import config

SIGNAL_SEARCH_STRATEGIES = {
    "rfp":           "RFP RFI tender procurement safety compliance EHS quality management software",
    "budget":        "technology budget investment compliance EHS modernization capital approval",
    "hiring":        "hiring EHS director safety manager quality compliance officer",
    "accreditation": "Joint Commission DNV CMS accreditation survey audit certification",
}

SOURCE_PRESETS = [
    {"type": "web_general",  "label": "General Web"},
    {"type": "news",         "label": "News"},
    {"type": "site_search",  "label": "LinkedIn",      "target": "linkedin.com"},
    {"type": "site_search",  "label": "SAM.gov",       "target": "sam.gov"},
    {"type": "domain_list",  "label": "G2 / Capterra", "target": "g2.com,capterra.com,trustradius.com"},
    {"type": "site_search",  "label": "SEC EDGAR",     "target": "sec.gov"},
    {"type": "api_osha",     "label": "OSHA API"},
    {"type": "api_cms",      "label": "CMS API"},
]


def _build_query(account_name: str, column_key: str, source: Dict) -> str:
    stype  = source.get("type", "web_general")
    hint   = source.get("query_hint") or SIGNAL_SEARCH_STRATEGIES.get(column_key, "")
    target = source.get("target", "")

    if stype == "site_search":
        base = f"site:{target} {account_name}"
        return f"{base} {hint}".strip() if hint else base

    if stype == "domain_list":
        domains = " OR ".join(f"site:{d.strip()}" for d in target.split(",") if d.strip())
        return f"({domains}) {account_name} {hint}".strip()

    if stype == "news":
        keyword = hint or "safety compliance EHS"
        return f"{account_name} {keyword} news 2024 2025"

    return f"{account_name} {hint}".strip() if hint else account_name


# ── Brave Search ──────────────────────────────────────────────────────────────

async def _brave_search(query: str, max_results: int = 5) -> List[Dict[str, str]]:
    api_key = config.get("BRAVE_API_KEY")
    if not api_key:
        return []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": max_results, "text_decorations": False},
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "X-Subscription-Token": api_key,
                },
            )
            r.raise_for_status()
            data = r.json()
            results = []
            for item in data.get("web", {}).get("results", [])[:max_results]:
                results.append({
                    "title":   item.get("title", ""),
                    "snippet": item.get("description", ""),
                    "url":     item.get("url", ""),
                })
            print(f"[brave] {len(results)} results for '{query[:70]}'")
            return results
    except Exception as e:
        print(f"[brave] failed: {e}")
        return []


# ── DuckDuckGo (fallback) ─────────────────────────────────────────────────────

def _ddg_search(query: str, max_results: int = 5) -> List[Dict[str, str]]:
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                results.append({
                    "title":   r.get("title", ""),
                    "snippet": r.get("body", ""),
                    "url":     r.get("href", ""),
                })
        print(f"[ddg] {len(results)} results for '{query[:70]}'")
        return results
    except Exception as e:
        print(f"[ddg] failed: {e}")
        return []


async def _search_async(query: str, max_results: int = 5) -> List[Dict[str, str]]:
    # Try Brave first (works from cloud); fall back to DDG (local dev)
    if config.get("BRAVE_API_KEY"):
        results = await _brave_search(query, max_results)
        if results:
            return results
        print(f"[websearch] Brave returned 0 — falling back to DDG")

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ddg_search, query, max_results)


# ── Formatting ────────────────────────────────────────────────────────────────

def _format_section(results: List[Dict], label: str, query: str) -> str:
    header = f"=== Source: {label} (query: {query}) ==="
    if not results:
        return f"{header}\nNo results found."
    lines = [header]
    for i, r in enumerate(results, 1):
        lines.append(
            f"{i}. {r['title']}\n"
            f"   {r['snippet']}\n"
            f"   URL: {r['url']}"
        )
    return "\n".join(lines)


# ── Main entry point ──────────────────────────────────────────────────────────

async def fetch_signal_for_column(
    account_name: str,
    column_key: str,
    sources=None,
    custom_query=None,
):
    """
    Returns (combined_text, sources_checked_labels, queries_trace).
    """
    if not sources:
        sources = [{"type": "web_general", "label": "General Web"}]

    web_sources  = [s for s in sources if s.get("type") not in ("api_osha", "api_cms")]
    api_sources  = [s for s in sources if s.get("type") in ("api_osha", "api_cms")]

    sections        = []
    sources_checked = []
    queries_trace   = []

    for s in api_sources:
        sources_checked.append(s.get("label", s.get("type")))

    async def _fetch_one(source):
        q      = f"{account_name} {custom_query}" if custom_query else _build_query(account_name, column_key, source)
        label  = source.get("label") or source.get("type", "Web")
        results = await _search_async(q, max_results=5)

        queries_trace.append({
            "label":        label,
            "type":         source.get("type", "web_general"),
            "query":        q,
            "result_count": len(results),
            "results":      results,
        })
        if results:
            sources_checked.append(label)
        return _format_section(results, label, q)

    if web_sources:
        fetched = await asyncio.gather(*[_fetch_one(s) for s in web_sources])
        sections.extend(fetched)

    combined = "\n\n".join(sections) if sections else "No web results found."
    return combined, sources_checked, queries_trace
