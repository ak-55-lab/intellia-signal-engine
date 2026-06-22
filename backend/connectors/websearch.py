"""
Web search connector — multi-source, per-column configurable.

Each column carries a `sources` list. Each source has:
  type        : web_general | site_search | domain_list | news | api_osha | api_cms
  label       : display name shown in UI and signal attribution
  target      : domain(s) for site_search / domain_list (comma-separated)
  query_hint  : overrides the default keyword strategy for this source

api_osha / api_cms sources are flagged here but fetched in main.py.
Returns (combined_text, sources_checked, queries_trace).
"""

import asyncio
from typing import List, Dict, Optional, Tuple, Any

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
        print(f"[websearch] {len(results)} results for '{query[:70]}'")
        return results
    except Exception as e:
        print(f"[websearch] DDG failed: {e}")
        return []


async def _search_async(query: str, max_results: int = 5) -> List[Dict[str, str]]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ddg_search, query, max_results)


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


async def fetch_signal_for_column(
    account_name: str,
    column_key: str,
    sources: Optional[List[Dict]] = None,
    custom_query: Optional[str] = None,
) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    """
    Returns (combined_text, sources_checked_labels, queries_trace).

    queries_trace is a list of dicts, one per web source:
      { label, type, query, result_count, results: [{title, snippet, url}] }

    api_osha / api_cms entries are noted in sources_checked but not searched here.
    """
    if not sources:
        sources = [{"type": "web_general", "label": "General Web"}]

    web_sources = [s for s in sources if s.get("type") not in ("api_osha", "api_cms")]
    api_sources = [s for s in sources if s.get("type") in ("api_osha", "api_cms")]

    sections:       List[str] = []
    sources_checked: List[str] = []
    queries_trace:   List[Dict[str, Any]] = []

    # Note API sources (fetched in main.py)
    for s in api_sources:
        sources_checked.append(s.get("label", s.get("type")))

    async def _fetch_one(source: Dict) -> Optional[str]:
        if custom_query:
            q = f"{account_name} {custom_query}"
        else:
            q = _build_query(account_name, column_key, source)
        label   = source.get("label") or source.get("type", "Web")
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
