"""
Dynamic signal column configuration.
Starts with 6 default columns matching the mockup.
Columns can be added/edited/deleted via API — stored in memory.
"""

import uuid
from typing import Dict, Any, List, Optional

# Default columns — match the mockup
_DEFAULT_COLUMNS: List[Dict[str, Any]] = [
    {
        "key": "osha",
        "label": "OSHA Enforcement",
        "source_type": "api",
        "on": True,
        "segment": ["commercial", "enterprise", "gov", "healthcare"],
        "prompt": (
            "Given OSHA inspection records for {account_name}, score 1-10 as a buying signal "
            "for EHS/safety compliance software. High score = recent inspection with violations "
            "or penalties suggesting the org needs better incident/compliance tracking. "
            "Cite the penalty amount and inspection date. "
            'Respond in JSON only: {{"score":int,"summary":str,"action":str,"excerpt":str}}'
        ),
        "threshold": 5,
        "cadence": "Weekly",
        "budget": 0,
        "has_prompt": True,
        "builtin": True,
        "sources": [{"type": "api_osha", "label": "OSHA DOL API"}],
    },
    {
        "key": "cms",
        "label": "CMS Deficiencies",
        "source_type": "api",
        "on": True,
        "segment": ["healthcare", "enterprise"],
        "prompt": (
            "Given CMS health deficiency citations for {account_name}, score 1-10 as a buying "
            "signal for quality/compliance management software. High score = serious deficiencies "
            "(severity G+) or financial penalties indicating gaps in documentation, audit trails, "
            "or corrective action tracking. Note the deficiency tag and severity code. "
            'Respond in JSON only: {{"score":int,"summary":str,"action":str,"excerpt":str}}'
        ),
        "threshold": 5,
        "cadence": "Weekly",
        "budget": 0,
        "has_prompt": True,
        "builtin": True,
        "sources": [{"type": "api_cms", "label": "CMS Health Deficiencies API"}],
    },
    {
        "key": "rfp",
        "label": "RFP / Tender",
        "source_type": "websearch",
        "on": True,
        "segment": ["commercial", "enterprise", "gov", "healthcare"],
        "prompt": (
            "Given web search results about {account_name}, score 1-10: does this indicate an active "
            "RFP, RFI, or procurement process for safety, compliance, quality, or EHS software? "
            "Exclude generic IT/facilities RFPs — require explicit safety, compliance, or quality scope. "
            "Cite the notice ID or source. "
            'Respond in JSON only: {{"score":int,"summary":str,"action":str,"excerpt":str}}'
        ),
        "threshold": 7,
        "cadence": "Daily",
        "budget": 8,
        "has_prompt": True,
        "builtin": False,
        "sources": [
            {"type": "site_search", "label": "SAM.gov", "target": "sam.gov"},
            {"type": "web_general", "label": "General Web", "query_hint": "RFP RFI tender procurement safety compliance EHS software"},
            {"type": "news",        "label": "News"},
        ],
    },
    {
        "key": "budget",
        "label": "Budget Signal",
        "source_type": "websearch",
        "on": True,
        "segment": ["commercial", "enterprise", "gov", "healthcare"],
        "prompt": (
            "Given web search results about {account_name}, score 1-10: does this indicate a budget "
            "line item, capital approval, or technology investment for compliance, safety, quality, "
            "or EHS modernization? Prioritize items with a dollar figure and explicit compliance scope. "
            'Respond in JSON only: {{"score":int,"summary":str,"action":str,"excerpt":str}}'
        ),
        "threshold": 6,
        "cadence": "Monthly",
        "budget": 6,
        "has_prompt": True,
        "builtin": False,
        "sources": [
            {"type": "web_general", "label": "General Web", "query_hint": "technology budget investment compliance EHS modernization capital approval"},
            {"type": "site_search", "label": "SEC EDGAR", "target": "sec.gov", "query_hint": "annual report safety compliance budget"},
            {"type": "news",        "label": "News"},
        ],
    },
    {
        "key": "hiring",
        "label": "Hiring",
        "source_type": "websearch",
        "on": True,
        "segment": ["commercial", "enterprise", "gov", "healthcare"],
        "prompt": (
            "Given web search results about {account_name}, score 1-10: does this indicate new hiring "
            "in safety, EHS, compliance, or quality management leadership? Weight director/manager-level "
            "titles higher. Treat 'modernization' or 'system implementation' language as stronger signals. "
            'Respond in JSON only: {{"score":int,"summary":str,"action":str,"excerpt":str}}'
        ),
        "threshold": 6,
        "cadence": "Every 3 days",
        "budget": 5,
        "has_prompt": True,
        "builtin": False,
        "sources": [
            {"type": "site_search", "label": "LinkedIn", "target": "linkedin.com", "query_hint": "EHS safety compliance quality manager director hiring"},
            {"type": "web_general", "label": "General Web", "query_hint": "hiring EHS director safety manager quality compliance officer"},
            {"type": "news",        "label": "News"},
        ],
    },
    {
        "key": "accreditation",
        "label": "Accreditation Cycle",
        "source_type": "websearch",
        "on": True,
        "segment": ["healthcare"],
        "prompt": (
            "Given web search results about {account_name}, score 1-10: does this indicate an upcoming "
            "accreditation survey (Joint Commission, DNV, CMS, CARF) creating urgency for audit-ready "
            "documentation systems? Only count surveys within the next 12 months as high urgency. "
            "Name the accrediting body. "
            'Respond in JSON only: {{"score":int,"summary":str,"action":str,"excerpt":str}}'
        ),
        "threshold": 6,
        "cadence": "Monthly",
        "budget": 4,
        "has_prompt": True,
        "builtin": False,
        "sources": [
            {"type": "web_general", "label": "General Web", "query_hint": "Joint Commission DNV CMS accreditation survey audit certification"},
            {"type": "news",        "label": "News"},
        ],
    },
]

# Live store — keyed by column key
_columns: Dict[str, Dict[str, Any]] = {c["key"]: c for c in _DEFAULT_COLUMNS}
# Preserve insertion order
_column_order: List[str] = [c["key"] for c in _DEFAULT_COLUMNS]


def get_all_columns() -> List[Dict[str, Any]]:
    return [_columns[k] for k in _column_order if k in _columns]


def get_active_columns() -> List[Dict[str, Any]]:
    return [c for c in get_all_columns() if c.get("on", True)]


def get_column(key: str) -> Optional[Dict[str, Any]]:
    return _columns.get(key)


def add_column(
    label: str,
    prompt: str,
    segment: List[str],
    threshold: int = 6,
    cadence: str = "Weekly",
    source_type: str = "websearch",
    sources: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    key = "col_" + str(uuid.uuid4())[:8]
    col = {
        "key": key,
        "label": label,
        "source_type": source_type,
        "on": True,
        "segment": segment,
        "prompt": prompt,
        "threshold": threshold,
        "cadence": cadence,
        "budget": 5,
        "has_prompt": True,
        "builtin": False,
        "sources": sources or [{"type": "web_general", "label": "General Web"}],
    }
    _columns[key] = col
    _column_order.append(key)
    return col


def update_column(key: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if key not in _columns:
        return None
    allowed = {"label", "prompt", "on", "threshold", "cadence", "segment", "source_type", "sources"}
    for field, value in updates.items():
        if field in allowed:
            _columns[key][field] = value
    return _columns[key]


def delete_column(key: str) -> bool:
    col = _columns.get(key)
    if not col or col.get("builtin"):
        return False
    del _columns[key]
    if key in _column_order:
        _column_order.remove(key)
    return True


def seed_from_db(cols: List[Dict[str, Any]]) -> None:
    """Replace in-memory column store with data loaded from Supabase."""
    global _columns, _column_order
    _columns = {c["key"]: c for c in cols}
    _column_order = [c["key"] for c in cols]
