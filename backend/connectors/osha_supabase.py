"""
Supabase-backed OSHA connector.
Queries the `osha_inspections` table instead of loading CSVs into RAM.
Benefits: fast indexed queries, no server memory overhead, incremental updates.

Data source priority in main.py:
  1. Supabase osha_inspections (this module) — if table has data
  2. In-memory CSV (osha_csv.py) — if CSVs are loaded
  3. DOL API fallback (osha.py) — last 90 days only
"""

from typing import List, Dict, Any, Optional
import db

# Cache availability check — set to None to force re-check
_available: Optional[bool] = None


def is_available() -> bool:
    """
    True if osha_inspections table has at least one record.
    Result is cached after first check; call invalidate_cache() after bulk loads.
    """
    global _available
    if _available is None:
        _available = db.get_osha_count() > 0
        print(f"[osha_supabase] Table available: {_available} ({db.get_osha_count():,} records)")
    return _available


def invalidate_cache():
    """Force re-check of table availability on next call to is_available()."""
    global _available
    _available = None


def search_osha_supabase(
    search_terms: List[str],
    days_back: int = 1825,
    limit_per_term: int = 50,
) -> List[Dict[str, Any]]:
    """Search osha_inspections by establishment name and date range."""
    return db.search_osha_supabase(search_terms, days_back, limit_per_term)


def format_supabase_osha_for_scoring(inspections: List[Dict[str, Any]]) -> str:
    """Format OSHA records for Claude scoring prompt."""
    if not inspections:
        return "No OSHA inspections found in the specified date range."

    lines = [f"OSHA Inspection Records — {len(inspections)} result(s) (Supabase):\n"]
    for i, rec in enumerate(inspections[:10], 1):
        lines.append(
            f"{i}. Establishment: {rec.get('estab_name', 'Unknown')}\n"
            f"   Location: {rec.get('site_city', '')}, {rec.get('site_state', '')}\n"
            f"   Opened: {rec.get('open_date', 'unknown')}\n"
            f"   Closed: {rec.get('close_case_date', '') or 'still open'}\n"
            f"   Inspection type: {rec.get('insp_type', 'unknown')}\n"
            f"   Employees at site: {rec.get('nr_in_estab', 'unknown')}\n"
            f"   NAICS: {rec.get('naics_code', '')}\n"
        )
    return "\n".join(lines)
