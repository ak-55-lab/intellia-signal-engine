"""
CMS Health Deficiencies connector — data.cms.gov Socrata API.
Dataset: Nursing Home Health Deficiencies (r5ix-sfxw)
Column name is discovered at runtime since CMS renames fields periodically.
"""

import httpx
import asyncio
from typing import List, Dict, Any, Optional

CMS_BASE = "https://data.cms.gov/provider-data/api/1/datastore/query"
DEFICIENCIES_RESOURCE = "r5ix-sfxw"
PROVIDER_RESOURCE = "4pq5-n9py"

# Cache the discovered name column so we only probe once per process
_name_col_cache: Optional[str] = None


async def _discover_name_column(resource: str) -> str:
    """Fetch one row and return whichever column looks like a provider/facility name."""
    global _name_col_cache
    if _name_col_cache:
        return _name_col_cache

    candidates = ["provname", "provider_name", "facility_name", "name",
                  "PROVNAME", "PROVIDER_NAME", "FACILITY_NAME"]
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{CMS_BASE}/{resource}/0",
                json={"limit": 1, "offset": 0},
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            rows = resp.json().get("results", [])
            if rows:
                cols = list(rows[0].keys())
                print(f"[cms] discovered columns: {cols[:10]}")
                for c in candidates:
                    if c in cols:
                        print(f"[cms] using name column: {c}")
                        _name_col_cache = c
                        return c
                # Fallback: pick column whose name contains 'name'
                name_cols = [c for c in cols if "name" in c.lower()]
                if name_cols:
                    _name_col_cache = name_cols[0]
                    print(f"[cms] fallback name column: {_name_col_cache}")
                    return _name_col_cache
    except Exception as e:
        print(f"[cms] schema probe failed: {e}")

    _name_col_cache = "provname"  # best guess
    return _name_col_cache


async def fetch_cms_deficiencies(
    search_terms: List[str],
    limit: int = 10,
) -> List[Dict[str, Any]]:
    name_col = await _discover_name_column(DEFICIENCIES_RESOURCE)
    seen = set()
    results = []

    async with httpx.AsyncClient(timeout=30.0) as client:
        for term in search_terms:
            try:
                payload = {
                    "conditions": [
                        {
                            "property": name_col,
                            "value": f"%{term}%",   # LIKE wildcard
                            "operator": "LIKE",
                        }
                    ],
                    "limit": limit,
                    "offset": 0,
                    "sort": [{"property": "survey_date", "order": "desc"}],
                }
                resp = await client.post(
                    f"{CMS_BASE}/{DEFICIENCIES_RESOURCE}/0",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                print(f"[cms] '{term}': HTTP {resp.status_code}")
                resp.raise_for_status()
                data = resp.json()
                rows = data.get("results", [])
                print(f"[cms] '{term}': {len(rows)} rows")

                for row in rows:
                    key = (row.get("provnum"), row.get("tag"), row.get("survey_date"))
                    if key not in seen:
                        seen.add(key)
                        results.append(row)

            except httpx.HTTPStatusError as e:
                print(f"[cms] HTTP {e.response.status_code} for '{term}': {e.response.text[:300]}")
            except Exception as e:
                print(f"[cms] Error for '{term}': {e}")

            await asyncio.sleep(0.2)

    results.sort(key=lambda r: r.get("survey_date", ""), reverse=True)
    return results


def format_cms_for_scoring(deficiencies: List[Dict[str, Any]]) -> str:
    if not deficiencies:
        return "No recent CMS health deficiencies found."

    lines = [f"CMS Health Deficiency Records — {len(deficiencies)} citation(s):\n"]
    for i, rec in enumerate(deficiencies[:5], 1):
        # Handle both old and new column names
        facility = (rec.get("provname") or rec.get("provider_name")
                    or rec.get("facility_name") or rec.get("name") or "Unknown")
        fine = rec.get("fine_amount")
        try:
            fine_str = f"${float(fine):,.0f}" if fine else "no fine"
        except Exception:
            fine_str = str(fine) if fine else "no fine"
        city = rec.get("citytown") or rec.get("city") or ""
        tag = rec.get("deficiency_tag_number") or rec.get("tag") or "unknown"
        tag_desc = rec.get("deficiency_description") or rec.get("tag_desc") or ""
        lines.append(
            f"{i}. Facility: {facility}\n"
            f"   Location: {city}, {rec.get('state', '')}\n"
            f"   Survey date: {rec.get('survey_date', 'unknown')}\n"
            f"   Tag: {tag} — {tag_desc[:120]}\n"
            f"   Severity: {rec.get('scope_severity_code', 'unknown')}\n"
            f"   Fine: {fine_str}\n"
        )
    return "\n".join(lines)
