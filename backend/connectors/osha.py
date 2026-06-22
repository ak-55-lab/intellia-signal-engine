"""
DOL OSHA Inspections connector — apiprod.dol.gov/v4/get/OSHA/inspection/json
Filter syntax: filter_object={"field":"estab_name","operator":"like","value":"%term%"}
Up to 10,000 records per request. Paginate with offset if needed.
"""

import os
import json
import httpx
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any


DOL_API_BASE = "https://apiprod.dol.gov/v4/get"


async def fetch_osha_inspections(
    search_terms: List[str],
    days_back: int = 1825,
    limit: int = 1000,
) -> List[Dict[str, Any]]:

    api_key = os.getenv("DOL_API_KEY", "")
    if not api_key:
        raise ValueError("DOL_API_KEY not set in environment")

    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    seen = set()
    results = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        for term in search_terms:
            try:
                # Use filter_object with like operator — documented DOL v4 syntax
                filter_obj = json.dumps({
                    "field": "estab_name",
                    "operator": "like",
                    "value": f"%{term}%",
                })
                params = {
                    "X-API-KEY": api_key,
                    "limit": str(limit),
                    "sort_by": "open_date",
                    "sort": "desc",
                    "filter_object": filter_obj,
                }
                resp = await client.get(
                    f"{DOL_API_BASE}/OSHA/inspection/json",
                    params=params,
                )
                print(f"[osha] HTTP {resp.status_code} (term='{term}')")

                if resp.status_code in (401, 403):
                    print(f"[osha] Auth error — check DOL_API_KEY")
                    break

                if resp.status_code == 204:
                    # 204 No Content = valid request, zero matching records
                    print(f"[osha] 0 rows for '{term}' (204 No Content)")
                    continue

                if resp.status_code != 200:
                    print(f"[osha] Unexpected status {resp.status_code}: {resp.text[:300]}")
                    continue

                data = resp.json()

                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    rows = (data.get("data") or data.get("results")
                            or data.get("rows") or data.get("records") or [])
                    if isinstance(rows, dict):
                        rows = rows.get("rows") or rows.get("records") or []
                else:
                    rows = []

                print(f"[osha] got {len(rows)} rows (term='{term}')")

                for row in rows:
                    # Client-side verify — LIKE "%UPS%" matches "UPSTREAM", "UPSTATE" etc.
                    estab = (row.get("estab_name") or row.get("ESTAB_NAME") or "").lower()
                    if term_lower not in estab:
                        continue
                    key = row.get("activity_nr") or row.get("ACTIVITY_NR")
                    if not key or key in seen:
                        continue
                    open_date = (row.get("open_date") or row.get("OPEN_DATE") or "")[:10]
                    if open_date and open_date >= cutoff:
                        seen.add(key)
                        results.append(row)

            except httpx.HTTPStatusError as e:
                print(f"[osha] HTTP {e.response.status_code} for '{term}': {e.response.text[:300]}")
            except Exception as e:
                print(f"[osha] Error for '{term}': {e}")

            await asyncio.sleep(0.3)

    results.sort(key=lambda r: (r.get("open_date") or r.get("OPEN_DATE") or ""), reverse=True)
    print(f"[osha] Final count after filter: {len(results)}")
    return results


def format_osha_for_scoring(inspections: List[Dict[str, Any]]) -> str:
    if not inspections:
        return "No OSHA inspections found in the specified date range."

    lines = [f"OSHA Inspection Records — {len(inspections)} result(s):\n"]
    for i, rec in enumerate(inspections[:5], 1):
        penalty = rec.get("total_current_penalty") or rec.get("TOTAL_CURRENT_PENALTY")
        try:
            penalty_str = f"${float(penalty):,.0f}" if penalty else "no penalty recorded"
        except Exception:
            penalty_str = str(penalty)
        lines.append(
            f"{i}. Establishment: {rec.get('estab_name') or rec.get('ESTAB_NAME', 'Unknown')}\n"
            f"   Location: {rec.get('site_city') or rec.get('SITE_CITY', '')}, "
            f"{rec.get('site_state') or rec.get('SITE_STATE', '')}\n"
            f"   Opened: {(rec.get('open_date') or rec.get('OPEN_DATE') or '')[:10]}\n"
            f"   Closed: {(rec.get('close_case_date') or rec.get('CLOSE_CASE_DATE') or 'open')[:10]}\n"
            f"   Type: {rec.get('insp_type') or rec.get('INSP_TYPE', 'unknown')}\n"
            f"   Penalty: {penalty_str}\n"
            f"   Employees at site: {rec.get('nr_in_estab') or rec.get('NR_IN_ESTAB', 'unknown')}\n"
        )
    return "\n".join(lines)
