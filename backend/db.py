"""
Supabase persistence layer.
- scored_signals / raw_signals: pipeline output
- accounts: persistent account store
- osha_inspections: OSHA data warehouse (replaces in-memory CSV)
"""

import os
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, timedelta
import config

_sb = None
_sb_url: str = ""
_sb_key: str = ""


def get_supabase():
    global _sb, _sb_url, _sb_key
    url = config.get("SUPABASE_URL")
    key = config.get("SUPABASE_KEY")
    if not url or not key:
        return None
    # Rebuild client if credentials changed
    if _sb is None or url != _sb_url or key != _sb_key:
        try:
            from supabase import create_client
            _sb = create_client(url, key)
            _sb_url, _sb_key = url, key
        except Exception as e:
            print(f"[db] Supabase init failed: {e}")
            return None
    return _sb


# ─── Signals ──────────────────────────────────────────────────────────────────

def upsert_scored_signal(
    account_id: str,
    account_name: str,
    source_type: str,
    signal: Optional[Dict[str, Any]],
) -> bool:
    sb = get_supabase()
    if not sb or not signal or signal.get("score") is None:
        return False

    row = {
        "account_id": account_id,
        "source_type": source_type,
        "score": signal.get("score"),
        "summary": signal.get("summary"),
        "action": signal.get("action"),
        "excerpt": signal.get("excerpt"),
        "model": signal.get("model"),
        "verified": signal.get("verified", False),
        "confidence": signal.get("confidence"),
        "raw_count": signal.get("raw_count", 0),
        "signal_date": signal.get("date"),
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        sb.table("scored_signals").insert(row).execute()
        return True
    except Exception as e:
        print(f"[db] upsert_scored_signal failed for {account_id}/{source_type}: {e}")
        return False


def save_raw_signals(
    account_id: str,
    source_type: str,
    raw_rows: List[Dict[str, Any]],
) -> bool:
    sb = get_supabase()
    if not sb or not raw_rows:
        return False

    rows = [
        {
            "account_id": account_id,
            "source_type": source_type,
            "raw_data": row,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        for row in raw_rows[:5]
    ]

    try:
        sb.table("raw_signals").insert(rows).execute()
        return True
    except Exception as e:
        print(f"[db] save_raw_signals failed for {account_id}/{source_type}: {e}")
        return False


def load_latest_signals() -> Dict[str, Any]:
    sb = get_supabase()
    if not sb:
        return {}

    try:
        resp = (
            sb.table("scored_signals")
            .select("*")
            .order("scored_at", desc=True)
            .limit(500)
            .execute()
        )
        rows = resp.data or []

        store: Dict[str, Any] = {}
        seen = set()
        for row in rows:
            key = (row["account_id"], row["source_type"])
            if key in seen:
                continue
            seen.add(key)
            acct_id = row["account_id"]
            src = row["source_type"]
            if acct_id not in store:
                store[acct_id] = {"updated_at": row.get("scored_at")}
            store[acct_id][src] = {
                "score": row.get("score"),
                "summary": row.get("summary"),
                "action": row.get("action"),
                "excerpt": row.get("excerpt"),
                "model": row.get("model"),
                "verified": row.get("verified", False),
                "confidence": row.get("confidence"),
                "raw_count": row.get("raw_count", 0),
                "date": row.get("signal_date"),
                "source_type": src,
            }
        print(f"[db] Loaded {len(store)} account signal(s) from Supabase")
        return store
    except Exception as e:
        print(f"[db] load_latest_signals failed: {e}")
        return {}


# ─── Accounts ─────────────────────────────────────────────────────────────────

def _account_to_row(acct: Dict[str, Any]) -> Dict[str, Any]:
    """Convert in-memory account dict to Supabase row (group → acct_group)."""
    return {
        "id": acct["id"],
        "name": acct["name"],
        "acct_group": acct.get("group", "OTHER"),
        "segment": acct.get("segment") or [],
        "description": acct.get("description", ""),
        "owner": acct.get("owner", ""),
        "child": acct.get("child", False),
        "customer": acct.get("customer", False),
        "crm_connected": acct.get("crm_connected", False),
        "osha_search": acct.get("osha_search") or [],
        "cms_search": acct.get("cms_search") or [],
        "hq_state": acct.get("hq_state", ""),
        "osha_confirmed": acct.get("osha_confirmed", False),
        "source": acct.get("source", "manual"),
    }


def _row_to_account(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Supabase row back to in-memory account dict."""
    return {
        "id": row["id"],
        "name": row["name"],
        "group": row.get("acct_group", "OTHER"),
        "segment": row.get("segment") or [],
        "description": row.get("description", ""),
        "owner": row.get("owner", ""),
        "child": row.get("child", False),
        "customer": row.get("customer", False),
        "crm_connected": row.get("crm_connected", False),
        "osha_search": row.get("osha_search") or [],
        "cms_search": row.get("cms_search") or [],
        "hq_state": row.get("hq_state", ""),
        "osha_confirmed": row.get("osha_confirmed", False),
    }


def load_accounts() -> List[Dict[str, Any]]:
    """Load all accounts from Supabase. Returns empty list if none or error."""
    sb = get_supabase()
    if not sb:
        return []
    try:
        resp = sb.table("accounts").select("*").order("created_at").execute()
        accounts = [_row_to_account(r) for r in (resp.data or [])]
        print(f"[db] Loaded {len(accounts)} account(s) from Supabase")
        return accounts
    except Exception as e:
        print(f"[db] load_accounts failed: {e}")
        return []


def save_account(acct: Dict[str, Any]) -> bool:
    """Upsert a single account."""
    sb = get_supabase()
    if not sb:
        return False
    try:
        sb.table("accounts").upsert(_account_to_row(acct), on_conflict="id").execute()
        return True
    except Exception as e:
        print(f"[db] save_account failed for {acct.get('id')}: {e}")
        return False


def save_accounts_bulk(accounts: List[Dict[str, Any]]) -> bool:
    """Upsert a list of accounts in batches of 100."""
    sb = get_supabase()
    if not sb or not accounts:
        return False
    rows = [_account_to_row(a) for a in accounts]
    try:
        for i in range(0, len(rows), 100):
            sb.table("accounts").upsert(rows[i:i+100], on_conflict="id").execute()
        return True
    except Exception as e:
        print(f"[db] save_accounts_bulk failed: {e}")
        return False


def delete_account(account_id: str) -> bool:
    """Delete a single account by id."""
    sb = get_supabase()
    if not sb:
        return False
    try:
        sb.table("accounts").delete().eq("id", account_id).execute()
        return True
    except Exception as e:
        print(f"[db] delete_account failed for {account_id}: {e}")
        return False


def delete_all_accounts() -> bool:
    """Delete all accounts (used for overwrite import)."""
    sb = get_supabase()
    if not sb:
        return False
    try:
        # Delete rows where id is not null = all rows
        sb.table("accounts").delete().neq("id", "").execute()
        return True
    except Exception as e:
        print(f"[db] delete_all_accounts failed: {e}")
        return False


# ─── OSHA Inspections ────────────────────────────────────────────────────────

def get_osha_count() -> int:
    """Return count of rows in osha_inspections table. 0 if error or empty."""
    sb = get_supabase()
    if not sb:
        return 0
    try:
        resp = (
            sb.table("osha_inspections")
            .select("activity_nr", count="exact")
            .limit(1)
            .execute()
        )
        return resp.count or 0
    except Exception as e:
        print(f"[db] get_osha_count failed: {e}")
        return 0


def search_osha_supabase(
    search_terms: List[str],
    days_back: int = 1825,
    limit_per_term: int = 50,
) -> List[Dict[str, Any]]:
    """
    Query osha_inspections table by establishment name (ilike) and date.
    Uses PostgreSQL trigram index for fast text search.
    """
    sb = get_supabase()
    if not sb:
        return []

    cutoff = (datetime.utcnow() - timedelta(days=days_back)).date().isoformat()
    seen_ids: set = set()
    results: List[Dict[str, Any]] = []

    for term in search_terms:
        try:
            resp = (
                sb.table("osha_inspections")
                .select(
                    "activity_nr,estab_name,site_address,site_city,site_state,"
                    "naics_code,insp_type,open_date,close_case_date,nr_in_estab,owner_type"
                )
                .ilike("estab_name", f"%{term}%")
                .gte("open_date", cutoff)
                .order("open_date", desc=True)
                .limit(limit_per_term)
                .execute()
            )
            for row in resp.data or []:
                act_nr = row.get("activity_nr", "")
                if not act_nr or act_nr in seen_ids:
                    continue
                seen_ids.add(act_nr)
                results.append({
                    "activity_nr": act_nr,
                    "estab_name": row.get("estab_name") or "",
                    "site_address": row.get("site_address") or "",
                    "site_city": row.get("site_city") or "",
                    "site_state": row.get("site_state") or "",
                    "naics_code": row.get("naics_code") or "",
                    "insp_type": row.get("insp_type") or "",
                    "open_date": str(row.get("open_date") or "")[:10],
                    "close_case_date": str(row.get("close_case_date") or "")[:10],
                    "nr_in_estab": str(row.get("nr_in_estab") or ""),
                    "owner_type": row.get("owner_type") or "",
                    "_source": "supabase",
                })
        except Exception as e:
            print(f"[db] search_osha_supabase error for '{term}': {e}")

    results.sort(key=lambda r: r.get("open_date", ""), reverse=True)
    print(f"[db] OSHA Supabase: {len(results)} record(s) for {search_terms}")
    return results


def upsert_osha_inspections(rows: List[Dict[str, Any]]) -> int:
    """
    Bulk upsert OSHA records into osha_inspections table.
    Returns count of records successfully sent.
    """
    sb = get_supabase()
    if not sb or not rows:
        return 0

    # Sanitize: ensure no empty strings for date columns (must be None/null)
    def _clean(row: Dict[str, Any]) -> Dict[str, Any]:
        for date_col in ("open_date", "close_case_date"):
            v = row.get(date_col)
            if not v or str(v).strip() in ("", "nan", "None", "NaT"):
                row[date_col] = None
        # Remove None activity_nr rows
        return row

    clean_rows = [_clean(r) for r in rows if r.get("activity_nr")]

    count = 0
    batch_size = 500
    for i in range(0, len(clean_rows), batch_size):
        batch = clean_rows[i:i + batch_size]
        try:
            sb.table("osha_inspections").upsert(batch, on_conflict="activity_nr").execute()
            count += len(batch)
        except Exception as e:
            print(f"[db] upsert_osha_inspections batch {i // batch_size} failed: {e}")
    return count


# ─── Signal Columns ──────────────────────────────────────────────────────────

def _col_to_row(col: Dict[str, Any], sort_order: int = 100) -> Dict[str, Any]:
    return {
        "key": col["key"],
        "label": col["label"],
        "source_type": col.get("source_type", "websearch"),
        "on_by_default": col.get("on", True),
        "segment": col.get("segment") or [],
        "prompt": col.get("prompt", ""),
        "threshold": col.get("threshold", 6),
        "cadence": col.get("cadence", "Weekly"),
        "budget": col.get("budget", 5),
        "builtin": col.get("builtin", False),
        "sort_order": sort_order,
        "sources": col.get("sources") or [],
    }


def _row_to_col(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "key": row["key"],
        "label": row["label"],
        "source_type": row.get("source_type", "websearch"),
        "on": row.get("on_by_default", True),
        "segment": row.get("segment") or [],
        "prompt": row.get("prompt", ""),
        "threshold": row.get("threshold", 6),
        "cadence": row.get("cadence", "Weekly"),
        "budget": row.get("budget", 5),
        "has_prompt": bool(row.get("prompt")),
        "builtin": row.get("builtin", False),
        "sources": row.get("sources") or [],
    }


def load_columns() -> List[Dict[str, Any]]:
    sb = get_supabase()
    if not sb:
        return []
    try:
        resp = sb.table("signal_columns").select("*").order("sort_order").execute()
        cols = [_row_to_col(r) for r in (resp.data or [])]
        print(f"[db] Loaded {len(cols)} column(s) from Supabase")
        return cols
    except Exception as e:
        print(f"[db] load_columns failed: {e}")
        return []


def save_column(col: Dict[str, Any], sort_order: int = 100) -> bool:
    sb = get_supabase()
    if not sb:
        return False
    try:
        sb.table("signal_columns").upsert(_col_to_row(col, sort_order), on_conflict="key").execute()
        return True
    except Exception as e:
        print(f"[db] save_column failed for {col.get('key')}: {e}")
        return False


def save_columns_bulk(cols: List[Dict[str, Any]]) -> bool:
    sb = get_supabase()
    if not sb or not cols:
        return False
    rows = [_col_to_row(c, i * 10) for i, c in enumerate(cols)]
    try:
        sb.table("signal_columns").upsert(rows, on_conflict="key").execute()
        return True
    except Exception as e:
        print(f"[db] save_columns_bulk failed: {e}")
        return False


def delete_column_db(key: str) -> bool:
    sb = get_supabase()
    if not sb:
        return False
    try:
        sb.table("signal_columns").delete().eq("key", key).execute()
        return True
    except Exception as e:
        print(f"[db] delete_column_db failed for {key}: {e}")
        return False


def update_column_db(key: str, updates: Dict[str, Any]) -> bool:
    """Patch specific fields on a column row."""
    sb = get_supabase()
    if not sb:
        return False
    field_map = {"on": "on_by_default"}
    row = {field_map.get(k, k): v for k, v in updates.items()
           if k in {"label", "prompt", "on", "threshold", "cadence", "segment", "source_type", "budget", "sources"}}
    if not row:
        return False
    try:
        sb.table("signal_columns").update(row).eq("key", key).execute()
        return True
    except Exception as e:
        print(f"[db] update_column_db failed for {key}: {e}")
        return False


# ─── Health ───────────────────────────────────────────────────────────────────

def is_connected() -> bool:
    sb = get_supabase()
    if not sb:
        return False
    try:
        sb.table("scored_signals").select("id").limit(1).execute()
        return True
    except Exception:
        return False
