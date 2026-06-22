"""
Intellia Signal Engine — FastAPI backend
"""

import os
import json
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, BackgroundTasks, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()
import config   # runtime config — priority: runtime > .env > default

from accounts import ACCOUNTS, get_account_by_id
from connectors.osha import fetch_osha_inspections, format_osha_for_scoring
from connectors.osha_csv import (
    load_csv_data, search_osha_csv, format_csv_osha_for_scoring,
    is_loaded as csv_is_loaded, csv_status,
)
from connectors.osha_supabase import (
    is_available as osha_sb_is_available,
    search_osha_supabase, format_supabase_osha_for_scoring,
    invalidate_cache as osha_sb_invalidate,
)
from connectors.cms import fetch_cms_deficiencies, format_cms_for_scoring
from connectors.websearch import fetch_signal_for_column
from scorer import score_all_columns, get_client
from column_config import (
    get_all_columns, get_active_columns, get_column,
    add_column, update_column, delete_column, seed_from_db,
)
import kb as knowledge_base
import db
from connectors.websearch import _brave_search

app = FastAPI(title="Intellia Signal Engine", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

signal_store: Dict[str, Any] = {}
trace_store:  Dict[str, Any] = {}   # keyed by "{acct_id}:{col_key}"
account_store: List[Dict[str, Any]] = list(ACCOUNTS)

run_state = {
    "running": False,
    "messages": [],
    "last_run": None,
    "cost_estimate": 0.0,
}


@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()

    sb_accounts = await loop.run_in_executor(None, db.load_accounts)
    if sb_accounts:
        account_store.clear()
        account_store.extend(sb_accounts)
        print(f"[startup] Loaded {len(sb_accounts)} account(s) from Supabase")
    else:
        seed = [dict(a, source="seed") for a in ACCOUNTS]
        await loop.run_in_executor(None, lambda: db.save_accounts_bulk(seed))
        print(f"[startup] Seeded {len(account_store)} default account(s) to Supabase")

    osha_count = await loop.run_in_executor(None, db.get_osha_count)
    if osha_count > 0:
        from connectors import osha_supabase as _osb
        _osb._available = True
        print(f"[startup] OSHA Supabase: {osha_count:,} records ready — CSV load skipped")
    else:
        print("[startup] OSHA Supabase table empty — loading CSV files into memory")
        await loop.run_in_executor(None, load_csv_data)

    sb_cols = await loop.run_in_executor(None, db.load_columns)
    if sb_cols:
        seed_from_db(sb_cols)
        print(f"[startup] Loaded {len(sb_cols)} column(s) from Supabase")
    else:
        # First boot: seed defaults to Supabase
        defaults = get_all_columns()
        await loop.run_in_executor(None, lambda: db.save_columns_bulk(defaults))
        print(f"[startup] Seeded {len(defaults)} default column(s) to Supabase")

    cached = await loop.run_in_executor(None, db.load_latest_signals)
    if cached:
        signal_store.update(cached)
        print(f"[startup] Loaded {len(cached)} account signal(s) from Supabase")


class RunFetchRequest(BaseModel):
    account_ids: Optional[List[str]] = None
    column_keys: Optional[List[str]] = None   # None = all active columns
    triage_threshold: int = 4
    verify_threshold: int = 6
    days_back: int = 365

class UpdateColumnRequest(BaseModel):
    label: Optional[str] = None
    prompt: Optional[str] = None
    on: Optional[bool] = None
    threshold: Optional[int] = None
    cadence: Optional[str] = None
    segment: Optional[List[str]] = None
    sources: Optional[List[Dict[str, Any]]] = None
    column_type: Optional[str] = None
    enrich_field: Optional[str] = None

class AddColumnRequest(BaseModel):
    label: str
    prompt: str = ""
    segment: List[str] = ["commercial", "enterprise", "gov", "healthcare"]
    threshold: int = 6
    cadence: str = "Weekly"
    sources: Optional[List[Dict[str, Any]]] = None
    column_type: str = "signal"
    enrich_field: str = ""

class ImportAccountsRequest(BaseModel):
    accounts: List[Dict[str, Any]]
    mode: str = "append"

class AddAccountRequest(BaseModel):
    name: str
    group: str = "OTHER"
    segment: List[str] = ["enterprise"]
    description: str = ""
    osha_search: List[str] = []
    cms_search: List[str] = []

class RefinePromptRequest(BaseModel):
    prompt: str
    label: str = ""
    segment: List[str] = []

class FetchUrlRequest(BaseModel):
    url: str
    title: Optional[str] = None

class DeleteAccountRequest(BaseModel):
    account_ids: List[str]


def emit(message: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    run_state["messages"].append(f"[{ts}] {message}")
    print(f"[run] {message}")


async def fetch_data_for_column(account: dict, col_key: str, days_back: int):
    """
    Returns (text, rows, sources_checked, fetch_trace).
    fetch_trace is a list of dicts describing what was fetched per source.
    """
    col = get_column(col_key)
    if not col:
        return "", [], [], []
    source = col.get("source_type", "websearch")

    if col_key == "osha" and source == "api":
        search_terms = account.get("osha_search", [account["name"]])
        api_source = "supabase" if osha_sb_is_available() else ("csv" if csv_is_loaded() else "dol_api")
        if osha_sb_is_available():
            rows = await asyncio.get_event_loop().run_in_executor(
                None, lambda: search_osha_supabase(search_terms, days_back)
            )
            if rows:
                ft = [{"label": "OSHA DOL API", "type": "api_osha", "source": api_source,
                       "search_terms": search_terms, "result_count": len(rows), "days_back": days_back}]
                return format_supabase_osha_for_scoring(rows), rows, ["OSHA DOL API"], ft
        if csv_is_loaded():
            rows = await asyncio.get_event_loop().run_in_executor(
                None, lambda: search_osha_csv(search_terms, days_back)
            )
            if rows:
                ft = [{"label": "OSHA CSV", "type": "api_osha", "source": "csv",
                       "search_terms": search_terms, "result_count": len(rows), "days_back": days_back}]
                return format_csv_osha_for_scoring(rows), rows, ["OSHA CSV"], ft
        try:
            rows = await fetch_osha_inspections(search_terms=search_terms, days_back=min(days_back, 90))
            ft = [{"label": "OSHA DOL API", "type": "api_osha", "source": "dol_api",
                   "search_terms": search_terms, "result_count": len(rows), "days_back": days_back}]
            return format_osha_for_scoring(rows), rows, ["OSHA DOL API"], ft
        except Exception:
            return "No OSHA inspections found in the specified date range.", [], [], []

    elif col_key == "cms" and source == "api":
        rows = await fetch_cms_deficiencies(
            search_terms=account.get("cms_search", [account["name"]]),
        )
        ft = [{"label": "CMS Health Deficiencies API", "type": "api_cms", "source": "cms_api",
               "search_terms": account.get("cms_search", [account["name"]]), "result_count": len(rows)}]
        return format_cms_for_scoring(rows), rows, ["CMS API"], ft

    elif source == "websearch":
        sources = col.get("sources") or []
        text, sources_checked, queries_trace = await fetch_signal_for_column(
            account_name=account["name"],
            column_key=col_key,
            sources=sources,
        )
        return text, [], sources_checked, queries_trace

    return "", [], [], []


async def _extract_field_for_column(
    account: Dict[str, Any], enrich_field: str, custom_prompt: str = ""
) -> tuple:
    """Extract a single enrichment field for an account using Brave + Haiku.
    Checks account.enrichment cache first to avoid redundant searches.
    Returns (value: str, trace: dict).
    """
    name = account["name"]
    existing = account.get("enrichment") or {}

    # Return cached value if available (avoids burning Brave quota)
    if enrich_field not in ("custom", "") and existing.get(enrich_field):
        cached_val = str(existing[enrich_field])
        return cached_val, {
            "query": "(cached)",
            "result_count": 0,
            "results": [],
            "source": "cache",
            "model": None,
        }

    field_queries = {
        "website":        f"{name} official website homepage domain",
        "employees":      f"{name} total employees headcount workforce 2024 2025",
        "annual_revenue": f"{name} annual revenue earnings 2024 2025",
        "industry":       f"{name} industry sector business overview",
        "hq_city":        f"{name} headquarters location city state",
        "founded":        f"{name} founded year company history",
    }
    query = field_queries.get(enrich_field) or f"{name} {custom_prompt or enrich_field}"

    results = await _brave_search(query, max_results=5)
    trace_base = {"query": query, "result_count": len(results), "results": results, "source": "brave"}

    if not results:
        return "", {**trace_base, "model": None}

    snippets = "\n\n".join(
        f"[{r['title']}]\n{r['snippet']}\nURL: {r['url']}"
        for r in results
    )

    field_instructions = {
        "website":        'Extract the primary corporate website domain only (e.g. "ups.com"). No https:// prefix. Return just the domain string.',
        "employees":      'Extract total employee headcount as a string (e.g. "500,000+", "~12,000"). Return just the value.',
        "annual_revenue": 'Extract most recent annual revenue as a string (e.g. "$97B", "$2.4B"). Return just the value.',
        "industry":       'Extract 2-4 word industry label (e.g. "Package Delivery & Logistics"). Return just the label.',
        "hq_city":        'Extract headquarters city and state (e.g. "Atlanta, GA"). Return just the location.',
        "founded":        'Extract founding year as a 4-digit number (e.g. "1907"). Return just the year.',
    }
    instruction = field_instructions.get(enrich_field) or (custom_prompt or f"Extract the {enrich_field} for this company.")
    system = (
        f"You are a B2B data extraction assistant. Given web search results about a company, {instruction}\n"
        "Return ONLY the extracted value as a plain string — no JSON, no labels, no prose. If not found, return empty string."
    )

    try:
        client = get_client()
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=64,
            system=system,
            messages=[{"role": "user", "content": f"Company: {name}\n\nSearch results:\n{snippets}"}],
        )
        value = resp.content[0].text.strip().strip('"')
        return value, {**trace_base, "model": "claude-haiku-4-5-20251001"}
    except Exception as e:
        print(f"[enrich-col] extraction failed for {name}/{enrich_field}: {e}")
        return "", {**trace_base, "model": None, "error": str(e)}


async def run_pipeline(account_ids, column_keys, triage_threshold, verify_threshold, days_back):
    run_state["running"] = True
    run_state["messages"] = []
    run_state["cost_estimate"] = 0.0

    target_accounts = (
        [a for a in account_store if a["id"] in account_ids]
        if account_ids else account_store
    )
    all_active_cols = get_active_columns()
    # Filter to specific columns if requested
    active_cols = (
        [c for c in all_active_cols if c["key"] in column_keys]
        if column_keys else all_active_cols
    )
    kb_context = knowledge_base.get_context()
    col_configs = {c["key"]: c for c in active_cols}

    # Split columns by type
    signal_cols  = [c for c in active_cols if c.get("column_type", "signal") != "enrichment"]
    enrich_cols  = [c for c in active_cols if c.get("column_type", "signal") == "enrichment"]

    col_label = f"column(s): {', '.join(column_keys)}" if column_keys else f"{len(active_cols)} active column(s)"
    emit(f"Starting pipeline -- {len(target_accounts)} account(s), {col_label}")
    if enrich_cols:
        emit(f"  Enrichment columns: {', '.join(c['label'] for c in enrich_cols)}")
    if kb_context:
        emit(f"Knowledge base loaded -- {len(kb_context):,} chars of context")

    for account in target_accounts:
        if not run_state["running"]:
            emit("Pipeline stopped by user.")
            break
        acct_id = account["id"]
        acct_name = account["name"]
        emit(f"-> {acct_name}")

        try:
            # ── Signal columns: fetch → score ──────────────────────────────────
            column_data: Dict[str, str] = {}
            raw_rows: Dict[str, list] = {}
            sources_checked_map: Dict[str, list] = {}
            fetch_trace_map:     Dict[str, list] = {}

            for col in signal_cols:
                col_key = col["key"]
                acct_segments = account.get("segment", [])
                col_segments = col.get("segment", [])
                if acct_segments and col_segments and not any(s in col_segments for s in acct_segments):
                    column_data[col_key] = "__NA__"
                    raw_rows[col_key] = []
                    continue
                try:
                    result = await fetch_data_for_column(account, col_key, days_back)
                    if isinstance(result, tuple) and len(result) == 4:
                        text, rows, src_checked, ft = result
                        sources_checked_map[col_key] = src_checked
                        fetch_trace_map[col_key] = ft
                    elif isinstance(result, tuple) and len(result) == 3:
                        text, rows, src_checked = result
                        sources_checked_map[col_key] = src_checked
                    elif isinstance(result, tuple):
                        text, rows = result
                    else:
                        text, rows = result, []
                    column_data[col_key] = text
                    raw_rows[col_key] = rows
                except Exception as e:
                    emit(f"  x Fetch error [{col_key}]: {e}")
                    column_data[col_key] = ""

            for col_key, val in column_data.items():
                if val == "__NA__":
                    if acct_id not in signal_store:
                        signal_store[acct_id] = {}
                    signal_store[acct_id][col_key] = {"status": "na"}

            scored: Dict[str, Any] = {}
            if signal_cols:
                data_found = [k for k, v in column_data.items() if v and v != "__NA__" and "No " not in v[:20]]
                emit(f"  Tier 1 done -- data in {len(data_found)}/{len(column_data)} column(s)")

                if data_found:
                    scoreable = {k: v for k, v in column_data.items() if v != "__NA__"}
                    emit(f"  Scoring with Claude...")
                    scored = await score_all_columns(
                        account_name=acct_name,
                        column_data=scoreable,
                        column_configs=col_configs,
                        triage_threshold=triage_threshold,
                        verify_threshold=verify_threshold,
                        kb_context=kb_context,
                    )

                    for col_key, result in scored.items():
                        if result:
                            rows = raw_rows.get(col_key, [])
                            result["raw_count"] = len(rows)
                            result["sources_checked"] = sources_checked_map.get(col_key, [])
                            if rows:
                                result["date"] = (
                                    rows[0].get("open_date") or
                                    rows[0].get("survey_date") or
                                    "recent"
                                )
                            scoring_trace = result.pop("_trace", None)
                            col_obj = get_column(col_key) or {}
                            trace_store[f"{acct_id}:{col_key}"] = {
                                "account_id":   acct_id,
                                "account_name": acct_name,
                                "column_key":   col_key,
                                "column_label": col_obj.get("label", col_key),
                                "fetch":        fetch_trace_map.get(col_key, []),
                                "scoring":      scoring_trace,
                                "ran_at":       datetime.now(timezone.utc).isoformat(),
                            }

            # ── Enrichment columns: extract field value ────────────────────────
            enrichment_results: Dict[str, Any] = {}
            for col in enrich_cols:
                col_key     = col["key"]
                enrich_field = col.get("enrich_field", "")
                enrich_prompt = col.get("prompt", "")
                acct_segments = account.get("segment", [])
                col_segments  = col.get("segment", [])
                if acct_segments and col_segments and not any(s in col_segments for s in acct_segments):
                    enrichment_results[col_key] = {"status": "na"}
                    continue
                try:
                    value, etrace = await _extract_field_for_column(account, enrich_field, enrich_prompt)
                    enrichment_results[col_key] = {
                        "value":        value,
                        "column_type":  "enrichment",
                        "enrich_field": enrich_field,
                    }
                    # Store trace for observability drawer
                    col_obj = get_column(col_key) or {}
                    trace_store[f"{acct_id}:{col_key}"] = {
                        "account_id":   acct_id,
                        "account_name": acct_name,
                        "column_key":   col_key,
                        "column_label": col_obj.get("label", col_key),
                        "column_type":  "enrichment",
                        "enrich_field": enrich_field,
                        "value":        value,
                        **etrace,
                        "ran_at": datetime.now(timezone.utc).isoformat(),
                    }
                    # Cache in account enrichment dict + persist
                    if enrich_field not in ("custom", ""):
                        existing_enr = account.get("enrichment") or {}
                        existing_enr[enrich_field] = value  # persist even empty so we know it was checked
                        account["enrichment"] = existing_enr
                        if value:
                            db.save_account_enrichment(acct_id, existing_enr)
                    emit(f"  [enrich] {col.get('label', col_key)}: {value or '(not found)'}")
                except Exception as e:
                    emit(f"  x Enrichment error [{col_key}]: {e}")
                    enrichment_results[col_key] = {"value": "", "column_type": "enrichment", "enrich_field": enrich_field}

            # ── Merge into signal_store ────────────────────────────────────────
            if acct_id not in signal_store:
                signal_store[acct_id] = {}
            signal_store[acct_id].update({
                **scored,
                **enrichment_results,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })

            for col_key, result in scored.items():
                db.upsert_scored_signal(acct_id, acct_name, col_key, result)
                if raw_rows.get(col_key):
                    db.save_raw_signals(acct_id, col_key, raw_rows[col_key])

            high = [(k, v["score"]) for k, v in scored.items() if v and v.get("score", 0) >= 7]
            if high:
                emit(f"  + High signals: {', '.join(f'{k}={s}' for k,s in high)}")
            elif signal_cols:
                emit(f"  + Scored -- no high signals this cycle")

        except Exception as e:
            emit(f"  x Error: {e}")
            continue

        await asyncio.sleep(0.5)

    run_state["last_run"] = datetime.now(timezone.utc).isoformat()
    run_state["running"] = False
    emit(f"Pipeline complete -- {len(target_accounts)} account(s) processed")


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    frontend_path = Path(__file__).parent.parent / "frontend" / "index.html"
    if not frontend_path.exists():
        raise HTTPException(status_code=404, detail="Frontend not found")
    return HTMLResponse(content=frontend_path.read_text(encoding="utf-8"))


@app.get("/api/accounts")
async def get_accounts():
    return {"accounts": account_store, "total": len(account_store)}

@app.post("/api/accounts")
async def add_account(request: AddAccountRequest):
    import re, time
    slug = re.sub(r'[^a-z0-9]+', '-', request.name.lower()).strip('-') + '-' + str(int(time.time()))[-4:]
    acct = {
        "id": slug,
        "name": request.name,
        "group": request.group,
        "segment": request.segment,
        "description": request.description,
        "owner": "",
        "child": False,
        "customer": False,
        "crm_connected": False,
        "osha_search": request.osha_search or [request.name],
        "cms_search": request.cms_search,
        "source": "manual",
    }
    account_store.append(acct)
    db.save_account(acct)
    return acct

@app.post("/api/accounts/import")
async def import_accounts(request: ImportAccountsRequest):
    added = 0
    new_accounts = []
    if request.mode == "overwrite":
        db.delete_all_accounts()
        account_store.clear()
    for acct in request.accounts:
        if not any(a["id"] == acct["id"] for a in account_store):
            acct["source"] = "import"
            account_store.append(acct)
            new_accounts.append(acct)
            added += 1
    if new_accounts:
        db.save_accounts_bulk(new_accounts)
    return {"added": added, "total": len(account_store)}

@app.post("/api/accounts/delete")
async def delete_accounts(request: DeleteAccountRequest):
    before = len(account_store)
    for aid in request.account_ids:
        db.delete_account(aid)
    account_store[:] = [a for a in account_store if a["id"] not in request.account_ids]
    removed = before - len(account_store)
    return {"removed": removed, "total": len(account_store)}


@app.get("/api/trace/{account_id}/{col_key}")
async def get_trace(account_id: str, col_key: str):
    key = f"{account_id}:{col_key}"
    if key not in trace_store:
        raise HTTPException(status_code=404, detail="No trace found — run the pipeline first")
    return trace_store[key]


@app.get("/api/signals")
async def get_signals():
    return {
        "signals": signal_store,
        "last_run": run_state.get("last_run"),
        "running": run_state["running"],
        "run_log": run_state["messages"][-30:],
    }

@app.get("/api/signals/{account_id}")
async def get_account_signals(account_id: str):
    account = get_account_by_id(account_id) or next(
        (a for a in account_store if a["id"] == account_id), None
    )
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"account": account, "signals": signal_store.get(account_id, {})}


@app.get("/api/columns")
async def get_columns():
    return {"columns": get_all_columns()}

@app.post("/api/columns")
async def create_column(request: AddColumnRequest):
    col = add_column(
        label=request.label,
        prompt=request.prompt,
        segment=request.segment,
        threshold=request.threshold,
        cadence=request.cadence,
        sources=request.sources,
        column_type=request.column_type,
        enrich_field=request.enrich_field,
    )
    # Persist to Supabase — sort_order = last position
    sort_order = len(get_all_columns()) * 10
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: db.save_column(col, sort_order))
    return col

@app.put("/api/columns/{key}")
async def update_col(key: str, request: UpdateColumnRequest):
    updates = request.model_dump(exclude_none=True)
    result = update_column(key, updates)
    if not result:
        raise HTTPException(status_code=404, detail="Column not found")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: db.update_column_db(key, updates))
    return result

@app.delete("/api/columns/{key}")
async def delete_col(key: str):
    ok = delete_column(key)
    if not ok:
        raise HTTPException(status_code=400, detail="Cannot delete built-in column or column not found")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: db.delete_column_db(key))
    return {"deleted": key}


@app.post("/api/stop-fetch")
async def stop_fetch():
    if run_state["running"]:
        run_state["running"] = False
        emit("Pipeline stopped by user")
    return {"status": "stopped", "was_running": run_state["running"]}


@app.post("/api/run-fetch")
async def run_fetch(request: RunFetchRequest, background_tasks: BackgroundTasks):
    if run_state["running"]:
        raise HTTPException(status_code=409, detail="Pipeline already running")
    background_tasks.add_task(
        run_pipeline,
        account_ids=request.account_ids,
        column_keys=request.column_keys,
        triage_threshold=request.triage_threshold,
        verify_threshold=request.verify_threshold,
        days_back=request.days_back,
    )
    return {"status": "started", "column_keys": request.column_keys}

@app.get("/api/run-status")
async def run_status():
    async def event_stream():
        sent = 0
        while True:
            msgs = run_state["messages"]
            if len(msgs) > sent:
                for msg in msgs[sent:]:
                    yield f"data: {json.dumps({'message': msg, 'running': run_state['running']})}\n\n"
                sent = len(msgs)
            if not run_state["running"] and sent >= len(run_state["messages"]):
                yield f"data: {json.dumps({'done': True, 'running': False})}\n\n"
                break
            await asyncio.sleep(0.4)
    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/kb")
async def get_kb():
    entries = knowledge_base.list_entries()
    for e in entries:
        full = knowledge_base.get_entry(e["id"])
        if full:
            e["preview"] = full["content"][:400].strip()
    return {"entries": entries}

@app.get("/api/kb/{entry_id}")
async def get_kb_entry(entry_id: str):
    entry = knowledge_base.get_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    return entry

@app.get("/api/kb/{entry_id}/summary")
async def get_kb_summary(entry_id: str):
    entry = knowledge_base.get_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    content = entry.get("content", "")
    if not content:
        raise HTTPException(status_code=400, detail="No content to summarize")
    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=(
                "You are a B2B sales intelligence analyst for Cority, an EHS software company. "
                "Summarize content into a clean JSON object. Be concise and sales-relevant."
            ),
            messages=[{
                "role": "user",
                "content": (
                    f"Summarize this content from '{entry['title']}' into JSON:\n"
                    '{"overview": "2-3 sentence summary", '
                    '"key_points": ["bullet 1", "bullet 2", "bullet 3"], '
                    '"cority_relevance": "1-2 sentences on EHS relevance", '
                    '"topics": ["topic1", "topic2"]}\n\n'
                    f"Content:\n{content[:6000]}"
                )
            }]
        )
        raw = resp.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        summary = json.loads(raw.strip())
        return {
            "entry_id": entry_id, "title": entry["title"], "type": entry["type"],
            "url": entry.get("url"), "char_count": entry.get("char_count"),
            "added_at": entry.get("added_at"), "summary": summary,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Summary failed: {str(e)}")

@app.post("/api/kb/upload")
async def upload_kb(file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8", errors="replace")
    entry = knowledge_base.add_markdown(title=file.filename or "Uploaded doc", content=content)
    return {k: v for k, v in entry.items() if k != "content"}

@app.post("/api/kb/fetch-url")
async def fetch_kb_url(request: FetchUrlRequest):
    try:
        entry = await knowledge_base.fetch_url(url=request.url, title=request.title)
        return {k: v for k, v in entry.items() if k != "content"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/kb/{entry_id}")
async def delete_kb(entry_id: str):
    ok = knowledge_base.remove_entry(entry_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Cannot delete seed entry or entry not found")
    return {"deleted": entry_id}


@app.post("/api/refine-prompt")
async def refine_prompt(request: RefinePromptRequest):
    try:
        import anthropic as _anthropic
        client = _anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
        seg_str = ", ".join(request.segment) if request.segment else "all segments"
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            system=(
                "You are a GTM analyst at Cority, an EHS software company. "
                "Your job is to sharpen a signal-scoring prompt that a user has already written. "
                "CRITICAL: Preserve the user's core intent, topic, and any specific criteria they mentioned. "
                "Only improve: clarity, EHS/safety specificity, and actionability for a B2B sales team. "
                "Do NOT replace their prompt with a generic one. Do NOT ignore their wording. "
                "Keep output under 120 words. Return only the refined prompt text, no explanation or preamble."
            ),
            messages=[{
                "role": "user",
                "content": (
                    f"Signal column: '{request.label}'\n"
                    f"Applies to segments: {seg_str}\n\n"
                    f"User's prompt (refine this, do not replace it):\n{request.prompt}\n\n"
                    "Sharpen the language, add EHS/safety specificity where it helps, "
                    "and make it more actionable — but keep all the user's original intent intact."
                )
            }]
        )
        return {"refined": resp.content[0].text.strip()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refinement failed: {str(e)}")


@app.get("/api/osha-csv-status")
async def osha_csv_status_endpoint():
    sb_count = db.get_osha_count()
    return {
        **csv_status(),
        "supabase_records": sb_count,
        "active_source": "supabase" if sb_count > 0 else ("csv" if csv_is_loaded() else "dol_api"),
    }


async def _sync_osha_task():
    run_state["syncing_osha"] = True
    total_new = 0
    try:
        all_terms: set = set()
        for acct in account_store:
            for term in acct.get("osha_search", [acct["name"]]):
                all_terms.add(term)

        print(f"[sync_osha] Fetching last 90 days for {len(all_terms)} search term(s)")
        new_rows = []
        for term in all_terms:
            try:
                rows = await fetch_osha_inspections(search_terms=[term], days_back=90)
                for r in rows:
                    row = {
                        "activity_nr": r.get("activity_nr"),
                        "estab_name": r.get("estab_name"),
                        "site_address": r.get("site_address"),
                        "site_city": r.get("site_city"),
                        "site_state": r.get("site_state"),
                        "site_zip": r.get("site_zip"),
                        "naics_code": r.get("naics_code"),
                        "sic_code": r.get("sic_code"),
                        "insp_type": r.get("insp_type"),
                        "open_date": r.get("open_date") or None,
                        "close_case_date": r.get("close_case_date") or None,
                        "nr_in_estab": r.get("nr_in_estab"),
                        "owner_type": r.get("owner_type"),
                        "data_source": "api_sync",
                    }
                    if row["activity_nr"]:
                        new_rows.append(row)
            except Exception as e:
                print(f"[sync_osha] Error for '{term}': {e}")

        if new_rows:
            total_new = db.upsert_osha_inspections(new_rows)
            osha_sb_invalidate()
            print(f"[sync_osha] Upserted {total_new} record(s) from DOL API")
        else:
            print("[sync_osha] No new records returned by DOL API")
    except Exception as e:
        print(f"[sync_osha] Fatal error: {e}")
    finally:
        run_state["syncing_osha"] = False
        run_state["last_osha_sync"] = datetime.now(timezone.utc).isoformat()
        run_state["last_osha_sync_count"] = total_new


@app.post("/api/sync-osha")
async def sync_osha_endpoint(background_tasks: BackgroundTasks):
    if run_state.get("syncing_osha"):
        raise HTTPException(status_code=409, detail="OSHA sync already running")
    run_state["syncing_osha"] = True
    background_tasks.add_task(_sync_osha_task)
    return {
        "status": "started",
        "accounts": len(account_store),
        "note": "Fetching last 90 days from DOL API. Check /api/osha-csv-status for result.",
    }

@app.get("/api/sync-osha/status")
async def sync_osha_status():
    return {
        "syncing": run_state.get("syncing_osha", False),
        "last_sync": run_state.get("last_osha_sync"),
        "last_sync_count": run_state.get("last_osha_sync_count", 0),
        "osha_supabase_records": db.get_osha_count(),
    }


@app.get("/api/health")
async def health():
    issues = []
    if not os.getenv("ANTHROPIC_API_KEY"):
        issues.append("ANTHROPIC_API_KEY not set")
    if not os.getenv("DOL_API_KEY"):
        issues.append("DOL_API_KEY not set")
    osha_sb = db.get_osha_count()
    return {
        "status": "ok" if not issues else "degraded",
        "issues": issues,
        "accounts_loaded": len(account_store),
        "signals_cached": len(signal_store),
        "kb_docs": len(knowledge_base.list_entries()),
        "osha_source": "supabase" if osha_sb_is_available() else ("csv" if csv_is_loaded() else "dol_api"),
    }


# ─────────────────────────────────────────────────────────────
# Account enrichment — Brave search + Claude Haiku extraction
# Fields: website, employees, annual_revenue, industry, hq_city
# ─────────────────────────────────────────────────────────────

ENRICH_SYSTEM = """You are a B2B data enrichment assistant. Given web search results about a company,
extract the following fields as a JSON object. Be concise and factual. Use null if unknown.
Return ONLY valid JSON, no prose.

Fields:
- website: primary corporate domain only (e.g. "ups.com"), no https:// prefix
- employees: headcount as a string (e.g. "500,000+", "~12,000")
- annual_revenue: most recent full-year revenue as a string (e.g. "$97B", "$2.4B")
- industry: 2-4 word industry label (e.g. "Package Delivery & Logistics")
- hq_city: headquarters city and state (e.g. "Atlanta, GA")
- founded: founding year as integer (e.g. 1907), or null
"""

async def _enrich_account(account: Dict[str, Any]) -> Dict[str, Any]:
    """Search + extract enrichment fields for one account. Returns enrichment dict."""
    name = account["name"]
    query = f"{name} company employees revenue headquarters industry overview"

    results = await _brave_search(query, max_results=8)
    if not results:
        return {}

    snippets = "\n\n".join(
        f"[{r['title']}]\n{r['snippet']}\nURL: {r['url']}"
        for r in results
    )
    user_msg = f"Company: {name}\n\nSearch results:\n{snippets}"

    try:
        client = get_client()
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=ENRICH_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        data["enriched_at"] = datetime.now(timezone.utc).isoformat()
        return data
    except Exception as e:
        print(f"[enrich] extraction failed for {name}: {e}")
        return {}


@app.post("/api/accounts/{account_id}/enrich")
async def enrich_account(account_id: str):
    """Enrich a single account with website, employees, revenue, industry via Brave + Claude."""
    acct = next((a for a in account_store if a["id"] == account_id), None)
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")
    if not config.get("BRAVE_API_KEY"):
        raise HTTPException(status_code=400, detail="BRAVE_API_KEY not configured")

    enrichment = await _enrich_account(acct)
    if not enrichment:
        raise HTTPException(status_code=502, detail="Enrichment returned no data")

    existing = acct.get("enrichment") or {}
    existing.update(enrichment)
    acct["enrichment"] = existing

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: db.save_account_enrichment(account_id, existing))

    return {"status": "ok", "account_id": account_id, "enrichment": existing}


@app.post("/api/accounts/enrich-all")
async def enrich_all_accounts(background_tasks: BackgroundTasks):
    """Kick off background enrichment for all accounts missing enrichment data."""
    if not config.get("BRAVE_API_KEY"):
        raise HTTPException(status_code=400, detail="BRAVE_API_KEY not configured")

    targets = [a for a in account_store if not a.get("enrichment", {}).get("website")]

    async def _run_all():
        for acct in targets:
            try:
                enrichment = await _enrich_account(acct)
                if enrichment:
                    existing = acct.get("enrichment") or {}
                    existing.update(enrichment)
                    acct["enrichment"] = existing
                    db.save_account_enrichment(acct["id"], existing)
                    print(f"[enrich] {acct['name']} -> {enrichment.get('website','?')}")
                await asyncio.sleep(1)
            except Exception as e:
                print(f"[enrich] {acct['name']} failed: {e}")

    background_tasks.add_task(_run_all)
    return {"status": "started", "targets": len(targets)}


# ─────────────────────────────────────────────────────────────
# Settings — runtime API key configuration
# ─────────────────────────────────────────────────────────────

class SettingsRequest(BaseModel):
    ANTHROPIC_API_KEY: Optional[str] = None
    BRAVE_API_KEY:     Optional[str] = None
    SUPABASE_URL:      Optional[str] = None
    SUPABASE_KEY:      Optional[str] = None


@app.get("/api/settings")
def get_settings():
    return {
        "configured": config.is_configured(),
        "keys": config.get_all_public(),
    }


@app.post("/api/settings")
def save_settings(req: SettingsRequest):
    changed = []
    if req.ANTHROPIC_API_KEY is not None:
        config.set_key("ANTHROPIC_API_KEY", req.ANTHROPIC_API_KEY)
        changed.append("ANTHROPIC_API_KEY")
    if req.BRAVE_API_KEY is not None:
        config.set_key("BRAVE_API_KEY", req.BRAVE_API_KEY)
        changed.append("BRAVE_API_KEY")
    if req.SUPABASE_URL is not None:
        config.set_key("SUPABASE_URL", req.SUPABASE_URL)
        changed.append("SUPABASE_URL")
    if req.SUPABASE_KEY is not None:
        config.set_key("SUPABASE_KEY", req.SUPABASE_KEY)
        changed.append("SUPABASE_KEY")
    return {"status": "ok", "updated": changed, "configured": config.is_configured()}


# ─────────────────────────────────────────────────────────────
# Static frontend
# ─────────────────────────────────────────────────────────────

_FRONTEND = Path(__file__).parent.parent / "frontend"


@app.get("/")
async def serve_frontend():
    index = _FRONTEND / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return HTMLResponse("<h2>Frontend not found</h2>", status_code=404)


if _FRONTEND.exists():
    app.mount("/static", StaticFiles(directory=str(_FRONTEND)), name="static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
