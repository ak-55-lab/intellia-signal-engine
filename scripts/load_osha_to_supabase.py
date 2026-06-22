#!/usr/bin/env python3
"""
One-time bulk load: reads OSHA CSV files → upserts into Supabase osha_inspections table.

Two modes:
  Default (--accounts):  Only loads records matching your 20 target accounts.
                         Results in ~5K-30K rows. Fits Supabase free tier easily.
  --all:                 Loads all CSV records (~5M rows). Needs Supabase paid plan
                         (free tier is 500MB; 5M rows ≈ 1GB+).

Usage:
  cd "C:\\Users\\vahit\\Claude\\Projects\\External Signal Agent"
  python scripts/load_osha_to_supabase.py              # account-filtered (recommended)
  python scripts/load_osha_to_supabase.py --all        # all records (large dataset)
  python scripts/load_osha_to_supabase.py --status     # check current row count

Prerequisites:
  pip install pandas supabase python-dotenv --break-system-packages
  OSHA CSV files must be in:  data/osha/*.csv
  .env file must have SUPABASE_URL and SUPABASE_KEY
"""

import sys
import os
import glob
import math
import time
from pathlib import Path
from datetime import datetime

# ── Setup paths ───────────────────────────────────────────────────────────────
script_dir = Path(__file__).parent
project_dir = script_dir.parent
sys.path.insert(0, str(project_dir / "backend"))

from dotenv import load_dotenv
load_dotenv(project_dir / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
CSV_DIR = project_dir / "data" / "osha"

COLS_NEEDED = [
    "ACTIVITY_NR", "ESTAB_NAME", "SITE_ADDRESS", "SITE_CITY", "SITE_STATE",
    "SITE_ZIP", "NAICS_CODE", "SIC_CODE", "INSP_TYPE", "OPEN_DATE",
    "CLOSE_CASE_DATE", "NR_IN_ESTAB", "OWNER_TYPE",
]


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_current_count(sb) -> int:
    try:
        resp = sb.table("osha_inspections").select("activity_nr", count="exact").limit(1).execute()
        return resp.count or 0
    except Exception as e:
        print(f"Count check failed: {e}")
        return -1


def _s(val) -> str:
    """Safe string extraction — handles NaN/None."""
    if val is None:
        return ""
    try:
        if isinstance(val, float) and math.isnan(val):
            return ""
    except Exception:
        pass
    return str(val).strip()


def _date(val) -> str | None:
    """Return ISO date string or None."""
    s = _s(val)
    if not s or s.lower() in ("nan", "nat", "none"):
        return None
    return s[:10] if len(s) >= 10 else None


def row_to_record(row, source: str = "csv") -> dict | None:
    """Convert a pandas row to an osha_inspections dict. Returns None if no activity_nr."""
    act_nr = _s(row.get("ACTIVITY_NR"))
    if not act_nr:
        return None
    return {
        "activity_nr": act_nr,
        "estab_name": _s(row.get("ESTAB_NAME")),
        "site_address": _s(row.get("SITE_ADDRESS")),
        "site_city": _s(row.get("SITE_CITY")),
        "site_state": _s(row.get("SITE_STATE")),
        "site_zip": _s(row.get("SITE_ZIP")),
        "naics_code": _s(row.get("NAICS_CODE")),
        "sic_code": _s(row.get("SIC_CODE")),
        "insp_type": _s(row.get("INSP_TYPE")),
        "open_date": _date(row.get("OPEN_DATE")),
        "close_case_date": _date(row.get("CLOSE_CASE_DATE")),
        "nr_in_estab": _s(row.get("NR_IN_ESTAB")),
        "owner_type": _s(row.get("OWNER_TYPE")),
        "data_source": source,
    }


def upsert_batch(sb, batch: list) -> int:
    """Upsert a batch of records. Returns count upserted."""
    try:
        sb.table("osha_inspections").upsert(batch, on_conflict="activity_nr").execute()
        return len(batch)
    except Exception as e:
        print(f"  Batch upsert failed ({len(batch)} rows): {e}")
        return 0


def get_account_terms() -> list[str]:
    """Load search terms from accounts.py."""
    from accounts import ACCOUNTS
    terms = []
    for acct in ACCOUNTS:
        for t in acct.get("osha_search", [acct["name"]]):
            terms.append(t.lower())
    return list(set(terms))


def load_all_mode(sb, batch_size: int = 500):
    """Load ALL CSV records into Supabase."""
    import pandas as pd

    csv_files = sorted(glob.glob(str(CSV_DIR / "*.csv")))
    if not csv_files:
        print(f"No CSV files found in {CSV_DIR}")
        sys.exit(1)

    print(f"Loading ALL records from {len(csv_files)} CSV file(s) into Supabase...")
    print("WARNING: This may exceed Supabase free tier (500MB). ~5M rows ≈ 1GB+")
    confirm = input("Continue? (y/N): ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    total_inserted = 0
    total_read = 0
    start = time.time()

    for idx, f in enumerate(csv_files, 1):
        print(f"\n[{idx}/{len(csv_files)}] {Path(f).name}")
        try:
            available = pd.read_csv(f, nrows=0).columns.tolist()
            use_cols = [c for c in COLS_NEEDED if c in available]
            df = pd.read_csv(f, usecols=use_cols, low_memory=False, dtype=str)
        except Exception as e:
            print(f"  Skipping — read error: {e}")
            continue

        batch = []
        for _, row in df.iterrows():
            rec = row_to_record(row)
            if rec:
                batch.append(rec)
                total_read += 1
            if len(batch) >= batch_size:
                total_inserted += upsert_batch(sb, batch)
                batch = []
                print(f"  {total_inserted:,} inserted so far…", end="\r")

        if batch:
            total_inserted += upsert_batch(sb, batch)

    elapsed = time.time() - start
    print(f"\n\nDone: {total_inserted:,} records in {elapsed:.0f}s")


def load_accounts_mode(sb, batch_size: int = 500):
    """Load only records matching target account search terms (recommended)."""
    import pandas as pd

    csv_files = sorted(glob.glob(str(CSV_DIR / "*.csv")))
    if not csv_files:
        print(f"No CSV files found in {CSV_DIR}")
        sys.exit(1)

    terms = get_account_terms()
    print(f"Account-filtered load: {len(terms)} search term(s) across {len(csv_files)} CSV file(s)")
    print(f"Terms: {', '.join(terms[:10])}{'...' if len(terms) > 10 else ''}\n")

    total_inserted = 0
    total_read = 0
    start = time.time()

    for idx, f in enumerate(csv_files, 1):
        print(f"[{idx}/{len(csv_files)}] {Path(f).name}", end=" … ")
        try:
            available = pd.read_csv(f, nrows=0).columns.tolist()
            use_cols = [c for c in COLS_NEEDED if c in available]
            df = pd.read_csv(f, usecols=use_cols, low_memory=False, dtype=str)
        except Exception as e:
            print(f"SKIP (error: {e})")
            continue

        # Filter to matching rows
        df["_name_lower"] = df["ESTAB_NAME"].fillna("").str.lower().str.strip()
        mask = df["_name_lower"].apply(lambda name: any(t in name for t in terms))
        matches = df[mask]

        if matches.empty:
            print("0 matches")
            continue

        batch = []
        for _, row in matches.iterrows():
            rec = row_to_record(row)
            if rec:
                batch.append(rec)
                total_read += 1

        if batch:
            inserted = upsert_batch(sb, batch)
            total_inserted += inserted
            print(f"{inserted} matched / inserted")
        else:
            print("0 valid records")

    elapsed = time.time() - start
    print(f"\nDone: {total_inserted:,} records inserted in {elapsed:.0f}s")
    print(f"Supabase table now has {get_current_count(sb):,} total rows")


def main():
    args = sys.argv[1:]

    print("=" * 60)
    print("Intellia Signal Engine — OSHA → Supabase Bulk Loader")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Set SUPABASE_URL and SUPABASE_KEY in your .env file.")
        sys.exit(1)

    sb = get_supabase()

    if "--status" in args:
        count = get_current_count(sb)
        print(f"osha_inspections table: {count:,} rows")
        return

    current = get_current_count(sb)
    print(f"Current osha_inspections rows: {current:,}")

    if "--all" in args:
        load_all_mode(sb)
    else:
        load_accounts_mode(sb)

    print("\nNext steps:")
    print("  1. Restart your FastAPI server")
    print("  2. The server will detect osha_inspections has data and use it as primary source")
    print("  3. Run POST /api/sync-osha periodically to add new inspections from DOL API")


if __name__ == "__main__":
    main()
