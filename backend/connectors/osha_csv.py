"""
CSV-based OSHA connector — loads all *.csv files from data/osha/ at startup.
These are OSHA inspection records downloaded directly from the DOL data portal.
Provides reliable entity-specific querying that the DOL REST API cannot do.
"""

import os
import glob
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

def _s(row, col: str) -> str:
    """Safe string extraction from a pandas row — handles NaN/None."""
    v = row.get(col)
    if v is None:
        return ""
    try:
        import math
        if isinstance(v, float) and math.isnan(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


# ── Module-level state ────────────────────────────────────────────────────────
_df = None          # pandas DataFrame, loaded once
_load_lock = threading.Lock()
_load_error: Optional[str] = None

COLS_NEEDED = [
    "ACTIVITY_NR", "ESTAB_NAME", "SITE_ADDRESS", "SITE_CITY", "SITE_STATE",
    "SITE_ZIP", "NAICS_CODE", "SIC_CODE", "INSP_TYPE", "OPEN_DATE",
    "CLOSE_CASE_DATE", "NR_IN_ESTAB", "OWNER_TYPE",
]

def _csv_dir() -> Path:
    return Path(__file__).parent.parent.parent / "data" / "osha"


def load_csv_data() -> bool:
    """Load all CSVs from data/osha/ into memory. Call at startup. Thread-safe."""
    global _df, _load_error
    with _load_lock:
        if _df is not None:
            return True
        d = _csv_dir()
        files = sorted(glob.glob(str(d / "*.csv")))
        if not files:
            _load_error = f"No CSV files found in {d}. Copy OSHA inspection CSV files there."
            print(f"[osha_csv] WARNING: {_load_error}")
            return False
        try:
            import pandas as pd
            print(f"[osha_csv] Loading {len(files)} CSV file(s) from {d} …")
            dfs = []
            for f in files:
                try:
                    available = pd.read_csv(f, nrows=0).columns.tolist()
                    use_cols = [c for c in COLS_NEEDED if c in available]
                    df = pd.read_csv(f, usecols=use_cols, low_memory=False, dtype=str)
                    dfs.append(df)
                except Exception as e:
                    print(f"[osha_csv] Skipping {f}: {e}")
            if not dfs:
                _load_error = "All CSV files failed to load."
                return False
            _df = pd.concat(dfs, ignore_index=True)
            # Normalize ESTAB_NAME for fast search
            _df["_name_lower"] = _df["ESTAB_NAME"].fillna("").str.lower().str.strip()
            # Parse dates once
            _df["_open_dt"] = pd.to_datetime(_df["OPEN_DATE"], errors="coerce", utc=True)
            total = len(_df)
            print(f"[osha_csv] Loaded {total:,} inspection records across {len(files)} file(s)")
            return True
        except ImportError:
            _load_error = "pandas not installed — run: pip install pandas --break-system-packages"
            print(f"[osha_csv] {_load_error}")
            return False
        except Exception as e:
            _load_error = str(e)
            print(f"[osha_csv] Load error: {e}")
            return False


def is_loaded() -> bool:
    return _df is not None


def csv_status() -> Dict[str, Any]:
    d = _csv_dir()
    files = sorted(glob.glob(str(d / "*.csv")))
    return {
        "loaded": _df is not None,
        "csv_dir": str(d),
        "csv_files_found": len(files),
        "records_in_memory": len(_df) if _df is not None else 0,
        "error": _load_error,
    }


def search_osha_csv(
    search_terms: List[str],
    days_back: int = 1825,
    limit_per_term: int = 50,
) -> List[Dict[str, Any]]:
    """
    Search in-memory OSHA DataFrame. Returns matching inspection records.
    search_terms: list of strings to search in ESTAB_NAME (case-insensitive contains).
    """
    if _df is None:
        return []

    import pandas as pd
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_back)

    seen_ids = set()
    results = []

    for term in search_terms:
        term_lower = term.lower().strip()
        # Filter by name
        mask = _df["_name_lower"].str.contains(term_lower, na=False, regex=False)
        # Filter by date
        mask &= _df["_open_dt"] >= cutoff
        matches = _df[mask].head(limit_per_term)

        for _, row in matches.iterrows():
            act_nr = _s(row, "ACTIVITY_NR")
            if not act_nr or act_nr in seen_ids:
                continue
            seen_ids.add(act_nr)
            results.append({
                "activity_nr": act_nr,
                "estab_name": _s(row, "ESTAB_NAME"),
                "site_address": _s(row, "SITE_ADDRESS"),
                "site_city": _s(row, "SITE_CITY"),
                "site_state": _s(row, "SITE_STATE"),
                "site_zip": _s(row, "SITE_ZIP"),
                "naics_code": _s(row, "NAICS_CODE"),
                "insp_type": _s(row, "INSP_TYPE"),
                "open_date": _s(row, "OPEN_DATE")[:10],
                "close_case_date": _s(row, "CLOSE_CASE_DATE")[:10],
                "nr_in_estab": _s(row, "NR_IN_ESTAB"),
                "owner_type": _s(row, "OWNER_TYPE"),
                "_source": "csv",
            })

    # Sort most recent first
    results.sort(key=lambda r: r.get("open_date", ""), reverse=True)
    print(f"[osha_csv] {len(results)} record(s) found for terms: {search_terms}")
    return results


def format_csv_osha_for_scoring(inspections: List[Dict[str, Any]]) -> str:
    if not inspections:
        return "No OSHA inspections found in the specified date range."

    lines = [f"OSHA Inspection Records — {len(inspections)} result(s) (CSV source):\n"]
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
