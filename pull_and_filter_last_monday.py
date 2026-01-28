"""
Pull keywords from API and save the last two weeks of rows to Excel.

This script combines the get_keywords.py pull step and the
last-two-weeks filter into a single run.

Usage:
  python pull_and_filter_last_monday.py
  python pull_and_filter_last_monday.py --output "C:/path/keywords_last_2_weeks.xlsx"
  python pull_and_filter_last_monday.py --url "https://host/api/ws/keywords"
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List

import pandas as pd
import requests
import urllib3

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chat_notifier import send_chat_message

# Disable SSL warnings (self-signed cert on the WS server)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_URL = "https://192.168.80.74/api/ws/keywords"


def _normalize_col(col: str) -> str:
    return "".join(str(col).strip().lower().split())


def _resolve_column(df: pd.DataFrame, preferred: str, candidates: List[str]) -> Optional[str]:
    normalized = {_normalize_col(c): c for c in df.columns.tolist()}
    for cand in [preferred] + candidates:
        key = _normalize_col(cand)
        if key in normalized:
            return normalized[key]
    return None


def last_monday(today: Optional[date] = None) -> date:
    """
    Return the date of last week's Monday (excluding today even if today is Monday).
    """
    today = today or date.today()
    days_since_monday = today.weekday()
    # If today is Monday (weekday = 0), go back 7 days to get last week's Monday
    if days_since_monday == 0:
        return today - timedelta(days=7)
    else:
        return today - timedelta(days=days_since_monday)


def pull_keywords(url: str, timeout_seconds: int = 120) -> pd.DataFrame:
    print("Fetching data from API...")
    print(f"URL: {url}")
    response = requests.get(url, verify=False, timeout=timeout_seconds)
    response.raise_for_status()

    data = response.json()
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        for key in ("data", "results", "keywords"):
            if key in data and isinstance(data[key], list):
                return pd.DataFrame(data[key])
        # Fallback: store top-level dict as a single row
        return pd.DataFrame([data])
    return pd.DataFrame([{"data": str(data)}])


def pull_and_filter_last_monday(
    url: str,
    output_path: Optional[str] = None,
    date_column: str = "DDate",
    keyword_column: str = "KeyWord",
) -> Path:
    df = pull_keywords(url)
    if df.empty:
        raise SystemExit("[ERROR] API returned no data.")

    date_col = _resolve_column(df, date_column, candidates=["date", "ddate", "d date"])
    if not date_col:
        raise SystemExit(f"[ERROR] Date column not found. Expected: {date_column}")

    keyword_col = _resolve_column(df, keyword_column, candidates=["keyword", "key word", "key_word"])
    if not keyword_col:
        raise SystemExit(f"[ERROR] Keyword column not found. Expected: {keyword_column}")

    parsed_dates = pd.to_datetime(df[date_col], errors="coerce").dt.date
    end_date = last_monday()
    start_date = end_date - timedelta(days=13)
    mask = (parsed_dates >= start_date) & (parsed_dates <= end_date)
    filtered = df.loc[mask].copy()

    if filtered.empty:
        raise SystemExit(
            f"[ERROR] No rows found for the last 2 weeks ({start_date} to {end_date}). "
            f"Check the {date_col} values in the API response."
        )

    # Drop the DDate column as it's only needed for filtering, not for processing
    if date_col in filtered.columns:
        filtered = filtered.drop(columns=[date_col])

    default_dir = SCRIPT_DIR / "data" / "input" / end_date.strftime("%Y-%m-%d")

    if output_path:
        out_file = Path(output_path).expanduser().resolve()
        if out_file.exists() and out_file.is_dir():
            out_file = out_file / f"keywords_last_2_weeks_{end_date}.xlsx"
    else:
        out_file = default_dir / f"keywords_last_2_weeks_{end_date}.xlsx"

    out_file.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_excel(out_file, index=False, sheet_name="Keywords")

    print("=" * 80)
    print("PULL + LAST 2 WEEKS FILTER COMPLETED")
    print("=" * 80)
    print(f"Output file: {out_file}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total rows pulled: {len(df)}")
    print(f"Filtered rows: {len(filtered)}")
    print("=" * 80)

    return out_file


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pull keywords from API and save last two weeks of rows to Excel."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="API URL for keywords")
    parser.add_argument("--output", default=None, help="Output Excel file or directory")
    parser.add_argument(
        "--date-column",
        default="DDate",
        help='Date column name (default: "DDate")',
    )
    parser.add_argument(
        "--keyword-column",
        default="KeyWord",
        help='Keyword column name (default: "KeyWord")',
    )
    args = parser.parse_args()

    # Ensure Windows console can print reliably
    try:
        import sys
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    try:
        output_file = pull_and_filter_last_monday(
            url=args.url,
            output_path=args.output,
            date_column=args.date_column,
            keyword_column=args.keyword_column,
        )
        send_chat_message(
            "\n".join(
                [
                    "Pull + filter completed (last 2 weeks)",
                    f"Output file: {output_file}",
                ]
            )
        )
    except Exception as e:
        try:
            send_chat_message(f"Pull + filter failed: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
