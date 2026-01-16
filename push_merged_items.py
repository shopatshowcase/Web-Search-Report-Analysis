"""
Push merged Excel rows to the WS Items API.

Default input file: automation/data/merged/merged_last_monday_YYYY-MM-DD.xlsx

Usage:
  python push_merged_items.py
  python push_merged_items.py --input "C:\path\merged.xlsx"
  python push_merged_items.py --base-url "https://host" --endpoint "/api/ws/items"
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chat_notifier import send_chat_message

import pandas as pd
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Disable SSL warnings (self-signed cert on the WS server)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_BASE_URL = "https://192.168.80.74"
DEFAULT_ENDPOINT = "/api/ws/items"
TARGET_COLUMNS: Tuple[str, str, str] = ("keyword", "U_line", "Item")


def last_monday(today: Optional[date] = None) -> date:
    today = today or date.today()
    return today - timedelta(days=today.weekday())


def _normalize_col(col: str) -> str:
    return "".join(str(col).strip().lower().split())


def _build_column_map(columns: Iterable[str]) -> Dict[str, str]:
    existing = list(columns)
    normalized = {_normalize_col(c): c for c in existing}

    keyword_candidates = [
        "keyword",
        "key word",
        "key_word",
        "key-word",
        "keyw",
        "keywrd",
        "keyword(s)",
    ]
    line_candidates = ["u_line", "uline", "line", "u line", "u-line"]
    item_candidates = ["item", "items"]

    def pick(cands: List[str]) -> str | None:
        for cand in cands:
            key = _normalize_col(cand)
            if key in normalized:
                return normalized[key]
        return None

    keyword_col = pick(keyword_candidates)
    line_col = pick(line_candidates)
    item_col = pick(item_candidates)

    mapping: Dict[str, str] = {}
    if keyword_col:
        mapping[keyword_col] = "keyword"
    if line_col:
        mapping[line_col] = "U_line"
    if item_col:
        mapping[item_col] = "Item"
    return mapping


def _clean_str_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .replace({"nan": "", "None": ""})
        .map(lambda x: x.strip() if isinstance(x, str) else x)
    )


def _chunk_list(items: List[dict], chunk_size: int) -> Iterable[List[dict]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _build_retry_session(
    total_retries: int,
    backoff_factor: float,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["PUT"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


@dataclass
class UploadResult:
    total_rows_in_excel: int
    valid_rows: int
    dropped_rows: int
    batches_sent: int
    received_total: int
    inserted_total: int


def push_items_from_excel(
    excel_path: str,
    base_url: str = DEFAULT_BASE_URL,
    endpoint: str = DEFAULT_ENDPOINT,
    sheet: str | int | None = None,
    batch_size: int = 300,
    timeout_seconds: int = 120,
    retries: int = 5,
    backoff_factor: float = 0.75,
    dry_run: bool = False,
    require_all_fields: bool = False,
    include_all_rows: bool = False,
    continue_on_error: bool = False,
) -> UploadResult:
    path = Path(excel_path).expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"[ERROR] Excel file not found: {path}")

    url = base_url.rstrip("/") + endpoint
    print("=" * 80)
    print("WS Items Bulk Uploader")
    print("=" * 80)
    print(f"Input Excel: {path}")
    print(f"URL: {url}")
    print(f"Batch size: {batch_size}")
    print(f"Retries: {retries} (backoff_factor={backoff_factor})")
    print(f"Timeout: {timeout_seconds}s")
    print(f"Dry run: {dry_run}")
    print("=" * 80)

    sheet_to_read = 0 if sheet is None else sheet
    try:
        df = pd.read_excel(path, sheet_name=sheet_to_read, dtype=str)
    except Exception as e:
        raise SystemExit(f"[ERROR] Failed to read Excel: {e}")

    total_rows = len(df)
    if df.empty:
        raise SystemExit("[ERROR] Excel sheet is empty.")

    col_map = _build_column_map(df.columns)
    renamed = df.rename(columns=col_map)

    missing = [c for c in TARGET_COLUMNS if c not in renamed.columns]
    if missing:
        raise SystemExit(
            "[ERROR] Missing required columns in Excel.\n"
            f"  Required: {', '.join(TARGET_COLUMNS)}\n"
            f"  Found: {', '.join(map(str, df.columns.tolist()))}"
        )

    out = pd.DataFrame()
    out["keyword"] = _clean_str_series(renamed["keyword"])
    out["U_line"] = _clean_str_series(renamed["U_line"])
    out["Item"] = _clean_str_series(renamed["Item"])

    all_empty = out["keyword"].eq("") & out["U_line"].eq("") & out["Item"].eq("")
    if include_all_rows:
        send_mask = pd.Series([True] * len(out))
    elif require_all_fields:
        send_mask = out["keyword"].ne("") & out["U_line"].ne("") & out["Item"].ne("")
    else:
        send_mask = ~all_empty

    to_send = out[send_mask].copy()
    dropped = total_rows - len(to_send)

    rows: List[dict] = to_send.to_dict(orient="records")
    print(f"Excel rows: {total_rows}")
    print(f"Rows to send: {len(rows)}")
    print(f"Skipped rows: {dropped}")
    print(
        "Blank-field counts (after cleaning): "
        f"keyword={int(out['keyword'].eq('').sum())}, "
        f"U_line={int(out['U_line'].eq('').sum())}, "
        f"Item={int(out['Item'].eq('').sum())}"
    )

    if not rows:
        raise SystemExit("[ERROR] No valid rows to upload after cleaning.")

    if dry_run:
        preview = {"rows": rows[: min(3, len(rows))]}
        print("-" * 80)
        print("[DRY RUN] Example payload preview (first up to 3 rows):")
        print(json.dumps(preview, indent=2))
        print("-" * 80)
        return UploadResult(
            total_rows_in_excel=total_rows,
            valid_rows=len(rows),
            dropped_rows=dropped,
            batches_sent=0,
            received_total=0,
            inserted_total=0,
        )

    session = _build_retry_session(total_retries=retries, backoff_factor=backoff_factor)
    headers = {"Content-Type": "application/json"}

    received_total = 0
    inserted_total = 0
    batches_sent = 0

    print("-" * 80)
    for idx, batch in enumerate(_chunk_list(rows, batch_size), start=1):
        payload = {"rows": batch}
        try:
            resp = session.put(url, json=payload, headers=headers, verify=False, timeout=timeout_seconds)
        except requests.exceptions.RequestException as e:
            msg = f"[ERROR] Batch {idx}: request failed after retries: {e}"
            if continue_on_error:
                print(msg)
                continue
            raise SystemExit(msg)

        if resp.status_code >= 400:
            body_preview = (resp.text or "")[:1000]
            msg = (
                f"[ERROR] Batch {idx}: HTTP {resp.status_code}\n"
                f"Response preview:\n{body_preview}"
            )
            if continue_on_error:
                print(msg)
                continue
            raise SystemExit(msg)

        batches_sent += 1

        received = 0
        inserted = 0
        try:
            data = resp.json()
            received = int(data.get("received", 0) or 0)
            inserted = int(data.get("inserted", 0) or 0)
        except Exception:
            data = None

        received_total += received
        inserted_total += inserted

        now = datetime.now().strftime("%H:%M:%S")
        if data is not None:
            print(
                f"[{now}] Batch {idx}: sent={len(batch)} status={resp.status_code} "
                f"received={received} inserted={inserted}"
            )
        else:
            print(
                f"[{now}] Batch {idx}: sent={len(batch)} status={resp.status_code} (non-JSON response)"
            )

    print("-" * 80)
    print("[SUCCESS] Upload complete.")
    print(f"Batches sent: {batches_sent}")
    print(f"Total sent rows: {len(rows)}")
    print(f"API received total: {received_total}")
    print(f"API inserted total: {inserted_total}")
    print("=" * 80)

    return UploadResult(
        total_rows_in_excel=total_rows,
        valid_rows=len(rows),
        dropped_rows=dropped,
        batches_sent=batches_sent,
        received_total=received_total,
        inserted_total=inserted_total,
    )

def push_with_existing_script(
    input_file: str,
    base_url: str,
    endpoint: str,
    batch_size: int,
    timeout_seconds: int,
    retries: int,
    backoff_factor: float,
    dry_run: bool,
    require_all_fields: bool,
    include_all_rows: bool,
    continue_on_error: bool,
) -> None:
    push_items_from_excel(
        excel_path=input_file,
        base_url=base_url,
        endpoint=endpoint,
        sheet=None,
        batch_size=batch_size,
        timeout_seconds=timeout_seconds,
        retries=retries,
        backoff_factor=backoff_factor,
        dry_run=dry_run,
        require_all_fields=require_all_fields,
        include_all_rows=include_all_rows,
        continue_on_error=continue_on_error,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push merged Excel rows to the WS Items API."
    )
    parser.add_argument("--input", default=None, help="Path to merged Excel file")
    parser.add_argument("--base-url", default="https://192.168.80.74", help="Base URL")
    parser.add_argument("--endpoint", default="/api/ws/items", help="Endpoint path")
    parser.add_argument("--batch-size", type=int, default=300, help="Rows per request")
    parser.add_argument("--timeout", type=int, default=120, help="Request timeout seconds")
    parser.add_argument("--retries", type=int, default=5, help="Retry count")
    parser.add_argument("--backoff", type=float, default=0.75, help="Retry backoff factor")
    parser.add_argument("--dry-run", action="store_true", help="Do not upload, print sample payload")
    parser.add_argument(
        "--require-all-fields",
        action="store_true",
        help="Only send rows where keyword, U_line, and Item are all non-empty",
    )
    parser.add_argument(
        "--include-all-rows",
        action="store_true",
        help="Send even fully-empty rows (default: skip fully-empty rows)",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with next batch if a batch fails",
    )
    args = parser.parse_args()

    target_date = last_monday()
    default_input = SCRIPT_DIR / "data" / "merged" / f"merged_last_monday_{target_date}.xlsx"

    input_file = str(Path(args.input).expanduser().resolve()) if args.input else str(default_input)

    try:
        push_with_existing_script(
            input_file=input_file,
            base_url=args.base_url,
            endpoint=args.endpoint,
            batch_size=args.batch_size,
            timeout_seconds=args.timeout,
            retries=args.retries,
            backoff_factor=args.backoff,
            dry_run=args.dry_run,
            require_all_fields=args.require_all_fields,
            include_all_rows=args.include_all_rows,
            continue_on_error=args.continue_on_error,
        )
        send_chat_message(
            "\n".join(
                [
                    "Push completed",
                    f"Input file: {input_file}",
                    f"Base URL: {args.base_url}",
                    f"Endpoint: {args.endpoint}",
                ]
            )
        )
    except Exception as e:
        try:
            send_chat_message(f"Push failed: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
