"""
Merge Assistant 2 output Excel files into a single Excel file.

Default input folder: automation/data/assistant2_output
Default output file: automation/data/merged/merged_last_monday_YYYY-MM-DD.xlsx

Usage:
  python merge_assistant2_output.py
  python merge_assistant2_output.py --input-folder "C:\path\assistant2_output"
  python merge_assistant2_output.py --output "C:\path\merged.xlsx"
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chat_notifier import send_chat_message

import pandas as pd


def last_monday(today: Optional[date] = None) -> date:
    today = today or date.today()
    return today - timedelta(days=today.weekday())


TARGET_COLUMNS: Tuple[str, str, str] = ("keyword", "U_line", "Item")


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
        "keywor d",
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


def _iter_excel_files(folder: Path, recursive: bool) -> List[Path]:
    patterns = ["*.xlsx", "*.xlsm", "*.xls"]
    files: List[Path] = []
    for pat in patterns:
        files.extend(folder.rglob(pat) if recursive else folder.glob(pat))

    cleaned: List[Path] = []
    for f in files:
        name = f.name.lower()
        if name.startswith("~$"):
            continue
        if name in {"merged.xlsx", "merged_output.xlsx"}:
            continue
        cleaned.append(f)

    cleaned.sort(key=lambda p: str(p).lower())
    return cleaned


def merge_folder(folder_path: str, output_path: str | None = None, recursive: bool = False) -> Path:
    folder = Path(folder_path).expanduser().resolve()
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"[ERROR] Folder not found or not a directory: {folder}")

    excel_files = _iter_excel_files(folder, recursive=recursive)
    if not excel_files:
        raise SystemExit(f"[ERROR] No Excel files found in: {folder}")

    merged_frames: List[pd.DataFrame] = []
    skipped: List[Tuple[Path, str]] = []

    for file_path in excel_files:
        try:
            df = pd.read_excel(file_path, dtype=str)
        except Exception as e:
            skipped.append((file_path, f"read failed: {e}"))
            continue

        if df.empty:
            skipped.append((file_path, "empty sheet"))
            continue

        col_map = _build_column_map(df.columns)

        out = pd.DataFrame(index=range(len(df)), columns=list(TARGET_COLUMNS))
        for c in TARGET_COLUMNS:
            out[c] = ""
        renamed = df.rename(columns=col_map)

        if "keyword" in renamed.columns:
            out["keyword"] = renamed["keyword"].fillna("").astype(str)
        if "U_line" in renamed.columns:
            out["U_line"] = renamed["U_line"].fillna("").astype(str)
        if "Item" in renamed.columns:
            out["Item"] = renamed["Item"].fillna("").astype(str)

        if out["keyword"].replace({"nan": ""}).astype(str).str.strip().eq("").all():
            skipped.append((file_path, "missing keyword column"))
            continue

        for c in TARGET_COLUMNS:
            out[c] = out[c].replace({"nan": "", "None": ""}).astype(str)

        merged_frames.append(out)

    if not merged_frames:
        details = "\n".join([f"  - {p}: {reason}" for p, reason in skipped[:50]])
        raise SystemExit(f"[ERROR] No valid chunk files to merge in: {folder}\n{details}")

    merged = pd.concat(merged_frames, ignore_index=True)

    if not output_path:
        output_file = folder / "merged.xlsx"
    else:
        output_file = Path(output_path).expanduser().resolve()
        if output_file.exists() and output_file.is_dir():
            output_file = output_file / "merged.xlsx"

    output_file.parent.mkdir(parents=True, exist_ok=True)
    merged.to_excel(output_file, index=False, sheet_name="Merged")

    print("=" * 80)
    print("MERGE COMPLETED")
    print("=" * 80)
    print(f"Input folder: {folder}")
    print(f"Excel files found: {len(excel_files)}")
    print(f"Files merged: {len(merged_frames)}")
    print(f"Rows merged: {len(merged)}")
    print(f"Output: {output_file}")
    if skipped:
        print("-" * 80)
        print(f"Skipped files: {len(skipped)} (showing up to 30)")
        for p, reason in skipped[:30]:
            print(f"  - {p.name}: {reason}")
    print("=" * 80)

    return output_file


def merge_with_existing_script(input_folder: str, output_path: str) -> Path:
    return merge_folder(input_folder, output_path=output_path, recursive=False)

    return merge_folder(input_folder, output_path=output_path, recursive=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge Assistant 2 output Excel files into one Excel file."
    )
    parser.add_argument(
        "--input-folder",
        default=None,
        help="Folder containing Assistant 2 output Excel files",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Excel file path",
    )
    args = parser.parse_args()

    default_input = SCRIPT_DIR / "data" / "assistant2_output"
    target_date = last_monday()
    default_output = SCRIPT_DIR / "data" / "merged" / f"merged_last_monday_{target_date}.xlsx"

    input_folder = str(Path(args.input_folder).expanduser().resolve()) if args.input_folder else str(default_input)
    output_path = str(Path(args.output).expanduser().resolve()) if args.output else str(default_output)

    try:
        output_file = merge_with_existing_script(input_folder, output_path)
        print(f"[SUCCESS] Merged file created: {output_file}")
        send_chat_message(
            "\n".join(
                [
                    "Merge completed",
                    f"Input folder: {input_folder}",
                    f"Output file: {output_file}",
                ]
            )
        )
    except Exception as e:
        try:
            send_chat_message(f"Merge failed: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
