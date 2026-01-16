"""
Split a single input Excel file into chunk files.

Usage:
  python split_input_excel.py --input "C:\path\keywords_last_monday.xlsx"
  python split_input_excel.py --input "C:\path\keywords_last_monday.xlsx" --chunk-size 200
  python split_input_excel.py --input "C:\path\keywords_last_monday.xlsx" --output-dir "C:\path\split\2026-01-20"
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chat_notifier import send_chat_message

def split_excel_into_chunks(input_file: str, output_dir: str, chunk_size: int = 100) -> Path:
    input_path = Path(input_file).expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f"[ERROR] Input Excel file not found: {input_path}")

    output_path = Path(output_dir).expanduser().resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Excel File Splitter")
    print("=" * 60)
    print(f"\nReading file: {input_path}")

    df = pd.read_excel(input_path)
    total_rows = len(df)
    if total_rows == 0:
        raise SystemExit("[ERROR] Input Excel sheet is empty.")

    print(f"Total rows: {total_rows}")
    print(f"Columns: {', '.join(df.columns.tolist())}")
    print(f"Chunk size: {chunk_size} rows")

    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    print(f"Number of files to create: {num_chunks}")
    print(f"\nOutput directory: {output_path}")

    print("\nSplitting file...")
    print("-" * 60)

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_rows)

        chunk_df = df.iloc[start_idx:end_idx]
        filename = f"keywords_chunk_{i+1:03d}_rows_{start_idx+1}-{end_idx}.xlsx"
        filepath = output_path / filename
        chunk_df.to_excel(filepath, index=False, sheet_name="Keywords")
        print(f"[{i+1:3d}/{num_chunks}] Created: {filename} ({len(chunk_df)} rows)")

    print("-" * 60)
    print("\n[SUCCESS] Split complete!")
    print(f"Created {num_chunks} files in: {output_path}")
    print("\nSummary:")
    print(f"  - Original file: {input_path}")
    print(f"  - Total rows: {total_rows}")
    print(f"  - Rows per file: {chunk_size}")
    print(f"  - Files created: {num_chunks}")
    print(f"  - Output folder: {output_path}")

    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split an Excel file into chunked Excel files."
    )
    parser.add_argument("--input", required=True, help="Path to input Excel file")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Rows per chunk (default: 100)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write split files (default: data\\split\\YYYY-MM-DD)",
    )
    args = parser.parse_args()

    if args.output_dir:
        output_dir = args.output_dir
    else:
        today_str = date.today().strftime("%Y-%m-%d")
        output_dir = str(SCRIPT_DIR / "data" / "split" / today_str)

    try:
        output_path = split_excel_into_chunks(
            input_file=args.input,
            output_dir=output_dir,
            chunk_size=args.chunk_size,
        )
        send_chat_message(
            "\n".join(
                [
                    "Split completed",
                    f"Input file: {Path(args.input).expanduser().resolve()}",
                    f"Output folder: {output_path}",
                    f"Chunk size: {args.chunk_size}",
                ]
            )
        )
    except Exception as e:
        try:
            send_chat_message(f"Split failed: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
