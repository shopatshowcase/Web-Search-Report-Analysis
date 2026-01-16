"""
Batch processing for Assistant 2.
Uses Assistant 1 output folder as input.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from batch_processor import process_files
from chat_notifier import send_chat_message, format_batch_summary

# ============================================================================
# CONFIGURE YOUR PARAMETERS HERE
# ============================================================================

# Your message/instruction to the assistant
USER_MESSAGE = """
You will be provided with an Excel attachment containing web search report Key Words, where some records may already have Line and Item values populated, and a structured key-value dataset, where each key represents a Line and each value represents an Item belonging to that Line. Only process Key Words where both the "Line" and "Item" columns are currently blank; do not re-evaluate or modify any rows that already have a Line and/or Item assigned.
For each eligible blank Key Word, compare it against all available Item values across all Lines using strict fuzzy matching, with strong emphasis on handling spelling mistakes, including missing letters, extra letters, swapped characters, and spacing differences. If a relevant Item is confidently identified, assign that exact Item to the Key Word and derive the corresponding Line from the Item's parent key. Assign only one single Item per Key Word; do not combine, list, or concatenate multiple Items in any form. If more than one Item appears relevant, you must select only the single best (highest-confidence) match; if no clear single best Item exists, leave both Line and Item blank—never output multiple Items separated by /, ;, |, commas, or new lines. Never assign a Line directly if a better match exists at the Item level; Item-level matches always take precedence over Line-level matches. If none of the Items are relevant to the Key Word, keep both Line and Item columns blank.
Finally, generate an Excel file that includes all original rows from the input along with any newly mapped Line and Item values, preserving existing assignments unchanged, ensuring no input data is dropped or omitted and that every Key Word appears in the final output. Make sure you always provide an output excel file in the response.
"""

# Which assistant to use (assistant_1.json or assistant_2.json)
ASSISTANT_FILE = str(SCRIPT_DIR / "assistant_2.json")

def _resolve_data_path(*parts: str) -> str:
    return str(SCRIPT_DIR / "data" / Path(*parts))


run_date = os.getenv("RUN_DATE", "").strip()

# Folder containing your Excel files to process (Assistant 1 output)
# NOTE: Set RUN_DATE (YYYY-MM-DD) to use dated subfolders.
INPUT_FOLDER = os.getenv("INPUT_FOLDER") or (
    _resolve_data_path("assistant1_output", run_date)
    if run_date
    else _resolve_data_path("assistant1_output")
)

# Optional Line -> Items mapping text file (required for Assistant 2)
INCLUDE_MAPPING_FILE = True
MAPPING_FILE = str(SCRIPT_DIR / "Items_Grouped_By_Line_ALL_ROWS.txt")

# Optional: Custom name for the results summary file
OUTPUT_SUMMARY = os.getenv("OUTPUT_SUMMARY") or _resolve_data_path(
    "logs", "batch_results_assistant2.json"
)

# Where to save the OUTPUT Excel files downloaded from OpenAI
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER") or (
    _resolve_data_path("assistant2_output", run_date)
    if run_date
    else _resolve_data_path("assistant2_output")
)

# Batch mode should be stateless per-file (recommended for chunk processing)
USE_CONVERSATION = False

# ============================================================================
# RUN THE BATCH PROCESSING
# ============================================================================

if __name__ == "__main__":
    # Avoid UnicodeEncodeError on Windows cp1252 console.
    try:
        import sys
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    print("=" * 80)
    print("STARTING BATCH PROCESSING - ASSISTANT 2")
    print("=" * 80)
    print(f"Assistant: {ASSISTANT_FILE}")
    print(f"Input Folder: {INPUT_FOLDER}")
    print("=" * 80)

    try:
        # Process all files
        extra_attachments = [MAPPING_FILE] if INCLUDE_MAPPING_FILE else None
        results = process_files(
            user_message=USER_MESSAGE,
            assistant_json_file=ASSISTANT_FILE,
            input_folder=INPUT_FOLDER,
            output_summary_file=OUTPUT_SUMMARY,
            extra_attachments=extra_attachments,
            output_dir=OUTPUT_FOLDER,
            use_conversation=USE_CONVERSATION,
        )

        # Show summary
        print("\n" + "=" * 80)
        print("BATCH PROCESSING COMPLETED - ASSISTANT 2")
        print("=" * 80)

        successful = [r for r in results if r["status"] == "success"]
        failed = [r for r in results if r["status"] == "error"]

        print(f"Total files processed: {len(results)}")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")

        if failed:
            print("\nFailed files:")
            for result in failed:
                print(f"  FAIL {result['input_file']}: {result.get('error', 'Unknown error')}")

        print(f"\nResults saved to: {OUTPUT_SUMMARY}")
        print(f"Output Excel files saved in: {OUTPUT_FOLDER or 'current directory'}")
        print("=" * 80)

        message_lines = [
            "Batch processing acknowledgement - Assistant 2",
            f"Total files: {len(results)}",
            f"Successful: {len(successful)}",
            f"Failed: {len(failed)}",
            f"Summary JSON: {OUTPUT_SUMMARY}",
        ]
        if successful:
            message_lines.append("Successful files:")
            for r in successful[:50]:
                message_lines.append(f"  OK  {r['input_file']}")
            if len(successful) > 50:
                message_lines.append(f"  ...and {len(successful) - 50} more")
        if failed:
            message_lines.append("Failed files:")
            for r in failed[:50]:
                message_lines.append(f"  FAIL {r['input_file']}: {r.get('error', 'Unknown error')}")
            if len(failed) > 50:
                message_lines.append(f"  ...and {len(failed) - 50} more")

        message = "\n".join(message_lines)
        try:
            send_chat_message(message)
        except Exception as notify_error:
            print(f"Failed to send Google Chat message: {notify_error}")
    except Exception as run_error:
        error_message = f"Batch run failed: {run_error}"
        try:
            send_chat_message(
                format_batch_summary(
                    total=0,
                    successful=0,
                    failed=0,
                    summary_path=OUTPUT_SUMMARY,
                    error_message=error_message,
                )
            )
        except Exception as notify_error:
            print(f"Failed to send Google Chat message: {notify_error}")
        raise
