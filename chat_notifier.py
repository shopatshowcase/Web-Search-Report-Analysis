"""
Google Chat webhook notifier for batch acknowledgements.
Reads webhook URL from .env via GOOGLE_CHAT_WEBHOOK_URL.
"""
import json
import os
import urllib.request
import logging
from typing import Optional

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def send_chat_message(text: str, webhook_url: Optional[str] = None) -> None:
    """
    Send a plain text message to Google Chat via webhook.
    """
    load_dotenv()
    url = webhook_url or os.getenv("GOOGLE_CHAT_WEBHOOK_URL")
    if not url:
        raise ValueError("GOOGLE_CHAT_WEBHOOK_URL is not set")

    payload = {"text": text}
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        _ = response.read()
        logger.info("Google Chat message sent (status: %s)", response.status)


def format_batch_summary(
    total: int,
    successful: int,
    failed: int,
    summary_path: Optional[str] = None,
    error_message: Optional[str] = None,
) -> str:
    """
    Build a simple acknowledgement message for batch runs.
    """
    lines = [
        "Batch processing acknowledgement",
        f"Total files: {total}",
        f"Successful: {successful}",
        f"Failed: {failed}",
    ]
    if summary_path:
        lines.append(f"Summary JSON: {summary_path}")
    if error_message:
        lines.append(f"Error: {error_message}")
    return "\n".join(lines)
