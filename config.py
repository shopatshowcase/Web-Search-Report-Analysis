"""
Configuration file for OpenAI Responses API integration
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY not found in environment variables")

# OpenAI Settings
OPENAI_API_VERSION = "v1"
OPENAI_BASE_URL = "https://api.openai.com/v1"

# Tool Calling Configuration
MAX_TOOL_ITERATIONS = 50  # Maximum tool call iterations
MAX_TOOL_CALLS = 40  # Maximum total tool calls per response

# Batch processing configuration
# How many files to process concurrently in batch mode.
# Keep this small to avoid rate limits; start with 2-5.
BATCH_WORKERS = int(os.getenv("BATCH_WORKERS", "1"))
