# Web Searches Report - Automated Pipeline

## Overview

This automation pipeline processes web search keywords weekly, mapping them to appropriate Line items and Items using OpenAI assistants, then pushing the results back to the server.

### Pipeline Flow

The complete workflow consists of 6 sequential steps:

1. **Pull & Filter** - Fetches keywords from API and filters for last Monday
2. **Split** - Divides the input file into manageable chunks
3. **Assistant 1** - Maps keywords to Lines using fuzzy matching
4. **Assistant 2** - Refines mappings and assigns specific Items
5. **Merge** - Combines all processed chunks into a single file
6. **Push** - Uploads the final results back to the server

All steps include Google Chat notifications for monitoring progress and status.

---

## Prerequisites

### System Requirements
- **OS**: Windows 10 or later
- **Python**: 3.8 or higher
- **Network**: Access to `192.168.80.74` API server

### Required Access
- OpenAI API key with GPT-5.2 access
- Google Chat webhook URL for notifications
- Server API credentials (if authentication required)

---

## Installation

### 1. Clone/Download Repository
Place this repository in your working directory.

### 2. Install Python Dependencies

Open PowerShell or Command Prompt in the project folder and run:

```bash
pip install -r requirements.txt
```

This installs:
- `pandas` - Data processing
- `requests` - HTTP API calls
- `openpyxl` - Excel file operations
- `python-dotenv` - Environment variable management
- `openai` - OpenAI API client
- `httpx` - Async HTTP client

### 3. Configure Environment Variables

Create a `.env` file in the project root folder with the following:

```env
# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here

# Google Chat Webhook
GOOGLE_CHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/.../messages?key=...&token=...

# Optional: Batch processing workers (default: 1)
BATCH_WORKERS=1
```

**⚠️ IMPORTANT**: Never commit the `.env` file to version control!

---

## Usage

### Running the Complete Pipeline

Simply execute the batch file:

```bash
run_weekly_pipeline.bat
```

Or with full path:

```bash
"Web Search Report Analysis\run_weekly_pipeline.bat"
```

The pipeline will:
- Automatically calculate last Monday's date
- Create dated folders for organization
- Process all steps sequentially
- Send notifications for each step
- Stop on any errors (check Google Chat for details)

### Running Individual Steps

You can run each step independently if needed:

#### 1. Pull and Filter
```bash
python pull_and_filter_last_monday.py
```

#### 2. Split Input
```bash
python split_input_excel.py --input "path\to\input.xlsx" --chunk-size 100 --output-dir "data\split\2026-01-13"
```

#### 3. Process with Assistant 1
```bash
set RUN_DATE=2026-01-13
set INPUT_FOLDER=data\split\2026-01-13
set OUTPUT_FOLDER=data\assistant1_output\2026-01-13
python run_batch_assistant1.py
```

#### 4. Process with Assistant 2
```bash
set RUN_DATE=2026-01-13
set INPUT_FOLDER=data\assistant1_output\2026-01-13
set OUTPUT_FOLDER=data\assistant2_output\2026-01-13
python run_batch_assistant2.py
```

#### 5. Merge Results
```bash
python merge_assistant2_output.py --input-folder "data\assistant2_output\2026-01-13" --output "data\merged\merged_last_monday_2026-01-13.xlsx"
```

#### 6. Push to Server
```bash
python push_merged_items.py --input "data\merged\merged_last_monday_2026-01-13.xlsx"
```

---

## Folder Structure

```
Web Search Report Analysis/
├── run_weekly_pipeline.bat         # Main orchestrator script
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables (create this!)
├── README.md                       # This file
├── Automation_Pipeline_Documentation.docx  # Detailed documentation
│
├── Python Scripts
├── pull_and_filter_last_monday.py  # Step 1: Pull & filter
├── split_input_excel.py            # Step 2: Split into chunks
├── run_batch_assistant1.py         # Step 3: OpenAI Assistant 1
├── run_batch_assistant2.py         # Step 4: OpenAI Assistant 2
├── merge_assistant2_output.py      # Step 5: Merge results
├── push_merged_items.py            # Step 6: Push to server
│
├── Core Dependencies
├── batch_processor.py              # Batch processing logic
├── main.py                         # OpenAI integration core
├── openai_service.py               # OpenAI service wrapper
├── config.py                       # Configuration loader
├── chat_notifier.py                # Google Chat notifications
│
├── Assistant Configurations
├── assistant_1.json                # OpenAI Assistant 1 config
├── assistant_2.json                # OpenAI Assistant 2 config
├── Items_Grouped_By_Line_ALL_ROWS.txt  # Line-Item mapping reference
│
└── data/                           # Auto-created during execution
    ├── input/                      # Raw filtered keywords
    │   └── YYYY-MM-DD/
    ├── split/                      # Chunked input files
    │   └── YYYY-MM-DD/
    ├── assistant1_output/          # Assistant 1 results
    │   └── YYYY-MM-DD/
    ├── assistant2_output/          # Assistant 2 results
    │   └── YYYY-MM-DD/
    ├── merged/                     # Final merged files
    └── logs/                       # Processing logs
```

---

## Configuration Details

### Assistant 1 Configuration

**Purpose**: Initial Line assignment using fuzzy matching

**User Message**:
> Check the attachment of Key words of web searches report. For each term, assign the most relevant Line item using fuzzy matching, ensuring that every Key Word receives a Line assignment. If you are not able to map a specific "Line" to an item, keep it blank. Once "Line" is mapped, you have to extract the exact item name from the Key words values...

**Settings**:
- Uses `assistant_1.json`
- No mapping file attached
- Processes split chunks independently

### Assistant 2 Configuration

**Purpose**: Refine mappings and assign specific Items

**User Message**:
> You will be provided with an Excel attachment containing web search report Key Words, where some records may already have Line and Item values populated, and a structured key-value dataset...

**Settings**:
- Uses `assistant_2.json`
- **Includes** `Items_Grouped_By_Line_ALL_ROWS.txt` with every request
- Only processes rows where Line and Item are blank
- Strict fuzzy matching for spelling variations

---

## Monitoring & Notifications

### Google Chat Alerts

Each step sends notifications with:
- ✅ **Success**: File paths, record counts, processing time
- ❌ **Failure**: Error message, failed step details

### Log Files

Processing logs are saved in:
- `data/logs/batch_results_assistant1.json`
- `data/logs/batch_results_assistant2.json`

---

## Troubleshooting

### Common Issues

#### 1. "OPENAI_API_KEY not found"
**Solution**: Create `.env` file with your API key

#### 2. "ModuleNotFoundError: No module named 'pandas'"
**Solution**: Run `pip install -r requirements.txt`

#### 3. "No rows found for last Monday"
**Solution**: Check if data exists on the server for that date

#### 4. API Connection Errors
**Solution**: 
- Verify network access to `192.168.80.74`
- Check if SSL certificate warnings need to be disabled
- Confirm API endpoint is accessible

#### 5. OpenAI Rate Limits
**Solution**: 
- Reduce `BATCH_WORKERS` in `.env` to `1`
- Add delays between requests if needed

#### 6. Chat Notification Failures
**Solution**: 
- Verify `GOOGLE_CHAT_WEBHOOK_URL` is correct
- Check webhook permissions
- Pipeline continues even if notifications fail

---

## Scheduling

### Windows Task Scheduler

To run automatically every Monday:

1. Open **Task Scheduler**
2. Create Basic Task → Name: "Weekly Keywords Processing"
3. Trigger: **Weekly** → Every Monday at your preferred time
4. Action: **Start a program**
   - Program: `C:\Users\YourUser\Desktop\...\Web Search Report Analysis\run_weekly_pipeline.bat`
   - Start in: `C:\Users\YourUser\Desktop\...\Web Search Report Analysis\`
5. Save and test

---

## Maintenance

### Regular Tasks

- **Weekly**: Review Google Chat notifications for any failures
- **Monthly**: Check `data/` folder size and archive old results
- **Quarterly**: Update `Items_Grouped_By_Line_ALL_ROWS.txt` if Line/Item catalog changes

### Updating Assistant Instructions

If you need to modify how assistants process data:

1. Update the `USER_MESSAGE` in `run_batch_assistant1.py` or `run_batch_assistant2.py`
2. Optionally update assistant configurations in `assistant_1.json` / `assistant_2.json`
3. Test with a small dataset before full run

---

## Security Notes

- Keep `.env` file secure and never commit to Git
- Add `.env` to `.gitignore`
- Rotate API keys periodically
- Restrict access to the project folder
- Consider encrypting sensitive configuration files

---

## Support

For issues or questions:
1. Check Google Chat notifications for specific error details
2. Review log files in `data/logs/`
3. Verify all prerequisites are met
4. Test individual steps to isolate issues

---

## Version History

- **v1.0** (January 2026) - Initial automated pipeline release
  - Full end-to-end automation
  - Dual OpenAI assistant processing
  - Google Chat integration
  - Date-based folder organization

---

## License

Internal use only. Do not distribute without authorization.
