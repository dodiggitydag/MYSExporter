# MYS Exporter

Small Python tool to fetch session proposals from a MYS API, filter fields, redact emails, and export CSV.

Features
- Fetch JSON from a MYS API endpoint with Basic authentication (username & password)
- Detect available fields and optionally filter by requested fields
- Remove any columns named like `email` and redact email-like values
- Export results to XLSX
- Optional scheduled runs via APScheduler (cron or interval)
- You can run this multiple times and it will update OUTPUT_FILE with the latest data from the API and ignore the user-created columns

Quick start

1. Create a virtualenv and install deps:

```bash
python -m venv .venv
.venv\\Scripts\\activate  # Windows
pip install -r requirements.txt
```

2. Copy `.env.example` → `.env` and configure the environment variables:

   **Required**
   - `MYS_USERNAME`: API username
   - `MYS_PASSWORD`: API password
   - `MYS_SHOW_CODE`: Show code for the conference

   **Export**
   - `OUTPUT_FILE`: Path to the output Excel file (default: `proposals.xlsx`)
   - `FIELDS`: Comma-separated list of fields to include. If empty, all available fields except emails are exported.

   **Scheduling** (optional — use one or neither)
   - `SCHEDULE_CRON`: Cron expression for scheduled runs (e.g. `0 2 * * *` for 2 AM daily)
   - `SCHEDULE_INTERVAL`: Interval in seconds between runs (e.g. `3600` for hourly)

3. Run once:

```bash
python run.py --once
```

---

# Other Useful Commands

## Run on a schedule (cron expression):

```bash
python run.py --schedule "0 2 * * *"
```

Or use `--interval 3600` to run every hour.

Files
- **src/exporter.py**: core export logic
- **src/merger.py**: logic to merge latest data into existing file
- **src/config.py**: environment/config loader
- **run.py**: CLI and scheduler

## Run once (again)

From a fresh PowerShell or CommandPrompt:

```bash
.venv\\Scripts\\activate  # Windows
python run.py --once
```
