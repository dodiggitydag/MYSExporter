# Community Summit MYS Exporter

Small Python tool to fetch proposals from a MYS API, filter fields, redact emails, and export CSV.

Features
- Fetch JSON from an API endpoint (supports Bearer token)
- Detect available fields and optionally filter by requested fields
- Remove any columns named like `email` and redact email-like values
- Export results to CSV
- Optional scheduled runs via APScheduler (cron or interval)

Quick start

1. Create a virtualenv and install deps:

```bash
python -m venv .venv
.venv\\Scripts\\activate  # Windows
pip install -r requirements.txt
```

2. Copy `.env.example` → `.env` and set `API_URL` and `API_KEY`.

3. Run once:

```bash
python run.py --once
```

4. Run on a schedule (cron expression):

```bash
python run.py --schedule "0 2 * * *"
```

Or use `--interval 3600` to run every hour.

Files
- **src/exporter.py**: core export logic
- **src/config.py**: environment/config loader
- **run.py**: CLI and scheduler
