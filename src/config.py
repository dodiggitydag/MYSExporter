import os
from dotenv import load_dotenv

load_dotenv()

def get_config():
    return {
        "api_url": os.getenv("MYS_API_BASE_URL"),
        "api_username": os.getenv("MYS_USERNAME"),
        "api_password": os.getenv("MYS_PASSWORD"),
        "api_show_code": os.getenv("MYS_SHOW_CODE"),
        "output_file": os.getenv("OUTPUT_FILE", "proposals.csv"),
        "fields": [f.strip() for f in os.getenv("FIELDS", "").split(",") if f.strip()],
        "schedule_cron": os.getenv("SCHEDULE_CRON", "").strip() or None,
        "schedule_interval": int(os.getenv("SCHEDULE_INTERVAL")) if os.getenv("SCHEDULE_INTERVAL") else None,
    }
