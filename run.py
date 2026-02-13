import argparse
import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.config import get_config
from src.exporter import run_export


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="MYS proposals exporter")
    parser.add_argument("--api-user", help="API username", default=None)
    parser.add_argument("--api-pass", help="API password", default=None)
    parser.add_argument("--show-code", help="Show code", default=None)
    parser.add_argument("--fields", help="Comma-separated fields to export", default=None)
    parser.add_argument("--out", help="Output CSV file", default=None)
    parser.add_argument("--schedule", help="Cron expression to schedule (quotes required)", default=None)
    parser.add_argument("--interval", type=int, help="Interval in seconds to run repeatedly", default=None)
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    args = parser.parse_args()

    cfg = get_config()
    api_username = args.api_user or cfg.get("api_username")
    api_password = args.api_pass or cfg.get("api_password")
    api_show_code = args.show_code or cfg.get("api_show_code")
    output = args.out or cfg.get("output_file")
    requested_fields = (args.fields.split(",") if args.fields else cfg.get("fields") or [])
    requested_fields = [f.strip() for f in requested_fields if f.strip()]
    schedule_cron = args.schedule or cfg.get("schedule_cron")
    schedule_interval = args.interval or cfg.get("schedule_interval")

    if not api_username:
        raise SystemExit("API username is required (env MYS_USERNAME or --api-user)")
    if not api_password:
        raise SystemExit("API password is required (env MYS_PASSWORD or --api-pass)")
    if not api_show_code:
        raise SystemExit("Show code is required (env MYS_SHOW_CODE or --show-code)")

    def job():
        run_export(api_username, api_password, api_show_code, output, requested_fields if requested_fields else None)

    if args.once or (not schedule_cron and not schedule_interval):
        job()
        return

    scheduler = BackgroundScheduler()
    if schedule_cron:
        # Expect a cron expression like: "0 2 * * *"
        parts = schedule_cron.strip().split()
        if len(parts) not in (5, 6):
            raise SystemExit("Cron expression must have 5 or 6 fields")
        # apscheduler CronTrigger accepts named args; pass expression via from_crontab
        scheduler.add_job(job, CronTrigger.from_crontab(schedule_cron))
    elif schedule_interval:
        scheduler.add_job(job, IntervalTrigger(seconds=schedule_interval))

    scheduler.start()
    logging.getLogger(__name__).info("Scheduler started. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
