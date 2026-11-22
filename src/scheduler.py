"""
Scheduler for UKCS Intelligence Platform.

Jobs:
  - Daily (06:00 UTC): prices, NSTA news, LSE RNS, then briefings.
  - Monthly (1st of month, 07:00 UTC): NSTA production refresh for the current year.

GitHub Actions (.github/workflows) is what actually runs these on a schedule.
`python -m src.scheduler --job serve` is only for a long-lived process.

Each job exits non-zero if a step fails. Failures used to be logged and then
ignored, so a workflow could go green without writing anything.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _run_step(name: str, fn) -> str | None:
    try:
        fn()
        return None
    except Exception as exc:
        logger.error("%s failed: %s", name, exc)
        return f"{name}: {exc}"


def job_daily() -> int:
    """Prices, news, RNS, then briefings. Returns 0 only if every step succeeded."""
    from src.intelligence import briefings
    from src.scrapers import investegate, market_data, nsta_news

    logger.info("=== Daily job started %s ===", datetime.now(timezone.utc).isoformat())
    errors = [
        err
        for err in (
            _run_step("Market data", market_data.run),
            _run_step("NSTA news", nsta_news.run),
            _run_step("RNS announcements", investegate.run),
            _run_step("Briefings", lambda: briefings.run(tickers=None)),
        )
        if err
    ]
    if errors:
        logger.error("=== Daily job finished with %d failure(s) ===", len(errors))
        return 1
    logger.info("=== Daily job complete ===")
    return 0


def job_monthly() -> int:
    """Current-year NSTA production refresh. Returns 0 only if rows were written."""
    from src.scrapers import nsta_production

    logger.info("=== Monthly production refresh started %s ===", datetime.now(timezone.utc).isoformat())
    try:
        n = nsta_production.run()
    except Exception as exc:
        logger.error("Production refresh failed: %s", exc)
        return 1
    logger.info("Production refresh: %d records upserted", n)
    if not n:
        logger.error("Production refresh wrote 0 rows")
        return 1
    logger.info("=== Monthly production refresh complete ===")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="UKCS Intelligence Platform scheduler")
    parser.add_argument(
        "--job",
        choices=["daily", "monthly", "serve"],
        default="serve",
        help="Run a single job immediately, or 'serve' (default) to run the scheduler daemon",
    )
    args = parser.parse_args()

    if args.job == "daily":
        sys.exit(job_daily())
    elif args.job == "monthly":
        sys.exit(job_monthly())
    else:
        scheduler = BlockingScheduler(timezone="UTC")
        scheduler.add_job(job_daily, CronTrigger(hour=6, minute=0), id="daily")
        scheduler.add_job(job_monthly, CronTrigger(day=1, hour=7, minute=0), id="monthly")

        logger.info("Scheduler started. Daily at 06:00 UTC, monthly on 1st at 07:00 UTC.")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
