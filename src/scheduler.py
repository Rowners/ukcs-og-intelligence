"""
Scheduler for UKCS Intelligence Platform.

Jobs:
  - Daily (06:00 UTC): NSTA news scrape, Investegate RNS scrape,
                       then generate briefings for companies with new data.
  - Monthly (1st of month, 07:00 UTC): Full NSTA production refresh.

Run as a long-lived process (e.g. via systemd or Docker):
    python -m src.scheduler

Or invoke individual jobs directly:
    python -m src.scheduler --job daily
    python -m src.scheduler --job monthly
"""

import argparse
import logging
import sys
from datetime import datetime

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


def job_daily() -> None:
    """Daily: market data, news + RNS scrape, then generate briefings for companies with new data."""
    from src.scrapers import investegate, nsta_news, market_data
    from src.intelligence import briefings
    from src.databricks.client import query

    logger.info("=== Daily job started %s ===", datetime.utcnow().isoformat())

    # 1. Market data (Brent + share prices)
    try:
        market_data.run()
        logger.info("Market data updated")
    except Exception as exc:
        logger.error("Market data scrape failed: %s", exc)

    # 2. Scrape NSTA news
    try:
        n = nsta_news.run()
        logger.info("NSTA news: %d records upserted", n)
    except Exception as exc:
        logger.error("NSTA news scrape failed: %s", exc)

    # 3. Scrape Investegate RNS
    try:
        n = investegate.run()
        logger.info("Investegate RNS: %d records upserted", n)
    except Exception as exc:
        logger.error("Investegate scrape failed: %s", exc)

    # 3. Identify companies that have new RNS data since last briefing
    try:
        rows = query("""
            SELECT DISTINCT r.ticker
            FROM rns_announcements_raw r
            LEFT JOIN company_briefings b ON UPPER(r.ticker) = UPPER(b.ticker)
            WHERE r.scraped_at > COALESCE(b.generated_at, '1900-01-01')
        """)
        tickers_with_new_data = [r["ticker"] for r in rows]
    except Exception as exc:
        logger.warning("Could not determine tickers with new data (%s); regenerating all", exc)
        tickers_with_new_data = None

    # 4. Generate briefings
    try:
        briefings.run(tickers=tickers_with_new_data)
    except Exception as exc:
        logger.error("Briefing generation failed: %s", exc)

    logger.info("=== Daily job complete ===")


def job_monthly() -> None:
    """Monthly: full NSTA production data refresh."""
    from src.scrapers import nsta_production

    logger.info("=== Monthly production refresh started %s ===", datetime.utcnow().isoformat())
    try:
        n = nsta_production.run()
        logger.info("Production refresh: %d records upserted", n)
    except Exception as exc:
        logger.error("Production refresh failed: %s", exc)

    logger.info("=== Monthly production refresh complete ===")


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
        job_daily()
    elif args.job == "monthly":
        job_monthly()
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
