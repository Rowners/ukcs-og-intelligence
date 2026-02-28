"""
Fetch Brent crude prices and LSE share prices via yfinance.

Tables written:
  brent_crude_prices   — daily OHLCV for ICE Brent (BZ=F), USD/bbl
  company_share_prices — daily OHLCV for each watchlist company, GBp (pence)

Default behaviour: fetches last 90 days (suitable for daily scheduler runs).
Use --history to backfill 5 years.
"""

import logging
from datetime import datetime, timedelta, date

import yfinance as yf

from src.databricks.client import upsert_market_data

logger = logging.getLogger(__name__)

BRENT_TICKER = "BZ=F"

LSE_TICKERS = [
    {"ticker": "HBR", "yf_symbol": "HBR.L",  "company_name": "Harbour Energy"},
    {"ticker": "ENQ", "yf_symbol": "ENQ.L",   "company_name": "EnQuest"},
    {"ticker": "SQZ", "yf_symbol": "SQZ.L",   "company_name": "Serica Energy"},
    {"ticker": "ITH", "yf_symbol": "ITH.L",   "company_name": "Ithaca Energy"},
]


def _df_to_records(df, extra_cols: dict) -> list[dict]:
    """Convert a yfinance OHLCV DataFrame to a list of dicts."""
    records = []
    for idx, row in df.iterrows():
        d = idx.date() if hasattr(idx, "date") else idx
        record = {
            "date": d,
            "open":   round(float(row.get("Open",  0) or 0), 4),
            "high":   round(float(row.get("High",  0) or 0), 4),
            "low":    round(float(row.get("Low",   0) or 0), 4),
            "close":  round(float(row.get("Close", 0) or 0), 4),
            "volume": int(row.get("Volume", 0) or 0),
        }
        record.update(extra_cols)
        records.append(record)
    return records


def fetch_brent(start: date, end: date) -> list[dict]:
    logger.info("Fetching Brent prices %s → %s", start, end)
    df = yf.download(BRENT_TICKER, start=start, end=end, progress=False, auto_adjust=True)
    if df.empty:
        logger.warning("No Brent data returned")
        return []
    # yfinance may return MultiIndex columns when downloading a single ticker
    if hasattr(df.columns, "levels"):
        df.columns = df.columns.droplevel(1)
    records = _df_to_records(df, {"currency": "USD"})
    logger.info("  %d Brent price rows", len(records))
    return records


def fetch_share_prices(start: date, end: date) -> list[dict]:
    all_records = []
    for co in LSE_TICKERS:
        logger.info("Fetching share price for %s (%s)", co["company_name"], co["yf_symbol"])
        df = yf.download(co["yf_symbol"], start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            logger.warning("  No data for %s", co["yf_symbol"])
            continue
        if hasattr(df.columns, "levels"):
            df.columns = df.columns.droplevel(1)
        records = _df_to_records(df, {
            "ticker": co["ticker"],
            "company_name": co["company_name"],
            "currency": "GBp",   # LSE quotes in pence
        })
        logger.info("  %d rows for %s", len(records), co["ticker"])
        all_records.extend(records)
    return all_records


def run(history: bool = False) -> None:
    """
    Fetch and upsert market data.
    Default: last 90 days. --history: last 5 years.
    """
    end = datetime.now().date()
    start = end - timedelta(days=5 * 365 if history else 90)

    brent = fetch_brent(start, end)
    upsert_market_data(brent, "brent_crude_prices")

    shares = fetch_share_prices(start, end)
    upsert_market_data(shares, "company_share_prices")

    logger.info("Market data run complete — %d Brent rows, %d share price rows",
                len(brent), len(shares))


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", action="store_true", help="Backfill 5 years of data")
    args = parser.parse_args()
    run(history=args.history)
