"""
Brent crude and LSE share prices from Yahoo Finance's chart API.

yfinance calls from GitHub-hosted runners were hanging with no socket timeout.
From 8 April 2026 the daily workflow then sat until GitHub cancelled it at 30
minutes, so nothing after the price fetch (news, RNS, briefings) ran. This
module calls the same v8 chart endpoint the web app uses, with a hard timeout.

Tables written:
  brent_crude_prices   — daily OHLCV for ICE Brent (BZ=F), USD/bbl
  company_share_prices — daily OHLCV for each watchlist company, GBp

Default: last 90 days. --history backfills 5 years.
Closes are split-adjusted when Yahoo provides adjclose.
"""

import logging
from datetime import date, datetime, timedelta, timezone

import requests

from src.companies import COMPANIES
from src.databricks.client import upsert_market_data

logger = logging.getLogger(__name__)

BRENT_TICKER = "BZ=F"
_CHART_HOSTS = (
    "https://query1.finance.yahoo.com/v8/finance/chart",
    "https://query2.finance.yahoo.com/v8/finance/chart",
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _fetch_chart(symbol: str, start: date, end: date) -> dict:
    params = {
        "interval": "1d",
        "period1": int(datetime.combine(start, datetime.min.time()).timestamp()),
        "period2": int(datetime.combine(end, datetime.min.time()).timestamp()),
        "events": "div,splits",
    }
    last_error: Exception | None = None
    for host in _CHART_HOSTS:
        url = f"{host}/{symbol}"
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=(10, 30))
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            last_error = exc
            logger.warning("Yahoo chart %s via %s failed: %s", symbol, host, exc)
            continue
        result = (payload.get("chart") or {}).get("result") or []
        if not result:
            err = (payload.get("chart") or {}).get("error")
            last_error = RuntimeError(f"Yahoo chart {symbol} returned no result: {err}")
            logger.warning("%s", last_error)
            continue
        return result[0]
    raise RuntimeError(f"Yahoo chart failed for {symbol}: {last_error}")


def _records_from_chart(chart: dict, extra: dict) -> list[dict]:
    timestamps = chart.get("timestamp") or []
    quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
    adj = ((chart.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    records = []
    for i, ts in enumerate(timestamps):
        raw_close = closes[i] if i < len(closes) else None
        if raw_close is None:
            continue
        adjusted = adj[i] if i < len(adj) and adj[i] is not None else raw_close
        factor = (adjusted / raw_close) if raw_close else 1.0

        def scaled(series: list, idx: int) -> float:
            value = series[idx] if idx < len(series) and series[idx] is not None else 0.0
            return round(float(value) * factor, 4)

        records.append(
            {
                "date": datetime.fromtimestamp(ts, timezone.utc).date(),
                "open": scaled(opens, i),
                "high": scaled(highs, i),
                "low": scaled(lows, i),
                "close": round(float(adjusted), 4),
                "volume": int(volumes[i] or 0) if i < len(volumes) else 0,
                **extra,
            }
        )
    return records


def fetch_brent(start: date, end: date) -> list[dict]:
    logger.info("Fetching Brent prices %s → %s", start, end)
    chart = _fetch_chart(BRENT_TICKER, start, end)
    records = _records_from_chart(chart, {"currency": "USD"})
    logger.info("  %d Brent price rows", len(records))
    return records


def fetch_share_prices(start: date, end: date) -> list[dict]:
    all_records = []
    for co in COMPANIES:
        logger.info("Fetching share price for %s (%s)", co["company_name"], co["yf_symbol"])
        try:
            chart = _fetch_chart(co["yf_symbol"], start, end)
        except Exception as exc:
            logger.warning("  No data for %s: %s", co["yf_symbol"], exc)
            continue
        records = _records_from_chart(
            chart,
            {
                "ticker": co["ticker"],
                "company_name": co["company_name"],
                "currency": "GBp",
            },
        )
        logger.info("  %d rows for %s", len(records), co["ticker"])
        all_records.extend(records)
    return all_records


def run(history: bool = False) -> None:
    """Fetch and upsert market data. Default: last 90 days. --history: 5 years."""
    end = datetime.now(timezone.utc).date() + timedelta(days=1)
    start = end - timedelta(days=5 * 365 if history else 90)

    brent = fetch_brent(start, end)
    if not brent:
        raise RuntimeError("Yahoo returned no Brent prices")
    upsert_market_data(brent, "brent_crude_prices")

    shares = fetch_share_prices(start, end)
    if not shares:
        raise RuntimeError("Yahoo returned no share prices")
    upsert_market_data(shares, "company_share_prices")

    logger.info(
        "Market data run complete — %d Brent rows, %d share price rows",
        len(brent),
        len(shares),
    )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", action="store_true", help="Backfill 5 years of data")
    args = parser.parse_args()
    run(history=args.history)
