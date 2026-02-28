"""
Scrape RNS announcements from Investegate for watchlist companies.

Listing page: https://www.investegate.co.uk/company/{TICKER}
  - Table rows: date | time | source | announcement link
Article content: div.news-window
"""

import logging
import time
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from src.databricks.client import upsert_news, query

logger = logging.getLogger(__name__)

BASE_URL = "https://www.investegate.co.uk/company/{ticker}"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UKCS-intel-scraper/1.0)"}

WATCHLIST = [
    {"ticker": "HBR", "company_name": "Harbour Energy"},
    {"ticker": "ENQ", "company_name": "EnQuest"},
    {"ticker": "SQZ", "company_name": "Serica Energy"},
    {"ticker": "ITH", "company_name": "Ithaca Energy"},
]

# Polite delay between requests
REQUEST_DELAY = 1.5


def _parse_date(date_str: str) -> date | None:
    for fmt in ("%d/%m/%Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except Exception:
            pass
    return None


def _fetch_listing(ticker: str) -> list[dict]:
    url = BASE_URL.format(ticker=ticker.lower())
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to fetch listing for %s: %s", ticker, exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    rows = []

    table = soup.find("table")
    if not table:
        logger.warning("No table found for ticker %s", ticker)
        return []

    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        date_str = cells[0].get_text(strip=True)
        parsed_date = _parse_date(date_str)

        a_tag = None
        for cell in cells:
            a_tag = cell.find("a", href=True)
            if a_tag:
                break

        if not a_tag:
            continue

        title = a_tag.get_text(strip=True)
        href = a_tag["href"]
        if not href.startswith("http"):
            href = "https://www.investegate.co.uk" + href

        rows.append({"date_str": date_str, "date": parsed_date, "title": title, "url": href})

    return rows


def _fetch_content(url: str) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        div = soup.find("div", class_="news-window")
        if div:
            return div.get_text(separator="\n", strip=True)
        body = soup.find("article") or soup.find("main")
        return body.get_text(separator="\n", strip=True) if body else ""
    except Exception as exc:
        logger.warning("Failed to fetch content %s: %s", url, exc)
        return ""


def run(tickers: list[str] | None = None, max_per_company: int = 20) -> int:
    """
    Scrape RNS for watchlist (or subset). Fetches content for most recent
    max_per_company announcements per company.
    Returns total count upserted.
    """
    companies = WATCHLIST
    if tickers:
        tickers_upper = {t.upper() for t in tickers}
        companies = [c for c in WATCHLIST if c["ticker"] in tickers_upper]

    # Load all known URLs upfront to avoid redundant HTTP fetches
    try:
        existing_rows = query("SELECT url FROM rns_announcements_raw")
        known_urls = {r["url"] for r in existing_rows}
    except Exception:
        known_urls = set()

    all_records = []

    for company in companies:
        ticker = company["ticker"]
        company_name = company["company_name"]
        logger.info("Fetching RNS listing for %s (%s)", company_name, ticker)

        listings = _fetch_listing(ticker)
        listings = listings[:max_per_company]

        new_listings = [item for item in listings if item["url"] not in known_urls]
        logger.info("  %d new announcements to fetch for %s (skipping %d already stored)",
                    len(new_listings), ticker, len(listings) - len(new_listings))

        for item in new_listings:
            time.sleep(REQUEST_DELAY)
            item["content"] = _fetch_content(item["url"])
            item["ticker"] = ticker
            item["company_name"] = company_name
            all_records.append(item)
            logger.debug("  fetched: %s", item["title"][:80])

        logger.info("  %d announcements fetched for %s", len(new_listings), ticker)

    count = upsert_news(all_records, "rns_announcements_raw")
    logger.info("Upserted %d RNS announcements total", count)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
