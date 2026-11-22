"""
RNS announcements for the watchlist.

Investegate company pages no longer return a server-rendered announcement
table. From this environment the host often accepts the connection and then
sends nothing, which used to burn the daily job's timeout. Listings now come
from the London Stock Exchange public search API used by the LSE company page:

    POST https://apidata.londonstockexchange.com/search/v1/query
    {"apiBody": {"type": "news", "group": false, "issuerCodes": "<code>"}}

The API returns the five most recent items for an issuer. The article HTML is
embedded in the news page as a JavaScript string assigned to `articleBody`.
"""

import json
import logging
import re
import time
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from src.companies import by_ticker
from src.databricks.client import query, upsert_news

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://apidata.londonstockexchange.com/search/v1/query"
_NRDP_URL = "https://apidata.londonstockexchange.com/nrdp/v1"
_LSE_ORIGIN = "https://www.londonstockexchange.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": _LSE_ORIGIN,
    "Referer": _LSE_ORIGIN + "/",
}
REQUEST_DELAY = 1.0
_JS_ESCAPE = re.compile(r"\\x[0-9a-fA-F]{2}|\\u[0-9a-fA-F]{4}|\\.")


def decode_js_string(raw: str) -> str:
    """Decode the escapes LSE uses inside `articleBody="..."`."""

    def repl(match: re.Match) -> str:
        token = match.group(0)
        if token in (r"\"", r"\'"):
            return token[-1]
        if token == r"\\":
            return "\\"
        if token == r"\/":
            return "/"
        if token == r"\n":
            return "\n"
        if token == r"\r":
            return "\r"
        if token == r"\t":
            return "\t"
        if token.startswith("\\x"):
            return chr(int(token[2:], 16))
        if token.startswith("\\u"):
            return chr(int(token[2:], 16))
        return token

    return _JS_ESCAPE.sub(repl, raw)


def extract_article_body(html: str) -> str:
    marker = 'articleBody="'
    start = html.find(marker)
    if start < 0:
        return ""
    start += len(marker)
    end = start
    while end < len(html):
        if html[end] == "\\" :
            end += 2
            continue
        if html[end] == '"':
            break
        end += 1
    decoded = decode_js_string(html[start:end])
    text = BeautifulSoup(decoded, "lxml").get_text(separator="\n", strip=True)
    return text[:15000]


def _issuer_code(company: dict) -> str:
    tidm = company["lse_tidm"]
    try:
        resp = requests.get(
            _NRDP_URL,
            params={"tidm": tidm},
            headers=_HEADERS,
            timeout=(10, 20),
        )
        resp.raise_for_status()
        payload = resp.json()
        code = ((payload.get("data") or {}).get("displayissuercode") or "").strip()
        if payload.get("statusCode") == 200 and code:
            return code
        logger.warning("LSE instrument lookup for %s did not return an issuer code", tidm)
    except Exception as exc:
        logger.warning("LSE instrument lookup for %s failed: %s", tidm, exc)
    return company["issuer_code"]


def _fetch_listing(issuer_code: str) -> list[dict]:
    body = {
        "apiBody": {
            "type": "news",
            "group": False,
            "issuerCodes": issuer_code,
            "prices": False,
        }
    }
    resp = requests.post(
        _SEARCH_URL,
        data=json.dumps(body),
        headers={**_HEADERS, "Content-Type": "application/json"},
        timeout=(10, 30),
    )
    resp.raise_for_status()
    payload = resp.json()
    rows = []
    for item in payload.get("Data") or []:
        if item.get("type") != "news":
            continue
        path = item.get("path") or ""
        url = path if path.startswith("http") else _LSE_ORIGIN + path
        released = item.get("releasedDate") or item.get("releasedUTC") or ""
        parsed: date | None = None
        if released:
            try:
                parsed = datetime.fromisoformat(released.replace("Z", "+00:00")).date()
            except ValueError:
                parsed = None
        rows.append(
            {
                "date": parsed,
                "date_str": released,
                "title": item.get("headline") or "",
                "url": url,
                "source": item.get("source") or "",
            }
        )
    return rows


def _fetch_content(url: str) -> str:
    try:
        resp = requests.get(url, headers={**_HEADERS, "Accept": "text/html"}, timeout=(10, 30))
        resp.raise_for_status()
        return extract_article_body(resp.text)
    except Exception as exc:
        logger.warning("Failed to fetch RNS content %s: %s", url, exc)
        return ""


def run(tickers: list[str] | None = None, max_per_company: int = 20) -> int:
    """
    Scrape recent RNS for the watchlist. Returns the number of rows upserted.
    Raises if every company listing fails.
    """
    companies = by_ticker(tickers)

    try:
        existing_rows = query("SELECT url FROM rns_announcements_raw")
        known_urls = {r["url"] for r in existing_rows}
    except Exception as exc:
        logger.warning("Could not load known RNS urls (%s); fetching listings anyway", exc)
        known_urls = set()

    all_records = []
    failures = 0

    for company in companies:
        ticker = company["ticker"]
        company_name = company["company_name"]
        logger.info("Fetching RNS listing for %s (%s / %s)", company_name, ticker, company["lse_tidm"])
        try:
            issuer = _issuer_code(company)
            listings = _fetch_listing(issuer)[:max_per_company]
        except Exception as exc:
            failures += 1
            logger.warning("Failed to fetch listing for %s: %s", ticker, exc)
            continue

        new_listings = [item for item in listings if item["url"] not in known_urls]
        logger.info(
            "  %d new announcements to fetch for %s (skipping %d already stored)",
            len(new_listings),
            ticker,
            len(listings) - len(new_listings),
        )

        for item in new_listings:
            time.sleep(REQUEST_DELAY)
            item["content"] = _fetch_content(item["url"])
            item["ticker"] = ticker
            item["company_name"] = company_name
            all_records.append(item)

    if failures == len(companies):
        raise RuntimeError("RNS listing failed for every watchlist company")

    count = upsert_news(all_records, "rns_announcements_raw")
    logger.info("Upserted %d RNS announcements total", count)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
