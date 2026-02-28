# UKCS Intelligence Platform — Codebase Walkthrough

Please walk me through this codebase step by step. I want to understand:
- What each file does and why it exists
- How the files connect to each other
- The key design decisions and patterns used
- Any important technical concepts I should understand (e.g. Delta MERGE, ArcGIS pagination, server components)

Start with an overview of the architecture, then go file by file in the order they are listed below.

---

## ARCHITECTURE OVERVIEW

The system has two parts:
1. **Python backend** — scrapes data, stores it in Databricks Delta tables, generates AI briefings
2. **Next.js frontend** — reads from Delta tables and serves briefings + production data to users

Data flows like this:
```
Public APIs → Python scrapers → Databricks Delta tables ← Next.js web app → User
                                        ↑
                              GPT-4o briefing generator
```

The four watched companies (UKCS-listed oil & gas operators):
- HBR — Harbour Energy (NSTA operator: HARBOUR ENERGY PLC)
- ENQ — EnQuest (NSTA operator: ENQUEST PLC)
- SQZ — Serica Energy (NSTA operator: SERICA ENERGY)
- ITH — Ithaca Energy (NSTA operator: ITHACA ENERGY)

Delta tables used:
- `nsta_field_production_raw` — monthly production per field (all UKCS operators)
- `nsta_news_raw` — NSTA regulatory news articles
- `rns_announcements_raw` — company RNS announcements from Investegate
- `brent_crude_prices` — daily Brent crude OHLCV (USD/bbl)
- `company_share_prices` — daily share price OHLCV for watchlist (GBp)
- `company_briefings` — GPT-4o generated analyst briefings

---

## FILE 1: src/databricks/client.py
**Purpose: Core database layer — all reads and writes to Databricks go through here**

```python
"""Databricks SQL connector for reading/writing Delta tables."""

import logging
import os
from contextlib import contextmanager
from typing import Any

logging.getLogger("databricks.sql").setLevel(logging.WARNING)

from databricks import sql
from dotenv import load_dotenv

load_dotenv()


def _get_connection():
    hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_ACCESS_TOKEN"]
    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token,
    )


@contextmanager
def get_cursor():
    conn = _get_connection()
    try:
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        finally:
            cursor.close()
    finally:
        conn.close()


def query(sql_text: str, params: list | None = None) -> list[dict[str, Any]]:
    """Run a SELECT and return rows as dicts."""
    with get_cursor() as cur:
        cur.execute(sql_text, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def execute(sql_text: str, params: list | None = None) -> None:
    """Run a non-SELECT statement (INSERT, MERGE, etc.)."""
    with get_cursor() as cur:
        cur.execute(sql_text, params or [])


def _merge_production_batch(cur, records: list[dict]) -> None:
    """Execute a single MERGE for a batch of production records."""
    def _val(v):
        return "NULL" if v is None or v == "" else str(v)

    row_literals = ",\n".join(
        f"('{r['FIELDNAME']}', '{r['ORGGRPNM']}', {r['PERIODYR']}, {r['PERIODMNTH']}, "
        f"{_val(r.get('OILPRODMBD'))}, {_val(r.get('AGASPROMMS'))}, "
        f"{_val(r.get('DGASPROMMS'))}, {_val(r.get('GCONDVOL'))}, "
        f"{_val(r.get('GASFLARVOL'))}, {_val(r.get('WATPRODVOL'))})"
        for r in records
    )
    cur.execute(f"""
        MERGE INTO nsta_field_production_raw AS target
        USING (
            SELECT * FROM (VALUES {row_literals}) AS t(
                FIELDNAME, ORGGRPNM, PERIODYR, PERIODMNTH,
                OILPRODMBD, AGASPROMMS, DGASPROMMS, GCONDVOL, GASFLARVOL, WATPRODVOL
            )
        ) AS source
        ON target.FIELDNAME = source.FIELDNAME
           AND target.PERIODYR = source.PERIODYR
           AND target.PERIODMNTH = source.PERIODMNTH
        WHEN MATCHED THEN UPDATE SET
            target.ORGGRPNM   = source.ORGGRPNM,
            target.OILPRODMBD = source.OILPRODMBD,
            target.AGASPROMMS = source.AGASPROMMS,
            target.DGASPROMMS = source.DGASPROMMS,
            target.GCONDVOL   = source.GCONDVOL,
            target.GASFLARVOL = source.GASFLARVOL,
            target.WATPRODVOL = source.WATPRODVOL
        WHEN NOT MATCHED THEN INSERT (
            FIELDNAME, ORGGRPNM, PERIODYR, PERIODMNTH,
            OILPRODMBD, AGASPROMMS, DGASPROMMS, GCONDVOL, GASFLARVOL, WATPRODVOL
        ) VALUES (
            source.FIELDNAME, source.ORGGRPNM, source.PERIODYR, source.PERIODMNTH,
            source.OILPRODMBD, source.AGASPROMMS, source.DGASPROMMS, source.GCONDVOL,
            source.GASFLARVOL, source.WATPRODVOL
        )
    """)


def upsert_production(records: list[dict], batch_size: int = 500) -> int:
    """
    Merge production records into nsta_field_production_raw in batches.
    Key: FIELDNAME + PERIODYR + PERIODMNTH.
    Returns count of records processed.
    """
    import logging
    logger = logging.getLogger(__name__)

    if not records:
        return 0

    # Aggregate to field+period level (ArcGIS returns sub-unit rows per field)
    agg: dict[tuple, dict] = {}
    vol_fields = ["OILPRODMBD", "AGASPROMMS", "DGASPROMMS", "GCONDVOL", "GASFLARVOL", "WATPRODVOL"]
    for r in records:
        key = (r["FIELDNAME"], r["PERIODYR"], r["PERIODMNTH"])
        if key not in agg:
            agg[key] = {"FIELDNAME": r["FIELDNAME"], "ORGGRPNM": r["ORGGRPNM"],
                        "PERIODYR": r["PERIODYR"], "PERIODMNTH": r["PERIODMNTH"],
                        **{f: 0.0 for f in vol_fields}}
        for f in vol_fields:
            agg[key][f] += r.get(f) or 0.0

    deduped = list(agg.values())
    logger.info("Aggregated %d raw rows to %d field+period records", len(records), len(deduped))

    with get_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS nsta_field_production_raw (
                FIELDNAME      STRING,
                ORGGRPNM       STRING,
                PERIODYR       INT,
                PERIODMNTH     INT,
                OILPRODMBD     DOUBLE,
                AGASPROMMS     DOUBLE,
                DGASPROMMS     DOUBLE,
                GCONDVOL       DOUBLE,
                GASFLARVOL     DOUBLE,
                WATPRODVOL     DOUBLE
            ) USING DELTA
        """)

        for i in range(0, len(deduped), batch_size):
            batch = deduped[i : i + batch_size]
            logger.info("Upserting production batch %d-%d of %d", i + 1, i + len(batch), len(deduped))
            _merge_production_batch(cur, batch)

    return len(deduped)


def _escape(v) -> str:
    """Escape a string value for inline SQL."""
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def upsert_news(records: list[dict], table: str) -> int:
    """
    Merge news/RNS records in a single batched MERGE. Key: url.
    table must be one of: nsta_news_raw, rns_announcements_raw.
    """
    allowed = {"nsta_news_raw", "rns_announcements_raw"}
    if table not in allowed:
        raise ValueError(f"table must be one of {allowed}")
    if not records:
        return 0

    with get_cursor() as cur:
        if table == "nsta_news_raw":
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nsta_news_raw (
                    title       STRING,
                    date_str    STRING,
                    date        DATE,
                    url         STRING,
                    content     STRING,
                    scraped_at  TIMESTAMP
                ) USING DELTA
            """)
            rows = ",\n".join(
                f"({_escape(r['title'])}, {_escape(r.get('date_str',''))}, "
                f"{_escape(r.get('date'))}, {_escape(r['url'])}, "
                f"{_escape(r.get('content',''))}, current_timestamp())"
                for r in records
            )
            cur.execute(f"""
                MERGE INTO nsta_news_raw AS t
                USING (
                    SELECT * FROM (VALUES {rows}) AS s(title, date_str, date, url, content, scraped_at)
                ) AS source ON t.url = source.url
                WHEN NOT MATCHED THEN INSERT (title, date_str, date, url, content, scraped_at)
                VALUES (source.title, source.date_str, source.date, source.url, source.content, source.scraped_at)
            """)

        else:  # rns_announcements_raw
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rns_announcements_raw (
                    ticker          STRING,
                    company_name    STRING,
                    date            DATE,
                    title           STRING,
                    url             STRING,
                    content         STRING,
                    scraped_at      TIMESTAMP
                ) USING DELTA
            """)
            rows = ",\n".join(
                f"({_escape(r['ticker'])}, {_escape(r['company_name'])}, "
                f"{_escape(r.get('date'))}, {_escape(r['title'])}, "
                f"{_escape(r['url'])}, {_escape(r.get('content',''))}, current_timestamp())"
                for r in records
            )
            cur.execute(f"""
                MERGE INTO rns_announcements_raw AS t
                USING (
                    SELECT * FROM (VALUES {rows}) AS s(ticker, company_name, date, title, url, content, scraped_at)
                ) AS source ON t.url = source.url
                WHEN NOT MATCHED THEN INSERT (ticker, company_name, date, title, url, content, scraped_at)
                VALUES (source.ticker, source.company_name, source.date, source.title,
                        source.url, source.content, source.scraped_at)
            """)

    return len(records)


def upsert_market_data(records: list[dict], table: str) -> int:
    """
    Upsert market price records.
    table: 'brent_crude_prices' or 'company_share_prices'
    Key: date (Brent) or ticker+date (shares).
    """
    allowed = {"brent_crude_prices", "company_share_prices"}
    if table not in allowed:
        raise ValueError(f"table must be one of {allowed}")
    if not records:
        return 0

    with get_cursor() as cur:
        if table == "brent_crude_prices":
            cur.execute("""
                CREATE TABLE IF NOT EXISTS brent_crude_prices (
                    date        DATE,
                    open        DOUBLE,
                    high        DOUBLE,
                    low         DOUBLE,
                    close       DOUBLE,
                    volume      BIGINT,
                    currency    STRING
                ) USING DELTA
            """)
            rows = ",\n".join(
                f"({_escape(str(r['date']))}, {r['open']}, {r['high']}, {r['low']}, "
                f"{r['close']}, {r['volume']}, {_escape(r['currency'])})"
                for r in records
            )
            cur.execute(f"""
                MERGE INTO brent_crude_prices AS t
                USING (
                    SELECT * FROM (VALUES {rows}) AS s(date, open, high, low, close, volume, currency)
                ) AS source ON t.date = source.date
                WHEN MATCHED THEN UPDATE SET
                    t.open=source.open, t.high=source.high, t.low=source.low,
                    t.close=source.close, t.volume=source.volume
                WHEN NOT MATCHED THEN INSERT (date, open, high, low, close, volume, currency)
                VALUES (source.date, source.open, source.high, source.low,
                        source.close, source.volume, source.currency)
            """)

        else:  # company_share_prices
            cur.execute("""
                CREATE TABLE IF NOT EXISTS company_share_prices (
                    ticker          STRING,
                    company_name    STRING,
                    date            DATE,
                    open            DOUBLE,
                    high            DOUBLE,
                    low             DOUBLE,
                    close           DOUBLE,
                    volume          BIGINT,
                    currency        STRING
                ) USING DELTA
            """)
            rows = ",\n".join(
                f"({_escape(r['ticker'])}, {_escape(r['company_name'])}, {_escape(str(r['date']))}, "
                f"{r['open']}, {r['high']}, {r['low']}, {r['close']}, "
                f"{r['volume']}, {_escape(r['currency'])})"
                for r in records
            )
            cur.execute(f"""
                MERGE INTO company_share_prices AS t
                USING (
                    SELECT * FROM (VALUES {rows}) AS s(
                        ticker, company_name, date, open, high, low, close, volume, currency
                    )
                ) AS source ON t.ticker = source.ticker AND t.date = source.date
                WHEN MATCHED THEN UPDATE SET
                    t.open=source.open, t.high=source.high, t.low=source.low,
                    t.close=source.close, t.volume=source.volume
                WHEN NOT MATCHED THEN INSERT (
                    ticker, company_name, date, open, high, low, close, volume, currency
                ) VALUES (
                    source.ticker, source.company_name, source.date, source.open,
                    source.high, source.low, source.close, source.volume, source.currency
                )
            """)

    return len(records)


def save_briefing(ticker: str, company_name: str, year: int, briefing: str, model: str) -> None:
    """Upsert a generated briefing into company_briefings."""
    with get_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS company_briefings (
                ticker          STRING,
                company_name    STRING,
                year            INT,
                briefing        STRING,
                generated_at    TIMESTAMP,
                model           STRING
            ) USING DELTA
        """)
        cur.execute("""
            MERGE INTO company_briefings AS t
            USING (SELECT ? AS ticker, ? AS year) AS s
                ON t.ticker = s.ticker AND t.year = s.year
            WHEN MATCHED THEN UPDATE SET
                company_name = ?,
                briefing = ?,
                generated_at = current_timestamp(),
                model = ?
            WHEN NOT MATCHED THEN INSERT (ticker, company_name, year, briefing, generated_at, model)
            VALUES (?, ?, ?, ?, current_timestamp(), ?)
        """, [ticker, year, company_name, briefing, model, ticker, company_name, year, briefing, model])
```

---

## FILE 2: src/scrapers/nsta_production.py
**Purpose: Fetches monthly field production data from NSTA's ArcGIS REST API**

```python
"""
Fetch UKCS field production data from NSTA ArcGIS REST API and upsert to Delta.

API endpoint returns up to 2000 features per request; uses resultOffset for
pagination until fewer than the batch size are returned.
"""

import logging
from datetime import datetime

import requests

from src.databricks.client import upsert_production

logger = logging.getLogger(__name__)

BASE_URL = (
    "https://services-eu1.arcgis.com/OZMfUznmLTnWccBc/arcgis/rest/services/"
    "UKCS_hydrocarbon_field_production_reports_PPRS_points_(WGS84)/FeatureServer/0/query"
)

BATCH_SIZE = 2000

FIELDS = [
    "FIELDNAME",
    "ORGGRPNM",
    "PERIODYR",
    "PERIODMNTH",
    "OILPRODMBD",
    "AGASPROMMS",
    "DGASPROMMS",
    "GCONDVOL",
    "GASFLARVOL",
    "WATPRODVOL",
]


def _fetch_page(offset: int, where: str = "1=1") -> list[dict]:
    params = {
        "where": where,
        "outFields": ",".join(FIELDS),
        "resultOffset": offset,
        "resultRecordCount": BATCH_SIZE,
        "f": "json",
        "returnGeometry": "false",
    }
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS API error: {data['error']}")

    features = data.get("features", [])
    return [f["attributes"] for f in features]


def fetch_for_year(year: int) -> list[dict]:
    """Fetch only records for a specific year — used for monthly refreshes."""
    where = f"PERIODYR = {year}"
    all_records = []
    offset = 0
    while True:
        logger.info("Fetching year=%d records at offset %d", year, offset)
        batch = _fetch_page(offset, where=where)
        all_records.extend(batch)
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
    logger.info("Fetched %d records for year %d", len(all_records), year)
    return all_records


def fetch_all() -> list[dict]:
    """Page through the full ArcGIS API. Use only for explicit full reloads."""
    all_records = []
    offset = 0
    while True:
        logger.info("Fetching all production records at offset %d", offset)
        batch = _fetch_page(offset)
        all_records.extend(batch)
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE
    logger.info("Fetched %d total production records", len(all_records))
    return all_records


def run(year: int | None = None, full_reload: bool = False) -> int:
    """
    Fetch and upsert production data.

    Default (no args): fetches current year only — suitable for monthly scheduler runs.
    --full: fetches all history — only needed if you want to resync everything.
    """
    if full_reload:
        logger.info("Full reload requested — fetching all years")
        records = fetch_all()
    else:
        target_year = year or datetime.now().year
        logger.info("Fetching year %d only (use --full for complete history)", target_year)
        records = fetch_for_year(target_year)

    count = upsert_production(records)
    logger.info("Upserted %d production records", count)
    return count


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Specific year to fetch (default: current year)")
    parser.add_argument("--full", action="store_true", help="Fetch full history (slow)")
    args = parser.parse_args()
    run(year=args.year, full_reload=args.full)
```

---

## FILE 3: src/scrapers/nsta_news.py
**Purpose: Scrapes NSTA regulatory news articles**

```python
"""
Scrape NSTA news articles from nstauthority.co.uk.

Note: The news listing page uses JavaScript to load articles dynamically.
Pagination via ?page=N returns the same 18 articles on every page.
This scraper therefore fetches page 1 only and treats those 18 articles
as the current static set. New articles appear when NSTA publishes them;
re-running will detect them via URL deduplication in Delta.
"""

import logging
from datetime import date

import requests
from bs4 import BeautifulSoup

from src.databricks.client import upsert_news

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nstauthority.co.uk/news-publications/news/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UKCS-intel-scraper/1.0)"}


def _parse_article_list(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    articles = soup.find_all("article", class_="article")
    results = []
    for art in articles:
        a_tag = art.find("a", href=True)
        if not a_tag:
            continue
        url = a_tag["href"]
        if not url.startswith("http"):
            url = "https://www.nstauthority.co.uk" + url

        title_tag = art.find(["h2", "h3", "h4"])
        title = title_tag.get_text(strip=True) if title_tag else a_tag.get_text(strip=True)

        time_tag = art.find("time")
        date_str = time_tag.get_text(strip=True) if time_tag else ""
        parsed_date = _parse_date(date_str)

        results.append({"title": title, "date_str": date_str, "date": parsed_date, "url": url})

    return results


def _parse_date(date_str: str) -> date | None:
    for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except Exception:
            pass
    return None


def _fetch_article_content(url: str) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        content_div = soup.find("div", class_="news-window")
        if content_div:
            return content_div.get_text(separator="\n", strip=True)
        body = soup.find("article") or soup.find("main")
        return body.get_text(separator="\n", strip=True) if body else ""
    except Exception as exc:
        logger.warning("Failed to fetch article %s: %s", url, exc)
        return ""


def _fetch_listing() -> str:
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def run() -> int:
    """Scrape NSTA news and upsert new articles to Delta. Returns count inserted."""
    from datetime import datetime

    html = _fetch_listing()
    articles = _parse_article_list(html)
    logger.info("Found %d articles on NSTA news listing", len(articles))

    for art in articles:
        art["content"] = _fetch_article_content(art["url"])

    count = upsert_news(articles, "nsta_news_raw")
    logger.info("Upserted %d NSTA news articles", count)
    return count
```

---

## FILE 4: src/scrapers/investegate.py
**Purpose: Scrapes RNS company announcements from Investegate for the four watchlist companies**

```python
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

from src.databricks.client import upsert_news

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
    companies = WATCHLIST
    if tickers:
        tickers_upper = {t.upper() for t in tickers}
        companies = [c for c in WATCHLIST if c["ticker"] in tickers_upper]

    all_records = []

    for company in companies:
        ticker = company["ticker"]
        company_name = company["company_name"]
        logger.info("Fetching RNS listing for %s (%s)", company_name, ticker)

        listings = _fetch_listing(ticker)
        listings = listings[:max_per_company]

        for item in listings:
            time.sleep(REQUEST_DELAY)
            item["content"] = _fetch_content(item["url"])
            item["ticker"] = ticker
            item["company_name"] = company_name
            all_records.append(item)

        logger.info("  %d announcements fetched for %s", len(listings), ticker)

    count = upsert_news(all_records, "rns_announcements_raw")
    logger.info("Upserted %d RNS announcements total", count)
    return count
```

---

## FILE 5: src/scrapers/market_data.py
**Purpose: Fetches Brent crude prices and LSE share prices via yfinance**

```python
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
    end = datetime.now().date()
    start = end - timedelta(days=5 * 365 if history else 90)

    brent = fetch_brent(start, end)
    upsert_market_data(brent, "brent_crude_prices")

    shares = fetch_share_prices(start, end)
    upsert_market_data(shares, "company_share_prices")

    logger.info("Market data run complete — %d Brent rows, %d share price rows",
                len(brent), len(shares))
```

---

## FILE 6: src/intelligence/briefings.py
**Purpose: Generates AI analyst briefings using GPT-4o, combining all data sources**

```python
"""
Generate AI intelligence briefings for UKCS operators using GPT-4o.

Context loaded per company:
  - Field production history (monthly, by field) from nsta_field_production_raw
  - Last 20 RNS announcements from rns_announcements_raw
  - Relevant NSTA news filtered by company/field keyword match
  - Market context: Brent crude + share price 90-day summary

Water cut calculation:
  - WATPRODVOL is in m3; OILPRODMBD is in thousand barrels (Mbbl) for the period
  - Convert: WATPRODVOL_Mbbl = WATPRODVOL * 0.0062898 (1 m3 = 6.2898 bbl)
  - Water cut = WATPRODVOL_Mbbl / (WATPRODVOL_Mbbl + OILPRODMBD)
"""

import logging
import os
from datetime import datetime

from openai import OpenAI

from src.databricks.client import query, save_briefing

logger = logging.getLogger(__name__)

M3_TO_BBL = 6.2898
M3_TO_MBBL = M3_TO_BBL / 1000.0

WATCHLIST = [
    {"ticker": "HBR", "company_name": "Harbour Energy", "nsta_operator": "HARBOUR ENERGY PLC"},
    {"ticker": "ENQ", "company_name": "EnQuest",         "nsta_operator": "ENQUEST PLC"},
    {"ticker": "SQZ", "company_name": "Serica Energy",   "nsta_operator": "SERICA ENERGY"},
    {"ticker": "ITH", "company_name": "Ithaca Energy",   "nsta_operator": "ITHACA ENERGY"},
]

BRIEFING_PROMPT = """You are an expert oil and gas analyst covering the UK Continental Shelf (UKCS).
Generate a structured intelligence briefing for {company_name} ({ticker}).

Write exactly four sections with these headings:
1. Production Performance
2. Corporate Developments
3. Regulatory Context
4. Commercial Outlook

Use the data below. Be analytical, specific, and concise. Cite figures where relevant.
Flag data gaps or uncertainties where they exist.

---
## Market Context
{market_context}

---
## Field Production Data (NSTA, monthly by field)
Units: Oil = Mbbl/period, Gas (assoc) = MMscf, Gas (disassoc) = MMscf,
       Condensate = Mbbl, Flare = MMscf, Water = Mbbl (converted from m3)
       Water Cut = water / (water + oil), expressed as %

{production_table}

---
## Recent RNS Announcements (last 20, newest first)
{rns_text}

---
## Relevant NSTA News
{nsta_news_text}
"""


def _load_production(nsta_operator: str, months: int = 36) -> list[dict]:
    """Load the most recent N months of production data for an operator."""
    rows = query(
        """
        SELECT FIELDNAME, PERIODYR, PERIODMNTH, OILPRODMBD, AGASPROMMS,
               DGASPROMMS, GCONDVOL, GASFLARVOL, WATPRODVOL
        FROM nsta_field_production_raw
        WHERE UPPER(ORGGRPNM) = UPPER(?)
        ORDER BY PERIODYR DESC, PERIODMNTH DESC, FIELDNAME
        LIMIT ?
        """,
        [nsta_operator, months * 20],  # ~20 fields per operator per month
    )
    return rows


def _format_production(rows: list[dict]) -> str:
    if not rows:
        return "(No production data found for this operator)"

    lines = [
        f"{'Field':<30} {'Yr':>4} {'Mo':>2} {'Oil(Mbbl)':>10} {'Gas-A(MMscf)':>12} "
        f"{'Gas-D(MMscf)':>12} {'Cond(Mbbl)':>10} {'Water(Mbbl)':>11} {'WatCut%':>8}"
    ]
    lines.append("-" * 105)

    for r in rows:
        oil = r.get("OILPRODMBD") or 0.0
        wat_m3 = r.get("WATPRODVOL") or 0.0
        wat_mbbl = wat_m3 * M3_TO_MBBL
        water_cut = (wat_mbbl / (wat_mbbl + oil) * 100) if (wat_mbbl + oil) > 0 else 0.0

        lines.append(
            f"{(r.get('FIELDNAME') or ''):<30} {r.get('PERIODYR', ''):>4} {r.get('PERIODMNTH', ''):>2} "
            f"{oil:>10.2f} {(r.get('AGASPROMMS') or 0):>12.2f} "
            f"{(r.get('DGASPROMMS') or 0):>12.2f} {(r.get('GCONDVOL') or 0):>10.2f} "
            f"{wat_mbbl:>11.2f} {water_cut:>7.1f}%"
        )

    return "\n".join(lines)


def _load_market_context(ticker: str) -> str:
    """Build a compact market context block: Brent price summary + share price performance."""
    lines = []

    try:
        brent = query("""
            SELECT date, close FROM brent_crude_prices
            ORDER BY date DESC LIMIT 90
        """)
        if brent:
            current = brent[0]["close"]
            avg_90 = sum(r["close"] for r in brent) / len(brent)
            oldest = brent[-1]
            change_pct = ((current - oldest["close"]) / oldest["close"] * 100) if oldest["close"] else 0
            lines.append(
                f"Brent crude: ${current:.2f}/bbl (current) | "
                f"${avg_90:.2f}/bbl (90d avg) | "
                f"{change_pct:+.1f}% over {len(brent)}d"
            )
    except Exception:
        lines.append("Brent crude: data not available")

    try:
        prices = query("""
            SELECT date, close, currency FROM company_share_prices
            WHERE UPPER(ticker) = UPPER(?)
            ORDER BY date DESC LIMIT 90
        """, [ticker])
        if prices:
            curr_p = prices[0]["close"]
            currency = prices[0]["currency"]
            avg_90 = sum(r["close"] for r in prices) / len(prices)
            oldest_p = prices[-1]["close"]
            change_pct = ((curr_p - oldest_p) / oldest_p * 100) if oldest_p else 0
            unit = "p" if currency == "GBp" else currency
            lines.append(
                f"{ticker} share price: {curr_p:.1f}{unit} (current) | "
                f"{avg_90:.1f}{unit} (90d avg) | "
                f"{change_pct:+.1f}% over {len(prices)}d"
            )
    except Exception:
        lines.append(f"{ticker} share price: data not available")

    return "\n".join(lines) if lines else "(No market data available)"


def _load_rns(ticker: str, limit: int = 20) -> list[dict]:
    return query(
        """
        SELECT date, title, content
        FROM rns_announcements_raw
        WHERE UPPER(ticker) = UPPER(?)
        ORDER BY date DESC
        LIMIT ?
        """,
        [ticker, limit],
    )


def _format_rns(rows: list[dict]) -> str:
    if not rows:
        return "(No RNS announcements found)"
    parts = []
    for r in rows:
        date_str = str(r.get("date", "unknown date"))
        title = r.get("title", "")
        content = (r.get("content") or "").strip()[:600]
        parts.append(f"[{date_str}] {title}\n{content}")
    return "\n\n---\n\n".join(parts)


def _load_nsta_news(company_name: str, field_names: list[str]) -> list[dict]:
    """Filter NSTA news by keyword match against company name or field names."""
    all_news = query("SELECT title, date, content FROM nsta_news_raw ORDER BY date DESC")

    keywords = {company_name.lower()}
    for f in field_names:
        keywords.add(f.lower())
    keywords.update(w for kw in list(keywords) for w in kw.split() if len(w) > 3)

    relevant = []
    for article in all_news:
        searchable = f"{article.get('title', '')} {article.get('content', '')}".lower()
        if any(kw in searchable for kw in keywords):
            relevant.append(article)

    return relevant[:10]


def _format_nsta_news(rows: list[dict]) -> str:
    if not rows:
        return "(No relevant NSTA news found)"
    parts = []
    for r in rows:
        date_str = str(r.get("date", ""))
        title = r.get("title", "")
        content = (r.get("content") or "").strip()[:600]
        parts.append(f"[{date_str}] {title}\n{content}")
    return "\n\n---\n\n".join(parts)


def generate_briefing(company: dict, year: int | None = None, model: str = "gpt-4o") -> str:
    ticker = company["ticker"]
    company_name = company["company_name"]
    nsta_operator = company["nsta_operator"]
    year = year or datetime.now().year

    logger.info("Generating briefing for %s (%s)", company_name, ticker)

    prod_rows = _load_production(nsta_operator)
    field_names = list({r["FIELDNAME"] for r in prod_rows if r.get("FIELDNAME")})

    production_table = _format_production(prod_rows)
    rns_rows = _load_rns(ticker)
    rns_text = _format_rns(rns_rows)
    nsta_news_rows = _load_nsta_news(company_name, field_names)
    nsta_news_text = _format_nsta_news(nsta_news_rows)
    market_context = _load_market_context(ticker)

    prompt = BRIEFING_PROMPT.format(
        company_name=company_name,
        ticker=ticker,
        market_context=market_context,
        production_table=production_table,
        rns_text=rns_text,
        nsta_news_text=nsta_news_text,
    )

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )

    briefing = response.choices[0].message.content.strip()
    save_briefing(ticker, company_name, year, briefing, model)
    logger.info("Saved briefing for %s", ticker)
    return briefing


def run(tickers: list[str] | None = None, year: int | None = None, model: str = "gpt-4o",
        delay: int = 30) -> None:
    import time

    companies = WATCHLIST
    if tickers:
        tickers_upper = {t.upper() for t in tickers}
        companies = [c for c in WATCHLIST if c["ticker"] in tickers_upper]

    for i, company in enumerate(companies):
        try:
            generate_briefing(company, year=year, model=model)
        except Exception as exc:
            logger.error("Failed to generate briefing for %s: %s", company["ticker"], exc)

        if i < len(companies) - 1:
            logger.info("Waiting %ds before next company (TPM rate limit)...", delay)
            time.sleep(delay)
```

---

## FILE 7: src/scheduler.py
**Purpose: Runs the pipeline automatically — daily scrapes and monthly production refresh**

```python
"""
Scheduler for UKCS Intelligence Platform.

Jobs:
  - Daily (06:00 UTC): market data, NSTA news, Investegate RNS, then briefings
  - Monthly (1st of month, 07:00 UTC): Full NSTA production refresh for current year
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
    from src.scrapers import investegate, nsta_news, market_data
    from src.intelligence import briefings
    from src.databricks.client import query

    logger.info("=== Daily job started %s ===", datetime.utcnow().isoformat())

    try:
        market_data.run()
    except Exception as exc:
        logger.error("Market data scrape failed: %s", exc)

    try:
        n = nsta_news.run()
        logger.info("NSTA news: %d records upserted", n)
    except Exception as exc:
        logger.error("NSTA news scrape failed: %s", exc)

    try:
        n = investegate.run()
        logger.info("Investegate RNS: %d records upserted", n)
    except Exception as exc:
        logger.error("Investegate scrape failed: %s", exc)

    # Only regenerate briefings for companies that have new RNS since last briefing
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

    try:
        briefings.run(tickers=tickers_with_new_data)
    except Exception as exc:
        logger.error("Briefing generation failed: %s", exc)

    logger.info("=== Daily job complete ===")


def job_monthly() -> None:
    from src.scrapers import nsta_production

    logger.info("=== Monthly production refresh started ===")
    try:
        n = nsta_production.run()
        logger.info("Production refresh: %d records upserted", n)
    except Exception as exc:
        logger.error("Production refresh failed: %s", exc)
    logger.info("=== Monthly production refresh complete ===")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", choices=["daily", "monthly", "serve"], default="serve")
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
```

---

## FILE 8: web/src/lib/databricks.ts
**Purpose: Server-side Databricks client for the Next.js web app**

```typescript
/**
 * Server-side Databricks query helper.
 * Only import from API routes or server components.
 */
import { DBSQLClient } from "@databricks/sql";

function getClient() {
  return new DBSQLClient();
}

async function openSession() {
  const client = getClient();
  await client.connect({
    host: process.env.DATABRICKS_SERVER_HOSTNAME!,
    path: process.env.DATABRICKS_HTTP_PATH!,
    token: process.env.DATABRICKS_ACCESS_TOKEN!,
  });
  const session = await client.openSession();
  return { client, session };
}

export async function dbQuery<T = Record<string, unknown>>(
  sql: string,
  params: any[] = []
): Promise<T[]> {
  const { client, session } = await openSession();
  try {
    const operation = await session.executeStatement(sql, {
      runAsync: true,
      queryTimeout: BigInt(60),
    });
    const result = await operation.fetchAll();
    await operation.close();
    return result as T[];
  } finally {
    await session.close();
    await client.close();
  }
}
```

---

## FILE 9: web/src/app/api/briefings/route.ts
**Purpose: API endpoint that serves briefings from the Delta table**

```typescript
import { NextRequest, NextResponse } from "next/server";
import { dbQuery } from "@/lib/databricks";

export interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const ticker = searchParams.get("ticker");

  try {
    let sql = `
      SELECT ticker, company_name, year, briefing, generated_at, model
      FROM company_briefings
    `;
    const params: string[] = [];

    if (ticker) {
      sql += " WHERE UPPER(ticker) = UPPER(?)";
      params.push(ticker);
    }

    sql += " ORDER BY year DESC, ticker ASC";

    const rows = await dbQuery<Briefing>(sql, params);
    return NextResponse.json(rows);
  } catch (err) {
    console.error("Briefings API error:", err);
    return NextResponse.json({ error: "Failed to load briefings" }, { status: 500 });
  }
}
```

---

## FILE 10: web/src/app/api/production/route.ts
**Purpose: API endpoint that serves production data with optional operator/field/year filters**

```typescript
import { NextRequest, NextResponse } from "next/server";
import { dbQuery } from "@/lib/databricks";

export interface ProductionRow {
  FIELDNAME: string;
  ORGGRPNM: string;
  PERIODYR: number;
  PERIODMNTH: number;
  OILPRODMBD: number | null;
  AGASPROMMS: number | null;
  DGASPROMMS: number | null;
  GCONDVOL: number | null;
  GASFLARVOL: number | null;
  WATPRODVOL: number | null;
  water_cut_pct: number | null;  // computed server-side
}

const M3_TO_MBBL = 6.2898 / 1000;

function computeWaterCut(row: ProductionRow): number | null {
  const oil = row.OILPRODMBD ?? 0;
  const watMbbl = (row.WATPRODVOL ?? 0) * M3_TO_MBBL;
  const total = oil + watMbbl;
  if (total <= 0) return null;
  return parseFloat(((watMbbl / total) * 100).toFixed(1));
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const operator = searchParams.get("operator");
  const field = searchParams.get("field");
  const year = searchParams.get("year");

  try {
    let sql = `
      SELECT FIELDNAME, ORGGRPNM, PERIODYR, PERIODMNTH,
             OILPRODMBD, AGASPROMMS, DGASPROMMS, GCONDVOL, GASFLARVOL, WATPRODVOL
      FROM nsta_field_production_raw
      WHERE 1=1
    `;
    const params: (string | number)[] = [];

    if (operator) { sql += " AND UPPER(ORGGRPNM) = UPPER(?)"; params.push(operator); }
    if (field)    { sql += " AND UPPER(FIELDNAME) = UPPER(?)"; params.push(field); }
    if (year)     { sql += " AND PERIODYR = ?"; params.push(parseInt(year)); }

    sql += " ORDER BY PERIODYR DESC, PERIODMNTH DESC, FIELDNAME ASC LIMIT 5000";

    const rows = await dbQuery<ProductionRow>(sql, params);
    const enriched = rows.map((r) => ({ ...r, water_cut_pct: computeWaterCut(r) }));
    return NextResponse.json(enriched);
  } catch (err) {
    console.error("Production API error:", err);
    return NextResponse.json({ error: "Failed to load production data" }, { status: 500 });
  }
}
```

---

## FILE 11: web/src/app/digest/page.tsx
**Purpose: Server component that renders the briefings digest page**

```typescript
import { dbQuery } from "@/lib/databricks";
import BriefingCard from "@/components/BriefingCard";

interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

async function getBriefings(): Promise<Briefing[]> {
  try {
    return await dbQuery<Briefing>(`
      SELECT ticker, company_name, year, briefing, generated_at, model
      FROM company_briefings
      ORDER BY year DESC, ticker ASC
    `);
  } catch {
    return [];
  }
}

export default async function DigestPage() {
  const briefings = await getBriefings();

  // Group briefings by year
  const byYear = briefings.reduce<Record<number, Briefing[]>>((acc, b) => {
    acc[b.year] = acc[b.year] ?? [];
    acc[b.year].push(b);
    return acc;
  }, {});

  const years = Object.keys(byYear).map(Number).sort((a, b) => b - a);

  if (briefings.length === 0) {
    return (
      <div className="text-center py-24">
        <p className="text-[#A2F3F3]/60 text-lg">No briefings available yet.</p>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-semibold text-[#00EDED]">Weekly Intelligence Digest</h1>
        <p className="text-[#A2F3F3]/70 text-sm mt-1">
          AI-generated briefings for UKCS-listed operators.
        </p>
      </div>

      {years.map((year) => (
        <section key={year} className="mb-12">
          <h2 className="text-lg font-medium text-[#A2F3F3] mb-4 border-b border-[#00EDED]/20 pb-2">
            {year}
          </h2>
          <div className="grid gap-6 md:grid-cols-2 xl:grid-cols-3">
            {byYear[year].map((b) => (
              <BriefingCard key={b.ticker} briefing={b} />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
```

---

## FILE 12: web/src/components/BriefingCard.tsx
**Purpose: Client component that displays a single company briefing, expandable by section**

```typescript
"use client";

import { useState } from "react";

interface Briefing {
  ticker: string;
  company_name: string;
  year: number;
  briefing: string;
  generated_at: string;
  model: string;
}

const SECTION_HEADINGS = [
  "Production Performance",
  "Corporate Developments",
  "Regulatory Context",
  "Commercial Outlook",
];

// Parses the GPT-4o markdown output into labelled sections
function parseSections(text: string): { heading: string; body: string }[] {
  const sections: { heading: string; body: string }[] = [];

  for (let i = 0; i < SECTION_HEADINGS.length; i++) {
    const heading = SECTION_HEADINGS[i];
    const next = SECTION_HEADINGS[i + 1];
    const re = new RegExp(
      `(?:#{1,3}\\s*\\d*\\.?\\s*)?${heading}[:\\s]*([\\s\\S]*?)${next ? `(?=(?:#{1,3}\\s*\\d*\\.?\\s*)?${next})` : "$"}`,
      "i"
    );
    const match = text.match(re);
    if (match) {
      sections.push({ heading, body: match[1].trim() });
    }
  }

  return sections.length === 0 ? [{ heading: "Briefing", body: text }] : sections;
}

function generatedAgo(ts: string): string {
  const diff = Date.now() - new Date(ts).getTime();
  const hours = Math.floor(diff / 3_600_000);
  if (hours < 1) return "< 1h ago";
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

export default function BriefingCard({ briefing }: { briefing: Briefing }) {
  const [expanded, setExpanded] = useState(false);
  const sections = parseSections(briefing.briefing);

  return (
    <div className="bg-[#304550] rounded-lg border border-[#00EDED]/15 overflow-hidden flex flex-col">
      <div className="bg-[#003059] px-5 py-4 flex items-center justify-between">
        <div>
          <span className="text-[#00EDED] font-semibold text-sm font-mono">{briefing.ticker}</span>
          <h3 className="text-[#A2F3F3] font-medium text-base leading-tight mt-0.5">
            {briefing.company_name}
          </h3>
        </div>
        <span className="text-[#A2F3F3]/40 text-xs">{generatedAgo(briefing.generated_at)}</span>
      </div>

      <div className="px-5 py-4 flex-1 text-sm text-[#A2F3F3]/85 space-y-4">
        {(expanded ? sections : sections.slice(0, 1)).map((s) => (
          <div key={s.heading}>
            <p className="text-[#00EDED] text-xs font-semibold uppercase tracking-wider mb-1">
              {s.heading}
            </p>
            <p className="leading-relaxed whitespace-pre-line">{s.body}</p>
          </div>
        ))}
      </div>

      <button
        onClick={() => setExpanded((v) => !v)}
        className="w-full text-center py-2.5 text-xs text-[#00EDED] hover:bg-[#00EDED]/10 transition-colors border-t border-[#00EDED]/15"
      >
        {expanded ? "Show less" : `Show all ${sections.length} sections`}
      </button>
    </div>
  );
}
```

---

## FILE 13: web/src/components/ProductionExplorer.tsx
**Purpose: Interactive client component — operator/field dropdowns, Recharts line chart, data table**

Note: The OPERATORS list in this file still includes the old 9-company list and needs trimming to match the current 4-company watchlist.

```typescript
"use client";

import { useEffect, useState, useMemo } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

interface ProductionRow {
  FIELDNAME: string; ORGGRPNM: string; PERIODYR: number; PERIODMNTH: number;
  OILPRODMBD: number | null; AGASPROMMS: number | null; DGASPROMMS: number | null;
  GCONDVOL: number | null; GASFLARVOL: number | null; WATPRODVOL: number | null;
  water_cut_pct: number | null;
}

// NOTE: This list still has the old 9 companies — needs trimming to the 4 active ones
const OPERATORS = [
  { label: "Harbour Energy", value: "HARBOUR ENERGY PLC" },
  { label: "EnQuest", value: "ENQUEST PLC" },
  { label: "Serica Energy", value: "SERICA ENERGY" },
  { label: "Ithaca Energy", value: "ITHACA ENERGY" },
  { label: "Tullow", value: "TULLOW UK" },
  { label: "Afentra", value: "ADURA ENERGY" },
  { label: "Gulf Keystone", value: "GULF KEYSTONE" },
  { label: "Kistos", value: "KISTOS" },
  { label: "Jersey Oil & Gas", value: "JERSEY OIL AND GAS" },
];

const METRICS = [
  { key: "OILPRODMBD", label: "Oil (Mbbl)", color: "#00EDED" },
  { key: "AGASPROMMS", label: "Gas-Assoc (MMscf)", color: "#A2F3F3" },
  { key: "DGASPROMMS", label: "Gas-Disassoc (MMscf)", color: "#7dd3fc" },
  { key: "water_cut_pct", label: "Water Cut (%)", color: "#fb923c" },
];

export default function ProductionExplorer() {
  const [operator, setOperator] = useState(OPERATORS[0].value);
  const [field, setField] = useState<string>("ALL");
  const [rows, setRows] = useState<ProductionRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setField("ALL");
    fetch(`/api/production?operator=${encodeURIComponent(operator)}`)
      .then((r) => r.json())
      .then((data) => { if (data.error) throw new Error(data.error); setRows(data); })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [operator]);

  const fields = useMemo(() => [...new Set(rows.map((r) => r.FIELDNAME))].sort(), [rows]);

  const filtered = useMemo(
    () => (field === "ALL" ? rows : rows.filter((r) => r.FIELDNAME === field)),
    [rows, field]
  );

  // Aggregate by period (sum across fields if "All" selected)
  const chartData = useMemo(() => {
    const byPeriod: Record<string, Record<string, number>> = {};
    for (const r of filtered) {
      const key = `${r.PERIODYR}-${String(r.PERIODMNTH).padStart(2, "0")}`;
      if (!byPeriod[key]) byPeriod[key] = {};
      for (const m of METRICS) {
        const v = r[m.key as keyof ProductionRow] as number | null;
        if (v != null) byPeriod[key][m.key] = (byPeriod[key][m.key] ?? 0) + v;
      }
    }
    return Object.entries(byPeriod).sort(([a], [b]) => a.localeCompare(b))
      .map(([period, vals]) => ({ period, ...vals }));
  }, [filtered]);

  // [JSX rendering of dropdowns, chart, and table omitted for brevity —
  //  uses Recharts LineChart with METRICS lines, and a scrollable table
  //  showing the top 200 rows of filtered data]
}
```

---

## FILE 14: web/src/components/Nav.tsx
**Purpose: Top navigation bar for the web app**

```typescript
import Link from "next/link";

export default function Nav() {
  return (
    <nav className="bg-[#003059] border-b border-[#00EDED]/20">
      <div className="max-w-7xl mx-auto px-4 h-14 flex items-center justify-between">
        <Link href="/" className="text-[#00EDED] font-semibold tracking-wide text-sm uppercase">
          UKCS Intelligence
        </Link>
        <div className="flex gap-6 text-sm">
          <Link href="/digest" className="text-[#A2F3F3] hover:text-[#00EDED] transition-colors">
            Weekly Digest
          </Link>
          <Link href="/explorer" className="text-[#A2F3F3] hover:text-[#00EDED] transition-colors">
            Data Explorer
          </Link>
        </div>
      </div>
    </nav>
  );
}
```

---

## KNOWN ISSUE TO FIX

`ProductionExplorer.tsx` (File 13) still has the old 9-operator dropdown list. The five removed companies (TLW, AET, GKP, KIST, JOG) should be removed so only Harbour Energy, EnQuest, Serica Energy, and Ithaca Energy appear as options.
