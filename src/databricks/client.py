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
