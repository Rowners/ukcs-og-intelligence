"""
Generate AI intelligence briefings for UKCS operators using GPT-4o.

Context loaded per company:
  - Field production history (monthly, by field) from nsta_field_production_raw
  - Last 20 RNS announcements from rns_announcements_raw
  - Relevant NSTA news filtered by company/field keyword match

Water cut fix:
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

# M3 to barrels conversion
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

    # Brent — last 90 days
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

    # Share price — last 90 days
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
        else:
            lines.append(f"{ticker} share price: data not available")
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
    # Add common abbreviated forms
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
    """Generate and save a briefing for one company. Returns the briefing text."""
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
    """
    Generate briefings for all watchlist companies (or a subset by ticker).
    delay: seconds to wait between companies to avoid TPM rate limits.
    """
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


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to run (e.g. HBR ENQ)")
    parser.add_argument("--delay", type=int, default=30, help="Seconds between companies (default: 30)")
    args = parser.parse_args()
    run(tickers=args.tickers, delay=args.delay)
