"""Watchlist shared by the scrapers and the briefing generator.

`ticker` is the key written to Delta (briefings, RNS, share prices).

Serica Energy (stored as SQZ, later LSE TIDM SER) is not on the watchlist.
Existing Databricks rows are left in place. The web app filters them out.
"""

from __future__ import annotations

COMPANIES: list[dict[str, str]] = [
    {
        "ticker": "HBR",
        "company_name": "Harbour Energy",
        "nsta_operator": "HARBOUR ENERGY PLC",
        "lse_tidm": "HBR",
        "yf_symbol": "HBR.L",
        "issuer_code": "PRCI",
    },
    {
        "ticker": "ENQ",
        "company_name": "EnQuest",
        "nsta_operator": "ENQUEST PLC",
        "lse_tidm": "ENQ",
        "yf_symbol": "ENQ.L",
        "issuer_code": "ENENQ",
    },
    {
        "ticker": "ITH",
        "company_name": "Ithaca Energy",
        "nsta_operator": "ITHACA ENERGY",
        "lse_tidm": "ITH",
        "yf_symbol": "ITH.L",
        "issuer_code": "ITHEPL",
    },
]


def by_ticker(tickers: list[str] | None) -> list[dict[str, str]]:
    if not tickers:
        return list(COMPANIES)
    wanted = {t.upper() for t in tickers}
    alias = {c["lse_tidm"].upper(): c["ticker"] for c in COMPANIES}
    wanted = {alias.get(t, t) for t in wanted}
    return [c for c in COMPANIES if c["ticker"] in wanted]
