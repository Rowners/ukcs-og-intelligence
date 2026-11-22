"""Watchlist shared by the scrapers and the briefing generator.

`ticker` is the key written to Delta (briefings, RNS, share prices). It stays
stable so rows already in Databricks still join.

Serica Energy moved from AIM to the LSE Main Market on 27 October 2026. Its
market TIDM changed from SQZ to SER. Yahoo's live series is SER.L, which still
carries the earlier history. SQZ.L after the move is a short, stale series.
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
        "ticker": "SQZ",
        "company_name": "Serica Energy",
        "nsta_operator": "SERICA ENERGY",
        "lse_tidm": "SER",
        "yf_symbol": "SER.L",
        "issuer_code": "SESQZ",
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
    # Accept the current LSE TIDM as an alias for the stored key (SER -> SQZ).
    alias = {c["lse_tidm"].upper(): c["ticker"] for c in COMPANIES}
    wanted = {alias.get(t, t) for t in wanted}
    return [c for c in COMPANIES if c["ticker"] in wanted]
