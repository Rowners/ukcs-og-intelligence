# UKCS Oil & Gas Intelligence Platform

An end-to-end data pipeline and intelligence platform for the UK Continental Shelf. Pulls public production data, regulatory news, and company announcements, generates AI analyst briefings, and serves everything through a web application.

## What it does

- **Scrapes** monthly field production data from the NSTA's public ArcGIS API (all UKCS fields)
- **Scrapes** NSTA regulatory news and RNS announcements from Investegate for four major independents
- **Tracks** Brent crude prices and LSE share prices daily via yfinance
- **Generates** structured GPT-4o analyst briefings covering production performance, corporate developments, regulatory context, and commercial outlook
- **Stores** everything in Databricks Delta tables
- **Serves** briefings, an interactive production explorer, and a live price analysis dashboard through a Next.js web app

## Watchlist companies

| Ticker | Company | NSTA Operator |
|--------|---------|---------------|
| HBR | Harbour Energy | HARBOUR ENERGY PLC |
| ENQ | EnQuest | ENQUEST PLC |
| SQZ | Serica Energy | SERICA ENERGY |
| ITH | Ithaca Energy | ITHACA ENERGY |

## Architecture

```
Public data sources
  ├── NSTA ArcGIS REST API  → nsta_field_production_raw  (Delta)
  ├── NSTA news website     → nsta_news_raw              (Delta)
  └── Investegate RNS       → rns_announcements_raw      (Delta)

yfinance
  ├── BZ=F (Brent crude)    → brent_crude_prices         (Delta)
  └── HBR/ENQ/SQZ/ITH .L   → company_share_prices       (Delta)

GPT-4o briefing generator   → company_briefings          (Delta)

Yahoo Finance v8 API (live) → Price Analysis charts (no Databricks)

Next.js web app
  ├── /digest          — AI briefing cards (latest per company)
  ├── /explorer        — interactive production data chart + table
  └── /price-analysis  — live share price vs Brent vs TTF gas charts
```

## Web app pages

### Weekly Intelligence Digest (`/digest`)
AI-generated briefings for each watchlist company covering production performance, corporate developments, regulatory context, and commercial outlook. Shows only the most recently generated briefing per company, with a "Last updated" date. Older briefings are retained in the database but not displayed.

### Data Explorer (`/explorer`)
Interactive production data viewer backed by Databricks. Features:
- Filter by operator, field, and year
- **Production volumes panel** — Oil (cyan), Associated Gas (green), Gas (purple) as separate line series
- **Water cut panel** — shown separately below the production chart, synced crosshair via `syncId`; displays average water cut across selected fields
- Sortable data table below the charts

### Price Analysis (`/price-analysis`)
Live market data fetched directly from Yahoo Finance's v8 chart API at request time (no Databricks). Three stacked, synced panels:
- **Share price** — in GBp (LSE tickers: HBR.L, ENQ.L, SQZ.L, ITH.L)
- **Brent Crude** — ICE Last Day Financial Futures (`BZ=F`), USD/bbl
- **TTF Natural Gas** — Dutch TTF Calendar Futures (`TTF=F`), EUR/MWh

All three panels share a hover crosshair. Y-axes auto-scale to the data range. Selector for 1Y / 2Y / 3Y periods.

## Project structure

```
ukoandganalysis/
├── src/
│   ├── scrapers/
│   │   ├── nsta_production.py  # ArcGIS API pagination
│   │   ├── nsta_news.py        # NSTA news scraper
│   │   ├── investegate.py      # RNS announcement scraper
│   │   └── market_data.py      # Brent + share prices via yfinance
│   ├── intelligence/
│   │   └── briefings.py        # GPT-4o briefing generator
│   ├── databricks/
│   │   └── client.py           # Delta table read/write helpers
│   └── scheduler.py            # APScheduler daily/monthly jobs
├── web/                        # Next.js 14 app (App Router)
│   └── src/
│       ├── app/
│       │   ├── digest/         # Briefings page
│       │   ├── explorer/       # Production data explorer
│       │   ├── price-analysis/ # Live price charts page
│       │   └── api/
│       │       ├── briefings/  # Databricks briefings endpoint
│       │       ├── production/ # Databricks production endpoint
│       │       └── prices/     # Yahoo Finance proxy endpoint
│       ├── components/
│       │   ├── BriefingCard.tsx
│       │   ├── ProductionExplorer.tsx
│       │   ├── PriceAnalysis.tsx
│       │   └── Nav.tsx
│       └── lib/
│           └── databricks.ts   # Server-side Databricks client
├── test_connections.py         # Quick connectivity check
├── requirements.txt
└── .env.example
```

## Setup

### Prerequisites

- Python 3.11+
- Node.js 18+ (for the web app)
- A [Databricks](https://databricks.com) workspace (Free Edition works)
- An OpenAI API key

### 1. Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Environment variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-personal-access-token
OPENAI_API_KEY=sk-proj-...
```

### 3. Test connections

```bash
python test_connections.py
```

This verifies Databricks connectivity, checks expected Delta tables exist, and confirms the OpenAI API key is valid.

### 4. Web app

```bash
cd web
npm install
cp .env.local.example .env.local   # add Databricks credentials
npm run dev                         # http://localhost:3000
```

## Running the pipeline

### Initial data load

```bash
# Full NSTA production history (slow — ~130k records across all years)
python -m src.scrapers.nsta_production --full

# Brent crude + share prices — last 5 years backfill
python -m src.scrapers.market_data --history

# NSTA news
python -m src.scrapers.nsta_news

# RNS announcements (last 20 per company)
python -m src.scrapers.investegate

# Generate briefings (30s delay between companies for rate limiting)
python -m src.intelligence.briefings
```

### Scheduled runs

```bash
# Start the scheduler (daily at 06:00 UTC, monthly on the 1st at 07:00 UTC)
python -m src.scheduler --job serve

# Run a specific job manually
python -m src.scheduler --job daily
python -m src.scheduler --job monthly
```

### Targeted briefing regeneration

```bash
# Single company
python -m src.intelligence.briefings --tickers HBR

# Subset
python -m src.intelligence.briefings --tickers ENQ SQZ --delay 15
```

## Delta tables

| Table | Key | Description |
|-------|-----|-------------|
| `nsta_field_production_raw` | FIELDNAME + PERIODYR + PERIODMNTH | Monthly production per field (oil Mbbl, gas MMscf, water m³, condensate Mbbl) |
| `nsta_news_raw` | url | NSTA regulatory news articles |
| `rns_announcements_raw` | url | Investegate RNS announcements |
| `brent_crude_prices` | date | Daily OHLCV for ICE Brent (USD/bbl) |
| `company_share_prices` | ticker + date | Daily OHLCV for watchlist companies (GBp) |
| `company_briefings` | ticker + year | GPT-4o generated analyst briefings (all versions retained; UI shows latest per ticker) |

## Notes

- **Databricks Free Edition**: The SQL warehouse auto-pauses after inactivity. Start it manually before running scripts or allow a 2–5 minute cold-start delay on first connection.
- **OpenAI rate limits**: Free-tier accounts have a 30,000 TPM limit. The briefing generator defaults to a 30-second delay between companies. Use `--delay` to adjust.
- **NSTA production data**: The ArcGIS API returns multiple sub-unit rows per field per period. These are aggregated (summed) before upsert so each FIELDNAME + PERIODYR + PERIODMNTH combination is unique.
- **Price Analysis**: Live data is fetched server-side from Yahoo Finance's v8 chart API with a browser-like User-Agent. No API key required. Data is not cached — each page load makes fresh requests.
- **Water cut**: Reported as a percentage average across all selected fields. WATPRODVOL (m³) is converted to Mbbl (× 0.0062898) before computing water cut = water / (water + oil).
