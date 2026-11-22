# UKCS Oil & Gas Intelligence Platform

An end-to-end data pipeline and intelligence platform for the UK Continental Shelf. Pulls public production data, regulatory news, and company announcements, generates AI analyst briefings, and serves everything through a web application.

## What it does

- **Scrapes** monthly field production data from the NSTA's public ArcGIS API (all UKCS fields)
- **Scrapes** NSTA regulatory news and RNS announcements from the London Stock Exchange for four operators
- **Tracks** Brent crude prices and LSE share prices daily from Yahoo Finance's chart API
- **Generates** structured GPT-4o analyst briefings covering production performance, corporate developments, regulatory context, and commercial outlook
- **Stores** everything in Databricks Delta tables
- **Serves** briefings, an interactive production explorer, and a live price analysis dashboard through a Next.js web app

## Watchlist companies

| Ticker | Company | NSTA Operator |
|--------|---------|---------------|
| HBR | Harbour Energy | HARBOUR ENERGY PLC |
| ENQ | EnQuest | ENQUEST PLC |
| SQZ (LSE: SER since 27 Oct 2026) | Serica Energy | SERICA ENERGY |
| ITH | Ithaca Energy | ITHACA ENERGY |

## Architecture

```
Public data sources
  ├── NSTA ArcGIS REST API  → nsta_field_production_raw  (Delta)
  ├── NSTA news website     → nsta_news_raw              (Delta)
  └── LSE RNS search API    → rns_announcements_raw      (Delta)

Yahoo Finance v8 chart API
  ├── BZ=F (Brent crude)    → brent_crude_prices         (Delta)
  └── HBR.L / ENQ.L / SER.L / ITH.L → company_share_prices (Delta)

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
- **Share price** — in GBp (LSE tickers: HBR.L, ENQ.L, SER.L, ITH.L). Serica's TIDM changed from SQZ to SER on 27 October 2026; stored rows stay under SQZ.
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
│   │   ├── investegate.py      # RNS announcements via the LSE search API
│   │   └── market_data.py      # Brent + share prices via the Yahoo chart API
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
OPENAI_MODEL=gpt-4o
```

`gpt-4o` still serves the API and shuts down on 14 January 2027. Set `OPENAI_MODEL` to the current replacement before then. The same four names are GitHub Actions secrets (`OPENAI_MODEL` is optional). The web app does not use the OpenAI key.

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

### 5. Deploy the web app on Netlify

The Next.js app is in `web/`. `netlify.toml` at the repo root is what Netlify reads. It sets the base directory, build command, and publish directory, and it loads `@netlify/plugin-nextjs` (v5, installed from `web/package.json`). That plugin is the Next.js runtime. Without it, Netlify publishes the raw `.next` folder, `plugin_state` stays `none`, no functions are created, and every route 404s.

Settings for the Netlify UI or the create-site API (`build_settings` / `repo`):

| Setting | Value | API field |
|---------|--------|-----------|
| Base directory | `web` | `base` |
| Build command | `npm run build` | `cmd` |
| Publish directory | `.next` | `dir` |
| Node.js | 20 | `NODE_VERSION` in `netlify.toml` |

`dir` is relative to the base directory, so the configured publish path is `web/.next`. Leave it as `.next` in the UI. `npm run build` runs `next build`. `@netlify/plugin-nextjs` reads that output, publishes the static assets, and deploys one server function for the App Router, including `/api/briefings`, `/api/production`, and `/api/prices`. Synchronous functions stop at 60 seconds. If the plugin does not run, Netlify uploads `.next` as a static folder and every route 404s.

Create the site with the API (token via `Authorization: Bearer <NETLIFY_AUTH_TOKEN>`):

```http
POST https://api.netlify.com/api/v1/{account_slug}/sites
```

```json
{
  "name": "ukcs-og-intelligence",
  "repo": {
    "provider": "github",
    "repo_path": "Rowners/ukcs-og-intelligence",
    "repo_branch": "main",
    "cmd": "npm run build",
    "dir": ".next"
  },
  "build_settings": {
    "base": "web",
    "cmd": "npm run build",
    "dir": ".next"
  }
}
```

`cmd` and `dir` are the fields on the public `repo` object. `base` is `build_settings.base`. `dir` is relative to that base, so the published folder is `web/.next`. The root `netlify.toml` sets the same three values and overrides the UI if they drift.

Set these as site environment variables (available to builds and functions). Do not put them in the repo:

```
DATABRICKS_SERVER_HOSTNAME=dbc-14a9e097-c58d.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/ec8cf8525e01cae5
DATABRICKS_ACCESS_TOKEN=<new personal access token>
```

The hostname and warehouse above are the existing workspace. The previous access token no longer exists, which is why the live queries fail immediately. Mint a new token in that workspace and store only the new value. The SQL warehouse `ec8cf8525e01cae5` is running and the Delta tables are intact, including the briefings from March 2026.

```bash
# From the repo root, after the three variables are in the environment
npx netlify build
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

# RNS announcements (the LSE API returns the latest five per company)
python -m src.scrapers.investegate

# Generate briefings (30s delay between companies for rate limiting)
python -m src.intelligence.briefings
```

### Scheduled runs

GitHub Actions runs the pipeline. Daily is `.github/workflows/daily.yml` (06:00 UTC). Monthly production is `.github/workflows/monthly.yml` (07:00 UTC on the 1st). GitHub turns those schedules off after 60 days without a commit to the repository. After that, enable both workflows on the Actions tab and run them once by hand.

```bash
# Run a job locally
python -m src.scheduler --job daily
python -m src.scheduler --job monthly

# Or leave a process running the same clock
python -m src.scheduler --job serve
```

A job exits non-zero if a step fails. The SQL warehouse has to be running first: Databricks Free Edition pauses it when idle, and a paused warehouse is not woken reliably by a short web request.

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
| `rns_announcements_raw` | url | LSE RNS announcements (latest five per company per run) |
| `brent_crude_prices` | date | Daily OHLCV for ICE Brent (USD/bbl) |
| `company_share_prices` | ticker + date | Daily OHLCV for watchlist companies (GBp) |
| `company_briefings` | ticker + year | GPT-4o generated analyst briefings (all versions retained; UI shows latest per ticker) |

## Notes

- **Databricks Free Edition**: The SQL warehouse auto-pauses after inactivity. Start it manually before running scripts or allow a 2–5 minute cold-start delay on first connection.
- **OpenAI rate limits**: The briefing generator waits 30 seconds between companies. Use `--delay` to adjust. `gpt-4o` shuts down on 14 January 2027; set `OPENAI_MODEL` before then.
- **NSTA production data**: The ArcGIS API returns multiple sub-unit rows per field per period. These are aggregated (summed) before upsert so each FIELDNAME + PERIODYR + PERIODMNTH combination is unique. Pages are followed while `exceededTransferLimit` is set (the service caps a page at 1000 rows).
- **RNS**: The LSE search API returns the five most recent announcements per issuer. Serica is requested under TIDM SER and stored as SQZ.
- **Price Analysis**: Live data is fetched server-side from Yahoo Finance's v8 chart API with a browser-like User-Agent. No API key required. Data is not cached — each page load makes fresh requests.
- **Water cut**: Reported as a percentage average across all selected fields. WATPRODVOL (m³) is converted to Mbbl (× 0.0062898) before computing water cut = water / (water + oil).
