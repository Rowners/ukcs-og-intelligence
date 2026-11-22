"""
Fetch UKCS field production data from NSTA ArcGIS REST API and upsert to Delta.

The feature service's maxRecordCount is 1000. Asking for more can return a
full page with exceededTransferLimit set, so pagination follows that flag
rather than assuming a fixed page size.
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

BATCH_SIZE = 1000

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


def _fetch_page(offset: int, where: str = "1=1") -> tuple[list[dict], bool]:
    params = {
        "where": where,
        "outFields": ",".join(FIELDS),
        "resultOffset": offset,
        "resultRecordCount": BATCH_SIZE,
        "f": "json",
        "returnGeometry": "false",
    }
    resp = requests.get(BASE_URL, params=params, timeout=(10, 60))
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS API error: {data['error']}")

    features = data.get("features", [])
    exceeded = bool(data.get("exceededTransferLimit"))
    return [f["attributes"] for f in features], exceeded


def _fetch_all_pages(where: str) -> list[dict]:
    all_records: list[dict] = []
    offset = 0
    while True:
        if offset > 500_000:
            raise RuntimeError(f"ArcGIS pagination ran past 500k rows for where={where}")
        logger.info("Fetching production records where=%s offset=%d", where, offset)
        batch, exceeded = _fetch_page(offset, where=where)
        all_records.extend(batch)
        # A short page is the end. A full page, or an explicit exceeded flag,
        # means the service has more rows at the next offset.
        if not batch or (not exceeded and len(batch) < BATCH_SIZE):
            break
        offset += len(batch)
    return all_records


def fetch_for_year(year: int) -> list[dict]:
    """Fetch only records for a specific year — used for monthly refreshes."""
    records = _fetch_all_pages(f"PERIODYR = {int(year)}")
    logger.info("Fetched %d records for year %d", len(records), year)
    return records


def fetch_all() -> list[dict]:
    """Page through the full ArcGIS API. Use only for explicit full reloads."""
    records = _fetch_all_pages("1=1")
    logger.info("Fetched %d total production records", len(records))
    return records


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
