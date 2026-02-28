"""
Scrape NSTA news articles from nstauthority.co.uk.

Note: The news listing page uses JavaScript to load articles dynamically.
Pagination via ?page=N returns the same 18 articles on every page.
This scraper therefore fetches page 1 only and treats those 18 articles
as the current static set. New articles appear when NSTA publishes them;
re-running will detect them via URL deduplication in Delta.

To get JS-rendered pagination in future, replace requests+BS4 with Playwright.
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
        # Fall back to main article body
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


if __name__ == "__main__":
    import logging
    from datetime import datetime
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
