"""
Scrape NSTA news articles.

The old listing path /news-publications/news/ now redirects to
/news-publications/. Article cards are still <article class="article">, but
the date sits in <span class="article-meta"> ("19 Nov 2026"), not a <time>
element, and the body is <div class="usercontent"> inside <section class="textblock">.
"""

import logging
import re
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from src.databricks.client import upsert_news

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nstauthority.co.uk/news-publications/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UKCS-intel-scraper/1.0)"}
_DATE_FORMATS = ("%d %b %Y", "%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d")


def _parse_date(date_str: str) -> date | None:
    cleaned = re.sub(r"\s+", " ", (date_str or "").strip())
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _parse_article_list(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen: set[str] = set()
    for art in soup.find_all("article", class_="article"):
        a_tag = art.find("a", class_="article-title", href=True) or art.find("a", href=True)
        if not a_tag:
            continue
        url = a_tag["href"]
        if not url.startswith("http"):
            url = "https://www.nstauthority.co.uk" + url
        if url in seen:
            continue
        seen.add(url)

        title = a_tag.get_text(strip=True)
        meta = art.find(class_="article-meta")
        time_tag = art.find("time")
        date_str = ""
        if meta:
            date_str = meta.get_text(strip=True)
        elif time_tag:
            date_str = time_tag.get_text(strip=True)

        results.append(
            {
                "title": title,
                "date_str": date_str,
                "date": _parse_date(date_str),
                "url": url,
            }
        )
    return results


def _fetch_article_content(url: str) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=(10, 30))
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        blocks = soup.select("main section.textblock div.usercontent")
        if blocks:
            parts = []
            seen = set()
            for block in blocks:
                text = block.get_text(separator="\n", strip=True)
                if text and text not in seen:
                    seen.add(text)
                    parts.append(text)
            if parts:
                return "\n\n".join(parts)
        body = soup.find("main") or soup.find("article")
        return body.get_text(separator="\n", strip=True) if body else ""
    except Exception as exc:
        logger.warning("Failed to fetch article %s: %s", url, exc)
        return ""


def _fetch_listing() -> str:
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=(10, 30))
    resp.raise_for_status()
    return resp.text


def run() -> int:
    """Scrape NSTA news and upsert articles to Delta. Returns count upserted."""
    html = _fetch_listing()
    articles = _parse_article_list(html)
    logger.info("Found %d articles on NSTA news listing", len(articles))
    if not articles:
        raise RuntimeError(
            "NSTA news listing returned no articles. The page markup may have changed: "
            + BASE_URL
        )

    for art in articles:
        art["content"] = _fetch_article_content(art["url"])

    count = upsert_news(articles, "nsta_news_raw")
    logger.info("Upserted %d NSTA news articles", count)
    return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
