"""Offline checks for the scrapers that broke against live sources."""

import unittest
from datetime import date

from src.companies import by_ticker
from src.scrapers.investegate import decode_js_string, extract_article_body
from src.scrapers.market_data import _records_from_chart
from src.scrapers.nsta_news import _parse_article_list, _parse_date
from src.scrapers.nsta_production import _fetch_all_pages


class NstaNewsTests(unittest.TestCase):
    def test_parses_current_listing_markup(self):
        html = """
        <article class="article">
          <div class="article-content">
            <span class="article-meta">19 Nov 2026</span>
            <a href="/news-publications/example-story/" class="article-title">
              <span class="underline">Example story</span>
            </a>
          </div>
        </article>
        """
        rows = _parse_article_list(html)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Example story")
        self.assertEqual(rows[0]["date"], date(2026, 11, 19))
        self.assertTrue(rows[0]["url"].endswith("/news-publications/example-story/"))

    def test_date_formats(self):
        self.assertEqual(_parse_date("19 Nov 2026"), date(2026, 11, 19))
        self.assertEqual(_parse_date("19 November 2026"), date(2026, 11, 19))
        self.assertIsNone(_parse_date(""))


class RnsTests(unittest.TestCase):
    def test_js_string_and_article_body(self):
        encoded = r"Harbour said \x22hello\u002dworld\nnext"
        self.assertEqual(decode_js_string(encoded), 'Harbour said "hello-world\nnext')
        html = '<script>articleBody="<p>Production rose.</p>";</script>'
        self.assertIn("Production rose.", extract_article_body(html))


class MarketDataTests(unittest.TestCase):
    def test_adjusted_close(self):
        chart = {
            "timestamp": [1_700_000_000],
            "indicators": {
                "quote": [{
                    "open": [100.0],
                    "high": [110.0],
                    "low": [90.0],
                    "close": [100.0],
                    "volume": [5],
                }],
                "adjclose": [{"adjclose": [50.0]}],
            },
        }
        rows = _records_from_chart(chart, {"currency": "USD"})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["close"], 50.0)
        self.assertEqual(rows[0]["open"], 50.0)
        self.assertEqual(rows[0]["volume"], 5)

    def test_serica_is_not_on_the_watchlist(self):
        tickers = {c["ticker"] for c in by_ticker(None)}
        self.assertEqual(tickers, {"HBR", "ENQ", "ITH"})
        self.assertEqual(by_ticker(["SQZ"]), [])
        self.assertEqual(by_ticker(["SER"]), [])


class PaginationTests(unittest.TestCase):
    def test_follows_exceeded_transfer_limit(self):
        pages = {
            0: ([{"FIELDNAME": "A"}], True),
            1: ([{"FIELDNAME": "B"}], False),
        }

        def fake_fetch(offset, where="1=1"):
            return pages[offset]

        import src.scrapers.nsta_production as mod
        original = mod._fetch_page
        mod._fetch_page = fake_fetch
        try:
            rows = _fetch_all_pages("1=1")
        finally:
            mod._fetch_page = original
        self.assertEqual([r["FIELDNAME"] for r in rows], ["A", "B"])


if __name__ == "__main__":
    unittest.main()
