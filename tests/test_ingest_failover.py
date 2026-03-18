from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pipeline.main import _ingest_with_primary_failover, _log_wj_config
from pipeline.wj_ingest import IngestResult, WJFeedUnavailableError, SourcePostInput


def _settings(**overrides: object) -> SimpleNamespace:
    defaults = {
        "wj_base_url": "https://www.worldjournal.com",
        "wj_category_paths": ["/wj/cate/breaking"],
        "request_timeout_seconds": 10,
        "max_posts_per_run": 5,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _post(source: str = "world_journal") -> SourcePostInput:
    return SourcePostInput(
        source=source,
        source_guid=f"{source}:guid-1",
        title="Headline",
        description="Description",
        link="https://www.worldjournal.com/wj/story/121006/12345678",
        published_at=datetime.now(),
        raw_payload={"ingest_source": "wj_scraper"},
    )


class IngestFailoverTests(unittest.TestCase):
    @patch("pipeline.main.LOGGER.info")
    def test_logs_wj_config(self, mock_info) -> None:
        _log_wj_config(wj_base_url="https://www.worldjournal.com")
        mock_info.assert_called_once()

    @patch("pipeline.main.fetch_wj_posts")
    def test_primary_success_returns_ingest_result(
        self,
        mock_fetch_wj_posts,
    ) -> None:
        mock_fetch_wj_posts.return_value = IngestResult(source="wj_scraper", posts=[_post()])

        ingest = _ingest_with_primary_failover(_settings())

        self.assertEqual(ingest.source, "wj_scraper")
        self.assertEqual(len(ingest.posts), 1)
        mock_fetch_wj_posts.assert_called_once_with(
            base_url="https://www.worldjournal.com",
            category_paths=["/wj/cate/breaking"],
            timeout_seconds=10,
            max_posts=5,
        )

    @patch("pipeline.main.fetch_wj_posts")
    def test_wj_feed_unavailable_raises_runtime_error(
        self,
        mock_fetch_wj_posts,
    ) -> None:
        mock_fetch_wj_posts.side_effect = WJFeedUnavailableError("WJ_BASE_URL is missing")

        with self.assertRaises(RuntimeError) as context:
            _ingest_with_primary_failover(_settings())
        self.assertIn("World Journal ingest failed", str(context.exception))

    @patch("pipeline.main.fetch_wj_posts")
    def test_unexpected_error_propagates(
        self,
        mock_fetch_wj_posts,
    ) -> None:
        mock_fetch_wj_posts.side_effect = RuntimeError("socket timeout")

        with self.assertRaises(RuntimeError):
            _ingest_with_primary_failover(_settings())


if __name__ == "__main__":
    unittest.main()
