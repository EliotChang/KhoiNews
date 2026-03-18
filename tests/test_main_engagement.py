from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pipeline.main import _select_ranked_candidates_with_floor, select_top_headlines_with_engagement
from pipeline.wj_ingest import IngestResult, SourcePostInput


def _settings(**overrides: object) -> SimpleNamespace:
    defaults = {
        "top_headlines_per_run": 2,
        "cadence_min_posts_per_run": 1,
        "engagement_scoring_enabled": True,
        "engagement_min_score": 0.62,
        "engagement_floor_score": 0.5,
        "content_mix_profile": "hard_news_culture",
        "pre_voice_metadata_enrichment_enabled": False,
        "request_timeout_seconds": 10,
        "article_context_min_words": 40,
        "article_context_max_words": 220,
        "fallback_feeds_enabled": True,
        "fallback_feeds_world_first": True,
        "fallback_feeds_max_posts": 20,
        "fallback_feed_urls": [
            "https://feeds.reuters.com/reuters/worldNews",
            "https://feeds.apnews.com/apf-topnews",
        ],
        "topic_blocklist_enabled": True,
        "topic_block_terms": ["review", "hands-on", "unboxing"],
        "source_domain_blocklist": ["engadget.com"],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _post(
    *,
    source: str,
    guid: str,
    title: str,
    description: str,
    link: str,
    age_hours: int,
) -> SourcePostInput:
    return SourcePostInput(
        source=source,
        source_guid=guid,
        title=title,
        description=description,
        link=link,
        published_at=datetime.now(timezone.utc) - timedelta(hours=age_hours),
        raw_payload={"feed_url": "https://feeds.reuters.com/reuters/worldNews"},
    )


class MainEngagementSelectionTests(unittest.TestCase):
    def test_higher_engagement_score_beats_newer_weaker_candidate(self) -> None:
        settings = _settings()
        newer_weaker = _post(
            source="world_journal",
            guid="weak-1",
            title="Morning review roundup",
            description="Hands-on comparison and deal highlights.",
            link="https://example.com/gadget-review",
            age_hours=1,
        )
        older_stronger = _post(
            source="world_journal",
            guid="strong-1",
            title="Congress passes emergency energy package after grid warnings",
            description="The bill includes federal support and immediate reliability mandates.",
            link="https://www.reuters.com/world/us/congress-energy-grid-package-2026-03-08/",
            age_hours=14,
        )

        selected, _stats = select_top_headlines_with_engagement(
            posts=[newer_weaker, older_stronger],
            settings=settings,
            candidate_origin="primary",
            top_n=2,
            min_score=0.0,
        )

        self.assertEqual(selected[0].source_guid, "strong-1")
        self.assertIn("engagement", selected[0].raw_payload)

    def test_hard_news_culture_profile_keeps_culture_hook_eligible(self) -> None:
        settings = _settings(engagement_min_score=0.5)
        hard_news = _post(
            source="world_journal",
            guid="hard-1",
            title="Fed signals potential rate pause as inflation cools",
            description="Officials cited labor and growth risks during policy remarks.",
            link="https://www.apnews.com/article/fed-rates-inflation-2026",
            age_hours=8,
        )
        culture_hook = _post(
            source="world_journal",
            guid="culture-1",
            title="Why did Taylor Swift pull the tour teaser minutes before launch?",
            description="Fan reaction surged as platform glitches and copyright claims emerged.",
            link="https://www.bbc.com/news/entertainment-arts-2026",
            age_hours=6,
        )

        selected, _stats = select_top_headlines_with_engagement(
            posts=[hard_news, culture_hook],
            settings=settings,
            candidate_origin="primary",
            top_n=2,
            min_score=0.5,
        )
        selected_ids = {post.source_guid for post in selected}
        self.assertIn("hard-1", selected_ids)
        self.assertIn("culture-1", selected_ids)

    def test_penalty_patterns_can_reject_low_quality_candidate(self) -> None:
        settings = _settings()
        low_signal = _post(
            source="world_journal",
            guid="low-1",
            title="Best phones review: top 10 deals this week",
            description="Affiliate picks with coupon links and compared battery charts.",
            link="https://example.com/phones-deals",
            age_hours=2,
        )

        selected, stats = select_top_headlines_with_engagement(
            posts=[low_signal],
            settings=settings,
            candidate_origin="primary",
            top_n=1,
            min_score=0.62,
        )

        self.assertEqual(selected, [])
        self.assertEqual(stats["below_threshold_count"], 1)

    @patch("pipeline.main.fetch_fallback_feed_posts")
    def test_primary_success_does_not_fetch_fallback_for_floor(self, mock_fetch_fallback_feed_posts) -> None:
        settings = _settings(top_headlines_per_run=1, cadence_min_posts_per_run=1)
        primary = _post(
            source="world_journal",
            guid="primary-1",
            title="UN security council advances ceasefire resolution",
            description="Diplomatic push follows overnight negotiations and regional pressure.",
            link="https://www.reuters.com/world/middle-east/un-ceasefire-resolution-2026-03-08/",
            age_hours=5,
        )

        ranked, stats = _select_ranked_candidates_with_floor(
            posts=[primary],
            ingest_source="primary_rss",
            settings=settings,
        )

        mock_fetch_fallback_feed_posts.assert_not_called()
        self.assertEqual(len(ranked), 1)
        self.assertEqual(stats["selected_primary"], 1)
        self.assertEqual(stats["selected_fallback"], 0)

    @patch("pipeline.main.fetch_fallback_feed_posts")
    def test_below_floor_primary_uses_fallback_backfill(self, mock_fetch_fallback_feed_posts) -> None:
        settings = _settings(top_headlines_per_run=2, cadence_min_posts_per_run=2)
        weak_primary = _post(
            source="world_journal",
            guid="primary-weak",
            title="Hands-on review of budget earbuds",
            description="Comparison charts and discount links.",
            link="https://example.com/earbuds-review",
            age_hours=1,
        )
        strong_fallback = _post(
            source="fallback_reuters_worldnews",
            guid="fallback-strong",
            title="Treasury warns of shipping shock after strait escalation",
            description="Officials cited energy risk and global trade disruptions.",
            link="https://www.reuters.com/world/strait-shipping-escalation-2026-03-08/",
            age_hours=4,
        )
        mock_fetch_fallback_feed_posts.return_value = IngestResult(
            source="trusted_fallback_feed",
            posts=[strong_fallback],
        )

        ranked, stats = _select_ranked_candidates_with_floor(
            posts=[weak_primary],
            ingest_source="primary_rss",
            settings=settings,
        )

        mock_fetch_fallback_feed_posts.assert_called_once()
        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0].source_guid, "fallback-strong")
        self.assertEqual(stats["selected_primary"], 0)
        self.assertEqual(stats["selected_fallback"], 1)
        self.assertEqual(ranked[0].raw_payload["engagement"]["status"], "selected_floor_backfill")

    @patch("pipeline.main.fetch_fallback_feed_posts")
    def test_primary_posts_remain_ahead_of_fallback_in_combined_ranked_order(self, mock_fetch_fallback_feed_posts) -> None:
        settings = _settings(top_headlines_per_run=2, cadence_min_posts_per_run=2, engagement_min_score=0.55)
        primary = _post(
            source="world_journal",
            guid="primary-strong",
            title="Senate passes border security funding package",
            description="Measure includes emergency funds and agency hiring provisions.",
            link="https://www.apnews.com/article/senate-border-security-funding",
            age_hours=7,
        )
        fallback = _post(
            source="fallback_reuters_worldnews",
            guid="fallback-strong-2",
            title="Oil markets jump after overnight supply disruption",
            description="Analysts cited elevated global freight and insurance risk.",
            link="https://www.reuters.com/markets/commodities/oil-supply-disruption-2026-03-08/",
            age_hours=6,
        )
        mock_fetch_fallback_feed_posts.return_value = IngestResult(
            source="trusted_fallback_feed",
            posts=[fallback],
        )

        ranked, stats = _select_ranked_candidates_with_floor(
            posts=[primary],
            ingest_source="primary_rss",
            settings=settings,
        )

        self.assertEqual(len(ranked), 2)
        self.assertEqual(stats["selected_primary"], 1)
        self.assertEqual(stats["selected_fallback"], 1)
        self.assertEqual(ranked[0].source, "world_journal")
        self.assertTrue(ranked[1].source.startswith("fallback_"))


if __name__ == "__main__":
    unittest.main()
