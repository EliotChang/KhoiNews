from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pipeline.main import (
    _blocked_reason_for_post,
    _buffer_jobs_target_for_run,
    _collect_live_covered_links,
    _covered_links_from_live_inventory,
    _fallback_media_from_web_thumbnail,
    _filter_blocked_posts,
    _is_publishable_mp4_url,
    _link_coverage_variants,
)
from pipeline.wj_ingest import SourcePostInput


def _post(
    *,
    title: str,
    description: str,
    link: str,
    source: str = "world_journal",
    raw_payload: dict[str, object] | None = None,
) -> SourcePostInput:
    return SourcePostInput(
        source=source,
        source_guid=f"{source}:{title}",
        title=title,
        description=description,
        link=link,
        published_at=None,
        raw_payload=raw_payload or {},
    )


class MainSourceFiltersTests(unittest.TestCase):
    def test_never_blocks_world_journal_posts(self) -> None:
        candidate = _post(
            title="Pixel 10 Review: Camera Test Results",
            description="Hands-on first look with benchmark charts.",
            link="https://www.theverge.com/2026/markets/opening-bell",
            source="world_journal",
            raw_payload={"feed_url": "https://feeds.engadget.com/rss.xml"},
        )
        reason = _blocked_reason_for_post(
            post=candidate,
            topic_blocklist_enabled=True,
            topic_block_terms=["review", "hands-on"],
            source_domain_blocklist=["theverge.com", "engadget.com"],
        )
        self.assertIsNone(reason)

    def test_blocks_review_term_in_title(self) -> None:
        candidate = _post(
            title="Pixel 10 Review: Camera Test Results",
            description="Hands-on first look with benchmark charts.",
            link="https://example.com/pixel-10",
            source="fallback_feed",
        )
        reason = _blocked_reason_for_post(
            post=candidate,
            topic_blocklist_enabled=True,
            topic_block_terms=["review", "hands-on"],
            source_domain_blocklist=[],
        )
        self.assertEqual(reason, "topic_blocked:review")

    def test_blocks_domain_from_article_link(self) -> None:
        candidate = _post(
            title="Market opens higher after policy update",
            description="Early trading reaction remains mixed.",
            link="https://www.theverge.com/2026/markets/opening-bell",
            source="fallback_feed",
        )
        reason = _blocked_reason_for_post(
            post=candidate,
            topic_blocklist_enabled=True,
            topic_block_terms=["review"],
            source_domain_blocklist=["theverge.com"],
        )
        self.assertEqual(reason, "domain_blocked:theverge.com")

    def test_blocks_domain_from_feed_url_payload(self) -> None:
        candidate = _post(
            title="Neutral title",
            description="Neutral description",
            link="https://example.com/neutral-story",
            source="fallback_feed",
            raw_payload={"feed_url": "https://feeds.engadget.com/rss.xml"},
        )
        reason = _blocked_reason_for_post(
            post=candidate,
            topic_blocklist_enabled=True,
            topic_block_terms=["review"],
            source_domain_blocklist=["engadget.com"],
        )
        self.assertEqual(reason, "domain_blocked:engadget.com")

    def test_allows_hard_news_when_no_block_signals(self) -> None:
        candidate = _post(
            title="UN Security Council adopts ceasefire resolution",
            description="Member states vote after overnight negotiations.",
            link="https://www.reuters.com/world/security-council-update",
        )
        reason = _blocked_reason_for_post(
            post=candidate,
            topic_blocklist_enabled=True,
            topic_block_terms=["review", "unboxing"],
            source_domain_blocklist=["theverge.com"],
        )
        self.assertIsNone(reason)

    def test_filter_returns_only_allowed_candidates(self) -> None:
        candidates = [
            _post(
                title="New vaccine rollout expands nationwide",
                description="Health ministry confirms expanded eligibility.",
                link="https://www.apnews.com/article/public-health-rollout",
            ),
            _post(
                title="Laptop Review roundup",
                description="Editors compare battery life.",
                link="https://example.com/review-roundup",
                source="fallback_feed",
            ),
            _post(
                title="Policy briefing released",
                description="Key fiscal measures are outlined.",
                link="https://www.engadget.com/some-policy-briefing",
                source="fallback_feed",
            ),
        ]
        allowed, blocked_count = _filter_blocked_posts(
            posts=candidates,
            topic_blocklist_enabled=True,
            topic_block_terms=["review"],
            source_domain_blocklist=["engadget.com"],
        )
        self.assertEqual(len(allowed), 1)
        self.assertEqual(blocked_count, 2)
        self.assertEqual(allowed[0].title, "New vaccine rollout expands nationwide")

    def test_publishable_mp4_url_helper_accepts_mp4(self) -> None:
        self.assertTrue(_is_publishable_mp4_url("https://cdn.example.com/video.mp4?token=abc"))

    def test_publishable_mp4_url_helper_rejects_manifest(self) -> None:
        self.assertFalse(_is_publishable_mp4_url("https://cdn.example.com/stream/index.m3u8"))

    def test_buffer_jobs_target_for_run_bootstraps_until_initial_queue_size(self) -> None:
        self.assertEqual(_buffer_jobs_target_for_run(total_buffer_jobs=0, initial_queue_size=10), 10)
        self.assertEqual(_buffer_jobs_target_for_run(total_buffer_jobs=7, initial_queue_size=10), 3)

    def test_buffer_jobs_target_for_run_returns_one_after_bootstrap(self) -> None:
        self.assertEqual(_buffer_jobs_target_for_run(total_buffer_jobs=10, initial_queue_size=10), 1)
        self.assertEqual(_buffer_jobs_target_for_run(total_buffer_jobs=25, initial_queue_size=10), 1)

    def test_link_coverage_variants_normalizes_trailing_slash(self) -> None:
        variants = _link_coverage_variants("https://example.com/story/")
        self.assertIn("https://example.com/story", variants)
        self.assertIn("https://example.com/story/", variants)

    @patch("pipeline.main.list_published_links_for_platforms")
    def test_collect_live_covered_links_builds_normalized_variants(self, mock_list_links: object) -> None:
        mock_list_links.return_value = [  # type: ignore[attr-defined]
            {"link": "https://example.com/story-1/", "platform": "metricool", "external_post_id": "id-1"}
        ]
        settings = SimpleNamespace(persona_key="default", publish_platforms=["metricool"], allow_duplicate_link_repost=False)
        covered_links = _collect_live_covered_links(conn=object(), settings=settings)
        self.assertIn("https://example.com/story-1", covered_links)
        self.assertIn("https://example.com/story-1/", covered_links)
        mock_list_links.assert_called_once()  # type: ignore[attr-defined]

    @patch("pipeline.main.list_published_links_for_platforms")
    def test_collect_live_covered_links_can_be_disabled_for_manual_reposts(self, mock_list_links: object) -> None:
        settings = SimpleNamespace(
            persona_key="default",
            publish_platforms=["metricool"],
            allow_duplicate_link_repost=True,
        )
        covered_links = _collect_live_covered_links(conn=object(), settings=settings)
        self.assertEqual(covered_links, set())
        mock_list_links.assert_not_called()  # type: ignore[attr-defined]

    def test_covered_links_from_live_inventory_matches_platform_and_external_id(self) -> None:
        rows = [
            {
                "link": "https://example.com/story-1",
                "platform": "youtube",
                "external_post_id": "yt_live_1",
            },
            {
                "link": "https://example.com/story-2",
                "platform": "instagram",
                "external_post_id": "ig_live_1",
            },
            {
                "link": "https://example.com/story-3",
                "platform": "youtube",
                "external_post_id": "yt_not_live",
            },
        ]
        live_ids = {
            "youtube": {"yt_live_1"},
            "instagram": {"ig_live_1"},
        }
        covered = _covered_links_from_live_inventory(
            published_link_rows=rows,
            live_external_ids_by_platform=live_ids,
        )
        self.assertIn("https://example.com/story-1", covered)
        self.assertIn("https://example.com/story-2", covered)
        self.assertNotIn("https://example.com/story-3", covered)

    def test_covered_links_from_live_inventory_splits_comma_separated_external_ids(self) -> None:
        rows = [
            {
                "link": "https://example.com/story-4",
                "platform": "youtube",
                "external_post_id": "yt_old,yt_live_4",
            }
        ]
        covered = _covered_links_from_live_inventory(
            published_link_rows=rows,
            live_external_ids_by_platform={"youtube": {"yt_live_4"}},
        )
        self.assertIn("https://example.com/story-4", covered)

    @patch("pipeline.main.upload_thumbnail_to_supabase", return_value="https://cdn.example.com/fallback.jpg")
    @patch("pipeline.main.generate_thumbnail")
    def test_fallback_media_from_web_thumbnail_returns_image_media(self, mock_generate_thumbnail: object, _mock_upload: object) -> None:
        mock_generate_thumbnail.return_value = type(  # type: ignore[attr-defined]
            "Thumb",
            (),
            {
                "source": "web-sourced",
                "description": "thumbnail from search",
                "url": "https://images.example.com/a.jpg",
            },
        )()
        media = _fallback_media_from_web_thumbnail(
            settings=object(),
            post_id="post-1",
            title="Market turbulence rises",
            script_text="Short script",
        )
        self.assertIsNotNone(media)
        assert media is not None
        self.assertEqual(media.media_type, "image")
        self.assertEqual(media.media_url, "https://cdn.example.com/fallback.jpg")
        self.assertEqual(media.selection_reason, "fallback:web-sourced")
        self.assertEqual(media.media_candidates[0].media_type, "image")
        self.assertEqual(media.media_candidates[0].media_url, "https://cdn.example.com/fallback.jpg")
        mock_generate_thumbnail.assert_called_once()  # type: ignore[attr-defined]
        strategy = mock_generate_thumbnail.call_args.kwargs.get("strategy")  # type: ignore[attr-defined]
        self.assertEqual(strategy, "web-sourced")

    @patch("pipeline.main.upload_thumbnail_to_supabase", return_value="https://cdn.example.com/gemini-fallback.jpg")
    @patch("pipeline.main.generate_thumbnail")
    def test_fallback_media_from_web_thumbnail_tries_gemini_after_web_sourced(self, mock_generate_thumbnail: object, _mock_upload: object) -> None:
        mock_generate_thumbnail.side_effect = [  # type: ignore[attr-defined]
            None,
            type(
                "Thumb",
                (),
                {
                    "source": "gemini-generated",
                    "description": "photoreal newsroom fallback",
                    "url": None,
                },
            )(),
        ]
        media = _fallback_media_from_web_thumbnail(
            settings=object(),
            post_id="post-2",
            title="No image found",
            script_text="Short script",
        )
        self.assertIsNotNone(media)
        assert media is not None
        self.assertEqual(media.selection_reason, "fallback:gemini-generated")
        self.assertEqual(media.media_url, "https://cdn.example.com/gemini-fallback.jpg")
        self.assertEqual(media.quality_summary, {
            "fallback_source": "gemini-generated",
            "fallback_description": "photoreal newsroom fallback",
            "fallback_url": None,
        })
        self.assertEqual(mock_generate_thumbnail.call_count, 2)  # type: ignore[attr-defined]
        strategy_calls = [call.kwargs.get("strategy") for call in mock_generate_thumbnail.call_args_list]  # type: ignore[attr-defined]
        self.assertEqual(strategy_calls, ["web-sourced", "gemini-generated"])

    @patch("pipeline.main.generate_thumbnail", side_effect=[None, None])
    def test_fallback_media_from_web_thumbnail_returns_none_when_no_thumbnail(self, mock_generate_thumbnail: object) -> None:
        media = _fallback_media_from_web_thumbnail(
            settings=object(),
            post_id="post-3",
            title="No image found",
            script_text="Short script",
        )
        self.assertIsNone(media)
        self.assertEqual(mock_generate_thumbnail.call_count, 2)  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
