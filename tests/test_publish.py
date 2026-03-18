from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from pipeline.content_gen import ContentGenerationResult
from pipeline.db import PublishJob
from pipeline.publish import (
    MAX_AUDIO_SECONDS,
    MAX_AUDIO_VIDEO_DELTA_SECONDS,
    MAX_VIDEO_SECONDS,
    MIN_AUDIO_SECONDS,
    MIN_SCRIPT_WORDS,
    MIN_VIDEO_SECONDS,
    MediaPublishPayload,
    _compliance_checks,
    _publish_metricool,
    dispatch_ready_publish_jobs,
    enqueue_publish_jobs_for_post,
)
from pipeline.wj_ingest import SourcePostInput


def _settings(**overrides: object) -> SimpleNamespace:
    defaults: dict[str, object] = {
        "request_timeout_seconds": 20,
        "metricool_publish_enabled": True,
        "metricool_user_token": "metricool-token",
        "metricool_api_url": "https://app.metricool.com/api",
        "metricool_user_id": "user-1",
        "metricool_blog_id": "blog-1",
        "metricool_target_platforms": ["tiktok", "instagram", "youtube"],
        "metricool_review_required": False,
        "publish_enabled": True,
        "publish_platforms": ["metricool"],
        "publish_max_retries": 3,
        "publish_max_jobs_per_run": 1,
        "publish_claim_stale_in_progress_minutes": 45,
        "publish_enforce_compliance": True,
        "allow_duplicate_link_repost": False,
        "publish_min_script_words": MIN_SCRIPT_WORDS,
        "publish_min_audio_seconds": MIN_AUDIO_SECONDS,
        "publish_max_audio_seconds": MAX_AUDIO_SECONDS,
        "publish_min_video_seconds": MIN_VIDEO_SECONDS,
        "publish_max_video_seconds": MAX_VIDEO_SECONDS,
        "publish_audio_video_max_delta_seconds": MAX_AUDIO_VIDEO_DELTA_SECONDS,
        "persona_key": "default",
        "metricool_link_in_bio_enabled": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _content(script_words: int = MIN_SCRIPT_WORDS) -> ContentGenerationResult:
    script = " ".join(["fact"] * script_words) + "."
    return ContentGenerationResult(
        script_10s=script,
        video_title_short="Title",
        caption_instagram="Instagram caption",
        caption_tiktok="TikTok caption",
        caption_youtube="YouTube caption",
        caption_x="X caption",
        hashtags=["#news", "#policy", "#world"],
        tone="neutral",
        language="en",
        model_name="test-model",
    )


def _post() -> SourcePostInput:
    return SourcePostInput(
        source="world_journal",
        source_guid="guid-1",
        title="Headline with enough context for newsroom brief",
        description="Description with enough details to satisfy pre voice context requirements and validation.",
        link="https://example.com/story",
        published_at=datetime.now(),
        raw_payload={"ingest_source": "wj_scraper"},
    )


class MetricoolPublishTests(unittest.TestCase):
    def test_compliance_blocks_script_below_word_floor(self) -> None:
        checks, blocker = _compliance_checks(
            platform="metricool",
            payload={
                "title": "Title",
                "media_type": "video",
                "media_url": "https://cdn.example.com/video.mp4",
                "caption_tiktok": "Normal caption",
                "script_10s": "too short script.",
                "audio_duration_sec": 32,
                "video_duration_sec": 33,
            },
            enforce_compliance=True,
        )

        self.assertIsNotNone(blocker)
        self.assertIn("script must contain at least", blocker or "")
        words_check = next(check for check in checks if check["name"] == "script_words_min")
        self.assertFalse(words_check["passed"])

    def test_compliance_blocks_duration_below_floor(self) -> None:
        checks, blocker = _compliance_checks(
            platform="metricool",
            payload={
                "title": "Title",
                "media_type": "video",
                "media_url": "https://cdn.example.com/video.mp4",
                "caption_tiktok": "Normal caption",
                "script_10s": " ".join(["fact"] * MIN_SCRIPT_WORDS) + ".",
                "audio_duration_sec": MIN_AUDIO_SECONDS - 4,
                "video_duration_sec": MIN_VIDEO_SECONDS - 4,
            },
            enforce_compliance=True,
        )

        self.assertIsNotNone(blocker)
        self.assertIn("audio duration must be between", blocker or "")
        self.assertIn("video duration must be between", blocker or "")
        audio_check = next(check for check in checks if check["name"] == "audio_duration_bounds")
        video_check = next(check for check in checks if check["name"] == "video_duration_bounds")
        self.assertFalse(audio_check["passed"])
        self.assertFalse(video_check["passed"])

    @patch("pipeline.publish.upsert_publish_job")
    def test_enqueue_blocks_non_mp4_media(self, mock_upsert: Mock) -> None:
        jobs = enqueue_publish_jobs_for_post(
            conn=Mock(),
            settings=_settings(allow_duplicate_link_repost=True),
            post=_post(),
            post_id="post-1",
            content=_content(),
            media=MediaPublishPayload(
                media_type="video",
                media_url="https://cdn.example.com/video.m3u8",
                selection_reason="render",
            ),
            voice=SimpleNamespace(audio_url="https://cdn.example.com/audio.mp3", audio_duration_sec=33),
            video_duration_sec=34,
        )

        self.assertEqual(jobs, [])
        mock_upsert.assert_not_called()

    @patch("pipeline.publish.has_published_link_for_platforms")
    @patch("pipeline.publish.upsert_publish_job")
    def test_enqueue_blocks_already_published_article_link_by_default(
        self,
        mock_upsert: Mock,
        mock_has_link: Mock,
    ) -> None:
        mock_has_link.return_value = True

        jobs = enqueue_publish_jobs_for_post(
            conn=Mock(),
            settings=_settings(allow_duplicate_link_repost=False),
            post=_post(),
            post_id="post-1",
            content=_content(),
            media=MediaPublishPayload(
                media_type="video",
                media_url="https://cdn.example.com/video.mp4",
                selection_reason="render",
            ),
            voice=SimpleNamespace(audio_url="https://cdn.example.com/audio.mp3", audio_duration_sec=33),
            video_duration_sec=34,
        )

        self.assertEqual(jobs, [])
        mock_has_link.assert_called_once()
        mock_upsert.assert_not_called()

    @patch("pipeline.publish.has_published_link_for_platforms")
    @patch("pipeline.publish.upsert_publish_job")
    def test_enqueue_allows_duplicate_link_when_override_enabled(
        self,
        mock_upsert: Mock,
        mock_has_link: Mock,
    ) -> None:
        mock_upsert.return_value = SimpleNamespace(id="job-1")

        jobs = enqueue_publish_jobs_for_post(
            conn=Mock(),
            settings=_settings(allow_duplicate_link_repost=True),
            post=_post(),
            post_id="post-1",
            content=_content(),
            media=MediaPublishPayload(
                media_type="video",
                media_url="https://cdn.example.com/video.mp4",
                selection_reason="render",
            ),
            voice=SimpleNamespace(audio_url="https://cdn.example.com/audio.mp3", audio_duration_sec=33),
            video_duration_sec=34,
        )

        self.assertEqual(len(jobs), 1)
        mock_has_link.assert_not_called()
        mock_upsert.assert_called_once()

    @patch("pipeline.publish.upsert_publish_job")
    def test_enqueue_payload_omits_instagram_carousel_items(self, mock_upsert: Mock) -> None:
        mock_upsert.return_value = SimpleNamespace(id="job-1")

        jobs = enqueue_publish_jobs_for_post(
            conn=Mock(),
            settings=_settings(allow_duplicate_link_repost=True),
            post=_post(),
            post_id="post-1",
            content=_content(),
            media=MediaPublishPayload(
                media_type="video",
                media_url="https://cdn.example.com/video.mp4",
                selection_reason="render",
            ),
            voice=SimpleNamespace(audio_url="https://cdn.example.com/audio.mp3", audio_duration_sec=33),
            video_duration_sec=34,
        )

        self.assertEqual(len(jobs), 1)
        payload = mock_upsert.call_args.kwargs["payload"]
        self.assertNotIn("instagram_carousel_items", payload)

    @patch("pipeline.publish._metricool_normalize_media_url")
    @patch("pipeline.publish.requests.post")
    def test_publish_metricool_instagram_reel_uses_single_media(
        self,
        mock_post: Mock,
        mock_normalize_media_url: Mock,
    ) -> None:
        settings = _settings(metricool_target_platforms=["instagram"])
        mock_normalize_media_url.return_value = "https://cdn.example.com/video.mp4"

        response = Mock()
        response.status_code = 200
        response.json.return_value = {"id": "metricool-1"}
        response.raise_for_status.return_value = None
        mock_post.return_value = response

        result = _publish_metricool(
            settings=settings,
            payload={
                "title": "Title",
                "caption_instagram": "Instagram caption",
                "media_url": "https://cdn.example.com/video.mp4",
                "article_url": "https://example.com/story",
            },
        )

        self.assertEqual(result.status, "published")
        request_payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(request_payload["media"], ["https://cdn.example.com/video.mp4"])
        self.assertEqual(request_payload["instagramData"]["type"], "REEL")
        self.assertTrue(request_payload["instagramData"]["showReelOnFeed"])

    @patch("pipeline.publish._publish_metricool")
    @patch("pipeline.publish.claim_publish_jobs_ready")
    @patch("pipeline.publish.create_publish_attempt")
    @patch("pipeline.publish.mark_publish_attempt_published")
    def test_dispatch_metricool_publishes_once(
        self,
        mock_mark_published: Mock,
        mock_create_attempt: Mock,
        mock_claim_ready: Mock,
        mock_publish_metricool: Mock,
    ) -> None:
        job = PublishJob(
            id="job-1",
            post_id="post-1",
            persona_key="default",
            platform="metricool",
            status="queued",
            request_payload={
                "title": "Title",
                "media_type": "video",
                "media_url": "https://cdn.example.com/video.mp4",
                "caption_tiktok": "Normal caption",
                "script_10s": " ".join(["fact"] * MIN_SCRIPT_WORDS) + ".",
                "audio_duration_sec": 33,
                "video_duration_sec": 34,
                "approval_status": "approved",
            },
            retry_count=0,
            max_retries=3,
        )
        mock_claim_ready.return_value = [job]
        mock_create_attempt.return_value = SimpleNamespace(id="attempt-1")
        mock_publish_metricool.return_value = SimpleNamespace(
            status="published",
            external_post_id="metricool-1",
            retryable=False,
            error_category=None,
            error_message=None,
            http_status=200,
            response_payload={"id": "metricool-1"},
        )

        counters = dispatch_ready_publish_jobs(
            conn=Mock(),
            settings=_settings(metricool_review_required=False),
            max_jobs=1,
        )

        self.assertEqual(counters["queued"], 1)
        self.assertEqual(counters["published"], 1)
        self.assertEqual(counters["failed"], 0)
        self.assertEqual(counters["skipped"], 0)
        self.assertEqual(mock_claim_ready.call_args.kwargs["stale_in_progress_minutes"], 45)
        self.assertFalse(mock_claim_ready.call_args.kwargs["require_review_approval"])
        mock_mark_published.assert_called_once()

    @patch("pipeline.publish.claim_publish_jobs_ready")
    def test_dispatch_sets_review_claim_flag_when_review_required(self, mock_claim_ready: Mock) -> None:
        mock_claim_ready.return_value = []

        counters = dispatch_ready_publish_jobs(
            conn=Mock(),
            settings=_settings(metricool_review_required=True),
            max_jobs=1,
        )

        self.assertEqual(counters, {"queued": 0, "published": 0, "failed": 0, "skipped": 0})
        self.assertTrue(mock_claim_ready.call_args.kwargs["require_review_approval"])


if __name__ == "__main__":
    unittest.main()
