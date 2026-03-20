from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from pipeline.article_media import MediaAssetResult, MediaCandidate
from pipeline.content_gen import ContentGenerationResult
from pipeline.main import _process_ranked_posts_batch
from pipeline.media_quality import ImageQualityGateResult, MediaQualityGateConfig
from pipeline.wj_ingest import SourcePostInput
from pipeline.video_gen import VideoAssetResult
from pipeline.voice_gen import VoiceAssetResult


def _settings(**overrides: object) -> SimpleNamespace:
    defaults = dict(
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        aws_region="us-east-1",
        anthropic_model="model",
        content_script_target_seconds=24,
        content_script_target_words=80,
        content_script_max_words_buffer=10,
        content_script_min_words=70,
        content_script_min_facts=3,
        content_script_min_sentences=4,
        content_script_max_sentences=5,
        pre_voice_description_min_words=8,
        pre_voice_metadata_enrichment_enabled=False,
        pre_voice_allow_title_only_fallback=True,
        pre_voice_title_only_min_words=10,
        pre_voice_fail_suppress_after=3,
        pre_voice_fail_suppress_days=7,
        article_context_min_words=8,
        article_context_max_words=220,
        request_timeout_seconds=10,
        video_require_image_media=False,
        elevenlabs_api_key="el-key",
        elevenlabs_voice_id="voice-1",
        supabase_url="https://example.supabase.co",
        supabase_service_role_key="svc-role",
        supabase_voice_bucket="voice-assets",
        elevenlabs_tts_model_id="eleven_multilingual_v2",
        elevenlabs_voice_stability=0.4,
        elevenlabs_voice_similarity_boost=0.8,
        elevenlabs_apply_text_normalization=True,
        thumbnail_generation_enabled=False,
        publish_enabled=True,
        publish_min_audio_seconds=28.0,
        publish_min_video_seconds=28.0,
        publish_audio_video_max_delta_seconds=6.0,
        buffer_schedule_spacing_hours=6,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _post(*, title: str, description: str) -> SourcePostInput:
    return SourcePostInput(
        source="world_journal",
        source_guid=f"guid:{title}",
        title=title,
        description=description,
        link="https://example.com/story",
        published_at=datetime.now(timezone.utc),
        raw_payload={"ingest_source": "wj_scraper"},
    )


def _content(words: int = 80) -> ContentGenerationResult:
    script = " ".join(["fact"] * words) + "."
    return ContentGenerationResult(
        script_10s=script,
        video_title_short="Headline",
        caption_instagram="Caption",
        caption_tiktok="Caption",
        caption_youtube="Caption",
        caption_x="Caption",
        hashtags=["#news", "#world", "#policy"],
        tone="neutral",
        language="en",
        model_name="model",
    )


def _media() -> MediaAssetResult:
    return MediaAssetResult(
        media_type="image",
        media_url="https://cdn.example.com/image.jpg",
        selection_reason="article_image",
        media_candidates=[
            MediaCandidate(
                media_type="image",
                media_url="https://cdn.example.com/image.jpg",
                selection_reason="article_image",
                priority=1,
            )
        ],
        quality_summary={},
    )


def _media_quality_config(*, enabled: bool = False) -> MediaQualityGateConfig:
    return MediaQualityGateConfig(
        enabled=enabled,
        max_candidates=1,
        timeout_seconds=10,
        min_image_width=100,
        min_image_height=100,
        min_image_bytes=100,
        min_aspect_ratio=0.3,
        max_aspect_ratio=3.0,
        min_entropy=0.1,
        min_sharpness=0.1,
        require_llm_pass=False,
        llm_model_name="",
        llm_min_quality_score=0.0,
        llm_min_relevance_score=0.0,
        min_composite_score=0.0,
        heuristic_weight=1.0,
        llm_weight=0.0,
        aspect_ratio_penalty=0.0,
        llm_assessment_retries=1,
        allow_llm_failure_fallback=True,
        llm_failure_heuristic_min_score=0.0,
    )


class PipelineQualityGateTests(unittest.TestCase):
    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.upsert_source_post")
    def test_weak_context_candidate_is_skipped_and_not_enqueued(
        self,
        mock_upsert_source_post: Mock,
        mock_enqueue: Mock,
    ) -> None:
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(article_context_min_words=40),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Brief", description="Too short")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 0)
        self.assertEqual(result.skip_reason_counts.get("pre_voice_gate_failed"), 1)
        mock_enqueue.assert_not_called()

    @patch("pipeline.main.clear_source_gate_failure")
    @patch("pipeline.main.record_source_gate_failure")
    @patch("pipeline.main.is_source_gate_suppressed")
    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.generate_fish_lipsync_video")
    @patch("pipeline.main.generate_elevenlabs_voice")
    @patch("pipeline.main.extract_best_media_from_article")
    @patch("pipeline.main.validate_script_for_profile")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.extract_article_context")
    @patch("pipeline.main.upsert_source_post")
    @patch("pipeline.main.upsert_content_asset")
    @patch("pipeline.main.upsert_media_asset")
    @patch("pipeline.main.upsert_voice_asset")
    @patch("pipeline.main.upsert_video_asset")
    def test_empty_description_uses_metadata_enrichment_before_pre_voice_gate(
        self,
        _mock_upsert_video_asset: Mock,
        _mock_upsert_voice_asset: Mock,
        _mock_upsert_media_asset: Mock,
        _mock_upsert_content_asset: Mock,
        mock_upsert_source_post: Mock,
        mock_extract_article_context: Mock,
        mock_generate_content: Mock,
        mock_validate_script: Mock,
        mock_extract_media: Mock,
        mock_generate_voice: Mock,
        mock_generate_video: Mock,
        mock_enqueue: Mock,
        mock_is_suppressed: Mock,
        mock_record_failure: Mock,
        mock_clear_failure: Mock,
    ) -> None:
        mock_is_suppressed.return_value = False
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_extract_article_context.return_value = (
            "Officials confirmed timing and implementation details across agencies involved in the policy update."
        )
        mock_generate_content.return_value = _content(words=80)
        mock_validate_script.return_value = []
        mock_extract_media.return_value = _media()
        mock_generate_voice.return_value = VoiceAssetResult(
            status="generated",
            audio_url="https://cdn.example.com/audio.mp3",
            audio_duration_sec=22.0,
            alignment=None,
            error=None,
        )
        mock_generate_video.return_value = VideoAssetResult(
            status="generated",
            video_url="https://cdn.example.com/video.mp4",
            subtitle_url=None,
            video_duration_sec=22.4,
            error=None,
        )
        mock_enqueue.return_value = [SimpleNamespace(platform="metricool")]

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(pre_voice_metadata_enrichment_enabled=True),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 1)
        self.assertEqual(
            mock_generate_content.call_args.kwargs["description"],
            "Officials confirmed timing and implementation details across agencies involved in the policy update.",
        )
        mock_record_failure.assert_not_called()
        mock_clear_failure.assert_called_once()
        mock_enqueue.assert_called_once()

    @patch("pipeline.main.clear_source_gate_failure")
    @patch("pipeline.main.record_source_gate_failure")
    @patch("pipeline.main.is_source_gate_suppressed")
    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.generate_fish_lipsync_video")
    @patch("pipeline.main.generate_elevenlabs_voice")
    @patch("pipeline.main.extract_best_media_from_article")
    @patch("pipeline.main.validate_script_for_profile")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.extract_article_context")
    @patch("pipeline.main.upsert_source_post")
    @patch("pipeline.main.upsert_content_asset")
    @patch("pipeline.main.upsert_media_asset")
    @patch("pipeline.main.upsert_voice_asset")
    @patch("pipeline.main.upsert_video_asset")
    def test_low_context_rejects_candidate_without_using_title_only_fallback(
        self,
        _mock_upsert_video_asset: Mock,
        _mock_upsert_voice_asset: Mock,
        _mock_upsert_media_asset: Mock,
        _mock_upsert_content_asset: Mock,
        mock_upsert_source_post: Mock,
        mock_extract_article_context: Mock,
        mock_generate_content: Mock,
        mock_validate_script: Mock,
        mock_extract_media: Mock,
        mock_generate_voice: Mock,
        mock_generate_video: Mock,
        mock_enqueue: Mock,
        mock_is_suppressed: Mock,
        mock_record_failure: Mock,
        mock_clear_failure: Mock,
    ) -> None:
        mock_is_suppressed.return_value = False
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_extract_article_context.return_value = ""
        mock_generate_content.return_value = _content(words=80)
        mock_validate_script.return_value = []
        mock_extract_media.return_value = _media()
        mock_generate_voice.return_value = VoiceAssetResult(
            status="generated",
            audio_url="https://cdn.example.com/audio.mp3",
            audio_duration_sec=22.0,
            alignment=None,
            error=None,
        )
        mock_generate_video.return_value = VideoAssetResult(
            status="generated",
            video_url="https://cdn.example.com/video.mp4",
            subtitle_url=None,
            video_duration_sec=22.4,
            error=None,
        )
        mock_enqueue.return_value = [SimpleNamespace(platform="metricool")]

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(article_context_min_words=40, pre_voice_metadata_enrichment_enabled=True),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Gulf sovereign wealth funds were built for a rainy day. This may be it.", description="")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 0)
        mock_generate_content.assert_not_called()
        mock_record_failure.assert_called_once()
        mock_clear_failure.assert_not_called()
        mock_enqueue.assert_not_called()

    @patch("pipeline.main.record_source_gate_failure")
    @patch("pipeline.main.is_source_gate_suppressed")
    @patch("pipeline.main.extract_article_context")
    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.upsert_source_post")
    def test_enrichment_failure_keeps_pre_voice_rejection_and_records_failure_for_short_title(
        self,
        mock_upsert_source_post: Mock,
        mock_enqueue: Mock,
        mock_extract_article_context: Mock,
        mock_is_suppressed: Mock,
        mock_record_failure: Mock,
    ) -> None:
        mock_is_suppressed.return_value = False
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_extract_article_context.return_value = ""

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(pre_voice_metadata_enrichment_enabled=True),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Wallace Shawn for podcast", description="")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 0)
        mock_record_failure.assert_called_once()
        mock_enqueue.assert_not_called()

    @patch("pipeline.main.is_source_gate_suppressed")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.upsert_source_post")
    def test_suppressed_pre_voice_candidate_is_skipped_before_upsert(
        self,
        mock_upsert_source_post: Mock,
        mock_generate_content: Mock,
        mock_is_suppressed: Mock,
    ) -> None:
        mock_is_suppressed.return_value = True

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="Context already present in RSS.")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 0)
        mock_upsert_source_post.assert_not_called()
        mock_generate_content.assert_not_called()

    @patch("pipeline.main.is_source_gate_suppressed")
    @patch("pipeline.main.upsert_source_post")
    def test_suppressed_candidate_with_empty_rss_description_is_rechecked(
        self,
        mock_upsert_source_post: Mock,
        mock_is_suppressed: Mock,
    ) -> None:
        mock_is_suppressed.return_value = True
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=False)

        _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        mock_upsert_source_post.assert_called_once()

    @patch("pipeline.main.clear_source_gate_failure")
    @patch("pipeline.main.is_source_gate_suppressed")
    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.generate_fish_lipsync_video")
    @patch("pipeline.main.generate_elevenlabs_voice")
    @patch("pipeline.main.extract_best_media_from_article")
    @patch("pipeline.main.validate_script_for_profile")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.upsert_source_post")
    @patch("pipeline.main.upsert_content_asset")
    @patch("pipeline.main.upsert_media_asset")
    @patch("pipeline.main.upsert_voice_asset")
    @patch("pipeline.main.upsert_video_asset")
    def test_pre_voice_failure_state_is_cleared_when_candidate_passes(
        self,
        _mock_upsert_video_asset: Mock,
        _mock_upsert_voice_asset: Mock,
        _mock_upsert_media_asset: Mock,
        _mock_upsert_content_asset: Mock,
        mock_upsert_source_post: Mock,
        mock_generate_content: Mock,
        mock_validate_script: Mock,
        mock_extract_media: Mock,
        mock_generate_voice: Mock,
        mock_generate_video: Mock,
        mock_enqueue: Mock,
        mock_is_suppressed: Mock,
        mock_clear_failure: Mock,
    ) -> None:
        mock_is_suppressed.return_value = False
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_generate_content.return_value = _content(words=80)
        mock_validate_script.return_value = []
        mock_extract_media.return_value = _media()
        mock_generate_voice.return_value = VoiceAssetResult(
            status="generated",
            audio_url="https://cdn.example.com/audio.mp3",
            audio_duration_sec=22.0,
            alignment=None,
            error=None,
        )
        mock_generate_video.return_value = VideoAssetResult(
            status="generated",
            video_url="https://cdn.example.com/video.mp4",
            subtitle_url=None,
            video_duration_sec=22.4,
            error=None,
        )
        mock_enqueue.return_value = [SimpleNamespace(platform="metricool")]

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="Officials confirmed timing, entities involved, and expected implementation windows for the next quarter.")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 1)
        mock_clear_failure.assert_called_once()

    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.generate_fish_lipsync_video")
    @patch("pipeline.main.generate_elevenlabs_voice")
    @patch("pipeline.main.extract_best_media_from_article")
    @patch("pipeline.main.validate_script_for_profile")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.upsert_source_post")
    @patch("pipeline.main.upsert_content_asset")
    @patch("pipeline.main.upsert_media_asset")
    @patch("pipeline.main.upsert_voice_asset")
    @patch("pipeline.main.upsert_video_asset")
    def test_valid_candidate_enqueues_metricool_job_once(
        self,
        _mock_upsert_video_asset: Mock,
        _mock_upsert_voice_asset: Mock,
        _mock_upsert_media_asset: Mock,
        _mock_upsert_content_asset: Mock,
        mock_upsert_source_post: Mock,
        mock_generate_content: Mock,
        mock_validate_script: Mock,
        mock_extract_media: Mock,
        mock_generate_voice: Mock,
        mock_generate_video: Mock,
        mock_enqueue: Mock,
    ) -> None:
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_generate_content.return_value = _content(words=80)
        mock_validate_script.return_value = []
        mock_extract_media.return_value = _media()
        mock_generate_voice.return_value = VoiceAssetResult(
            status="generated",
            audio_url="https://cdn.example.com/audio.mp3",
            audio_duration_sec=22.0,
            alignment=None,
            error=None,
        )
        mock_generate_video.return_value = VideoAssetResult(
            status="generated",
            video_url="https://cdn.example.com/video.mp4",
            subtitle_url=None,
            video_duration_sec=22.4,
            error=None,
        )
        mock_enqueue.return_value = [SimpleNamespace(platform="metricool")]

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="Officials confirmed timing, entities involved, and expected implementation windows for the next quarter.")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 1)
        self.assertEqual(result.skip_reason_counts, {})
        mock_enqueue.assert_called_once()

    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.generate_fish_lipsync_video")
    @patch("pipeline.main.generate_elevenlabs_voice")
    @patch("pipeline.main.extract_best_media_from_article")
    @patch("pipeline.main.validate_script_for_profile")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.upsert_source_post")
    @patch("pipeline.main.upsert_content_asset")
    @patch("pipeline.main.upsert_media_asset")
    @patch("pipeline.main.upsert_voice_asset")
    @patch("pipeline.main.upsert_video_asset")
    def test_short_audio_video_are_blocked_before_publish(
        self,
        _mock_upsert_video_asset: Mock,
        _mock_upsert_voice_asset: Mock,
        _mock_upsert_media_asset: Mock,
        _mock_upsert_content_asset: Mock,
        mock_upsert_source_post: Mock,
        mock_generate_content: Mock,
        mock_validate_script: Mock,
        mock_extract_media: Mock,
        mock_generate_voice: Mock,
        mock_generate_video: Mock,
        mock_enqueue: Mock,
    ) -> None:
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_generate_content.return_value = _content(words=80)
        mock_validate_script.return_value = []
        mock_extract_media.return_value = _media()
        mock_generate_voice.return_value = VoiceAssetResult(
            status="generated",
            audio_url="https://cdn.example.com/audio.mp3",
            audio_duration_sec=12.0,
            alignment=None,
            error=None,
        )
        mock_generate_video.return_value = VideoAssetResult(
            status="generated",
            video_url="https://cdn.example.com/video.mp4",
            subtitle_url=None,
            video_duration_sec=14.0,
            error=None,
        )

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="Officials confirmed timing, entities involved, and expected implementation windows for the next quarter.")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 0)
        self.assertEqual(result.skip_reason_counts.get("post_voice_gate_failed"), 1)
        mock_enqueue.assert_not_called()

    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.generate_fish_lipsync_video")
    @patch("pipeline.main.generate_elevenlabs_voice")
    @patch("pipeline.main.extract_best_media_from_article")
    @patch("pipeline.main.validate_script_for_profile")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.upsert_source_post")
    @patch("pipeline.main.upsert_content_asset")
    @patch("pipeline.main.upsert_media_asset")
    @patch("pipeline.main.upsert_voice_asset")
    @patch("pipeline.main.upsert_video_asset")
    def test_long_audio_video_are_blocked_before_publish(
        self,
        _mock_upsert_video_asset: Mock,
        _mock_upsert_voice_asset: Mock,
        _mock_upsert_media_asset: Mock,
        _mock_upsert_content_asset: Mock,
        mock_upsert_source_post: Mock,
        mock_generate_content: Mock,
        mock_validate_script: Mock,
        mock_extract_media: Mock,
        mock_generate_voice: Mock,
        mock_generate_video: Mock,
        mock_enqueue: Mock,
    ) -> None:
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_generate_content.return_value = _content(words=80)
        mock_validate_script.return_value = []
        mock_extract_media.return_value = _media()
        mock_generate_voice.return_value = VoiceAssetResult(
            status="generated",
            audio_url="https://cdn.example.com/audio.mp3",
            audio_duration_sec=55.0,
            alignment=None,
            error=None,
        )
        mock_generate_video.return_value = VideoAssetResult(
            status="generated",
            video_url="https://cdn.example.com/video.mp4",
            subtitle_url=None,
            video_duration_sec=55.0,
            error=None,
        )

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(),
            media_quality_config=_media_quality_config(),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="Officials confirmed timing, entities involved, and expected implementation windows for the next quarter.")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 0)
        mock_enqueue.assert_not_called()

    @patch("pipeline.main.enqueue_publish_jobs_for_post")
    @patch("pipeline.main.generate_fish_lipsync_video")
    @patch("pipeline.main.generate_elevenlabs_voice")
    @patch("pipeline.main._fallback_media_from_web_thumbnail")
    @patch("pipeline.main.enforce_image_quality_gate")
    @patch("pipeline.main.extract_best_media_from_article")
    @patch("pipeline.main.validate_script_for_profile")
    @patch("pipeline.main.generate_content_pack")
    @patch("pipeline.main.upsert_source_post")
    @patch("pipeline.main.upsert_content_asset")
    @patch("pipeline.main.upsert_media_asset")
    @patch("pipeline.main.upsert_voice_asset")
    @patch("pipeline.main.upsert_video_asset")
    def test_quality_gate_failure_recovers_with_thumbnail_fallback(
        self,
        _mock_upsert_video_asset: Mock,
        _mock_upsert_voice_asset: Mock,
        _mock_upsert_media_asset: Mock,
        _mock_upsert_content_asset: Mock,
        mock_upsert_source_post: Mock,
        mock_generate_content: Mock,
        mock_validate_script: Mock,
        mock_extract_media: Mock,
        mock_enforce_image_quality_gate: Mock,
        mock_fallback_media: Mock,
        mock_generate_voice: Mock,
        mock_generate_video: Mock,
        mock_enqueue: Mock,
    ) -> None:
        mock_upsert_source_post.return_value = SimpleNamespace(post_id="post-1", is_new=True)
        mock_generate_content.return_value = _content(words=80)
        mock_validate_script.return_value = []
        mock_extract_media.return_value = _media()
        mock_fallback_media.return_value = _media()
        mock_enforce_image_quality_gate.side_effect = [
            ImageQualityGateResult(media_result=None, assessments=[], quality_summary={"gate_enabled": True, "reason": "no_pass"}),
            ImageQualityGateResult(media_result=_media(), assessments=[], quality_summary={"gate_enabled": True, "reason": "fallback_pass"}),
        ]
        mock_generate_voice.return_value = VoiceAssetResult(
            status="generated",
            audio_url="https://cdn.example.com/audio.mp3",
            audio_duration_sec=22.0,
            alignment=None,
            error=None,
        )
        mock_generate_video.return_value = VideoAssetResult(
            status="generated",
            video_url="https://cdn.example.com/video.mp4",
            subtitle_url=None,
            video_duration_sec=22.2,
            error=None,
        )
        mock_enqueue.return_value = [SimpleNamespace(platform="metricool")]

        result = _process_ranked_posts_batch(
            conn=Mock(),
            settings=_settings(),
            media_quality_config=_media_quality_config(enabled=True),
            ranked_posts=[_post(title="Policy update sets new export controls across regions", description="Officials confirmed timing, entities involved, and expected implementation windows for the next quarter.")],
            target_processed_posts=1,
            posts_processed_start=0,
            utc_schedule_anchor=datetime.now(timezone.utc),
            live_covered_links=set(),
            recent_series_tags=None,
        )

        self.assertEqual(result.jobs_enqueued, 1)
        self.assertEqual(mock_enforce_image_quality_gate.call_count, 2)
        mock_fallback_media.assert_called_once()
        mock_enqueue.assert_called_once()


if __name__ == "__main__":
    unittest.main()
