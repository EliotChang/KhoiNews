from __future__ import annotations

import unittest
from unittest.mock import patch

from pipeline.config import DEFAULT_FALLBACK_FEED_URLS, load_settings


def _base_env() -> dict[str, str]:
    return {
        "SUPABASE_DB_URL": "postgresql://postgres.testref:password@db.local:5432/postgres",
        "SUPABASE_SERVICE_ROLE_KEY": "service-role",
        "ELEVENLABS_API_KEY": "elevenlabs-key",
        "ELEVENLABS_VOICE_ID": "voice-id",
        "AWS_ACCESS_KEY_ID": "test-access-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret-key",
        "AWS_REGION": "us-east-1",
    }


class SettingsConfigTests(unittest.TestCase):
    def test_default_wj_base_url_points_to_worldjournal(self) -> None:
        env = _base_env()

        with patch.dict("os.environ", env, clear=True):
            settings = load_settings()

        self.assertEqual(settings.wj_base_url, "https://www.worldjournal.com")
        self.assertIsInstance(settings.wj_category_paths, list)
        self.assertTrue(len(settings.wj_category_paths) > 0)
        self.assertEqual(settings.content_language, "zh-TW")

    def test_blank_fallback_feed_urls_use_trusted_defaults(self) -> None:
        env = _base_env()
        env["FALLBACK_FEED_URLS"] = "   "

        with patch.dict("os.environ", env, clear=True):
            settings = load_settings()

        self.assertEqual(settings.fallback_feed_urls, DEFAULT_FALLBACK_FEED_URLS)

    def test_blank_topic_block_terms_remain_empty_when_explicitly_set(self) -> None:
        env = _base_env()
        env["TOPIC_BLOCK_TERMS"] = "   "

        with patch.dict("os.environ", env, clear=True):
            settings = load_settings()

        self.assertEqual(settings.topic_block_terms, [])

    def test_pre_voice_defaults_are_configured(self) -> None:
        env = _base_env()

        with patch.dict("os.environ", env, clear=True):
            settings = load_settings()

        self.assertEqual(settings.pre_voice_description_min_words, 8)
        self.assertTrue(settings.pre_voice_metadata_enrichment_enabled)
        self.assertTrue(settings.pre_voice_allow_title_only_fallback)
        self.assertEqual(settings.pre_voice_title_only_min_words, 10)
        self.assertEqual(settings.pre_voice_fail_suppress_after, 3)
        self.assertEqual(settings.pre_voice_fail_suppress_days, 7)
        self.assertEqual(settings.article_context_min_words, 40)
        self.assertEqual(settings.article_context_max_words, 220)
        self.assertTrue(settings.engagement_scoring_enabled)
        self.assertEqual(settings.engagement_min_score, 0.55)
        self.assertEqual(settings.engagement_floor_score, 0.5)
        self.assertEqual(settings.cadence_min_posts_per_run, 2)
        self.assertEqual(settings.content_mix_profile, "hard_news_culture")

    def test_publish_stability_defaults(self) -> None:
        env = _base_env()

        with patch.dict("os.environ", env, clear=True):
            settings = load_settings()

        self.assertEqual(settings.publish_claim_stale_in_progress_minutes, 45)
        self.assertFalse(settings.allow_duplicate_link_repost)

    def test_video_overlay_audio_defaults_are_configured(self) -> None:
        env = _base_env()

        with patch.dict("os.environ", env, clear=True):
            settings = load_settings()

        self.assertEqual(settings.video_intro_music_path, "")
        self.assertEqual(settings.video_media_max_images, 3)

    def test_video_overlay_audio_mix_settings_are_clamped(self) -> None:
        env = _base_env()
        env["VIDEO_INTRO_MUSIC_VOLUME"] = "4"

        with patch.dict("os.environ", env, clear=True):
            settings = load_settings()

        self.assertEqual(settings.video_intro_music_volume, 1.0)


if __name__ == "__main__":
    unittest.main()
