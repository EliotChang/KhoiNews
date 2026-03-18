from __future__ import annotations

import base64
import unittest
from unittest.mock import patch

from pipeline.voice_gen import _prepare_tts_text, generate_elevenlabs_voice


class _MockJsonResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class VoiceGenerationTests(unittest.TestCase):
    @patch("pipeline.voice_gen._duration_from_audio_bytes", return_value=3.21)
    @patch("pipeline.voice_gen._upload_audio_to_supabase_storage", return_value="https://cdn.example/audio.mp3")
    @patch("pipeline.voice_gen.requests.post")
    def test_uses_runtime_tts_settings_in_payload(
        self,
        mock_post: object,
        _: object,
        __: object,
    ) -> None:
        encoded_audio = base64.b64encode(b"fake-mp3").decode("utf-8")
        mock_post.return_value = _MockJsonResponse(
            {"audio_base64": encoded_audio, "normalized_alignment": {"characters": [], "character_start_times_seconds": []}}
        )

        result = generate_elevenlabs_voice(
            api_key="fake-key",
            voice_id="voice-123",
            text="Breaking update -- details at https://example.com/live #markets",
            post_id="post-123",
            supabase_url="https://supabase.example",
            supabase_service_role_key="service-key",
            supabase_voice_bucket="voice-assets",
            timeout_seconds=20,
            model_id="eleven_turbo_v2_5",
            voice_stability=0.62,
            voice_similarity_boost=0.71,
            apply_text_normalization=False,
        )

        self.assertEqual(result.status, "generated")
        self.assertEqual(mock_post.call_count, 1)
        kwargs = mock_post.call_args.kwargs
        payload = kwargs["json"]
        self.assertEqual(payload["model_id"], "eleven_turbo_v2_5")
        self.assertEqual(payload["apply_text_normalization"], "off")
        self.assertEqual(payload["voice_settings"]["stability"], 0.62)
        self.assertEqual(payload["voice_settings"]["similarity_boost"], 0.71)
        self.assertNotIn("https://", payload["text"])
        self.assertNotIn("#", payload["text"])
        self.assertTrue(payload["text"].endswith("."))

    def test_prepare_tts_text_cleans_artifacts_and_trailing_fragment(self) -> None:
        cleaned = _prepare_tts_text(
            "This is the latest update... [Details](https://example.com) and #breaking and"
        )
        self.assertNotIn("https://", cleaned)
        self.assertNotIn("#", cleaned)
        self.assertNotIn("  ", cleaned)
        self.assertFalse(cleaned.lower().endswith(" and."))
        self.assertTrue(cleaned.endswith("."))


if __name__ == "__main__":
    unittest.main()
