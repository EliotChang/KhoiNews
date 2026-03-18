from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline.video_gen import (
    FALLBACK_MIN_IMAGE_HEIGHT,
    FALLBACK_MIN_IMAGE_WIDTH,
    MAX_RENDER_TITLE_CHARS,
    MIN_IMAGE_PAYLOAD_BYTES,
    _compute_render_timeline,
    _enforce_render_stream_compliance,
    _fallback_image_meets_min_dimensions,
    _is_likely_supported_image_bytes,
    _is_likely_supported_video_bytes,
    _is_render_stream_compliant,
    _is_usable_image_candidate,
    _normalize_pixel_format,
    _normalize_runtime_video_asset,
    _prepare_overlay_audio_assets,
    _prepare_render_post_title,
)


class VideoHeadlinePreparationTests(unittest.TestCase):
    def test_prepare_render_post_title_clamps_to_max_characters(self) -> None:
        long_title = "This is a very long emotional cliffhanger title that definitely exceeds the maximum allowed character limit for rendered video titles"
        prepared = _prepare_render_post_title(long_title)
        self.assertLessEqual(len(prepared), MAX_RENDER_TITLE_CHARS)
        self.assertTrue(prepared.endswith("..."))

    def test_prepare_render_post_title_returns_fallback_for_empty_text(self) -> None:
        prepared = _prepare_render_post_title("   ")
        self.assertEqual(prepared, "News Update")

    def test_prepare_render_post_title_normalizes_whitespace(self) -> None:
        prepared = _prepare_render_post_title("  Budget   Deal   Reached  ")
        self.assertEqual(prepared, "Budget Deal Reached")

    def test_video_bytes_validator_rejects_html(self) -> None:
        self.assertFalse(_is_likely_supported_video_bytes(b"<!doctype html><html></html>"))

    def test_video_bytes_validator_accepts_mp4_signature(self) -> None:
        self.assertTrue(_is_likely_supported_video_bytes(b"\x00\x00\x00\x18ftypisom\x00\x00\x00\x01isom"))


class ImageUsabilityTests(unittest.TestCase):
    def test_min_image_payload_bytes_is_4kb(self) -> None:
        self.assertEqual(MIN_IMAGE_PAYLOAD_BYTES, 4_000)

    def test_rejects_image_below_min_bytes(self) -> None:
        small_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
        self.assertFalse(_is_usable_image_candidate(media_url="https://example.com/photo.png", payload_bytes=small_png))

    def test_accepts_image_above_min_bytes(self) -> None:
        large_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * MIN_IMAGE_PAYLOAD_BYTES
        self.assertTrue(_is_usable_image_candidate(media_url="https://example.com/photo.png", payload_bytes=large_png))

    def test_rejects_low_value_url(self) -> None:
        large_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * MIN_IMAGE_PAYLOAD_BYTES
        self.assertFalse(_is_usable_image_candidate(media_url="https://example.com/logo.png", payload_bytes=large_png))

    def test_rejects_unsupported_format(self) -> None:
        garbage_bytes = b"\x00\x01\x02\x03" * 2000
        self.assertFalse(_is_usable_image_candidate(media_url="https://example.com/photo.bin", payload_bytes=garbage_bytes))

    def test_accepts_jpeg(self) -> None:
        jpeg_bytes = b"\xff\xd8\xff\xe0" + b"\x00" * MIN_IMAGE_PAYLOAD_BYTES
        self.assertTrue(_is_usable_image_candidate(media_url="https://example.com/photo.jpg", payload_bytes=jpeg_bytes))

    def test_accepts_webp(self) -> None:
        webp_bytes = b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * MIN_IMAGE_PAYLOAD_BYTES
        self.assertTrue(_is_usable_image_candidate(media_url="https://example.com/photo.webp", payload_bytes=webp_bytes))

    def test_image_bytes_validator_rejects_html(self) -> None:
        self.assertFalse(_is_likely_supported_image_bytes(b"<!doctype html><html></html>"))

    def test_image_bytes_validator_accepts_gif(self) -> None:
        self.assertTrue(_is_likely_supported_image_bytes(b"GIF89a" + b"\x00" * 100))


class FallbackImageDimensionTests(unittest.TestCase):
    @staticmethod
    def _make_png_bytes(width: int, height: int) -> bytes:
        from io import BytesIO
        from PIL import Image
        buf = BytesIO()
        Image.new("RGB", (width, height), color=(128, 128, 128)).save(buf, format="PNG")
        return buf.getvalue()

    def test_fallback_min_dimensions_constants(self) -> None:
        self.assertEqual(FALLBACK_MIN_IMAGE_WIDTH, 640)
        self.assertEqual(FALLBACK_MIN_IMAGE_HEIGHT, 640)

    def test_rejects_wj_default_image_dimensions(self) -> None:
        small_image = self._make_png_bytes(640, 400)
        self.assertFalse(_fallback_image_meets_min_dimensions(small_image))

    def test_accepts_large_article_image(self) -> None:
        large_image = self._make_png_bytes(1280, 720)
        self.assertTrue(_fallback_image_meets_min_dimensions(large_image))

    def test_rejects_narrow_image(self) -> None:
        narrow = self._make_png_bytes(200, 800)
        self.assertFalse(_fallback_image_meets_min_dimensions(narrow))

    def test_rejects_short_image(self) -> None:
        short = self._make_png_bytes(800, 200)
        self.assertFalse(_fallback_image_meets_min_dimensions(short))

    def test_accepts_exact_minimum(self) -> None:
        exact = self._make_png_bytes(640, 640)
        self.assertTrue(_fallback_image_meets_min_dimensions(exact))

    def test_rejects_corrupt_bytes(self) -> None:
        self.assertFalse(_fallback_image_meets_min_dimensions(b"\x00\x01\x02\x03" * 1000))

    def test_rejects_wj_default_og_image_url(self) -> None:
        large_png = b"\x89PNG\r\n\x1a\n" + b"\x00" * MIN_IMAGE_PAYLOAD_BYTES
        self.assertFalse(_is_usable_image_candidate(
            media_url="https://www.worldjournal.com/static/img/og_image.png",
            payload_bytes=large_png,
        ))


class RenderStreamComplianceTests(unittest.TestCase):
    def test_render_stream_compliance_accepts_yuv420p_tv(self) -> None:
        compliant, reasons = _is_render_stream_compliant(
            stream_info={"pix_fmt": "yuv420p", "color_range": "tv"},
            target_pixel_format="yuv420p",
            target_color_range="tv",
        )
        self.assertTrue(compliant)
        self.assertEqual(reasons, [])

    def test_render_stream_compliance_rejects_yuvj420p_pc(self) -> None:
        compliant, reasons = _is_render_stream_compliant(
            stream_info={"pix_fmt": "yuvj420p", "color_range": "pc"},
            target_pixel_format="yuv420p",
            target_color_range="tv",
        )
        self.assertFalse(compliant)
        self.assertIn("pix_fmt=yuvj420p", reasons)
        self.assertIn("color_range=pc", reasons)

    @patch("pipeline.video_gen.subprocess.run")
    def test_normalize_pixel_format_command_includes_limited_range_flags(self, mock_subprocess_run: Mock) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            media_path = Path(tmp_dir) / "input.mp4"
            media_path.write_bytes(b"\x00" * 4_096)

            def _fake_run(command: list[str], **_: object) -> Mock:
                output_path = Path(command[-1])
                output_path.write_bytes(b"\x01" * 2_048)
                return Mock()

            mock_subprocess_run.side_effect = _fake_run
            normalized = _normalize_pixel_format(
                media_path=media_path,
                target_pixel_format="yuv420p",
                target_color_range="tv",
                crf=18,
            )
            self.assertTrue(normalized)
            command = list(mock_subprocess_run.call_args.args[0])
            self.assertIn("-vf", command)
            self.assertIn("format=yuv420p", command)
            self.assertIn("-pix_fmt", command)
            self.assertIn("yuv420p", command)
            self.assertIn("-color_range", command)
            self.assertIn("tv", command)

    @patch("pipeline.video_gen.subprocess.run")
    def test_normalize_runtime_video_asset_forces_stable_decode_settings(self, mock_subprocess_run: Mock) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "source.mov"
            output_path = Path(tmp_dir) / "source-normalized.mp4"
            input_path.write_bytes(b"\x00" * 8_192)

            def _fake_run(command: list[str], **_: object) -> Mock:
                Path(command[-1]).write_bytes(b"\x01" * 4_096)
                return Mock()

            mock_subprocess_run.side_effect = _fake_run
            normalized = _normalize_runtime_video_asset(
                input_path=input_path,
                output_path=output_path,
                crf=18,
                target_fps=30,
            )
            self.assertTrue(normalized)
            command = list(mock_subprocess_run.call_args.args[0])
            self.assertIn("-vf", command)
            self.assertIn("fps=30,scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p", command)
            self.assertIn("-pix_fmt", command)
            self.assertIn("yuv420p", command)
            self.assertIn("-color_range", command)
            self.assertIn("tv", command)
            self.assertIn("-an", command)

    @patch("pipeline.video_gen._normalize_pixel_format", return_value=True)
    @patch(
        "pipeline.video_gen._video_stream_info_from_ffprobe",
        side_effect=[
            {"codec_name": "h264", "pix_fmt": "yuvj420p", "color_range": "pc", "avg_frame_rate": "30/1"},
            {"codec_name": "h264", "pix_fmt": "yuvj420p", "color_range": "pc", "avg_frame_rate": "30/1"},
        ],
    )
    def test_enforce_render_stream_compliance_surfaces_post_normalization_failure(
        self,
        _mock_probe: Mock,
        _mock_normalize: Mock,
    ) -> None:
        stream_info, error = _enforce_render_stream_compliance(
            media_path=Path("/tmp/fake.mp4"),
            post_id="post-1",
            target_pixel_format="yuv420p",
            target_color_range="tv",
            crf=18,
        )
        self.assertIsNotNone(stream_info)
        self.assertIsNotNone(error)
        self.assertIn("non-compliant", str(error))

    def test_compute_render_timeline_with_intro_music_starts_voice_after_intro(self) -> None:
        intro_seconds, voice_start_seconds, outro_start_seconds, render_seconds = (
            _compute_render_timeline(
                audio_track_duration_seconds=12.34,
                configured_intro_duration_seconds=0.5,
                intro_music_duration_seconds=3.0,
                outro_duration_seconds=None,
            )
        )
        self.assertAlmostEqual(intro_seconds, 3.0, places=3)
        self.assertAlmostEqual(voice_start_seconds, 3.0, places=3)
        self.assertAlmostEqual(outro_start_seconds, 15.34, places=2)
        self.assertAlmostEqual(render_seconds, 15.34, places=2)

    def test_compute_render_timeline_without_intro_music_starts_voice_immediately(self) -> None:
        intro_seconds, voice_start_seconds, outro_start_seconds, render_seconds = (
            _compute_render_timeline(
                audio_track_duration_seconds=12.34,
                configured_intro_duration_seconds=0.5,
                intro_music_duration_seconds=None,
                outro_duration_seconds=None,
            )
        )
        self.assertAlmostEqual(intro_seconds, 0.5, places=3)
        self.assertAlmostEqual(voice_start_seconds, 0.0, places=3)
        self.assertAlmostEqual(outro_start_seconds, 12.34, places=2)
        self.assertAlmostEqual(render_seconds, 12.34, places=2)


class OverlayAudioPreflightTests(unittest.TestCase):
    def _build_settings(self, **overrides: object) -> Mock:
        defaults = {
            "video_intro_music_path": "intro.wav",
            "video_outro_audio_path": "",
        }
        defaults.update(overrides)
        return Mock(**defaults)

    def test_overlay_preflight_skips_missing_intro(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            runtime_dir = repo_root / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            settings = self._build_settings(video_intro_music_path="missing.wav")

            prepared = _prepare_overlay_audio_assets(
                settings=settings,
                runtime_dir=runtime_dir,
                runtime_token="token",
                repo_root=repo_root,
                post_id="post-1",
            )
            self.assertIsNone(prepared.intro_music)

    def test_overlay_preflight_returns_runtime_tracks_when_assets_are_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            runtime_dir = repo_root / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            (repo_root / "intro.wav").write_bytes(b"\x00\x01")
            settings = self._build_settings()

            with patch("pipeline.video_gen._duration_from_ffprobe", return_value=1.8):
                prepared = _prepare_overlay_audio_assets(
                    settings=settings,
                    runtime_dir=runtime_dir,
                    runtime_token="token",
                    repo_root=repo_root,
                    post_id="post-1",
                )

        self.assertIsNotNone(prepared.intro_music)
        self.assertEqual(prepared.intro_music.relative_path if prepared.intro_music else "", "runtime/token/intro-music.wav")


if __name__ == "__main__":
    unittest.main()
