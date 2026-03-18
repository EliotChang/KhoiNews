from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pipeline.thumbnail_gen import (
    ThumbnailResult,
    _normalize_thumbnail_strategy,
    _strategy_gemini_generated,
    generate_thumbnail,
)


class ThumbnailStrategyPolicyTests(unittest.TestCase):
    def test_normalize_thumbnail_strategy_keeps_gemini_generated(self) -> None:
        self.assertEqual(_normalize_thumbnail_strategy("gemini-generated"), "gemini-generated")

    def test_generate_thumbnail_routes_gemini_strategy(self) -> None:
        settings = SimpleNamespace(request_timeout_seconds=5)
        expected = ThumbnailResult(
            image_bytes=b"bytes",
            content_type="image/jpeg",
            source="gemini-generated",
            description="generated image",
        )
        with patch("pipeline.thumbnail_gen._strategy_gemini_generated", return_value=expected) as mock_gemini:
            with patch("pipeline.thumbnail_gen._strategy_article_image") as mock_article:
                result = generate_thumbnail(
                    settings=settings,
                    strategy="gemini-generated",
                    title="Example title",
                    script="Example script",
                    article_image_url=None,
                )

        self.assertEqual(result, expected)
        mock_gemini.assert_called_once()
        mock_article.assert_not_called()

    @patch("pipeline.thumbnail_gen._resize_to_thumbnail", return_value=b"resized")
    @patch("pipeline.thumbnail_gen._generate_gemini_image_bytes", return_value=b"raw")
    def test_strategy_gemini_generated_returns_thumbnail_result(self, mock_generate: object, _mock_resize: object) -> None:
        settings = SimpleNamespace(
            gemini_api_key="key",
            gemini_image_model="gemini-nano-banana-pro",
            gemini_image_fallback_model="gemini-2.5-flash-image-preview",
            request_timeout_seconds=5,
        )
        result = _strategy_gemini_generated(
            settings=settings,
            title="Breaking market update",
            script="Analysts report major policy shifts with global impact.",
            article_image_url=None,
        )
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.source, "gemini-generated")
        self.assertEqual(result.image_bytes, b"resized")
        self.assertEqual(result.content_type, "image/jpeg")
        self.assertIn("gemini-nano-banana-pro", result.description)
        mock_generate.assert_called_once()  # type: ignore[attr-defined]
        self.assertEqual(mock_generate.call_args.kwargs["model_name"], "gemini-nano-banana-pro")  # type: ignore[attr-defined]

    @patch("pipeline.thumbnail_gen._resize_to_thumbnail", return_value=b"resized")
    @patch("pipeline.thumbnail_gen._generate_gemini_image_bytes")
    def test_strategy_gemini_generated_retries_with_fallback_model(self, mock_generate: object, _mock_resize: object) -> None:
        mock_generate.side_effect = [RuntimeError("unsupported model"), b"raw"]  # type: ignore[attr-defined]
        settings = SimpleNamespace(
            gemini_api_key="key",
            gemini_image_model="gemini-nano-banana-pro",
            gemini_image_fallback_model="gemini-2.5-flash-image-preview",
            request_timeout_seconds=5,
        )
        result = _strategy_gemini_generated(
            settings=settings,
            title="Severe weather alert",
            script="Emergency responders mobilize after storm warnings.",
            article_image_url=None,
        )
        self.assertIsNotNone(result)
        self.assertEqual(mock_generate.call_count, 2)  # type: ignore[attr-defined]
        model_calls = [call.kwargs["model_name"] for call in mock_generate.call_args_list]  # type: ignore[attr-defined]
        self.assertEqual(model_calls, ["gemini-nano-banana-pro", "gemini-2.5-flash-image-preview"])

    def test_strategy_gemini_generated_returns_none_without_api_key(self) -> None:
        settings = SimpleNamespace(
            gemini_api_key="",
            gemini_image_model="gemini-nano-banana-pro",
            gemini_image_fallback_model="gemini-2.5-flash-image-preview",
            request_timeout_seconds=5,
        )
        result = _strategy_gemini_generated(
            settings=settings,
            title="Elections update",
            script="Officials announce final ballot count.",
            article_image_url=None,
        )
        self.assertIsNone(result)

    @patch("pipeline.thumbnail_gen._generate_gemini_image_bytes", side_effect=[RuntimeError("model"), RuntimeError("model")])
    def test_strategy_gemini_generated_returns_none_when_all_models_fail(self, _mock_generate: object) -> None:
        settings = SimpleNamespace(
            gemini_api_key="key",
            gemini_image_model="gemini-nano-banana-pro",
            gemini_image_fallback_model="gemini-2.5-flash-image-preview",
            request_timeout_seconds=5,
        )
        result = _strategy_gemini_generated(
            settings=settings,
            title="Supply chain disruptions",
            script="Ports report delays due to extreme weather.",
            article_image_url=None,
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
