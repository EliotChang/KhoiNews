from __future__ import annotations

from io import BytesIO
import json
import unittest
from unittest.mock import patch

from PIL import Image

from pipeline.article_media import MediaAssetResult, MediaCandidate
from pipeline.media_quality import MediaQualityGateConfig, enforce_image_quality_gate


class _MockHttpResponse:
    def __init__(self, payload: bytes) -> None:
        self.content = payload

    def raise_for_status(self) -> None:
        return None


class _MockTextBlock:
    def __init__(self, text: str) -> None:
        self.type = "text"
        self.text = text


class _MockMessageAPI:
    def __init__(self, payload: dict[str, float | list[str]]) -> None:
        self._payload = payload

    def create(self, **_: object) -> object:
        return type("MessageResult", (), {"content": [_MockTextBlock(json.dumps(self._payload))]})()


class _MockAnthropicClient:
    def __init__(self, *, payload: dict[str, float | list[str]]) -> None:
        self.messages = _MockMessageAPI(payload=payload)


def _png_bytes(*, width: int, height: int) -> bytes:
    image = Image.new("RGB", (width, height), color=(140, 80, 200))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _base_config() -> MediaQualityGateConfig:
    return MediaQualityGateConfig(
        enabled=True,
        max_candidates=6,
        timeout_seconds=5,
        min_image_width=200,
        min_image_height=200,
        min_image_bytes=120,
        min_aspect_ratio=0.5,
        max_aspect_ratio=2.2,
        min_entropy=0.0,
        min_sharpness=0.0,
        require_llm_pass=True,
        llm_model_name="fake-model",
        llm_min_quality_score=0.6,
        llm_min_relevance_score=0.5,
        min_composite_score=0.6,
        heuristic_weight=0.45,
        llm_weight=0.55,
        aspect_ratio_penalty=0.2,
        llm_assessment_retries=1,
        allow_llm_failure_fallback=True,
        llm_failure_heuristic_min_score=0.55,
    )


class MediaQualityGateTests(unittest.TestCase):
    def test_passes_and_selects_image_when_quality_good(self) -> None:
        media = MediaAssetResult(
            media_type="image",
            media_url="https://example.com/image-a.png",
            selection_reason="og:image",
            media_candidates=[
                MediaCandidate(
                    media_type="image",
                    media_url="https://example.com/image-a.png",
                    selection_reason="og:image",
                    priority=1,
                )
            ],
        )

        with (
            patch("pipeline.media_quality.requests.get", return_value=_MockHttpResponse(_png_bytes(width=1200, height=1200))),
            patch(
                "pipeline.media_quality.AnthropicBedrock",
                return_value=_MockAnthropicClient(payload={"quality_score": 0.92, "relevance_score": 0.9, "reject_reasons": []}),
            ),
        ):
            result = enforce_image_quality_gate(
                media_result=media,
                title="Strong funding announcement",
                description="Startup raises Series B and launches new platform.",
                article_url="https://example.com/article",
                aws_access_key_id="fake-key",
                aws_secret_access_key="fake-secret",
                aws_region="us-east-1",
                config=_base_config(),
            )

        assert result.media_result is not None
        assert result.quality_summary["candidate_count_passed"] == 1
        assert any(candidate.media_type == "image" for candidate in result.media_result.media_candidates)

    def test_rejects_when_heuristics_fail(self) -> None:
        media = MediaAssetResult(
            media_type="image",
            media_url="https://example.com/image-b.png",
            selection_reason="img_tag",
            media_candidates=[
                MediaCandidate(
                    media_type="image",
                    media_url="https://example.com/image-b.png",
                    selection_reason="img_tag",
                    priority=1,
                )
            ],
        )

        with patch("pipeline.media_quality.requests.get", return_value=_MockHttpResponse(_png_bytes(width=80, height=80))):
            result = enforce_image_quality_gate(
                media_result=media,
                title="Tiny image article",
                description="Low quality content",
                article_url="https://example.com/article-2",
                aws_access_key_id="fake-key",
                aws_secret_access_key="fake-secret",
                aws_region="us-east-1",
                config=_base_config(),
            )

        assert result.media_result is None
        assert result.quality_summary["candidate_count_passed"] == 0
        reject_reasons = result.quality_summary["assessments"][0]["reject_reasons"]
        assert any(reason.startswith("too_narrow") for reason in reject_reasons)

    def test_keeps_video_when_no_image_passes(self) -> None:
        media = MediaAssetResult(
            media_type="video",
            media_url="https://example.com/source-video.mp4",
            selection_reason="og:video",
            media_candidates=[
                MediaCandidate(
                    media_type="video",
                    media_url="https://example.com/source-video.mp4",
                    selection_reason="og:video",
                    priority=0,
                ),
                MediaCandidate(
                    media_type="image",
                    media_url="https://example.com/logo-thumb.png",
                    selection_reason="img_tag",
                    priority=1,
                ),
            ],
        )

        with patch("pipeline.media_quality.requests.get", return_value=_MockHttpResponse(_png_bytes(width=120, height=120))):
            result = enforce_image_quality_gate(
                media_result=media,
                title="Video-heavy article",
                description="Bad fallback image",
                article_url="https://example.com/article-3",
                aws_access_key_id="fake-key",
                aws_secret_access_key="fake-secret",
                aws_region="us-east-1",
                config=_base_config(),
            )

        assert result.media_result is not None
        assert result.media_result.media_type == "video"
        assert result.quality_summary["candidate_count_passed"] == 0
        assert result.quality_summary["video_fallback_applied"] is True


if __name__ == "__main__":
    unittest.main()
