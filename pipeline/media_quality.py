from __future__ import annotations

from dataclasses import dataclass
import base64
import json
from io import BytesIO
from typing import Any

from anthropic import AnthropicBedrock
from PIL import Image, ImageFilter, ImageStat, UnidentifiedImageError
import requests

from pipeline.article_media import MediaAssetResult, MediaCandidate, is_low_value_image_url


@dataclass(frozen=True)
class MediaQualityGateConfig:
    enabled: bool
    max_candidates: int
    timeout_seconds: int
    min_image_width: int
    min_image_height: int
    min_image_bytes: int
    min_aspect_ratio: float
    max_aspect_ratio: float
    min_entropy: float
    min_sharpness: float
    require_llm_pass: bool
    llm_model_name: str
    llm_min_quality_score: float
    llm_min_relevance_score: float
    min_composite_score: float
    heuristic_weight: float
    llm_weight: float
    aspect_ratio_penalty: float
    llm_assessment_retries: int
    allow_llm_failure_fallback: bool
    llm_failure_heuristic_min_score: float


@dataclass(frozen=True)
class ImageCandidateAssessment:
    media_url: str
    selection_reason: str
    width: int | None
    height: int | None
    image_bytes: int | None
    heuristic_score: float
    llm_quality_score: float | None
    llm_relevance_score: float | None
    composite_score: float
    is_usable: bool
    reject_reasons: list[str]
    hard_reject_reasons: list[str]
    soft_reject_reasons: list[str]
    llm_reject_reasons: list[str]
    llm_fallback_applied: bool
    aspect_ratio: float | None
    entropy: float | None
    sharpness: float | None
    decision_stage: str


@dataclass(frozen=True)
class ImageQualityGateResult:
    media_result: MediaAssetResult | None
    assessments: list[ImageCandidateAssessment]
    quality_summary: dict[str, Any]


def _clamp_score(value: float) -> float:
    return max(0.0, min(1.0, value))


def _normalize_numeric_score(raw_value: Any) -> float | None:
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    if value > 1.0:
        value = value / 100.0
    return _clamp_score(value)


def _is_low_value_image_url(url: str) -> bool:
    return is_low_value_image_url(url)


def _parse_text_blocks(content_blocks: Any) -> str:
    rows: list[str] = []
    for block in content_blocks:
        if getattr(block, "type", "") != "text":
            continue
        rows.append(str(getattr(block, "text", "")).strip())
    return "\n".join(row for row in rows if row).strip()


def _parse_json_response(payload_text: str) -> dict[str, Any]:
    normalized = payload_text.strip()
    if normalized.startswith("```"):
        normalized = normalized.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    parsed = json.loads(normalized)
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _safe_entropy(image: Image.Image) -> float:
    try:
        return float(image.convert("L").entropy())
    except Exception:  # noqa: BLE001
        return 0.0


def _safe_sharpness(image: Image.Image) -> float:
    try:
        edges = image.convert("L").filter(ImageFilter.FIND_EDGES)
        variance = ImageStat.Stat(edges).var
        return float(variance[0]) if variance else 0.0
    except Exception:  # noqa: BLE001
        return 0.0


def _heuristic_assessment(
    *,
    candidate: MediaCandidate,
    image_bytes: bytes,
    config: MediaQualityGateConfig,
) -> tuple[float, list[str], list[str], int | None, int | None, int | None, float | None, float | None, float | None]:
    hard_reject_reasons: list[str] = []
    soft_reject_reasons: list[str] = []
    width: int | None = None
    height: int | None = None
    entropy: float | None = None
    sharpness: float | None = None
    aspect_ratio: float | None = None
    payload_size = len(image_bytes)
    if payload_size < config.min_image_bytes:
        hard_reject_reasons.append(f"too_small_bytes:{payload_size}")

    if _is_low_value_image_url(candidate.media_url):
        soft_reject_reasons.append("low_value_url_hint")

    try:
        with Image.open(BytesIO(image_bytes)) as image:
            width, height = image.size
            entropy = _safe_entropy(image)
            sharpness = _safe_sharpness(image)
    except (UnidentifiedImageError, OSError):
        return (
            0.0,
            [*hard_reject_reasons, "invalid_image_bytes"],
            soft_reject_reasons,
            width,
            height,
            payload_size,
            aspect_ratio,
            entropy,
            sharpness,
        )

    if width < config.min_image_width:
        hard_reject_reasons.append(f"too_narrow:{width}")
    if height < config.min_image_height:
        hard_reject_reasons.append(f"too_short:{height}")

    aspect_ratio = (float(width) / float(height)) if height else 0.0
    if aspect_ratio < config.min_aspect_ratio or aspect_ratio > config.max_aspect_ratio:
        soft_reject_reasons.append(f"bad_aspect:{round(aspect_ratio, 3)}")
    if entropy is not None and entropy < config.min_entropy:
        soft_reject_reasons.append(f"low_entropy:{round(entropy, 3)}")
    if sharpness is not None and sharpness < config.min_sharpness:
        soft_reject_reasons.append(f"low_sharpness:{round(sharpness, 3)}")

    width_score = _clamp_score(width / max(float(config.min_image_width), 1.0))
    height_score = _clamp_score(height / max(float(config.min_image_height), 1.0))
    bytes_score = _clamp_score(payload_size / max(float(config.min_image_bytes), 1.0))
    entropy_score = _clamp_score((entropy or 0.0) / max(config.min_entropy, 1.0))
    sharpness_score = _clamp_score((sharpness or 0.0) / max(config.min_sharpness, 1.0))
    aspect_penalty = (
        0.0
        if config.min_aspect_ratio <= aspect_ratio <= config.max_aspect_ratio
        else _clamp_score(config.aspect_ratio_penalty)
    )

    raw_score = (width_score + height_score + bytes_score + entropy_score + sharpness_score) / 5.0
    heuristic_score = _clamp_score(raw_score - aspect_penalty)
    return (
        heuristic_score,
        hard_reject_reasons,
        soft_reject_reasons,
        width,
        height,
        payload_size,
        aspect_ratio,
        entropy,
        sharpness,
    )


def _mime_type_from_image_bytes(image_bytes: bytes) -> str:
    try:
        with Image.open(BytesIO(image_bytes)) as image:
            image_format = (image.format or "").upper()
    except (UnidentifiedImageError, OSError):
        return "image/jpeg"

    if image_format == "PNG":
        return "image/png"
    if image_format == "WEBP":
        return "image/webp"
    if image_format == "GIF":
        return "image/gif"
    if image_format == "AVIF":
        return "image/avif"
    return "image/jpeg"


def _llm_assessment(
    *,
    candidate: MediaCandidate,
    image_bytes: bytes,
    title: str,
    description: str,
    article_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str,
    model_name: str,
) -> tuple[float | None, float | None, list[str]]:
    try:
        client = AnthropicBedrock(
            aws_access_key=aws_access_key_id,
            aws_secret_key=aws_secret_access_key,
            aws_region=aws_region,
        )
        mime_type = _mime_type_from_image_bytes(image_bytes)
        encoded_bytes = base64.b64encode(image_bytes).decode("utf-8")
        prompt = (
            "Evaluate this image for short-form vertical news video usage. "
            "Return valid JSON only as: "
            '{"quality_score": number, "relevance_score": number, "reject_reasons": string[]}. '
            "Scores can be 0-1 or 0-100. "
            "quality_score focuses on visual quality (clarity, composition, lack of overlays/watermarks/logos). "
            "relevance_score focuses on relevance to the article topic. "
            "Reject reasons should be short machine-friendly tokens."
        )
        response = client.messages.create(
            model=model_name,
            temperature=0.0,
            max_tokens=350,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                f"Article title: {title}\n"
                                f"Article description: {description}\n"
                                f"Article URL: {article_url}\n"
                                f"Candidate image URL: {candidate.media_url}\n\n"
                                f"{prompt}"
                            ),
                        },
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": mime_type,
                                "data": encoded_bytes,
                            },
                        },
                    ],
                }
            ],
        )
        payload = _parse_json_response(_parse_text_blocks(response.content))
        llm_quality_score = _normalize_numeric_score(payload.get("quality_score"))
        llm_relevance_score = _normalize_numeric_score(payload.get("relevance_score"))
        raw_reject_reasons = payload.get("reject_reasons")
        reject_reasons = [str(reason).strip() for reason in raw_reject_reasons] if isinstance(raw_reject_reasons, list) else []
        reject_reasons = [reason for reason in reject_reasons if reason]
        return llm_quality_score, llm_relevance_score, reject_reasons
    except Exception:  # noqa: BLE001
        return None, None, ["llm_assessment_failed"]


def _to_media_candidate(assessment: ImageCandidateAssessment) -> MediaCandidate:
    return MediaCandidate(
        media_type="image",
        media_url=assessment.media_url,
        selection_reason=f"quality_gate:{assessment.composite_score:.3f}",
        priority=0,
    )


def _quality_summary(
    *,
    assessments: list[ImageCandidateAssessment],
    config: MediaQualityGateConfig,
) -> dict[str, Any]:
    passed = [assessment for assessment in assessments if assessment.is_usable]
    rejected = [assessment for assessment in assessments if not assessment.is_usable]
    reject_reason_counts: dict[str, int] = {}
    hard_reject_reason_counts: dict[str, int] = {}
    soft_reject_reason_counts: dict[str, int] = {}
    llm_reject_reason_counts: dict[str, int] = {}
    decision_stage_counts: dict[str, int] = {}

    for assessment in assessments:
        decision_stage_counts[assessment.decision_stage] = decision_stage_counts.get(assessment.decision_stage, 0) + 1
        for reason in assessment.reject_reasons:
            reject_reason_counts[reason] = reject_reason_counts.get(reason, 0) + 1
        for reason in assessment.hard_reject_reasons:
            hard_reject_reason_counts[reason] = hard_reject_reason_counts.get(reason, 0) + 1
        for reason in assessment.soft_reject_reasons:
            soft_reject_reason_counts[reason] = soft_reject_reason_counts.get(reason, 0) + 1
        for reason in assessment.llm_reject_reasons:
            llm_reject_reason_counts[reason] = llm_reject_reason_counts.get(reason, 0) + 1

    return {
        "gate_enabled": config.enabled,
        "max_candidates": config.max_candidates,
        "min_composite_score": config.min_composite_score,
        "require_llm_pass": config.require_llm_pass,
        "aspect_ratio_penalty": config.aspect_ratio_penalty,
        "llm_assessment_retries": config.llm_assessment_retries,
        "allow_llm_failure_fallback": config.allow_llm_failure_fallback,
        "llm_failure_heuristic_min_score": config.llm_failure_heuristic_min_score,
        "candidate_count_assessed": len(assessments),
        "candidate_count_passed": len(passed),
        "candidate_count_rejected": len(rejected),
        "best_passed_image_url": passed[0].media_url if passed else None,
        "reject_reason_counts": reject_reason_counts,
        "hard_reject_reason_counts": hard_reject_reason_counts,
        "soft_reject_reason_counts": soft_reject_reason_counts,
        "llm_reject_reason_counts": llm_reject_reason_counts,
        "decision_stage_counts": decision_stage_counts,
        "assessments": [
            {
                "media_url": assessment.media_url,
                "selection_reason": assessment.selection_reason,
                "width": assessment.width,
                "height": assessment.height,
                "image_bytes": assessment.image_bytes,
                "heuristic_score": round(assessment.heuristic_score, 4),
                "llm_quality_score": None if assessment.llm_quality_score is None else round(assessment.llm_quality_score, 4),
                "llm_relevance_score": (
                    None if assessment.llm_relevance_score is None else round(assessment.llm_relevance_score, 4)
                ),
                "composite_score": round(assessment.composite_score, 4),
                "is_usable": assessment.is_usable,
                "reject_reasons": assessment.reject_reasons,
                "hard_reject_reasons": assessment.hard_reject_reasons,
                "soft_reject_reasons": assessment.soft_reject_reasons,
                "llm_reject_reasons": assessment.llm_reject_reasons,
                "llm_fallback_applied": assessment.llm_fallback_applied,
                "aspect_ratio": None if assessment.aspect_ratio is None else round(assessment.aspect_ratio, 4),
                "entropy": None if assessment.entropy is None else round(assessment.entropy, 4),
                "sharpness": None if assessment.sharpness is None else round(assessment.sharpness, 4),
                "decision_stage": assessment.decision_stage,
            }
            for assessment in assessments
        ],
    }


def enforce_image_quality_gate(
    *,
    media_result: MediaAssetResult | None,
    title: str,
    description: str,
    article_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str,
    config: MediaQualityGateConfig,
) -> ImageQualityGateResult:
    if not media_result:
        return ImageQualityGateResult(media_result=None, assessments=[], quality_summary={"gate_enabled": config.enabled})
    if not config.enabled:
        return ImageQualityGateResult(
            media_result=media_result,
            assessments=[],
            quality_summary={"gate_enabled": False, "reason": "disabled"},
        )

    image_candidates = [candidate for candidate in media_result.media_candidates if candidate.media_type == "image"]
    selected_candidates = image_candidates[: max(1, config.max_candidates)]
    assessments: list[ImageCandidateAssessment] = []

    for candidate in selected_candidates:
        try:
            response = requests.get(candidate.media_url, timeout=config.timeout_seconds)
            response.raise_for_status()
            payload = response.content
        except requests.RequestException:
            assessments.append(
                ImageCandidateAssessment(
                    media_url=candidate.media_url,
                    selection_reason=candidate.selection_reason,
                    width=None,
                    height=None,
                    image_bytes=None,
                    heuristic_score=0.0,
                    llm_quality_score=None,
                    llm_relevance_score=None,
                    composite_score=0.0,
                    is_usable=False,
                    reject_reasons=["image_download_failed"],
                    hard_reject_reasons=["image_download_failed"],
                    soft_reject_reasons=[],
                    llm_reject_reasons=[],
                    llm_fallback_applied=False,
                    aspect_ratio=None,
                    entropy=None,
                    sharpness=None,
                    decision_stage="hard_reject",
                )
            )
            continue

        (
            heuristic_score,
            hard_reject_reasons,
            soft_reject_reasons,
            width,
            height,
            image_size,
            aspect_ratio,
            entropy,
            sharpness,
        ) = _heuristic_assessment(
            candidate=candidate,
            image_bytes=payload,
            config=config,
        )

        llm_quality_score: float | None = None
        llm_relevance_score: float | None = None
        llm_reject_reasons: list[str] = []
        if not hard_reject_reasons:
            llm_attempts = max(1, config.llm_assessment_retries)
            for attempt in range(llm_attempts):
                llm_quality_score, llm_relevance_score, llm_reject_reasons = _llm_assessment(
                    candidate=candidate,
                    image_bytes=payload,
                    title=title,
                    description=description,
                    article_url=article_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_region=aws_region,
                    model_name=config.llm_model_name,
                )
                if "llm_assessment_failed" not in llm_reject_reasons:
                    break
                if attempt + 1 < llm_attempts:
                    llm_reject_reasons = [*llm_reject_reasons, f"llm_retry_attempt:{attempt + 1}"]

        llm_fallback_applied = False
        llm_quality = 0.0 if llm_quality_score is None else llm_quality_score
        llm_relevance = 0.0 if llm_relevance_score is None else llm_relevance_score
        llm_score = (llm_quality + llm_relevance) / 2.0
        if (
            config.allow_llm_failure_fallback
            and "llm_assessment_failed" in llm_reject_reasons
            and heuristic_score >= config.llm_failure_heuristic_min_score
        ):
            llm_fallback_applied = True
            llm_score = heuristic_score
            llm_reject_reasons = [reason for reason in llm_reject_reasons if reason != "llm_assessment_failed"]
            llm_reject_reasons.append("llm_failure_fallback_applied")

        composite_score = _clamp_score((heuristic_score * config.heuristic_weight) + (llm_score * config.llm_weight))
        reject_reasons = [*hard_reject_reasons, *soft_reject_reasons, *llm_reject_reasons]

        llm_pass = True
        if config.require_llm_pass:
            llm_scores_meet_threshold = (
                llm_quality_score is not None
                and llm_relevance_score is not None
                and llm_quality_score >= config.llm_min_quality_score
                and llm_relevance_score >= config.llm_min_relevance_score
            )
            llm_pass = llm_scores_meet_threshold or llm_fallback_applied
            if not llm_pass:
                if llm_quality_score is None or llm_relevance_score is None:
                    reject_reasons.append("llm_unavailable")
                else:
                    reject_reasons.append("llm_threshold_not_met")

        is_usable = not hard_reject_reasons and composite_score >= config.min_composite_score and llm_pass
        if composite_score < config.min_composite_score:
            reject_reasons.append(f"composite_too_low:{round(composite_score, 3)}")

        decision_stage = "passed"
        if hard_reject_reasons:
            decision_stage = "hard_reject"
        elif not llm_pass:
            decision_stage = "llm_reject"
        elif composite_score < config.min_composite_score:
            decision_stage = "score_reject"

        assessments.append(
            ImageCandidateAssessment(
                media_url=candidate.media_url,
                selection_reason=candidate.selection_reason,
                width=width,
                height=height,
                image_bytes=image_size,
                heuristic_score=heuristic_score,
                llm_quality_score=llm_quality_score,
                llm_relevance_score=llm_relevance_score,
                composite_score=composite_score,
                is_usable=is_usable,
                reject_reasons=list(dict.fromkeys(reject_reasons)),
                hard_reject_reasons=list(dict.fromkeys(hard_reject_reasons)),
                soft_reject_reasons=list(dict.fromkeys(soft_reject_reasons)),
                llm_reject_reasons=list(dict.fromkeys(llm_reject_reasons)),
                llm_fallback_applied=llm_fallback_applied,
                aspect_ratio=aspect_ratio,
                entropy=entropy,
                sharpness=sharpness,
                decision_stage=decision_stage,
            )
        )

    sorted_assessments = sorted(
        assessments,
        key=lambda item: (0 if item.is_usable else 1, -item.composite_score, item.media_url),
    )
    passed_assessments = [assessment for assessment in sorted_assessments if assessment.is_usable]
    passed_image_candidates = [_to_media_candidate(assessment) for assessment in passed_assessments]

    video_candidates = [candidate for candidate in media_result.media_candidates if candidate.media_type == "video"]
    if not passed_image_candidates:
        quality_summary = _quality_summary(assessments=sorted_assessments, config=config)
        if video_candidates:
            quality_summary = {
                **quality_summary,
                "video_fallback_applied": True,
                "fallback_reason": "no_passing_images",
            }
            retained_video_candidate = video_candidates[0]
            return ImageQualityGateResult(
                media_result=MediaAssetResult(
                    media_type=retained_video_candidate.media_type,
                    media_url=retained_video_candidate.media_url,
                    selection_reason=f"{retained_video_candidate.selection_reason};quality_gate_video_fallback",
                    media_candidates=[retained_video_candidate],
                    quality_summary=quality_summary,
                ),
                assessments=sorted_assessments,
                quality_summary=quality_summary,
            )
        return ImageQualityGateResult(
            media_result=None,
            assessments=sorted_assessments,
            quality_summary=quality_summary,
        )

    retained_candidates: list[MediaCandidate] = []
    if video_candidates:
        retained_candidates.append(video_candidates[0])
    retained_candidates.extend(passed_image_candidates[: 7 if retained_candidates else 8])

    gated_media_result: MediaAssetResult | None = None
    if retained_candidates:
        primary_candidate = retained_candidates[0]
        gated_media_result = MediaAssetResult(
            media_type=primary_candidate.media_type,
            media_url=primary_candidate.media_url,
            selection_reason=primary_candidate.selection_reason,
            media_candidates=retained_candidates,
            quality_summary=None,
        )

    quality_summary = _quality_summary(assessments=sorted_assessments, config=config)
    if gated_media_result is not None:
        gated_media_result = MediaAssetResult(
            media_type=gated_media_result.media_type,
            media_url=gated_media_result.media_url,
            selection_reason=gated_media_result.selection_reason,
            media_candidates=gated_media_result.media_candidates,
            quality_summary=quality_summary,
        )
    return ImageQualityGateResult(
        media_result=gated_media_result,
        assessments=sorted_assessments,
        quality_summary=quality_summary,
    )
