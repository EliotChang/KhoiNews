from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from statistics import mean
from typing import Any

import psycopg

from pipeline.config import Settings
from pipeline.db import create_optimization_recommendation, list_recent_quality_performance_rows, list_signal_performance_rows

LOGGER = logging.getLogger("wj_quality_feedback")


def _avg(values: list[float]) -> float:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return 0.0
    return float(mean(cleaned))


def analyze_quality_performance_feedback(
    conn: psycopg.Connection,
    *,
    settings: Settings,
) -> str | None:
    rows = list_recent_quality_performance_rows(
        conn,
        persona_key=settings.persona_key,
        lookback_days=settings.quality_baseline_lookback_days,
        limit=250,
    )
    if len(rows) < 8:
        LOGGER.info("Quality feedback skipped: insufficient rows (%d)", len(rows))
        return None

    with_completion = [row for row in rows if row.get("completion_rate") is not None]
    if len(with_completion) < 6:
        LOGGER.info("Quality feedback skipped: insufficient completion rows (%d)", len(with_completion))
        return None

    sorted_by_completion = sorted(with_completion, key=lambda item: float(item.get("completion_rate") or 0.0))
    cutoff = max(2, len(sorted_by_completion) // 3)
    low_group = sorted_by_completion[:cutoff]
    high_group = sorted_by_completion[-cutoff:]

    low_variety = _avg([float(row.get("visual_variety_score") or 0.0) for row in low_group])
    high_variety = _avg([float(row.get("visual_variety_score") or 0.0) for row in high_group])
    low_specificity = _avg([float(row.get("script_specificity_score") or 0.0) for row in low_group])
    high_specificity = _avg([float(row.get("script_specificity_score") or 0.0) for row in high_group])

    completion_low = _avg([float(row.get("completion_rate") or 0.0) for row in low_group])
    completion_high = _avg([float(row.get("completion_rate") or 0.0) for row in high_group])
    engagement_low = _avg([float(row.get("engagement_rate") or 0.0) for row in low_group])
    engagement_high = _avg([float(row.get("engagement_rate") or 0.0) for row in high_group])

    recommendation: dict[str, Any] = {}
    rationale_parts: list[str] = []
    confidence = 0.55
    diagnosis = "quality-performance-tuning"
    if high_variety - low_variety > 0.12:
        recommendation["VIDEO_MEDIA_MAX_IMAGES"] = max(3, settings.video_media_max_images + 1)
        rationale_parts.append(
            f"High-completion posts show stronger visual variety ({high_variety:.3f} vs {low_variety:.3f})."
        )
        confidence += 0.15
    if high_specificity - low_specificity > 0.1:
        recommendation["CONTENT_SCRIPT_MIN_FACTS"] = max(settings.content_script_min_facts, settings.content_script_min_facts + 1)
        rationale_parts.append(
            f"High-completion posts show higher script specificity ({high_specificity:.3f} vs {low_specificity:.3f})."
        )
        confidence += 0.12
    if completion_high - completion_low > 0.15:
        rationale_parts.append(
            f"Completion gap indicates optimization opportunity ({completion_high:.3f} vs {completion_low:.3f})."
        )
        confidence += 0.08
    if engagement_high - engagement_low > 0.02:
        rationale_parts.append(
            f"Engagement gap reinforces signal ({engagement_high:.3f} vs {engagement_low:.3f})."
        )
        confidence += 0.05

    if not recommendation:
        LOGGER.info("Quality feedback: no strong recommendation signal")
        return None

    recommendation_id = create_optimization_recommendation(
        conn,
        persona_key=settings.persona_key,
        diagnosis=diagnosis,
        confidence=max(0.0, min(0.95, confidence)),
        sample_size=len(rows),
        window_start=datetime.now(timezone.utc),
        window_end=datetime.now(timezone.utc),
        recommended_overrides=recommendation,
        rationale=" ".join(rationale_parts),
        status="proposed",
    )
    LOGGER.info(
        "Quality feedback created recommendation_id=%s overrides=%s confidence=%.3f",
        recommendation_id,
        recommendation,
        confidence,
    )
    return recommendation_id


SIGNAL_PERFORMANCE_MIN_ROWS = 8
SIGNAL_PERFORMANCE_MIN_GROUP_SIZE = 3
SIGNAL_PERFORMANCE_BASELINE_RATIO_THRESHOLD = 1.3
SIGNAL_PERFORMANCE_MAX_BOOST = 0.08
SIGNAL_DIMENSIONS = ("topic_category", "hook_type", "length_bucket", "title_formula")


@dataclass(frozen=True)
class SignalBoost:
    dimension: str
    value: str
    boost: float
    avg_views: float
    sample_size: int


def analyze_signal_performance(
    conn: psycopg.Connection,
    *,
    settings: Settings,
    platform_filter: str | None = None,
) -> list[SignalBoost]:
    rows = list_signal_performance_rows(
        conn,
        persona_key=settings.persona_key,
        lookback_days=settings.quality_baseline_lookback_days,
    )
    if len(rows) < SIGNAL_PERFORMANCE_MIN_ROWS:
        LOGGER.info("Signal performance skipped: insufficient rows (%d)", len(rows))
        return []

    if platform_filter:
        rows = [r for r in rows if r.get("platform") == platform_filter]
        if len(rows) < SIGNAL_PERFORMANCE_MIN_ROWS:
            LOGGER.info(
                "Signal performance skipped: insufficient rows for platform=%s (%d)",
                platform_filter,
                len(rows),
            )
            return []

    baseline_views = _avg([float(r.get("views") or 0) for r in rows])
    if baseline_views <= 0:
        LOGGER.info("Signal performance skipped: zero baseline views")
        return []

    boosts: list[SignalBoost] = []
    for dimension in SIGNAL_DIMENSIONS:
        groups: dict[str, list[float]] = {}
        for row in rows:
            signals = row.get("content_signals")
            if not isinstance(signals, dict):
                continue
            dim_value = str(signals.get(dimension, "")).strip()
            if not dim_value:
                continue
            views = float(row.get("views") or 0)
            groups.setdefault(dim_value, []).append(views)

        for value, view_list in groups.items():
            if len(view_list) < SIGNAL_PERFORMANCE_MIN_GROUP_SIZE:
                continue
            avg = _avg(view_list)
            ratio = avg / baseline_views
            if ratio >= SIGNAL_PERFORMANCE_BASELINE_RATIO_THRESHOLD:
                boost_magnitude = min(
                    SIGNAL_PERFORMANCE_MAX_BOOST,
                    round((ratio - 1.0) * 0.04, 4),
                )
                boosts.append(SignalBoost(
                    dimension=dimension,
                    value=value,
                    boost=boost_magnitude,
                    avg_views=round(avg, 1),
                    sample_size=len(view_list),
                ))
                LOGGER.info(
                    "Signal boost: %s=%s boost=+%.4f avg_views=%.1f baseline=%.1f ratio=%.2f n=%d",
                    dimension,
                    value,
                    boost_magnitude,
                    avg,
                    baseline_views,
                    ratio,
                    len(view_list),
                )
            elif ratio <= (1.0 / SIGNAL_PERFORMANCE_BASELINE_RATIO_THRESHOLD):
                penalty_magnitude = max(
                    -SIGNAL_PERFORMANCE_MAX_BOOST,
                    round((ratio - 1.0) * 0.04, 4),
                )
                boosts.append(SignalBoost(
                    dimension=dimension,
                    value=value,
                    boost=penalty_magnitude,
                    avg_views=round(avg, 1),
                    sample_size=len(view_list),
                ))
                LOGGER.info(
                    "Signal penalty: %s=%s boost=%.4f avg_views=%.1f baseline=%.1f ratio=%.2f n=%d",
                    dimension,
                    value,
                    penalty_magnitude,
                    avg,
                    baseline_views,
                    ratio,
                    len(view_list),
                )

    if boosts:
        overrides = {
            "signal_boosts": [
                {
                    "dimension": b.dimension,
                    "value": b.value,
                    "boost": b.boost,
                    "avg_views": b.avg_views,
                    "sample_size": b.sample_size,
                }
                for b in boosts
            ],
        }
        rationale = "; ".join(
            f"{b.dimension}={b.value} {'outperforms' if b.boost > 0 else 'underperforms'} baseline by {abs(b.boost):.4f} (n={b.sample_size})"
            for b in boosts
        )
        create_optimization_recommendation(
            conn,
            persona_key=settings.persona_key,
            diagnosis="content-signal-performance",
            confidence=min(0.90, 0.50 + 0.03 * len(rows)),
            sample_size=len(rows),
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            recommended_overrides=overrides,
            rationale=rationale,
            status="proposed",
        )
        LOGGER.info("Signal performance: created recommendation with %d boosts", len(boosts))

    return boosts
