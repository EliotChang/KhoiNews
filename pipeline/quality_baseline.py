from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
import re

from pipeline.config import bootstrap_runtime_env, load_settings
from pipeline.db import db_connection, get_quality_baseline_summary, list_recent_video_metrics

LOGGER = logging.getLogger("wj_quality_baseline")

_LOG_PATTERNS = {
    "script_validation_failures": re.compile(r"Fallback script failed profile validation|post_script_gate_failed"),
    "media_fetch_failures": re.compile(r"no_image_or_video_media|Article page fetch failed|Media extraction failed"),
    "video_generation_failures": re.compile(r"no_video_publish_media|Fish video generation not successful"),
    "render_failures": re.compile(r"Remotion render attempt .* failed|Rendered video stream non-compliant"),
    "quality_gate_reductions": re.compile(r"candidate quality gates reduced output"),
}


def _scan_recent_logs(*, workspace_root: Path, lookback_days: int) -> dict[str, int]:
    logs_dir = workspace_root / "tmp_logs"
    if not logs_dir.exists():
        return {key: 0 for key in _LOG_PATTERNS}
    threshold = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))
    counts = Counter({key: 0 for key in _LOG_PATTERNS})
    for path in sorted(logs_dir.glob("*.log")):
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if modified_at < threshold:
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for key, pattern in _LOG_PATTERNS.items():
            counts[key] += len(pattern.findall(content))
    return dict(counts)


def _build_markdown_report(
    *,
    lookback_days: int,
    db_summary: dict[str, object],
    log_counts: dict[str, int],
    recent_metrics: list[dict[str, object]],
) -> str:
    top_lines = [
        f"# Publishing Quality Baseline ({lookback_days} days)",
        "",
        "## Pipeline Quality Snapshot",
        f"- Evaluations: {int(db_summary.get('evaluations') or 0)}",
        f"- Passed evaluations: {int(db_summary.get('passed_evaluations') or 0)}",
        f"- Avg composite score: {float(db_summary.get('avg_composite') or 0.0):.3f}",
        f"- Avg script specificity: {float(db_summary.get('avg_script_specificity') or 0.0):.3f}",
        f"- Avg narrative flow: {float(db_summary.get('avg_narrative_flow') or 0.0):.3f}",
        f"- Avg visual relevance: {float(db_summary.get('avg_visual_relevance') or 0.0):.3f}",
        f"- Avg visual variety: {float(db_summary.get('avg_visual_variety') or 0.0):.3f}",
        f"- Avg first-2s hook: {float(db_summary.get('avg_first_two_seconds_hook') or 0.0):.3f}",
        "",
        "## Publishing Outcomes",
        f"- Jobs total: {int(db_summary.get('jobs_total') or 0)}",
        f"- Published: {int(db_summary.get('jobs_published') or 0)}",
        f"- Failed: {int(db_summary.get('jobs_failed') or 0)}",
        f"- Skipped: {int(db_summary.get('jobs_skipped') or 0)}",
        f"- Avg completion rate: {float(db_summary.get('avg_completion_rate') or 0.0):.3f}",
        f"- Avg engagement rate: {float(db_summary.get('avg_engagement_rate') or 0.0):.3f}",
        "",
        "## Failure/Degradation Signals (logs)",
        f"- Script validation failures: {log_counts.get('script_validation_failures', 0)}",
        f"- Media fetch failures: {log_counts.get('media_fetch_failures', 0)}",
        f"- Video generation failures: {log_counts.get('video_generation_failures', 0)}",
        f"- Render failures: {log_counts.get('render_failures', 0)}",
        f"- Quality-gate output reductions: {log_counts.get('quality_gate_reductions', 0)}",
        "",
        "## Recent Performance Samples",
    ]
    if not recent_metrics:
        top_lines.append("- No recent performance rows in `video_performance_metrics`.")
        return "\n".join(top_lines) + "\n"

    top_lines.append("| timestamp | completion_rate | engagement_rate | views | likes | shares | saves |")
    top_lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in recent_metrics[:10]:
        top_lines.append(
            "| {timestamp} | {completion:.3f} | {engagement:.3f} | {views} | {likes} | {shares} | {saves} |".format(
                timestamp=str(row.get("metric_timestamp") or ""),
                completion=float(row.get("completion_rate") or 0.0),
                engagement=float(row.get("engagement_rate") or 0.0),
                views=int(row.get("views") or 0),
                likes=int(row.get("likes") or 0),
                shares=int(row.get("shares") or 0),
                saves=int(row.get("saves") or 0),
            )
        )
    return "\n".join(top_lines) + "\n"


def main() -> None:
    bootstrap_runtime_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    settings = load_settings()
    workspace_root = Path(__file__).resolve().parent.parent
    lookback_days = max(1, settings.quality_baseline_lookback_days)
    log_counts = _scan_recent_logs(workspace_root=workspace_root, lookback_days=lookback_days)
    with db_connection(settings.supabase_db_url) as conn:
        db_summary = get_quality_baseline_summary(
            conn,
            persona_key=settings.persona_key,
            lookback_days=lookback_days,
        )
        recent_metrics = list_recent_video_metrics(
            conn,
            persona_key=settings.persona_key,
            lookback_days=lookback_days,
            limit=20,
        )
    report = _build_markdown_report(
        lookback_days=lookback_days,
        db_summary=db_summary,
        log_counts=log_counts,
        recent_metrics=recent_metrics,
    )
    output_dir = workspace_root / "tmp_logs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "quality-baseline-latest.md"
    output_path.write_text(report, encoding="utf-8")
    LOGGER.info("Wrote quality baseline report to %s", output_path)
    print(report)


if __name__ == "__main__":
    main()
