#!/usr/bin/env python3
"""Fetch Instagram Reels analytics from Metricool, match to DB transcripts,
and produce a structured engagement analysis report."""
from __future__ import annotations

import argparse
import logging
import os
import statistics
import sys
from collections import defaultdict
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import bootstrap_runtime_env, load_settings
from pipeline.db import db_connection
from pipeline.metricool_analytics import (
    MatchedReel,
    fetch_db_published_posts,
    fetch_instagram_reels,
    match_reels_to_posts,
    store_reel_metrics,
)

LOGGER = logging.getLogger("instagram_analytics")

TIER_VIRAL = 1000
TIER_STRONG = 500
TIER_AVERAGE = 100


def _tier_label(views: int) -> str:
    if views >= TIER_VIRAL:
        return "Viral"
    if views >= TIER_STRONG:
        return "Strong"
    if views >= TIER_AVERAGE:
        return "Average"
    return "Underperforming"


def _safe_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return statistics.mean(values)


def _safe_median(values: list[float]) -> float:
    if not values:
        return 0.0
    return statistics.median(values)


def _composite_score(m: MatchedReel, *, all_matched: list[MatchedReel]) -> float:
    """0-100 composite score per analytics.md formula."""
    all_views = [r.reel.views for r in all_matched]
    all_eng = [_engagement_rate(r) for r in all_matched]
    all_like = [_like_ratio(r) for r in all_matched]
    all_comp = [_completion_rate(r) for r in all_matched]

    def _norm(val: float, vals: list[float]) -> float:
        lo, hi = min(vals), max(vals)
        if hi == lo:
            return 50.0
        return ((val - lo) / (hi - lo)) * 100.0

    views_n = _norm(m.reel.views, all_views) if all_views else 0
    eng_n = _norm(_engagement_rate(m), all_eng) if all_eng else 0
    like_n = _norm(_like_ratio(m), all_like) if all_like else 0
    comp_n = _norm(_completion_rate(m), all_comp) if all_comp else 0

    return views_n * 0.40 + eng_n * 0.25 + like_n * 0.15 + comp_n * 0.20


def _engagement_rate(m: MatchedReel) -> float:
    if m.reel.views == 0:
        return 0.0
    return (m.reel.likes + m.reel.comments + m.reel.shares + m.reel.saved) / m.reel.views


def _like_ratio(m: MatchedReel) -> float:
    total = m.reel.likes + m.reel.comments
    if total == 0:
        return 0.0
    return m.reel.likes / total


def _completion_rate(m: MatchedReel) -> float:
    if m.reel.duration_seconds <= 0 or m.reel.average_watch_time <= 0:
        return 0.0
    return min(1.0, m.reel.average_watch_time / m.reel.duration_seconds)


def _print_divider(char: str = "=", width: int = 100) -> None:
    print(char * width)


def _print_header(title: str) -> None:
    print()
    _print_divider()
    print(f"  {title}")
    _print_divider()
    print()


def _truncate(text: str, max_len: int = 60) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def report_per_video_table(matched: list[MatchedReel]) -> None:
    _print_header("PER-VIDEO PERFORMANCE")

    sorted_matched = sorted(matched, key=lambda m: m.reel.views, reverse=True)
    print(f"{'#':<3} {'Title':<45} {'Views':>7} {'Likes':>6} {'Cmnts':>6} "
          f"{'Shares':>7} {'Saves':>6} {'Eng%':>6} {'AvgWatch':>8} {'Skip%':>6} {'Tier':<16} {'Score':>6}")
    _print_divider("-")

    for idx, m in enumerate(sorted_matched, 1):
        eng = _engagement_rate(m) * 100
        skip = m.reel.reels_skip_rate * 100 if m.reel.reels_skip_rate else 0
        score = _composite_score(m, all_matched=matched)
        print(
            f"{idx:<3} {_truncate(m.db_post.title, 44):<45} {m.reel.views:>7,} {m.reel.likes:>6,} "
            f"{m.reel.comments:>6,} {m.reel.shares:>7,} {m.reel.saved:>6,} {eng:>5.1f}% "
            f"{m.reel.average_watch_time:>7.1f}s {skip:>5.1f}% {_tier_label(m.reel.views):<16} {score:>5.1f}"
        )

    print()
    print(f"Total reels analyzed: {len(matched)}")
    print(f"Total views: {sum(m.reel.views for m in matched):,}")
    print(f"Total likes: {sum(m.reel.likes for m in matched):,}")
    print(f"Total comments: {sum(m.reel.comments for m in matched):,}")
    print(f"Total shares: {sum(m.reel.shares for m in matched):,}")
    print(f"Total saves: {sum(m.reel.saved for m in matched):,}")


def report_tier_distribution(matched: list[MatchedReel]) -> None:
    _print_header("PERFORMANCE TIER DISTRIBUTION")

    tiers: dict[str, list[MatchedReel]] = defaultdict(list)
    for m in matched:
        tiers[_tier_label(m.reel.views)].append(m)

    total = len(matched) or 1
    for tier_name in ["Viral", "Strong", "Average", "Underperforming"]:
        count = len(tiers.get(tier_name, []))
        pct = (count / total) * 100
        bar = "#" * int(pct / 2)
        print(f"  {tier_name:<18} {count:>3} ({pct:>5.1f}%)  {bar}")

    print()
    underperforming_pct = (len(tiers.get("Underperforming", [])) / total) * 100
    if underperforming_pct > 25:
        print("  WARNING: Over 25% of videos are underperforming. Review topic selection and title formulas.")
    viral_count = len(tiers.get("Viral", []))
    if viral_count == 0:
        print("  NOTE: No viral videos in this period. Consider testing stronger hooks or trending topics.")


def report_signal_correlation(matched: list[MatchedReel]) -> None:
    _print_header("CONTENT SIGNAL CORRELATION ANALYSIS")

    signal_dimensions = ["topic_category", "hook_type", "length_bucket", "title_formula"]

    for dimension in signal_dimensions:
        groups: dict[str, list[MatchedReel]] = defaultdict(list)
        for m in matched:
            val = m.db_post.content_signals.get(dimension)
            if val:
                groups[str(val)].append(m)
            else:
                groups["(untagged)"].append(m)

        if not groups or (len(groups) == 1 and "(untagged)" in groups):
            print(f"  {dimension}: No signal data available")
            print()
            continue

        print(f"  Signal: {dimension}")
        print(f"  {'Value':<25} {'Count':>5} {'AvgViews':>10} {'AvgEng%':>8} {'AvgWatch':>9} {'AvgScore':>9}")
        print(f"  {'-' * 70}")

        sorted_groups = sorted(
            groups.items(),
            key=lambda kv: _safe_mean([m.reel.views for m in kv[1]]),
            reverse=True,
        )

        for val, group_matched in sorted_groups:
            avg_views = _safe_mean([m.reel.views for m in group_matched])
            avg_eng = _safe_mean([_engagement_rate(m) * 100 for m in group_matched])
            avg_watch = _safe_mean([m.reel.average_watch_time for m in group_matched])
            avg_score = _safe_mean([_composite_score(m, all_matched=matched) for m in group_matched])
            print(
                f"  {_truncate(val, 24):<25} {len(group_matched):>5} {avg_views:>10,.0f} "
                f"{avg_eng:>7.1f}% {avg_watch:>8.1f}s {avg_score:>8.1f}"
            )

        print()


def report_transcript_analysis(matched: list[MatchedReel]) -> None:
    _print_header("TRANSCRIPT ANALYSIS (Top vs Bottom Performers)")

    sorted_by_views = sorted(matched, key=lambda m: m.reel.views, reverse=True)
    n = max(1, len(sorted_by_views) // 4)
    top = sorted_by_views[:n]
    bottom = sorted_by_views[-n:] if len(sorted_by_views) > n else []

    if top:
        print("  TOP PERFORMERS (by views):")
        print()
        for m in top:
            script_preview = _truncate(m.db_post.script_10s, 120)
            signals = m.db_post.content_signals
            print(f"    Title: {_truncate(m.db_post.title, 80)}")
            print(f"    Views: {m.reel.views:,} | Engagement: {_engagement_rate(m) * 100:.1f}% | "
                  f"Avg Watch: {m.reel.average_watch_time:.1f}s / {m.reel.duration_seconds}s")
            print(f"    Signals: topic={signals.get('topic_category', '?')} | "
                  f"hook={signals.get('hook_type', '?')} | "
                  f"length={signals.get('length_bucket', '?')} | "
                  f"formula={signals.get('title_formula', '?')}")
            print(f"    Script: {script_preview}")
            print()

    if bottom:
        print("  BOTTOM PERFORMERS (by views):")
        print()
        for m in bottom:
            script_preview = _truncate(m.db_post.script_10s, 120)
            signals = m.db_post.content_signals
            print(f"    Title: {_truncate(m.db_post.title, 80)}")
            print(f"    Views: {m.reel.views:,} | Engagement: {_engagement_rate(m) * 100:.1f}% | "
                  f"Avg Watch: {m.reel.average_watch_time:.1f}s / {m.reel.duration_seconds}s")
            print(f"    Signals: topic={signals.get('topic_category', '?')} | "
                  f"hook={signals.get('hook_type', '?')} | "
                  f"length={signals.get('length_bucket', '?')} | "
                  f"formula={signals.get('title_formula', '?')}")
            print(f"    Script: {script_preview}")
            print()


def report_diagnose_underperformers(matched: list[MatchedReel]) -> None:
    _print_header("UNDERPERFORMER DIAGNOSIS")

    all_views = [m.reel.views for m in matched]
    all_eng = [_engagement_rate(m) for m in matched]
    all_comp = [_completion_rate(m) for m in matched]

    median_views = _safe_median([float(v) for v in all_views])
    median_eng = _safe_median(all_eng)
    median_comp = _safe_median(all_comp)

    underperformers = [m for m in matched if m.reel.views < TIER_AVERAGE]
    if not underperformers:
        print("  No underperforming videos found. All reels have 100+ views.")
        return

    diagnoses: dict[str, list[MatchedReel]] = defaultdict(list)
    for m in underperformers:
        eng = _engagement_rate(m)
        comp = _completion_rate(m)
        low_views = m.reel.views < median_views
        low_eng = eng < median_eng
        low_comp = comp < median_comp

        if low_views and low_eng:
            diagnoses["Topic + title failure (low views AND low engagement)"].append(m)
        elif low_views and not low_eng:
            diagnoses["Poor topic selection (low views but normal engagement)"].append(m)
        elif not low_views and low_eng:
            diagnoses["Weak script quality (normal views but low engagement)"].append(m)
        elif not low_views and low_comp:
            diagnoses["Too long or weak hook (normal views but low completion)"].append(m)
        else:
            diagnoses["Other"].append(m)

    for diagnosis, group in diagnoses.items():
        print(f"  {diagnosis}: {len(group)} video(s)")
        for m in group[:3]:
            print(f"    - {_truncate(m.db_post.title, 70)} (views={m.reel.views:,})")
        if len(group) > 3:
            print(f"    ... and {len(group) - 3} more")
        print()


def report_recommendations(matched: list[MatchedReel]) -> None:
    _print_header("ACTIONABLE RECOMMENDATIONS")

    if not matched:
        print("  Insufficient data for recommendations.")
        return

    recommendations: list[str] = []

    topic_performance: dict[str, list[float]] = defaultdict(list)
    hook_performance: dict[str, list[float]] = defaultdict(list)
    length_performance: dict[str, list[float]] = defaultdict(list)
    formula_performance: dict[str, list[float]] = defaultdict(list)

    for m in matched:
        signals = m.db_post.content_signals
        score = _composite_score(m, all_matched=matched)
        topic = signals.get("topic_category")
        hook = signals.get("hook_type")
        length = signals.get("length_bucket")
        formula = signals.get("title_formula")
        if topic:
            topic_performance[topic].append(score)
        if hook:
            hook_performance[hook].append(score)
        if length:
            length_performance[length].append(score)
        if formula:
            formula_performance[formula].append(score)

    if topic_performance:
        best_topic = max(topic_performance, key=lambda k: _safe_mean(topic_performance[k]))
        worst_topic = min(topic_performance, key=lambda k: _safe_mean(topic_performance[k]))
        best_avg = _safe_mean(topic_performance[best_topic])
        worst_avg = _safe_mean(topic_performance[worst_topic])
        if best_avg - worst_avg > 15 and len(topic_performance[best_topic]) >= 2:
            best_count = len(topic_performance[best_topic])
            total = sum(len(v) for v in topic_performance.values())
            current_share = (best_count / total) * 100 if total else 0
            target_share = min(80, current_share + 20)
            recommendations.append(
                f"Increase '{best_topic}' topic share from ~{current_share:.0f}% to ~{target_share:.0f}% "
                f"(avg score {best_avg:.0f} vs '{worst_topic}' at {worst_avg:.0f})"
            )

    if hook_performance:
        best_hook = max(hook_performance, key=lambda k: _safe_mean(hook_performance[k]))
        worst_hook = min(hook_performance, key=lambda k: _safe_mean(hook_performance[k]))
        best_avg = _safe_mean(hook_performance[best_hook])
        worst_avg = _safe_mean(hook_performance[worst_hook])
        if best_avg - worst_avg > 10 and len(hook_performance[best_hook]) >= 2:
            recommendations.append(
                f"Favor '{best_hook}' hooks over '{worst_hook}' hooks "
                f"(avg score {best_avg:.0f} vs {worst_avg:.0f})"
            )

    if length_performance:
        best_len = max(length_performance, key=lambda k: _safe_mean(length_performance[k]))
        worst_len = min(length_performance, key=lambda k: _safe_mean(length_performance[k]))
        best_avg = _safe_mean(length_performance[best_len])
        worst_avg = _safe_mean(length_performance[worst_len])
        if best_avg - worst_avg > 10 and best_len != worst_len:
            recommendations.append(
                f"Target '{best_len}' script length over '{worst_len}' "
                f"(avg score {best_avg:.0f} vs {worst_avg:.0f})"
            )

    if formula_performance:
        best_formula = max(formula_performance, key=lambda k: _safe_mean(formula_performance[k]))
        worst_formula = min(formula_performance, key=lambda k: _safe_mean(formula_performance[k]))
        best_avg = _safe_mean(formula_performance[best_formula])
        worst_avg = _safe_mean(formula_performance[worst_formula])
        if best_avg - worst_avg > 10 and len(formula_performance[best_formula]) >= 2:
            recommendations.append(
                f"Use '{best_formula}' title formula more often "
                f"(avg score {best_avg:.0f} vs '{worst_formula}' at {worst_avg:.0f})"
            )

    avg_eng = _safe_mean([_engagement_rate(m) * 100 for m in matched])
    avg_views = _safe_mean([float(m.reel.views) for m in matched])
    avg_watch = _safe_mean([m.reel.average_watch_time for m in matched])

    print(f"  Channel averages: {avg_views:,.0f} views | {avg_eng:.1f}% engagement | {avg_watch:.1f}s avg watch time")
    print()

    if not recommendations:
        print("  Insufficient variation in content signals to generate specific recommendations.")
        print("  Consider tagging more content with content_signals for better analysis.")
        return

    for idx, rec in enumerate(recommendations, 1):
        print(f"  {idx}. {rec}")

    print()
    print("  These recommendations are based on composite score differences across signal groups.")
    print("  Differences >15 points are actionable; 5-15 are directional; <5 are inconclusive.")


def report_summary_stats(matched: list[MatchedReel]) -> None:
    _print_header("SUMMARY STATISTICS")

    if not matched:
        print("  No data available.")
        return

    views = [m.reel.views for m in matched]
    eng_rates = [_engagement_rate(m) * 100 for m in matched]
    watch_times = [m.reel.average_watch_time for m in matched]
    durations = [m.reel.duration_seconds for m in matched if m.reel.duration_seconds > 0]
    comp_rates = [_completion_rate(m) * 100 for m in matched if _completion_rate(m) > 0]

    def _stats_line(label: str, vals: list[float], fmt: str = ",.0f", suffix: str = "") -> None:
        if not vals:
            print(f"  {label:<25} (no data)")
            return
        print(
            f"  {label:<25} mean={format(_safe_mean(vals), fmt)}{suffix}  "
            f"median={format(_safe_median(vals), fmt)}{suffix}  "
            f"min={format(min(vals), fmt)}{suffix}  "
            f"max={format(max(vals), fmt)}{suffix}"
        )

    _stats_line("Views", [float(v) for v in views])
    _stats_line("Engagement rate", eng_rates, ".2f", "%")
    _stats_line("Avg watch time", watch_times, ".1f", "s")
    _stats_line("Duration", [float(d) for d in durations], ".0f", "s")
    _stats_line("Completion rate", comp_rates, ".1f", "%")


def run_report(matched: list[MatchedReel]) -> None:
    print()
    print("=" * 100)
    print("  NEWS FISH NOW -- INSTAGRAM REELS ANALYTICS REPORT")
    print(f"  Generated from {len(matched)} matched reels")
    print("=" * 100)

    report_summary_stats(matched)
    report_per_video_table(matched)
    report_tier_distribution(matched)
    report_signal_correlation(matched)
    report_transcript_analysis(matched)
    report_diagnose_underperformers(matched)
    report_recommendations(matched)


def _synthetic_matched_from_reels(reels: list[Any]) -> list[MatchedReel]:
    """Build MatchedReel objects from raw Metricool reels without DB data."""
    from pipeline.metricool_analytics import DBPublishedPost, MetricoolReel

    results: list[MatchedReel] = []
    for reel in reels:
        caption = reel.content or ""
        first_line = caption.split("\n")[0].strip() if caption else "(no caption)"
        title = first_line[:120] if first_line else "(no caption)"
        synthetic_post = DBPublishedPost(
            post_id="",
            publish_job_id="",
            external_post_id=reel.reel_id,
            published_at=reel.published_at,
            title=title,
            link=reel.url,
            script_10s=caption,
            caption_instagram=caption,
            content_signals={},
        )
        results.append(MatchedReel(
            reel=reel,
            db_post=synthetic_post,
            match_method="metricool_only",
            match_score=1.0,
        ))
    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Instagram Reels analytics from Metricool and analyze engagement vs. transcripts",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--no-store",
        action="store_true",
        help="Skip storing metrics in the database",
    )
    parser.add_argument(
        "--metricool-only",
        action="store_true",
        help="Report from Metricool data only (skip DB connection)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser


def main() -> None:
    bootstrap_runtime_env()
    parser = _build_parser()
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    settings = load_settings()

    reels = fetch_instagram_reels(settings=settings, lookback_days=args.lookback_days)
    if not reels:
        print("No reels fetched from Metricool. Check credentials and date range.")
        sys.exit(1)

    if args.metricool_only:
        matched = _synthetic_matched_from_reels(reels)
        run_report(matched)
        return

    with db_connection(settings.supabase_db_url) as conn:
        posts = fetch_db_published_posts(
            conn,
            persona_key=settings.persona_key,
            lookback_days=args.lookback_days,
        )
        if not posts:
            print(f"No published posts found in DB for the last {args.lookback_days} days.")
            sys.exit(1)

        matched = match_reels_to_posts(reels, posts)
        if not matched:
            print(
                f"Could not match any of the {len(reels)} Metricool reels "
                f"to the {len(posts)} DB posts. Caption matching may need adjustment."
            )
            sys.exit(1)

        if not args.no_store:
            stored = store_reel_metrics(conn, matched, persona_key=settings.persona_key)
            LOGGER.info("Stored %d metrics", stored)

    run_report(matched)


if __name__ == "__main__":
    main()
