# News Fish Now -- Analytics and A/B Testing

## Engagement Scoring (Pre-Processing)

Before content generation, the pipeline scores RSS candidates for engagement potential. Only posts above the threshold are selected for processing.

### Thresholds

| Source   | Env var               | Default | Purpose                          |
|----------|------------------------|---------|----------------------------------|
| Primary  | `ENGAGEMENT_MIN_SCORE` | 0.55    | Clone.fyi and primary feeds      |
| Fallback | `ENGAGEMENT_FLOOR_SCORE` | 0.50  | NPR, BBC, Reuters, AP fallbacks  |

### Impact Keywords

Stories receive impact points when title/description/context contain these terms (up to 4 hits = 1.0):

- **Policy/Economy**: policy, congress, senate, house, supreme court, scotus, federal, government, sanction, war, ceasefire, military, inflation, interest rate, fed, unemployment, recession, gdp, tariff, regulation, lawsuit, investigation, safety, health, earthquake, hurricane, outbreak, security, pentagon
- **Tech/Business**: ai, artificial intelligence, ceo, startup, acquisition, antitrust, data breach, privacy, layoff, ipo, openai, google, apple, microsoft, meta, amazon, cybersecurity, hack, exploit, outage

### Hook Patterns

Additional score from title/description patterns: question marks, numbers/percentages, conflict words (accused, charged, banned, fired, leaked, crash), breaking/urgent phrasing, name-drops (musk, trump, biden, zuckerberg, swift, bezos), and executive/business events (ceo, founder, steps down, resign, launch, unveil, acquire).

### Civic Importance Score

A separate scoring dimension that measures whether a story matters to people's lives, independent of engagement potential. Keywords are grouped into four categories (5 total hits = 1.0):

- **Scale indicators**: billion, million, trillion, percent, nationwide, worldwide, every American, all users/workers/students, thousands, hundreds of thousands
- **Institutional action**: signed, enacted, ruled, ordered, indicted, charged, banned, approved, vetoed, overturned, struck down, sentenced, convicted, sanctioned, blocked
- **Precedent**: first, unprecedented, new law, new rule, landmark, historic, never before, record high/low, all-time, highest/lowest ever
- **Population-affecting**: jobs, healthcare, housing, wages, prices, taxes, benefits, schools, pensions, insurance, rent, tuition, social security, medicare, medicaid, veterans, drinking water, food safety, drug prices

**Scoring formula weights:**

| Component         | Weight | Description                                    |
|-------------------|--------|------------------------------------------------|
| timeliness        | 0.20   | Recency (1.0 if <=6h, down to 0.1 for >72h)  |
| impact            | 0.22   | Keyword matches for policy/tech/business topics |
| civic_importance  | 0.12   | Scale, institutional action, precedent, population |
| hook_strength     | 0.14   | Engagement hooks (questions, numbers, names)    |
| context_richness  | 0.16   | Word count + vocabulary specificity             |
| credibility       | 0.12   | Source reputation                               |
| platform_fit      | 0.04   | Cross-platform packaging (IG title fit + TikTok controversy/name/emotion signals) |
| mix_adjustment    | additive | Topic bias from content mix profile            |
| performance_boost | additive | Historical signal performance feedback (capped +/-0.08) |

### Importance Slot

After normal score-sorted selection, the pipeline checks if any post in the top-3 by civic_importance was NOT selected. If so (and it meets the floor score and has civic_importance >= 0.4), it swaps in for the lowest-civic-importance selected post. This guarantees that at least one high-importance story is covered per run, even if a celebrity or culture story scored higher on engagement.

### Flexible Duration for High-Importance Stories

When a story's civic_importance score is >= 0.6, the target word count is increased by 15 (e.g. 95 -> 110, producing ~37s). This gives complex policy and government stories room to include the "why it matters" beat without exceeding the 40s publish ceiling. Simple stories stay at the standard word target.

### Content Mix Profiles

The `CONTENT_MIX_PROFILE` env var controls topic prioritization in engagement scoring:

| Profile              | government_geo | hard_news_other | culture         | general |
|----------------------|----------------|-----------------|-----------------|---------|
| `balanced_geo`       | +0.06          | +0.02           | +0.01           | 0.0     |
| `hard_news_only`     | +0.08          | +0.08           | -0.06           | -0.01   |
| `culture_forward`    | -0.01          | -0.01           | +0.04 to +0.08  | 0.0     |
| `hard_news_culture`  | +0.06          | +0.06           | +0.01 to +0.04  | 0.0     |
| `tiktok_optimized`   | +0.00 to +0.03 | +0.01 to +0.04 | +0.05 to +0.08  | 0.0     |
| `engagement_adaptive`| +0.04          | +0.04           | +0.05 (hook-gated) | +0.01 |

The default profile is `hard_news_culture`. The `tiktok_optimized` profile favors culture and celebrity-adjacent stories with strong hooks, reflecting TikTok's algorithm preferences. The `engagement_adaptive` profile prioritizes hook strength over topic category, boosting any story with a hook_strength >= 0.5.

### Penalty Patterns

Title/description content matching these patterns receives an engagement score penalty:

| Pattern              | Regex matches                                                         | Penalty |
|----------------------|-----------------------------------------------------------------------|---------|
| `listicle_or_review` | review, hands-on, unboxing, first look, comparison, vs.              | 0.20    |
| `commerce_or_affiliate` | deal, coupon, buy now, affiliate, discount                         | 0.18    |
| `opinion_or_editorial` | opinion, editorial, op-ed, columnist                                | 0.16    |
| `abstract_literary`  | memoir, essay, literary, diary, chronicles, meditation, reflection    | 0.15    |

### Overflow Selection

When primary selection yields fewer than `floor_required_posts` (default 2), fallback feeds fill the gap. An overflow multiplier (3x) selects extra candidates so that when some already exist in the DB, others can be processed. Example: floor=2 → up to 6 candidates selected, allowing DB dedup without zero output.

### Safety Net

If the main processing loop produces 0 posts (all candidates skipped due to existing, pre-voice gate, or other quality gates), a safety net runs: it force-recycles the best remaining candidate (deletes and re-inserts the DB row) and reprocesses it. This guarantees at least one post per run when any candidate passes the pre-voice gate.

## Performance Tiers

Classify each video by views within the first 48 hours of publication:

| Tier             | Views (48h) | Signal                                     |
|------------------|-------------|--------------------------------------------|
| Viral            | 1000+       | Strong topic + title + algorithm pickup     |
| Strong           | 500--999    | Good topic resonance, decent distribution   |
| Average          | 100--499    | Acceptable baseline performance             |
| Underperforming  | < 100       | Topic, title, or distribution failure       |

## Composite Scoring

Each video receives a composite score from 0 to 100:

```
composite = (
    views_normalized     * 0.40
  + engagement_rate_norm * 0.25
  + like_ratio_norm      * 0.15
  + completion_rate_norm * 0.20
)
```

Normalization: each metric is scaled 0--100 against the channel's rolling 30-day range (min to max). A score above 70 is strong. Below 30 warrants investigation.

### Metric Definitions

| Metric           | Formula                                           |
|------------------|---------------------------------------------------|
| Views            | Raw view count within measurement window          |
| Engagement rate  | (likes + comments + shares + saves) / views       |
| Like ratio       | likes / (likes + dislikes) as percentage           |
| Completion rate  | Fraction of viewers who watched to the end         |

## Content Signal Taxonomy

Every video is tagged with four signal dimensions for correlation analysis.

### Topic Category

| Category      | Description                                         | Examples                          |
|---------------|-----------------------------------------------------|-----------------------------------|
| `government`  | Courts, legislation, executive orders, regulation   | SCOTUS rulings, DOJ actions       |
| `geopolitical`| International relations, conflicts, diplomacy       | Iran talks, cartel operations     |
| `tech`        | AI, software, hardware, platforms                   | Anthropic ban, Google trade secrets|
| `culture`     | Media, lifestyle, essays, sports                    | Fashion Week, literary essays     |
| `finance`     | Markets, bonds, economic indicators                 | Bond market, oil prices           |
| `science`     | Health, research, environment                       | Chronic fatigue, AI safety        |
| `celebrity`   | Named public figures as central subject             | Zuckerberg, Trump                 |

### Hook Type

| Hook Type         | Pattern                                              | Example opener                                |
|-------------------|------------------------------------------------------|-----------------------------------------------|
| `breaking-event`  | "{Subject} just {did X}"                             | "Iran just shut down the world's oil"         |
| `question`        | Poses a question the viewer needs answered            | "Can't answer this gun question"              |
| `number-lead`     | Opens with a specific number or statistic             | "20% of global oil supply"                    |
| `name-drop`       | Leads with a recognizable name                        | "Mark Zuckerberg sits front row at Prada"     |
| `controversy`     | Frames opposing forces or scandal                     | "Justice Department withheld Epstein files"   |

### Script Length Bucket

| Bucket   | Duration    | Word count (approx) |
|----------|-------------|----------------------|
| `short`  | 22--28s     | 65--80 words         |
| `medium` | 29--35s     | 81--105 words        |
| `long`   | 36--40s     | 106--120 words       |

### Title Formula

| Formula                 | Pattern                                        | Example                                        |
|-------------------------|------------------------------------------------|------------------------------------------------|
| `verb-subject-suspense` | Action verb + subject + open loop               | "Supreme Court Just Froze New York's Map"      |
| `subject-did-thing`     | Subject + past tense action                     | "Feds Just Killed Brooklyn's Passport Access"  |
| `question-hook`         | Question that demands an answer                 | "SCOTUS Can't Answer This Gun Question"        |
| `name-drop-event`       | Famous name + event                             | "Mark Zuckerberg sits front row at Prada"      |
| `danger-statement`      | Threat or danger framing                        | "Iran Just Shut Down the World's Oil"          |

## Analytics Input Protocol

When pasting YouTube Studio data for analysis, include:

1. **Video title** (full text including emojis)
2. **Duration** (e.g., 0:18, 0:49)
3. **Visibility** (Public/Private/Unlisted)
4. **Publish date** (e.g., Mar 2, 2026)
5. **Views** (numeric)
6. **Comments** (numeric)
7. **Likes vs. dislikes** (percentage and count if available)

The standard YouTube Studio "Channel content" list view provides all of these fields. Copy the entire table including headers.

### Parsing Format

The system expects each video to appear as a block of lines:

```
Video thumbnail: {title}
{duration}
{title}
{description snippet}
{visibility}
{restrictions}
{date}
{status}
{views}
{comments}
{like_info}
```

This matches the default YouTube Studio copy-paste format. No reformatting needed.

## Interpretation Decision Tree

When analyzing a batch of videos, follow this diagnostic flow:

### Step 1: Tier Distribution

Count how many videos fall into each performance tier. Healthy distribution for a growing channel:

- At least 1 viral per week
- At least 2 strong per week
- No more than 25% underperforming

### Step 2: Diagnose Underperformers

| Symptom                        | Likely cause          | Action                                           |
|--------------------------------|-----------------------|--------------------------------------------------|
| Low views, normal engagement   | Poor topic selection  | Shift topic mix toward proven categories          |
| Low views, low engagement      | Topic + title failure | Both topic and packaging need change              |
| Normal views, low engagement   | Weak script quality   | Improve detail density, stakes, implications      |
| Normal views, low completion   | Too long or weak hook | Shorten script or strengthen opening 3 seconds    |
| High views, low like ratio     | Polarizing content    | Acceptable if engagement is high; monitor comments|

### Step 3: Signal Correlation

Group videos by each signal dimension and compare average composite scores:

1. **Topic category**: Which categories consistently outperform? Shift the topic mix.
2. **Hook type**: Which hook styles drive higher views? Favor them.
3. **Length bucket**: Which length range retains viewers best? Adjust script targets.
4. **Title formula**: Which title patterns drive higher click-through? Favor them.

### Step 4: Actionable Recommendations

Based on the analysis, produce 2--3 specific, actionable changes:

- "Increase government/geopolitical topic share from 40% to 60%"
- "Switch from question hooks to breaking-event hooks for government stories"
- "Reduce target script length from 55s to 40s for tech topics"

## A/B Testing Protocol

### Principles

- Test one variable at a time to isolate causation
- Run each experiment for at least 7 days (28 videos minimum)
- Allocate 2 videos per day to variant A, 2 to variant B (balanced daily)
- Do not start a new experiment until the current one concludes
- Topic selection is NOT controlled -- natural topic variation is expected noise

### Experiment Lifecycle

1. **Hypothesis**: State what you expect (e.g., "Question hooks will produce 20% higher views than statement hooks")
2. **Setup**: Define the variable, variants, and duration
3. **Run**: Pipeline auto-assigns variants via round-robin
4. **Measure**: After the experiment window, compare composite scores across variants
5. **Conclude**: If one variant wins with >15% composite score advantage, adopt it as the new default. Otherwise, declare inconclusive and re-test or move on.

### Testable Variables

| Variable         | Variant A                  | Variant B                     | Variant C             | Metric to watch    |
|------------------|----------------------------|-------------------------------|-----------------------|--------------------|
| Hook type        | `breaking-event`           | `question`                    | `number-lead` / `name-drop` / `controversy` | Views, retention   |
| Script length    | 30s target (85 words)      | 50s target (140 words)        | --                    | Completion, views  |
| Title formula    | `verb-subject-suspense`    | `name-drop-event`             | --                    | Views              |
| Caption style    | Casual/texting tone         | Authoritative/insider tone    | --                    | Engagement rate    |
| Thumbnail source | `article-image`            | `web-sourced`                 | `gemini-generated`    | Views, CTR         |
| Topic mix        | 75% important / 25% variety | 50% important / 50% variety  | --                    | Views, engagement  |

### Minimum Sample Size

For directional confidence (not full statistical significance):

- 14 videos per variant (7 days at 2/day)
- Compare mean composite scores
- Report the difference as a percentage
- Flag if the difference exceeds 15% as "actionable"
- Flag if the difference is below 5% as "inconclusive"

### Recording Results

After each experiment:

1. Update the experiment record with status `completed` and a written conclusion
2. If a winner is identified, update the relevant pipeline defaults (e.g., change `CONTENT_SCRIPT_TARGET_WORDS`)
3. Log the result in the `optimization_recommendations` table for audit trail
4. Start the next experiment from the testable variables queue

### Auto-Rotation

The experiment system is self-evolving. At the start of each pipeline run, `maybe_rotate_experiment()` checks whether the active experiment has sufficient data:

1. If the active experiment has >= 14 samples per variant, it is evaluated
2. If verdict is `actionable` (>15% composite advantage): winner is adopted, experiment is completed
3. If the experiment has run for 21+ days without a clear winner: force-concluded as inconclusive
4. After concluding, the next experiment in the queue is auto-created

**Experiment Queue** (circular rotation):

1. `hook_type` (5 variants)
2. `thumbnail_source` (3 variants: article-image, web-sourced, gemini-generated)
3. `script_length` (2 variants)
4. `title_formula` (2 variants)
5. `caption_style` (2 variants)

Variables tested within the last 30 days are skipped in the rotation.

## YouTube Analytics Integration

The pipeline fetches per-video metrics directly from the YouTube Analytics API every 6 hours:

| Parameter                     | Default | Env var                              |
|-------------------------------|---------|--------------------------------------|
| YouTube Analytics enabled     | true    | `YOUTUBE_ANALYTICS_ENABLED`          |
| Lookback days                 | 7       | `YOUTUBE_ANALYTICS_LOOKBACK_DAYS`    |
| Fetch limit                   | 50      | `YOUTUBE_ANALYTICS_FETCH_LIMIT`      |

### Metrics Fetched

- Views, likes, dislikes, comments, shares
- Average view duration (seconds)
- Average view percentage (retention)
- Estimated minutes watched

Metrics are stored in `video_performance_metrics` with `platform='youtube'` and `source='youtube_analytics_api'`. Both Metricool and YouTube Analytics data feed into the same autotune decision engine.

## TikTok Analytics Integration

TikTok video metrics are fetched from Metricool alongside Instagram Reels during each pipeline run when `METRICOOL_ANALYTICS_ENABLED=true`.

### Data Flow

1. `fetch_tiktok_videos()` hits the Metricool TikTok analytics endpoint
2. Videos are matched to DB posts using caption similarity (TikTok caption or Instagram caption) or timestamp proximity
3. Metrics are stored in `video_performance_metrics` with `platform='tiktok'`

### Metrics Fetched

- Views, likes, comments, shares, saves
- Average watch time and total watch time
- Duration, engagement rate, reach, impressions
- Completion rate (derived: average_watch_time / duration)

### Matching

TikTok videos match to DB posts via two methods (same as Instagram):
- **Caption match**: SequenceMatcher ratio >= 0.7 between TikTok video caption and the stored `caption_tiktok` or `caption_instagram`
- **Timestamp match**: Published within 2 hours of DB publish timestamp (fallback when caption match fails)

## Signal Performance Feedback Loop

The pipeline automatically correlates content signal dimensions with actual video performance and feeds the results back into engagement scoring.

### How It Works

1. At the start of each pipeline run, `analyze_signal_performance()` joins `video_performance_metrics` with `content_assets.content_signals`
2. For each signal dimension (`topic_category`, `hook_type`, `length_bucket`, `title_formula`), it groups posts by signal value and computes average views
3. Signal values that outperform the baseline by >= 1.3x receive a positive boost; those underperforming by <= 1/1.3x receive a negative penalty
4. Boosts are capped at +/-0.08 per dimension and stored as `optimization_recommendations` with diagnosis `content-signal-performance`
5. During engagement scoring, `_compute_performance_boost()` checks if the candidate post's predicted topic/hook profile matches a historically high or low-performing pattern

### Parameters

| Parameter                              | Value  | Description                                          |
|----------------------------------------|--------|------------------------------------------------------|
| Minimum rows for analysis              | 8      | Must have 8+ posts with both metrics and signals     |
| Minimum group size                     | 3      | Need 3+ posts per signal value to compute a boost    |
| Baseline ratio threshold               | 1.3    | Must outperform baseline by 30% to earn a boost      |
| Maximum boost per candidate            | +/-0.08| Total performance_boost is capped to prevent runaway |

### Cold Start

Until enough data accumulates (minimum 8 posts with metrics across platforms), the feedback loop is inactive. Signal boosts only apply when there is statistically meaningful performance data. The pipeline operates normally using static scoring until then.

### Platform Filtering

`analyze_signal_performance()` accepts an optional `platform_filter` parameter to compute boosts from a single platform (e.g., `platform_filter="tiktok"`). When omitted, all platforms are pooled together.

## Platform Fit Scoring

The `platform_fit` component (4% weight) replaces the former `ig_fit` (2% weight). It blends Instagram and TikTok engagement signals:

### Instagram Fit (50%)
- Title length sweet-spot (7-15 words = 1.0)
- Hook strength contribution
- Punctuation bonus (question marks, colons)

### TikTok Fit (50%)
- **Controversy score**: Matches against controversy terms (accused, charged, banned, exposed, scandal, backlash, etc.)
- **Name recognition score**: Matches against recognizable public figures (politicians, tech leaders, celebrities, athletes)
- **Emotional valence score**: Matches against high-emotion terms (shocking, insane, terrifying, heartbreaking, etc.)
- **Hook strength**: Strong hooks drive TikTok shares

## Thumbnail A/B Testing

Custom thumbnails are generated and uploaded to YouTube for each video. Three strategies are tested via the experiment system:

| Strategy          | Description                                                      |
|-------------------|------------------------------------------------------------------|
| `article-image`   | Best image from the source article, cropped to 1280x720         |
| `web-sourced`     | Google Custom Search finds a more relevant image via LLM query   |
| `gemini-generated`| Gemini `gemini-3-pro-image-preview` creates a photorealistic thumbnail |

Thumbnail URL and source are stored in `content_assets.thumbnail_url` and `content_assets.thumbnail_source` for correlation analysis.
