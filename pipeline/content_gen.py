from __future__ import annotations

from dataclasses import dataclass
import difflib
import json
import logging
import re
from urllib.parse import urlparse

from anthropic import Anthropic

_CJK_CHAR_RE = re.compile(
    r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff"
    r"\U00020000-\U0002a6df\U0002a700-\U0002ebef]"
)


def _count_script_words(text: str) -> int:
    """Count words in a script, treating each CJK character as one word."""
    cjk_count = len(_CJK_CHAR_RE.findall(text))
    if cjk_count > 0:
        non_cjk = _CJK_CHAR_RE.sub("", text)
        latin_count = len(non_cjk.split())
        return cjk_count + latin_count
    return len(text.split())
from pipeline.text_sanitize import strip_urls_from_text

LOGGER = logging.getLogger("wj_content")


INSTAGRAM_LIMIT = 2200
TIKTOK_LIMIT = 2200
YOUTUBE_LIMIT = 5000
X_LIMIT = 280
DEFAULT_SCRIPT_TARGET_SECONDS = 35
DEFAULT_SCRIPT_TARGET_WORDS = 130
DEFAULT_SCRIPT_MAX_WORDS_BUFFER = 15
DEFAULT_SCRIPT_MIN_FACTS = 3
DEFAULT_SCRIPT_MIN_SENTENCES = 5
DEFAULT_SCRIPT_MAX_SENTENCES = 8
EDITORIAL_MIN_SECONDS = 28
EDITORIAL_MAX_SECONDS = 38
EDITORIAL_MIN_WORDS = 100
EDITORIAL_MAX_WORDS = 150
EDITORIAL_MIN_SENTENCES = 5
EDITORIAL_MAX_SENTENCES = 10
VIDEO_TITLE_MAX_CHARS = 40
DEFAULT_HASHTAGS = ["#國際新聞", "#時事", "#世界日報"]
DEFAULT_TONE = "professional_neutral"
DEFAULT_SCRIPT_MAX_CHARS = 1200
IMPLICATION_CUES = (
    "這意味著",
    "這很重要因為",
    "這將影響",
    "這可能影響",
    "也就是說",
    "影響到",
    "衝擊",
    "對此",
    "這代表",
    "這顯示",
    "值得關注的是",
    "可能改變",
    "預計將影響",
    "可能導致",
    "恐將",
)
FALLBACK_NEUTRAL_IMPACT_TEMPLATES = (
    "後續發展可能影響相關政策或營運決策。",
    "這項進展預計將影響相關組織的規劃與時程。",
    "下一步的走向可能改變相關機構的因應方式。",
    "事態如何發展，將牽動各方的佈局與應對。",
)
DISALLOWED_PHRASES = (
    "你絕對想不到",
    "請持續關注",
    "留言告訴我們",
    "只有時間能證明",
    "讓我們拭目以待",
    "未來幾個月將",
    "仍有待觀察",
    "改變了所有人的遊戲規則",
    "重塑整個產業的運作方式",
    "真正的問題是接下來會怎樣",
    "連鎖效應才剛開始",
    "必看",
    "太誇張",
    "震驚全場",
    "顛覆一切",
)
NON_NEUTRAL_CUES = (
    "瘋狂",
    "誇張",
    "離譜",
    "驚爆",
    "爆炸性",
    "駭人",
    "荒唐",
    "驚人",
    "毀滅性",
    "英勇",
    "災難性",
    "難以置信",
    "徹底失敗",
    "明顯掩蓋",
)
FILLER_PHRASE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"事實上[，,]?\s*", ""),
    (r"換句話說[，,]?\s*", ""),
    (r"簡單來說[，,]?\s*", ""),
    (r"坦白說[，,]?\s*", ""),
    (r"基本上[，,]?\s*", ""),
    (r"其實[，,]?\s*", ""),
)
_SENTENCE_ABBREVIATION_PATTERN = re.compile(
    r"\b(?:U\.S\.|U\.K\.|U\.N\.|E\.U\.|Mr\.|Mrs\.|Ms\.|Dr\.|Prof\.|St\.|No\.|Inc\.|Ltd\.)"
)
_SENTENCE_INITIALISM_PATTERN = re.compile(r"\b(?:[A-Z]\.){2,}")

_KNOWN_SOURCE_DOMAINS: dict[str, str] = {
    "worldjournal.com": "世界日報",
    "reuters.com": "路透社",
    "apnews.com": "美聯社",
    "bbc.com": "BBC",
    "bbc.co.uk": "BBC",
    "nytimes.com": "紐約時報",
    "washingtonpost.com": "華盛頓郵報",
    "cnn.com": "CNN",
    "cnbc.com": "CNBC",
    "bloomberg.com": "彭博社",
    "wsj.com": "華爾街日報",
    "udn.com": "聯合新聞網",
    "ltn.com.tw": "自由時報",
    "chinatimes.com": "中時新聞網",
    "cna.com.tw": "中央社",
    "storm.mg": "風傳媒",
}


def _source_display_name(article_url: str) -> str:
    try:
        host = urlparse(article_url).netloc.lower()
    except Exception:
        return ""
    if not host:
        return ""
    host = host.removeprefix("www.")
    if host in _KNOWN_SOURCE_DOMAINS:
        return _KNOWN_SOURCE_DOMAINS[host]
    parts = host.rsplit(".", 2)
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return host.capitalize()


def _append_source_attribution(caption: str, source_name: str, max_len: int) -> str:
    if not source_name:
        return caption
    attribution = f"\n\n來源：{source_name}"
    if len(caption) + len(attribution) <= max_len:
        return caption + attribution
    return caption


@dataclass(frozen=True)
class ContentGenerationResult:
    script_10s: str
    video_title_short: str
    caption_instagram: str
    caption_tiktok: str
    caption_youtube: str
    caption_x: str
    hashtags: list[str]
    tone: str
    language: str
    model_name: str
    prompt_version: str = "v7"
    series_tag: str | None = None
    series_part: int | None = None
    content_signals: dict[str, str] | None = None
    script_10s_en: str = ""


@dataclass(frozen=True)
class ScriptPolicy:
    target_seconds: int
    target_words: int
    min_words: int
    max_words: int
    min_facts: int
    min_sentences: int
    max_sentences: int


def _clamp_text(value: str, max_len: int) -> str:
    text = value.strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _normalize_hashtags(raw_hashtags: list[str]) -> list[str]:
    normalized: list[str] = []
    for tag in raw_hashtags:
        cleaned = re.sub(r"[^\w#\u4e00-\u9fff\u3400-\u4dbf]", "", str(tag).strip())
        if not cleaned:
            continue
        if not cleaned.startswith("#"):
            cleaned = f"#{cleaned}"
        normalized.append(cleaned)

    deduped = list(dict.fromkeys(normalized))
    bounded = deduped[:7]
    if len(bounded) >= 3:
        return bounded
    return list(dict.fromkeys([*bounded, *DEFAULT_HASHTAGS]))[:7]


def _normalize_short_form_text(value: str) -> str:
    normalized = strip_urls_from_text(value)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = re.sub(r"!{2,}", "!", normalized)
    normalized = re.sub(r"\?{2,}", "?", normalized)
    return normalized


def _remove_disallowed_phrases(value: str) -> str:
    cleaned = value
    for phrase in DISALLOWED_PHRASES:
        cleaned = re.sub(re.escape(phrase), "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.;:-")
    return cleaned


def _remove_filler_phrases(value: str) -> str:
    cleaned = value
    for pattern, replacement in FILLER_PHRASE_PATTERNS:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.;:-")
    return cleaned


def _remove_non_neutral_language(value: str) -> str:
    cleaned = value
    for phrase in NON_NEUTRAL_CUES:
        cleaned = re.sub(rf"\b{re.escape(phrase)}\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.;:-")
    return cleaned


def _smart_truncate(text: str, max_len: int) -> str:
    stripped = text.strip()
    if len(stripped) <= max_len:
        return stripped
    truncated = stripped[:max_len]
    last_space = truncated.rfind(" ")
    if last_space > max_len * 0.5:
        truncated = truncated[:last_space].rstrip(" ,.;:-")
    else:
        truncated = truncated.rstrip()
    return truncated + "…" if len(truncated) < len(stripped) else truncated


def _extract_hashtags_from_text(text: str) -> list[str]:
    return re.findall(r"#[a-zA-Z0-9_]+", text.lower())


def _ensure_hashtags_in_caption(caption: str, hashtags: list[str], max_len: int) -> str:
    if not caption.strip():
        return caption
    existing = set(_extract_hashtags_from_text(caption))
    missing = [tag for tag in hashtags if tag.lower() not in existing]
    if not missing:
        return caption
    suffix = " " + " ".join(missing)
    if len(caption) + len(suffix) <= max_len:
        return caption + suffix
    available = max_len - len(caption)
    if available < 4:
        return caption
    tags_to_add: list[str] = []
    running_len = 0
    for tag in missing:
        needed = len(tag) + (1 if tags_to_add else 1)
        if running_len + needed <= available:
            tags_to_add.append(tag)
            running_len += needed
        else:
            break
    if not tags_to_add:
        return caption
    return caption + " " + " ".join(tags_to_add)


def _trim_script_words(script_text: str, max_words: int) -> str:
    word_count = _count_script_words(script_text)
    if word_count <= max_words:
        return script_text
    cjk_count = len(_CJK_CHAR_RE.findall(script_text))
    if cjk_count > 0:
        hard_cap = max_words + max(25, max_words // 5)
        chars = list(script_text)
        cjk_seen = 0
        cut_pos = len(chars)
        for i, ch in enumerate(chars):
            if _CJK_CHAR_RE.match(ch):
                cjk_seen += 1
            if cjk_seen >= max_words:
                cut_pos = i + 1
                break
        extended_pos = cut_pos
        for i in range(cut_pos, min(len(chars), cut_pos + (hard_cap - max_words))):
            extended_pos = i + 1
            if chars[i] in "。！？.!?":
                return "".join(chars[: i + 1]).strip()
        for i in range(cut_pos - 1, -1, -1):
            if chars[i] in "。！？.!?":
                return "".join(chars[: i + 1]).strip()
        return "".join(chars[:cut_pos]).rstrip("，、；：,;: ")
    words = script_text.split()
    if len(words) <= max_words:
        return script_text
    hard_cap = max_words + max(25, max_words // 5)
    extended = " ".join(words[: min(len(words), hard_cap)])
    search_start = len(" ".join(words[:max_words]))
    earliest = -1
    for punct in ".!?":
        pos = extended.find(punct, search_start)
        if pos >= 0 and (earliest < 0 or pos < earliest):
            earliest = pos
    if earliest >= 0:
        return extended[: earliest + 1].strip()
    prefix = " ".join(words[:max_words])
    last_boundary = -1
    for punct in ".!?":
        pos = prefix.rfind(punct)
        if pos > last_boundary:
            last_boundary = pos
    if last_boundary >= 0:
        return prefix[: last_boundary + 1].strip()
    return prefix.rstrip(" ,.;:-")


def _build_script_policy(
    *,
    script_target_seconds: int,
    script_target_words: int,
    script_max_words_buffer: int,
    script_min_words: int,
    script_min_facts: int,
    script_min_sentences: int,
    script_max_sentences: int,
) -> ScriptPolicy:
    target_seconds = min(EDITORIAL_MAX_SECONDS, max(EDITORIAL_MIN_SECONDS, int(script_target_seconds)))
    min_words = min(EDITORIAL_MAX_WORDS, max(20, int(script_min_words)))
    target_words = min(EDITORIAL_MAX_WORDS, max(min_words, int(script_target_words)))
    max_words = min(EDITORIAL_MAX_WORDS, max(target_words, target_words + int(script_max_words_buffer)))
    min_facts = max(DEFAULT_SCRIPT_MIN_FACTS, int(script_min_facts))
    min_sentences = min(EDITORIAL_MAX_SENTENCES, max(1, int(script_min_sentences)))
    max_sentences = min(EDITORIAL_MAX_SENTENCES, max(min_sentences, int(script_max_sentences)))
    return ScriptPolicy(
        target_seconds=target_seconds,
        target_words=target_words,
        min_words=min_words,
        max_words=max_words,
        min_facts=min_facts,
        min_sentences=min_sentences,
        max_sentences=max_sentences,
    )


def _extract_source_fact_signals(*, title: str, description: str, article_url: str) -> set[str]:
    combined = f"{title} {description} {article_url}"
    raw_tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9\-_/.:%]*", combined)
    signals: set[str] = set()
    for token in raw_tokens:
        normalized = token.strip(".,;:!?()[]{}\"'").lower()
        if len(normalized) <= 2:
            continue
        if normalized in {"breaking", "news", "http", "https", "www", "com"}:
            continue
        if any(char.isdigit() for char in normalized) or "-" in normalized:
            signals.add(normalized)
            continue
        if token.isupper() and len(normalized) >= 3:
            signals.add(normalized)
            continue
        if normalized[0].isalpha() and token[:1].isupper():
            signals.add(normalized)

    cjk_runs = re.findall(
        r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]{2,}",
        f"{title} {description}",
    )
    for run in cjk_runs:
        if len(run) >= 2:
            signals.add(run)

    if len(signals) >= 4:
        return signals
    for token in raw_tokens:
        normalized = token.strip(".,;:!?()[]{}\"'").lower()
        if len(normalized) >= 4:
            signals.add(normalized)
        if len(signals) >= 8:
            break
    return signals


def _count_source_fact_hits(*, script_text: str, source_signals: set[str]) -> int:
    lowered_script = script_text.lower()
    return sum(1 for signal in source_signals if signal in lowered_script)


def _split_sentences(value: str) -> list[str]:
    normalized = _normalize_short_form_text(value)
    if not normalized:
        return []
    protected = _SENTENCE_ABBREVIATION_PATTERN.sub(lambda m: m.group(0).replace(".", "<DOT>"), normalized)
    protected = _SENTENCE_INITIALISM_PATTERN.sub(lambda m: m.group(0).replace(".", "<DOT>"), protected)
    parts = [part.strip().replace("<DOT>", ".") for part in re.split(r"[.!?。！？]+", protected) if part.strip()]
    return parts


def _script_sentence_count(script_text: str) -> int:
    return len(_split_sentences(script_text))


def _has_implication_cue(script_text: str) -> bool:
    lowered = script_text.lower()
    if any(cue in lowered for cue in IMPLICATION_CUES):
        return True
    if re.search(r"\b(?:could|may|might|can)\s+affect\b", lowered):
        return True
    if re.search(
        r"\b(?:could|may|might|can|is expected to|is likely to)\s+"
        r"(?:shape|affect|change|alter|influence|shift)\b",
        lowered,
    ):
        return True
    if re.search(r"\b(?:impact|impacts|implication|implications)\s+(?:for|on)\b", lowered):
        return True
    return False


def _has_specificity_anchor(value: str) -> bool:
    lowered = value.lower()
    return bool(
        _STAKES_NUMBER_PATTERN.search(lowered)
        or _STAKES_DEADLINE_PATTERN.search(lowered)
        or re.search(
            r"\b(?:department|agency|court|senate|congress|ceo|ministry|federal|state)\b",
            lowered,
        )
    )


def _has_consequence_anchor(value: str) -> bool:
    lowered = value.lower()
    return bool(
        re.search(
            r"\b(?:will|starting|effective|deadline|requires|blocks|allows|bans|cuts|adds|"
            r"raises|lowers|affect|changes|means)\b",
            lowered,
        )
    )


_STAKES_NUMBER_PATTERN = re.compile(r"\b\d[\d,.]*\s*(?:million|billion|trillion|percent|%|people|workers|users|jobs|dollars)?\b")
_STAKES_GROUP_PATTERN = re.compile(
    r"\b(?:americans?|residents|consumers|patients|taxpayers|workers|employees|students"
    r"|homeowners|renters|drivers|veterans|recipients|subscribers|citizens|families|voters)\b"
)
_STAKES_DEADLINE_PATTERN = re.compile(
    r"\b(?:january|february|march|april|may|june|july|august|september|october|november|december"
    r"|starting|by|before|after|effective|deadline)\s+\d"
)


def _has_stakes_marker(script_text: str) -> bool:
    lowered = script_text.lower()
    if _STAKES_NUMBER_PATTERN.search(lowered):
        return True
    if _STAKES_GROUP_PATTERN.search(lowered):
        return True
    if _STAKES_DEADLINE_PATTERN.search(lowered):
        return True
    return False


def _contains_non_neutral_language(value: str) -> bool:
    lowered = value.lower()
    return any(
        re.search(rf"\b{re.escape(phrase)}\b", lowered) is not None
        for phrase in NON_NEUTRAL_CUES
    )


def _is_script_substantive(*, script_text: str, source_signals: set[str], policy: ScriptPolicy) -> bool:
    min_words = policy.min_words
    word_count = _count_script_words(script_text)
    if word_count < min_words:
        return False
    extended_max = policy.max_words
    if word_count > extended_max:
        return False
    if script_text.rstrip()[-1:] not in ".!?。！？":
        return False
    sentence_count = _script_sentence_count(script_text)
    if sentence_count < policy.min_sentences:
        return False
    if sentence_count > policy.max_sentences:
        return False
    effective_min_facts = policy.min_facts if len(source_signals) >= 4 else max(1, policy.min_facts - 1)
    if len(source_signals) >= 6:
        effective_min_facts = max(effective_min_facts, 3)
    if _count_source_fact_hits(script_text=script_text, source_signals=source_signals) < effective_min_facts:
        return False
    has_impact = _has_implication_cue(script_text)
    if not has_impact and sentence_count < policy.min_sentences:
        return False
    if _contains_non_neutral_language(script_text):
        return False
    sentences = _split_sentences(script_text)
    lead_sentences = " ".join(sentences[:2]) if sentences else script_text
    if not _has_specificity_anchor(lead_sentences):
        LOGGER.info("Script lacks specificity anchor in lead (soft-pass; core metrics met)")
    if sentences and not _has_consequence_anchor(sentences[-1]):
        LOGGER.info("Script lacks consequence anchor in final sentence (soft-pass; core metrics met)")
    return True


def _script_validation_issues(*, script_text: str, source_signals: set[str], policy: ScriptPolicy) -> list[str]:
    issues: list[str] = []
    word_count = _count_script_words(script_text)
    min_words = policy.min_words
    if word_count < min_words:
        issues.append("script is too short and lacks enough context")
    extended_max = policy.max_words
    if word_count > extended_max:
        issues.append("script exceeds max words for short-form pacing")
    if script_text.rstrip()[-1:] not in ".!?。！？":
        issues.append("script appears to end mid-sentence — must end with a period, exclamation mark, or question mark")
    sentence_count = _script_sentence_count(script_text)
    if sentence_count < policy.min_sentences:
        issues.append("script needs at least three sentences to provide enough context")
    if sentence_count > policy.max_sentences:
        issues.append("script has too many sentences")
    effective_min_facts = policy.min_facts if len(source_signals) >= 4 else max(1, policy.min_facts - 1)
    if len(source_signals) >= 6:
        effective_min_facts = max(effective_min_facts, 3)
    fact_hits = _count_source_fact_hits(script_text=script_text, source_signals=source_signals)
    if fact_hits < effective_min_facts:
        issues.append("script does not include enough concrete facts from source context")
    has_impact = _has_implication_cue(script_text)
    if not has_impact and sentence_count < policy.min_sentences:
        issues.append("script needs a brief neutral impact line or one more concrete context sentence")
    if _contains_non_neutral_language(script_text):
        issues.append("script includes non-neutral wording")
    core_metrics_pass = not issues
    sentences = _split_sentences(script_text)
    lead_sentences = " ".join(sentences[:2]) if sentences else script_text
    if not _has_specificity_anchor(lead_sentences):
        if core_metrics_pass:
            LOGGER.info("Script lacks specificity anchor in lead (soft-warn; core metrics passed)")
        else:
            LOGGER.info("Script lacks specificity anchor in lead (soft-warn; core metrics failed)")
    if sentences and not _has_consequence_anchor(sentences[-1]):
        if core_metrics_pass:
            LOGGER.info("Script lacks consequence anchor in final sentence (soft-warn; core metrics passed)")
        else:
            LOGGER.info("Script lacks consequence anchor in final sentence (soft-warn; core metrics failed)")
    if not _has_stakes_marker(script_text):
        LOGGER.info("Script lacks specific stakes (non-blocking); repair will attempt to add them")
    return issues


def _select_fallback_impact_sentence(*, title: str, description: str, offset: int = 0) -> str:
    seed_text = f"{title} {description}".strip().lower()
    if not seed_text:
        return FALLBACK_NEUTRAL_IMPACT_TEMPLATES[0]
    index = (sum(ord(char) for char in seed_text) + max(0, offset)) % len(FALLBACK_NEUTRAL_IMPACT_TEMPLATES)
    return FALLBACK_NEUTRAL_IMPACT_TEMPLATES[index]


def _normalize_script_text(*, script_text: str, policy: ScriptPolicy) -> str:
    normalized = _normalize_short_form_text(script_text)
    normalized = _remove_disallowed_phrases(normalized)
    normalized = _remove_non_neutral_language(normalized)
    normalized = _remove_filler_phrases(normalized)
    normalized = _trim_script_words(normalized, policy.max_words)
    normalized = _clamp_text(normalized, DEFAULT_SCRIPT_MAX_CHARS)
    if normalized and normalized[-1] not in ".!?。！？":
        normalized += "。"
    return normalized


def _normalize_caption_text(value: str, *, max_len: int) -> str:
    normalized = _normalize_short_form_text(value)
    normalized = _remove_disallowed_phrases(normalized)
    normalized = _remove_non_neutral_language(normalized)
    normalized = _remove_filler_phrases(normalized)
    return _clamp_text(normalized, max_len)


def _normalize_tone(_: str) -> str:
    return DEFAULT_TONE


def _normalize_video_title(value: str, *, max_len: int = VIDEO_TITLE_MAX_CHARS) -> str:
    normalized = _strip_emojis(re.sub(r"\s+", " ", value).strip(" \t\r\n-:;,."))
    normalized = _remove_disallowed_phrases(normalized)
    normalized = _remove_non_neutral_language(normalized)
    normalized = _remove_filler_phrases(normalized)
    if not normalized:
        return ""
    if len(normalized) <= max_len:
        return normalized
    return f"{normalized[: max(0, max_len - 3)].rstrip()}..."


_EMOJI_PATTERN = re.compile(r"[\U0001F300-\U0001FAFF\u2600-\u27BF\uFE00-\uFE0F]")


def _strip_emojis(value: str) -> str:
    return _EMOJI_PATTERN.sub("", value).strip()


def _is_llm_title_wellformed(raw_title: str) -> bool:
    """Accept any non-empty title with at least 10 characters of real text.

    Titles shorter than that likely indicate a truncated or degenerate LLM
    response.  Returning False causes the caller to fall through to the
    RSS-title fallback, which is always a safe complete phrase.
    """
    cleaned = _strip_emojis(raw_title).strip()
    return len(cleaned) >= 10


_TOPIC_KEYWORDS: dict[str, list[str]] = {
    "government": [
        "國會", "參議院", "眾議院", "立法", "行政命令", "司法部", "聯邦",
        "最高法院", "州長", "總檢察長", "法規", "裁決", "政府", "白宮",
        "總統", "行政院", "立法院",
    ],
    "geopolitical": [
        "伊朗", "中國", "俄羅斯", "烏克蘭", "北約", "聯合國",
        "核武", "制裁", "飛彈", "荷莫茲海峽", "軍事", "國防部",
        "外交", "戰爭", "條約", "停火", "兩岸", "台海",
    ],
    "tech": [
        "人工智慧", "AI", "OpenAI", "Google", "蘋果", "Meta",
        "微軟", "Anthropic", "機器學習", "軟體", "演算法",
        "晶片", "半導體", "台積電", "輝達", "矽谷",
    ],
    "culture": [
        "時尚", "藝術", "音樂", "電影", "書籍", "文學",
        "奧運", "體育", "世界盃", "NBA", "MLB", "奧斯卡",
        "金馬獎", "金曲獎",
    ],
    "finance": [
        "債市", "股市", "油價", "華爾街", "聯準會",
        "利率", "國債", "經濟", "衰退", "通膨",
        "市場", "投資人", "美元", "台幣",
    ],
    "science": [
        "疫苗", "氣候", "研究", "醫學", "疾病", "NASA",
        "太空", "實驗", "科學", "新冠", "健康",
    ],
    "celebrity": [
        "川普", "拜登", "馬斯克", "黃仁勳",
        "名人", "網紅", "演藝", "明星",
    ],
}

_HOOK_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("question", re.compile(r"^.{0,40}[？?]", re.IGNORECASE)),
    ("number-lead", re.compile(r"^[\s]*[\$￥]?\d[\d,.%]*\s", re.IGNORECASE)),
    ("breaking-event", re.compile(r"剛剛|最新|速報|快訊", re.IGNORECASE)),
    ("name-drop", re.compile(r"^[\u4e00-\u9fff]{2,4}\s", re.IGNORECASE)),
    ("controversy", re.compile(r"醜聞|隱瞞|撤職|禁止|解僱|指控|起訴", re.IGNORECASE)),
]

_TITLE_FORMULA_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("question-hook", re.compile(r"[？?]", re.IGNORECASE)),
    ("danger-statement", re.compile(r"關閉|死亡|危機|恐懼|威脅|崩盤", re.IGNORECASE)),
    ("verb-subject-suspense", re.compile(r"^[\u4e00-\u9fff].*(?:竟然|居然|突然)", re.IGNORECASE)),
    ("name-drop-event", re.compile(r"^[\u4e00-\u9fff]{2,4}(?:表示|宣布|確認|報導)", re.IGNORECASE)),
    ("subject-did-thing", re.compile(r"^[\u4e00-\u9fff].*(?:宣布|證實|報導|揭露)", re.IGNORECASE)),
]


def classify_content_signals(
    *,
    script_text: str,
    video_title: str,
    description: str,
    target_seconds: int,
) -> dict[str, str]:
    combined_text = f"{script_text} {description}".lower()

    topic_scores: dict[str, int] = {}
    for category, keywords in _TOPIC_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in combined_text)
        if score > 0:
            topic_scores[category] = score
    topic_category = max(topic_scores, key=topic_scores.get, default="culture") if topic_scores else "culture"

    first_sentence = script_text.split(".")[0] if script_text else ""
    hook_type = "breaking-event"
    for hook_name, pattern in _HOOK_PATTERNS:
        if pattern.search(first_sentence):
            hook_type = hook_name
            break

    word_count = _count_script_words(script_text)
    if word_count <= 70 or target_seconds <= 25:
        length_bucket = "short"
    elif word_count <= 115 or target_seconds <= 40:
        length_bucket = "medium"
    else:
        length_bucket = "long"

    title_formula = "verb-subject-suspense"
    for formula_name, pattern in _TITLE_FORMULA_PATTERNS:
        if pattern.search(video_title):
            title_formula = formula_name
            break

    return {
        "topic_category": topic_category,
        "hook_type": hook_type,
        "length_bucket": length_bucket,
        "title_formula": title_formula,
    }


def _extract_sentences(value: str, limit: int = 3) -> list[str]:
    return _split_sentences(value)[:limit]


def _fallback_result(
    *,
    title: str,
    description: str,
    model_name: str,
    policy: ScriptPolicy,
    source_signals: set[str],
    source_name: str = "",
) -> ContentGenerationResult:
    event_statement = _normalize_short_form_text(title).strip(" .。")
    if not event_statement:
        event_statement = "一則重要新聞更新"

    desc_sentences = _extract_sentences(description, limit=max(policy.max_sentences, policy.min_sentences + 1))
    detail_sentences = [sentence.strip(" .") for sentence in desc_sentences if sentence.strip(" .")]
    detail_sentence = detail_sentences[0] if detail_sentences else event_statement
    closing_sentence = detail_sentences[1] if len(detail_sentences) >= 2 else ""

    script_parts = [event_statement]
    if detail_sentence and detail_sentence != event_statement:
        script_parts.append(detail_sentence)
    if closing_sentence and closing_sentence != detail_sentence and closing_sentence != event_statement:
        script_parts.append(closing_sentence)
    for candidate in detail_sentences[2:]:
        if len(script_parts) >= policy.min_sentences:
            break
        if candidate not in script_parts:
            script_parts.append(candidate)
    if len(script_parts) < policy.min_sentences and not _has_implication_cue(". ".join(script_parts)):
        script_parts.append(_select_fallback_impact_sentence(title=title, description=description))
    template_offset = 1
    while len(script_parts) < policy.min_sentences:
        impact_sentence = _select_fallback_impact_sentence(
            title=title,
            description=description,
            offset=template_offset,
        )
        template_offset += 1
        if impact_sentence in script_parts:
            continue
        script_parts.append(impact_sentence)
    raw_word_count = _count_script_words(" ".join(script_parts))
    while raw_word_count < policy.min_words and template_offset < len(FALLBACK_NEUTRAL_IMPACT_TEMPLATES) + 10:
        pad_sentence = _select_fallback_impact_sentence(
            title=title,
            description=description,
            offset=template_offset,
        )
        template_offset += 1
        if pad_sentence in script_parts:
            continue
        script_parts.append(pad_sentence)
        raw_word_count = _count_script_words(" ".join(script_parts))
    script_10s = _normalize_script_text(
        script_text="。".join(script_parts) + "。",
        policy=policy,
    )

    compact_description = _normalize_caption_text(detail_sentence, max_len=YOUTUBE_LIMIT)
    closing_for_caption = closing_sentence or detail_sentence
    hashtag_suffix = " ".join(DEFAULT_HASHTAGS)
    instagram_caption = _normalize_caption_text(
        f"{event_statement}。\n\n{compact_description}。{closing_for_caption}。\n\n{hashtag_suffix}",
        max_len=INSTAGRAM_LIMIT,
    )
    tiktok_caption = _normalize_caption_text(
        f"{event_statement}。{compact_description}。{closing_for_caption}。{hashtag_suffix}",
        max_len=TIKTOK_LIMIT,
    )
    youtube_caption = _normalize_caption_text(
        f"{event_statement}\n\n{compact_description}。{closing_for_caption}。",
        max_len=YOUTUBE_LIMIT,
    )
    x_caption = _smart_truncate(
        _normalize_caption_text(f"{event_statement}。{closing_for_caption}。{hashtag_suffix}", max_len=X_LIMIT),
        X_LIMIT,
    )

    instagram_caption = _append_source_attribution(instagram_caption, source_name, INSTAGRAM_LIMIT)
    tiktok_caption = _append_source_attribution(tiktok_caption, source_name, TIKTOK_LIMIT)
    youtube_caption = _append_source_attribution(youtube_caption, source_name, YOUTUBE_LIMIT)
    x_caption = _append_source_attribution(x_caption, source_name, X_LIMIT)

    return ContentGenerationResult(
        script_10s=script_10s,
        video_title_short=_normalize_video_title(title) or "新聞快報",
        caption_instagram=instagram_caption,
        caption_tiktok=tiktok_caption,
        caption_youtube=youtube_caption,
        caption_x=x_caption,
        hashtags=DEFAULT_HASHTAGS,
        tone=DEFAULT_TONE,
        language="zh-TW",
        model_name=model_name,
        prompt_version="v8",
    )


def _request_generation_payload(
    *,
    client: Anthropic,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
) -> dict[str, object] | None:
    try:
        response = client.messages.create(
            model=model_name,
            temperature=temperature,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception:
        return None

    if getattr(response, "stop_reason", None) == "max_tokens":
        return None

    content_blocks = [block.text for block in response.content if getattr(block, "type", "") == "text"]
    content = "\n".join(content_blocks).strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.DOTALL).strip()
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def validate_source_context(
    *,
    title: str,
    description: str,
    article_url: str,
    min_description_words: int = 8,
) -> list[str]:
    issues: list[str] = []
    if _count_script_words(_normalize_short_form_text(title)) < 4:
        issues.append("title lacks enough context")
    if _count_script_words(_normalize_short_form_text(description)) < max(0, int(min_description_words)):
        issues.append("description lacks enough context")
    parsed = urlparse(article_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        issues.append("article URL is invalid")
    return issues


def validate_script_for_profile(
    *,
    script_text: str,
    title: str,
    description: str,
    article_url: str,
    script_target_seconds: int = DEFAULT_SCRIPT_TARGET_SECONDS,
    script_target_words: int = DEFAULT_SCRIPT_TARGET_WORDS,
    script_max_words_buffer: int = DEFAULT_SCRIPT_MAX_WORDS_BUFFER,
    script_min_words: int = EDITORIAL_MIN_WORDS,
    script_min_facts: int = DEFAULT_SCRIPT_MIN_FACTS,
    script_min_sentences: int = DEFAULT_SCRIPT_MIN_SENTENCES,
    script_max_sentences: int = DEFAULT_SCRIPT_MAX_SENTENCES,
) -> list[str]:
    policy = _build_script_policy(
        script_target_seconds=script_target_seconds,
        script_target_words=script_target_words,
        script_max_words_buffer=script_max_words_buffer,
        script_min_words=script_min_words,
        script_min_facts=script_min_facts,
        script_min_sentences=script_min_sentences,
        script_max_sentences=script_max_sentences,
    )
    normalized_script = _normalize_script_text(script_text=script_text, policy=policy)
    source_signals = _extract_source_fact_signals(title=title, description=description, article_url=article_url)
    return _script_validation_issues(script_text=normalized_script, source_signals=source_signals, policy=policy)


def _translate_script_to_english(
    *,
    client: Anthropic,
    model_name: str,
    chinese_script: str,
) -> str:
    if not chinese_script.strip():
        return ""
    try:
        response = client.messages.create(
            model=model_name,
            temperature=0.1,
            max_tokens=1024,
            system=(
                "Translate the following Traditional Chinese news script into natural, concise English. "
                "Keep the same number of sentences and sentence order. "
                "Use fluent broadcast-style English, not word-for-word translation. "
                "Return ONLY the English translation text, nothing else."
            ),
            messages=[{"role": "user", "content": chinese_script}],
        )
        blocks = [block.text for block in response.content if getattr(block, "type", "") == "text"]
        return "\n".join(blocks).strip()
    except Exception:
        LOGGER.warning("Fallback English translation failed; captions will be Chinese-only")
        return ""


def generate_content_pack(
    *,
    api_key: str,
    model_name: str,
    title: str,
    description: str,
    article_url: str,
    source_name: str = "",
    script_target_seconds: int = DEFAULT_SCRIPT_TARGET_SECONDS,
    script_target_words: int = DEFAULT_SCRIPT_TARGET_WORDS,
    script_max_words_buffer: int = DEFAULT_SCRIPT_MAX_WORDS_BUFFER,
    script_min_words: int = EDITORIAL_MIN_WORDS,
    script_min_facts: int = DEFAULT_SCRIPT_MIN_FACTS,
    script_min_sentences: int = DEFAULT_SCRIPT_MIN_SENTENCES,
    script_max_sentences: int = DEFAULT_SCRIPT_MAX_SENTENCES,
    recent_series_tags: list[str] | None = None,
    experiment_prompt_modifier: str = "",
) -> ContentGenerationResult:
    policy = _build_script_policy(
        script_target_seconds=script_target_seconds,
        script_target_words=script_target_words,
        script_max_words_buffer=script_max_words_buffer,
        script_min_words=script_min_words,
        script_min_facts=script_min_facts,
        script_min_sentences=script_min_sentences,
        script_max_sentences=script_max_sentences,
    )
    if not source_name:
        source_name = _source_display_name(article_url)
    source_signals = _extract_source_fact_signals(title=title, description=description, article_url=article_url)
    fallback = _fallback_result(
        title=title,
        description=description,
        model_name=model_name,
        policy=policy,
        source_signals=source_signals,
        source_name=source_name,
    )
    fallback_issues = _script_validation_issues(
        script_text=fallback.script_10s,
        source_signals=source_signals,
        policy=policy,
    )
    if fallback_issues:
        LOGGER.warning("Fallback script failed profile validation issues=%s", fallback_issues)
    client = Anthropic(api_key=api_key)

    series_context = ""
    if recent_series_tags:
        tags_list = ", ".join(f'"{tag}"' for tag in recent_series_tags[:10])
        series_context = (
            f"\nRecent series tags from previous posts: [{tags_list}]. "
            "If this story continues one of these series, reuse that exact series_tag and increment series_part. "
        )

    system_prompt = (
        "你是一位專業的短影音新聞播報撰稿人，為TikTok等短影音平台撰寫約35秒的繁體中文直式影片腳本。"
        "僅回傳有效JSON，包含以下欄位：script_10s, script_10s_en, video_title_short, caption_instagram, "
        "caption_tiktok, caption_youtube, caption_x, hashtags, tone, language, series_tag, series_part。\n\n"

        "=== 全域規則 ===\n"
        "只使用提示中提供的事實。不得推測、評論、說服或選邊站。"
        "不使用煽動性形容詞、誇大用語、標題黨或修辭性問句。"
        "語氣：專業權威但平易近人——像一位見多識廣的內行人在向同事簡報，而非照稿唸。"
        "用說故事的方式傳達，像告訴朋友一件緊急的事。"
        "使用現在式和主動語態。語氣自信、直接、零廢話。\n"
        "所有腳本和字幕必須使用繁體中文（Traditional Chinese）。\n\n"

        "=== 腳本（script_10s）===\n"
        f"約{policy.target_seconds}秒的繁體中文口語播報"
        f"（約{policy.target_words}字，最少{policy.min_words}字，最多{policy.max_words}字）。"
        f"使用{policy.min_sentences}到{policy.max_sentences}個句子。\n\n"
        "留住觀眾原則——每個句子必須製造張力或解決張力。"
        "沒有任何一句話應該只是鋪墊；每個句子本身就必須傳遞價值。"
        "每個句子都要獨立傳遞價值。"
        "每個句子的轉場都要製造微型懸念，讓下一句話變得不可錯過。"
        "如果刪掉某句話不會損失關鍵事實，就刪掉它。\n\n"
        "結構（三段式：鉤子／簡單解說／翻轉）：\n"
        "目標：讓觀眾看完後震驚。\n\n"
        "1）鉤子（0-5秒，1-2句，約15字）："
        "故事中最吸引眼球的事實。"
        "TikTok觀眾在1-2秒內決定是否繼續看——鉤子必須只靠聲音就能抓住注意力。"
        "前3個字就要製造即時好奇或震撼。"
        "必須在前5個字內包含具體數字、金額、日期或具名實體。"
        "輪替使用以下鉤子公式：\n"
        "  - 數據炸彈：用意想不到的數字開頭。例：「零元。這就是蘋果十年來在歐盟繳的稅。」\n"
        "  - 身分鉤子：直接點名觀眾族群。例：「如果你用Gmail，這件事你必須知道。」\n"
        "  - 利害升級：一開始就說什麼在危險中。例：「你的菜價即將漲23%。」\n"
        "  - 權威投彈：說出一個有權勢的實體做了意想不到的事。例：「美國國防部剛封殺了自家的AI承包商。」\n"
        "  - 對比反差：用承諾vs現實製造張力。例：「OpenAI承諾安全AI，結果他們簽了五角大廈合約。」\n\n"
        "2）簡單解說（5-30秒，3-5句，約85字）："
        "清楚解釋事情經過，讓觀眾完全理解發生了什麼以及為什麼重要。"
        "用像跟聰明朋友喝咖啡聊天的方式寫——平易近人但不幼稚化。"
        "每句話都必須帶來收穫，而不是背景鋪墊。"
        "逐步建立理解：發生了什麼、為什麼、利害關係是什麼。"
        "任何數字都要用快速比較讓觀眾秒懂。"
        "每個句子的轉場都要製造微型懸念，讓下一句話變得不可錯過。"
        "這段是整支影片的骨幹——如果解說無聊，觀眾就滑走了。\n\n"
        "3）翻轉／亮點（30-35秒，1-2句，約15字）："
        "用翻轉、揭露或重新框架作結，讓觀眾驚訝。"
        "這個時刻要讓觀眾想重看或分享。"
        "技巧：揭露意想不到的後果、翻轉常見假設、挖出來源中的隱藏細節、"
        "點名具體受影響對象並說出觀眾沒想到的後果。"
        "使用具體資訊——誰、什麼、什麼時候。"
        "用最強的事實作結。\n\n"
        "不好的結尾：「這可能改變整個產業。」\n"
        "好的結尾：「四千兩百萬健保受益人七月一日起，自付額將調漲。」\n"
        "不好的結尾：「機器人產業即將變得更競爭。」\n"
        "好的結尾：「兩年後，幫你打包網購訂單的機器人，上面可能印著蘋果的商標。」\n"
        "結尾必須包含具體的翻轉或後果，帶有行動/時間語言。\n"
        "避免廢話、開場套路和公式化導語如「這很重要因為」。\n\n"

        f"至少包含{policy.min_facts}個來源中的具體事實。\n\n"

        "=== 英文翻譯（script_10s_en）===\n"
        "提供script_10s的自然、簡潔英文翻譯。"
        "翻譯必須逐句對應中文腳本——中文有幾個句子，英文就有幾個句子，順序一致。"
        "用流暢的新聞英語，不要逐字直譯。\n\n"

        "=== 影片標題（video_title_short）===\n"
        "前8個字放最具體的事實、名稱或數字。"
        "最多40個字。不要用表情符號。不用煽動性詞語。\n\n"

        "=== 系列 ===\n"
        "series_tag：如果是持續追蹤的故事，回傳小寫英文代號如 'iran-conflict'。獨立故事 = null。"
        "series_part：如果有series_tag，回傳整數期數，否則null。\n\n"

        "=== 來源標註 ===\n"
        "不要在字幕中包含來源標註，系統會自動附加。\n\n"

        "=== 平台字幕 ===\n"
        "所有字幕必須保持中立且基於事實，使用繁體中文。\n\n"
        "caption_instagram（最多2200字元）：2-3個短段落加上結尾的hashtag行。\n\n"
        "caption_tiktok（最多2200字元）：一個簡潔段落加上hashtag。\n\n"
        "caption_youtube（最多5000字元）：2個包含搜尋關鍵字的短段落。不含hashtag。\n\n"
        "caption_x（最多250字元）：一句簡潔事實陳述。不含hashtag。\n\n"

        "=== HASHTAGS ===\n"
        "hashtags：3到7個相關的繁體中文hashtag的JSON陣列"
        "（例：[\"#國際新聞\", \"#時事\", \"#科技\"]）。"
        "language欄位請填入 \"zh-TW\"。"
    )
    if experiment_prompt_modifier:
        system_prompt = f"{system_prompt}\n\n{experiment_prompt_modifier}"
    source_line = f"來源：{source_name}\n" if source_name else ""
    user_prompt = (
        f"標題：{title}\n"
        f"摘要：{description}\n"
        f"文章連結：{article_url}\n"
        f"{source_line}"
        f"{series_context}"
        "請為Instagram、TikTok、YouTube Shorts和X（Twitter）撰寫各平台的繁體中文字幕。"
        "腳本和字幕必須基於事實、易於在短影音情境中快速瀏覽。"
    )
    payload = _request_generation_payload(
        client=client,
        model_name=model_name,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.2,
    )
    if payload is None:
        LOGGER.warning("LLM returned no payload for title=%r — using fallback script", title)
        return fallback

    candidate_script = _normalize_script_text(script_text=str(payload.get("script_10s", "")).strip(), policy=policy)
    LOGGER.info("LLM script candidate (%s words): %s", _count_script_words(candidate_script), candidate_script)
    repair_temperatures = [0.2, 0.1]
    prior_candidates: list[str] = [candidate_script]
    for repair_attempt, repair_temp in enumerate(repair_temperatures, start=1):
        if _is_script_substantive(script_text=candidate_script, source_signals=source_signals, policy=policy):
            break
        issues = _script_validation_issues(script_text=candidate_script, source_signals=source_signals, policy=policy)
        max_similarity = max(
            (
                difflib.SequenceMatcher(None, previous, candidate_script).ratio()
                for previous in prior_candidates
            ),
            default=0.0,
        )
        if max_similarity >= 0.92:
            issues.append("rewrite script structure and wording; current draft is too similar to prior attempt")
        LOGGER.info("Script not substantive (attempt %s/%s), issues=%s — sending repair prompt", repair_attempt, len(repair_temperatures), issues)
        repair_prompt = (
            f"{user_prompt}\n"
            f"Current script candidate: {candidate_script}\n"
            f"Fix these issues: {', '.join(issues) or 'insufficient substance'}.\n"
            "Do not paraphrase the same structure. Rewrite with a different sentence structure while preserving facts.\n"
            "Return full JSON again and keep the same keys."
        )
        repair_payload = _request_generation_payload(
            client=client,
            model_name=model_name,
            system_prompt=system_prompt,
            user_prompt=repair_prompt,
            temperature=repair_temp,
        )
        if repair_payload is not None:
            payload = repair_payload
            candidate_script = _normalize_script_text(script_text=str(payload.get("script_10s", "")).strip(), policy=policy)
            prior_candidates.append(candidate_script)
            LOGGER.info("Repair script candidate attempt %s (%s words): %s", repair_attempt, _count_script_words(candidate_script), candidate_script)
    if not _is_script_substantive(script_text=candidate_script, source_signals=source_signals, policy=policy):
        LOGGER.warning("Script still not substantive after %s repairs — falling back to title+description script", len(repair_temperatures))
        candidate_script = fallback.script_10s

    hashtags = payload.get("hashtags")
    if not isinstance(hashtags, list):
        hashtags = []
    normalized_hashtags = _normalize_hashtags(hashtags)

    caption_instagram = _normalize_caption_text(
        str(payload.get("caption_instagram", "")).strip(),
        max_len=INSTAGRAM_LIMIT,
    )
    caption_tiktok = _normalize_caption_text(
        str(payload.get("caption_tiktok", "")).strip(),
        max_len=TIKTOK_LIMIT,
    )
    caption_youtube = _normalize_caption_text(
        str(payload.get("caption_youtube", "")).strip(),
        max_len=YOUTUBE_LIMIT,
    )
    caption_x = _smart_truncate(
        _normalize_caption_text(str(payload.get("caption_x", "")).strip(), max_len=X_LIMIT),
        X_LIMIT,
    )

    caption_instagram = _ensure_hashtags_in_caption(caption_instagram, normalized_hashtags, INSTAGRAM_LIMIT)
    caption_tiktok = _ensure_hashtags_in_caption(caption_tiktok, normalized_hashtags, TIKTOK_LIMIT)
    caption_x = _ensure_hashtags_in_caption(caption_x, normalized_hashtags, X_LIMIT)

    caption_instagram = _append_source_attribution(caption_instagram, source_name, INSTAGRAM_LIMIT)
    caption_tiktok = _append_source_attribution(caption_tiktok, source_name, TIKTOK_LIMIT)
    caption_youtube = _append_source_attribution(caption_youtube, source_name, YOUTUBE_LIMIT)
    caption_x = _append_source_attribution(caption_x, source_name, X_LIMIT)

    raw_series_tag = payload.get("series_tag")
    series_tag = str(raw_series_tag).strip().lower() if raw_series_tag and str(raw_series_tag).strip() != "null" else None
    raw_series_part = payload.get("series_part")
    series_part: int | None = None
    if raw_series_part is not None and str(raw_series_part).strip() != "null":
        try:
            series_part = int(raw_series_part)
        except (TypeError, ValueError):
            pass

    raw_llm_title = str(payload.get("video_title_short", "")).strip()
    llm_title = _normalize_video_title(raw_llm_title) if _is_llm_title_wellformed(raw_llm_title) else ""
    if llm_title and not _is_llm_title_wellformed(llm_title):
        llm_title = ""
    if llm_title and _contains_non_neutral_language(llm_title):
        llm_title = ""

    script_10s_en = str(payload.get("script_10s_en", "")).strip()

    if caption_instagram and _contains_non_neutral_language(caption_instagram):
        caption_instagram = ""
    if caption_tiktok and _contains_non_neutral_language(caption_tiktok):
        caption_tiktok = ""
    if caption_youtube and _contains_non_neutral_language(caption_youtube):
        caption_youtube = ""
    if caption_x and _contains_non_neutral_language(caption_x):
        caption_x = ""

    final_script = candidate_script or fallback.script_10s
    final_title = llm_title or _normalize_video_title(title) or fallback.video_title_short
    script_source = "llm" if candidate_script and candidate_script != fallback.script_10s else "fallback"
    LOGGER.info("Final script path=%s title=%r words=%s", script_source, final_title, _count_script_words(final_script))

    if not script_10s_en:
        script_10s_en = _translate_script_to_english(
            client=client,
            model_name=model_name,
            chinese_script=final_script,
        )

    signals = classify_content_signals(
        script_text=final_script,
        video_title=final_title,
        description=description,
        target_seconds=script_target_seconds,
    )

    return ContentGenerationResult(
        script_10s=final_script,
        video_title_short=final_title,
        caption_instagram=caption_instagram or fallback.caption_instagram,
        caption_tiktok=caption_tiktok or fallback.caption_tiktok,
        caption_youtube=caption_youtube or fallback.caption_youtube,
        caption_x=caption_x or fallback.caption_x,
        hashtags=normalized_hashtags,
        tone=_normalize_tone(str(payload.get("tone", DEFAULT_TONE)).strip()),
        language=str(payload.get("language", "zh-TW")).strip() or "zh-TW",
        model_name=model_name,
        prompt_version="v8",
        series_tag=series_tag,
        series_part=series_part,
        content_signals=signals,
        script_10s_en=script_10s_en,
    )
