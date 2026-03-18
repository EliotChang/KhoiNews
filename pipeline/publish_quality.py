from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


_CJK_CHAR_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]")

_SPECIFICITY_PATTERN = re.compile(
    r"\b(?:\d[\d,.%]*|jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|"
    r"department|agency|court|senate|house|congress|federal|state|ceo|minister|"
    r"company|university|institute|foundation|commission|council|"
    r"president|director|founder|chairman|secretary|governor|mayor)\b",
    re.IGNORECASE,
)
_CJK_SPECIFICITY_PATTERN = re.compile(
    r"(?:\d[\d,.%]*"
    r"|[一二三四五六七八九十百千萬億兆]\d*"
    r"|總統|主席|部長|院長|總理|市長|省長|局長|署長|委員|議員"
    r"|法院|國會|參議院|眾議院|政府|聯邦|白宮|五角大樓"
    r"|公司|集團|大學|研究院|基金會|委員會|協會|組織"
    r"|執行長|董事長|創辦人|總裁|秘書長|發言人|專家|教授"
    r"|月|日|年|時|分|號|億|萬|美元|元|台幣|人民幣|%)"
)
_QUOTED_SPEECH_PATTERN = re.compile(r'["\u201c\u300c].{4,}?["\u201d\u300d]')
_PROPER_NOUN_PATTERN = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b")
_CONSEQUENCE_PATTERN = re.compile(
    r"\b(?:will|starting|effective|deadline|affect|changes|means|requires|blocks|"
    r"allows|bans|cuts|adds|raises|lowers)\b",
    re.IGNORECASE,
)
_CJK_CONSEQUENCE_PATTERN = re.compile(
    r"(?:將會|將|即將|預計|可能|恐|勢必|必須|導致|造成|影響"
    r"|改變|要求|禁止|削減|增加|降低|提高|允許|開始|生效"
    r"|截止|面臨|衝擊|引發|促使|迫使|帶來|意味著)"
)


@dataclass(frozen=True)
class PublishQualityScores:
    script_specificity: float
    narrative_flow: float
    visual_relevance: float
    visual_variety: float
    first_two_seconds_hook: float
    composite: float
    failing_dimensions: list[str]


def _cjk_word_count(text: str) -> int:
    cjk_chars = len(_CJK_CHAR_RE.findall(text))
    non_cjk = _CJK_CHAR_RE.sub("", text).strip()
    latin_words = len(non_cjk.split()) if non_cjk else 0
    return cjk_chars + latin_words


def _split_sentences(value: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", value).strip()
    if not normalized:
        return []
    return [part.strip() for part in re.split(r"(?<=[.!?。！？])\s*", normalized) if part.strip()]


def _tokenize(value: str) -> set[str]:
    tokens = {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9\-]{2,}", value.lower())
        if token not in {"breaking", "news", "reported"}
    }
    cjk_runs = re.findall(r"[\u4e00-\u9fff\u3400-\u4dbf]{2,}", value)
    tokens.update(cjk_runs)
    return tokens


def _has_specificity(text: str) -> bool:
    return bool(_SPECIFICITY_PATTERN.search(text) or _CJK_SPECIFICITY_PATTERN.search(text))


def _has_consequence(text: str) -> bool:
    return bool(_CONSEQUENCE_PATTERN.search(text) or _CJK_CONSEQUENCE_PATTERN.search(text))


def _score_script_specificity(*, script_text: str) -> float:
    word_count = _cjk_word_count(script_text)
    if word_count == 0:
        return 0.0
    specificity_hits = len(_SPECIFICITY_PATTERN.findall(script_text))
    specificity_hits += len(_CJK_SPECIFICITY_PATTERN.findall(script_text))
    specificity_hits += len(_PROPER_NOUN_PATTERN.findall(script_text))
    specificity_hits += len(_QUOTED_SPEECH_PATTERN.findall(script_text))
    density = min(1.0, specificity_hits / max(2.0, word_count / 22.0))
    return round(max(0.0, min(1.0, density)), 3)


def _score_narrative_flow(*, script_text: str) -> float:
    sentences = _split_sentences(script_text)
    if len(sentences) < 3:
        return 0.2
    lead_specific = _has_specificity(" ".join(sentences[:2]))
    ending_has_consequence = _has_consequence(sentences[-1])
    sentence_count_score = 1.0 if 4 <= len(sentences) <= 5 else 0.75 if len(sentences) == 3 else 0.4
    flow = sentence_count_score
    if lead_specific:
        flow += 0.15
    if ending_has_consequence:
        flow += 0.15
    return round(max(0.0, min(1.0, flow)), 3)


def _score_visual_relevance(
    *,
    title: str,
    description: str,
    media_candidates: list[dict[str, Any]],
) -> float:
    if not media_candidates:
        return 0.0
    source_tokens = _tokenize(f"{title} {description}")
    if not source_tokens:
        return 0.5
    relevances: list[float] = []
    for candidate in media_candidates:
        media_url = str(candidate.get("media_url") or "").lower()
        reason = str(candidate.get("selection_reason") or "").lower()
        candidate_tokens = _tokenize(f"{media_url} {reason}")
        if not candidate_tokens:
            relevances.append(0.4)
            continue
        overlap = len(source_tokens & candidate_tokens)
        relevances.append(min(1.0, overlap / 3.0))
    if not relevances:
        return 0.0
    return round(sum(relevances) / len(relevances), 3)


def _score_visual_variety(*, media_candidates: list[dict[str, Any]]) -> float:
    if not media_candidates:
        return 0.0
    image_count = sum(1 for candidate in media_candidates if str(candidate.get("media_type")) == "image")
    has_video = any(str(candidate.get("media_type")) == "video" for candidate in media_candidates)
    if has_video and image_count >= 2:
        return 1.0
    if has_video:
        return 0.85
    if image_count >= 5:
        return 1.0
    if image_count >= 3:
        return 0.8
    if image_count == 2:
        return 0.55
    return 0.25


_NUMBER_LEAD_PATTERN = re.compile(r"^\s*\$?\d", re.IGNORECASE)
_ACTIVE_VERB_PATTERN = re.compile(
    r"\b(?:is|are|says|bans|cuts|fires|drops|hits|launches|unveils|signs|blocks|orders|announces)\b",
    re.IGNORECASE,
)
_CJK_ACTIVE_VERB_PATTERN = re.compile(
    r"(?:宣布|表示|指出|批評|警告|透露|呼籲|證實|否認|下令|簽署|推出|揭露|公布|決定)"
)


def _score_first_two_seconds_hook(*, script_text: str, media_candidates: list[dict[str, Any]]) -> float:
    sentences = _split_sentences(script_text)
    if not sentences:
        return 0.0
    lead = sentences[0]
    lead_has_specificity = _has_specificity(lead)
    lead_word_count = _cjk_word_count(lead)
    image_count = sum(1 for candidate in media_candidates if str(candidate.get("media_type")) == "image")
    hook = 0.25
    if lead_has_specificity:
        hook += 0.35
    if 5 <= lead_word_count <= 20:
        hook += 0.25
    elif lead_word_count <= 30:
        hook += 0.1
    if _NUMBER_LEAD_PATTERN.search(lead):
        hook += 0.15
    if _ACTIVE_VERB_PATTERN.search(lead) or _CJK_ACTIVE_VERB_PATTERN.search(lead):
        hook += 0.1
    if image_count >= 3:
        hook += 0.15
    return round(max(0.0, min(1.0, hook)), 3)


def evaluate_publish_quality(
    *,
    title: str,
    description: str,
    script_text: str,
    media_candidates: list[dict[str, Any]],
    score_threshold: float,
    per_dimension_min: float,
) -> PublishQualityScores:
    script_specificity = _score_script_specificity(script_text=script_text)
    narrative_flow = _score_narrative_flow(script_text=script_text)
    visual_relevance = _score_visual_relevance(
        title=title,
        description=description,
        media_candidates=media_candidates,
    )
    visual_variety = _score_visual_variety(media_candidates=media_candidates)
    first_two_seconds_hook = _score_first_two_seconds_hook(
        script_text=script_text,
        media_candidates=media_candidates,
    )
    composite = round(
        (
            (script_specificity * 0.24)
            + (narrative_flow * 0.24)
            + (visual_relevance * 0.2)
            + (visual_variety * 0.18)
            + (first_two_seconds_hook * 0.14)
        ),
        3,
    )
    dimension_scores = {
        "ScriptSpecificityScore": script_specificity,
        "NarrativeFlowScore": narrative_flow,
        "VisualRelevanceScore": visual_relevance,
        "VisualVarietyScore": visual_variety,
        "First2sHookScore": first_two_seconds_hook,
        "CompositeScore": composite,
    }
    _SOFT_WARN_DIMENSIONS = {"VisualRelevanceScore", "VisualVarietyScore", "ScriptSpecificityScore", "First2sHookScore"}
    failing_dimensions = [
        metric_name
        for metric_name, metric_value in dimension_scores.items()
        if metric_name not in _SOFT_WARN_DIMENSIONS
        and (
            (
                metric_name == "CompositeScore"
                and metric_value < max(0.0, min(1.0, score_threshold))
            ) or (
                metric_name != "CompositeScore"
                and metric_value < max(0.0, min(1.0, per_dimension_min))
            )
        )
    ]
    return PublishQualityScores(
        script_specificity=script_specificity,
        narrative_flow=narrative_flow,
        visual_relevance=visual_relevance,
        visual_variety=visual_variety,
        first_two_seconds_hook=first_two_seconds_hook,
        composite=composite,
        failing_dimensions=failing_dimensions,
    )
