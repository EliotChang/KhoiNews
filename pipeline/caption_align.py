from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any

import jieba


LOGGER = logging.getLogger("wj_caption_align")
_LEADING_PUNCTUATION_RE = re.compile(r"^[^\w\u4e00-\u9fff\u3400-\u4dbf']+")
_TRAILING_PUNCTUATION_RE = re.compile(r"[^\w\u4e00-\u9fff\u3400-\u4dbf']+$")
_CJK_CHAR_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]")
_CJK_PUNCTUATION_RE = re.compile(r"[，。、；：！？「」『』（）【】《》\u3000]")


@dataclass(frozen=True)
class AlignedWord:
    text: str
    start_sec: float
    end_sec: float


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_cjk_dominant(text: str) -> bool:
    if not text:
        return False
    cjk_count = len(_CJK_CHAR_RE.findall(text))
    return cjk_count > 3


def _extract_character_timing(alignment_payload: dict[str, Any]) -> tuple[list[str], list[float | None], list[float | None]]:
    characters = alignment_payload.get("characters")
    starts = alignment_payload.get("character_start_times_seconds")
    ends = alignment_payload.get("character_end_times_seconds")
    if not isinstance(characters, list):
        return [], [], []
    if not isinstance(starts, list) or not isinstance(ends, list):
        return [], [], []
    if len(characters) != len(starts) or len(characters) != len(ends):
        return [], [], []
    normalized_characters = [str(char) for char in characters]
    normalized_starts = [_to_float(value) for value in starts]
    normalized_ends = [_to_float(value) for value in ends]
    return normalized_characters, normalized_starts, normalized_ends


def _normalize_token(raw_token: str) -> str:
    if not raw_token:
        return ""
    normalized = raw_token.strip().lower()
    normalized = normalized.replace("\u2019", "'").replace("\u2018", "'")
    normalized = _LEADING_PUNCTUATION_RE.sub("", normalized)
    normalized = _TRAILING_PUNCTUATION_RE.sub("", normalized)
    return normalized


def _build_alignment_tokens(
    *,
    characters: list[str],
    starts: list[float | None],
    ends: list[float | None],
) -> list[AlignedWord]:
    if not characters:
        return []
    alignment_tokens: list[AlignedWord] = []
    pending_text: list[str] = []
    pending_starts: list[float] = []
    pending_ends: list[float] = []

    def _flush_pending() -> None:
        if pending_text and pending_starts and pending_ends:
            token_start = min(pending_starts)
            token_end = max(pending_ends)
            if token_end > token_start:
                alignment_tokens.append(
                    AlignedWord(
                        text="".join(pending_text),
                        start_sec=token_start,
                        end_sec=token_end,
                    )
                )

    for idx, character in enumerate(characters):
        start_sec = starts[idx] if idx < len(starts) else None
        end_sec = ends[idx] if idx < len(ends) else None

        if character.isspace():
            _flush_pending()
            pending_text = []
            pending_starts = []
            pending_ends = []
            continue

        is_cjk = bool(_CJK_CHAR_RE.match(character))
        is_cjk_punct = bool(_CJK_PUNCTUATION_RE.match(character))

        if is_cjk and not is_cjk_punct:
            _flush_pending()
            pending_text = []
            pending_starts = []
            pending_ends = []
            if start_sec is not None and end_sec is not None and end_sec > start_sec:
                alignment_tokens.append(
                    AlignedWord(text=character, start_sec=start_sec, end_sec=end_sec)
                )
            continue

        if is_cjk_punct:
            _flush_pending()
            pending_text = []
            pending_starts = []
            pending_ends = []
            continue

        pending_text.append(character)
        if start_sec is not None:
            pending_starts.append(start_sec)
        if end_sec is not None:
            pending_ends.append(end_sec)

    _flush_pending()
    return alignment_tokens


def _segment_chinese_text(text: str) -> list[str]:
    words = list(jieba.cut(text, cut_all=False))
    result: list[str] = []
    for word in words:
        stripped = word.strip()
        if not stripped:
            continue
        if _CJK_PUNCTUATION_RE.fullmatch(stripped):
            continue
        if re.fullmatch(r"[，。、；：！？\s]+", stripped):
            continue
        result.append(stripped)
    return result


def _build_word_alignments_cjk(
    *,
    script_text: str,
    alignment_tokens: list[AlignedWord],
    min_coverage_ratio: float,
) -> list[AlignedWord]:
    script_words = _segment_chinese_text(script_text)
    if not script_words:
        return []
    if not alignment_tokens:
        return []

    char_timing: list[tuple[str, float, float]] = []
    for token in alignment_tokens:
        if len(token.text) == 1 and _CJK_CHAR_RE.match(token.text):
            char_timing.append((token.text, token.start_sec, token.end_sec))
        else:
            for i, ch in enumerate(token.text):
                frac = i / max(1, len(token.text))
                duration = token.end_sec - token.start_sec
                ch_start = token.start_sec + frac * duration
                ch_end = token.start_sec + (frac + 1.0 / max(1, len(token.text))) * duration
                char_timing.append((ch, ch_start, ch_end))

    matched: list[AlignedWord] = []
    char_idx = 0

    for word in script_words:
        word_chars = [ch for ch in word if _CJK_CHAR_RE.match(ch)]
        if not word_chars:
            latin_normalized = _normalize_token(word)
            found = False
            for search_idx in range(char_idx, min(char_idx + 10, len(char_timing))):
                ct_text = char_timing[search_idx][0]
                if not _CJK_CHAR_RE.match(ct_text):
                    candidate_text_parts = []
                    candidate_start = char_timing[search_idx][1]
                    candidate_end = char_timing[search_idx][2]
                    for j in range(search_idx, min(search_idx + len(word) + 2, len(char_timing))):
                        if _CJK_CHAR_RE.match(char_timing[j][0]):
                            break
                        candidate_text_parts.append(char_timing[j][0])
                        candidate_end = char_timing[j][2]
                        joined = "".join(candidate_text_parts)
                        if _normalize_token(joined) == latin_normalized:
                            matched.append(AlignedWord(text=word, start_sec=candidate_start, end_sec=candidate_end))
                            char_idx = j + 1
                            found = True
                            break
                    if found:
                        break
            continue

        word_start: float | None = None
        word_end: float | None = None
        chars_matched = 0

        search_start = char_idx
        for ci, target_char in enumerate(word_chars):
            for search_idx in range(search_start, min(search_start + 5, len(char_timing))):
                if char_timing[search_idx][0] == target_char:
                    if word_start is None:
                        word_start = char_timing[search_idx][1]
                    word_end = char_timing[search_idx][2]
                    chars_matched += 1
                    search_start = search_idx + 1
                    break

        if word_start is not None and word_end is not None and chars_matched >= max(1, len(word_chars) // 2):
            matched.append(AlignedWord(text=word, start_sec=word_start, end_sec=word_end))
            char_idx = search_start

    script_total = len(script_words)
    matched_coverage = len(matched) / script_total if script_total > 0 else 0.0
    coverage_threshold = max(0.0, min(1.0, min_coverage_ratio))
    if matched_coverage < coverage_threshold:
        LOGGER.warning(
            "CJK alignment coverage %.2f below threshold %.2f (%s/%s words matched)",
            matched_coverage,
            coverage_threshold,
            len(matched),
            script_total,
        )
        return []
    return matched


def _build_word_alignments_latin(
    *,
    script_text: str,
    alignment_tokens: list[AlignedWord],
    min_coverage_ratio: float,
) -> list[AlignedWord]:
    script_words = [match.group(0) for match in re.finditer(r"\S+", script_text)]
    if not script_words:
        return []
    if not alignment_tokens:
        return []

    script_normalized = [_normalize_token(word) for word in script_words]
    alignment_normalized = [_normalize_token(token.text) for token in alignment_tokens]
    script_total = sum(1 for token in script_normalized if token)
    if script_total == 0:
        return []

    matched: list[AlignedWord] = []
    script_index = 0
    align_index = 0
    max_align_lookahead = 5
    max_script_lookahead = 2

    while script_index < len(script_words) and align_index < len(alignment_tokens):
        script_token = script_normalized[script_index]
        align_token = alignment_normalized[align_index]
        if not script_token:
            script_index += 1
            continue
        if not align_token:
            align_index += 1
            continue
        if script_token == align_token:
            source = alignment_tokens[align_index]
            matched.append(
                AlignedWord(
                    text=script_words[script_index],
                    start_sec=source.start_sec,
                    end_sec=source.end_sec,
                )
            )
            script_index += 1
            align_index += 1
            continue

        next_align_match = -1
        for offset in range(1, max_align_lookahead + 1):
            candidate_index = align_index + offset
            if candidate_index >= len(alignment_tokens):
                break
            if alignment_normalized[candidate_index] == script_token:
                next_align_match = candidate_index
                break
        if next_align_match >= 0:
            align_index = next_align_match
            continue

        next_script_match = -1
        for offset in range(1, max_script_lookahead + 1):
            candidate_index = script_index + offset
            if candidate_index >= len(script_words):
                break
            if script_normalized[candidate_index] == align_token:
                next_script_match = candidate_index
                break
        if next_script_match >= 0:
            script_index = next_script_match
            continue

        script_index += 1
        align_index += 1

    matched_coverage = len(matched) / script_total
    coverage_threshold = max(0.0, min(1.0, min_coverage_ratio))
    if matched_coverage < coverage_threshold:
        LOGGER.warning(
            "Alignment coverage %.2f below threshold %.2f (%s/%s words matched)",
            matched_coverage,
            coverage_threshold,
            len(matched),
            script_total,
        )
        return []
    return matched


def _segment_words_into_cues(
    *,
    aligned_words: list[AlignedWord],
    pause_gap_seconds: float,
    max_cue_duration_seconds: float,
    max_words_per_cue: int,
) -> list[list[AlignedWord]]:
    if not aligned_words:
        return []
    safe_pause_gap = max(0.05, pause_gap_seconds)
    safe_max_cue_duration = max(0.4, max_cue_duration_seconds)
    safe_max_words = max(1, max_words_per_cue)
    sorted_words = sorted(aligned_words, key=lambda word: word.start_sec)
    cues: list[list[AlignedWord]] = []
    current_words: list[AlignedWord] = []

    for word in sorted_words:
        if not current_words:
            current_words.append(word)
            continue

        last_word = current_words[-1]
        gap_seconds = max(0.0, word.start_sec - last_word.end_sec)
        cue_duration = max(0.0, word.end_sec - current_words[0].start_sec)
        exceeds_word_limit = len(current_words) >= safe_max_words
        exceeds_duration_limit = cue_duration > safe_max_cue_duration
        has_pause_boundary = gap_seconds >= safe_pause_gap

        if has_pause_boundary or exceeds_word_limit or exceeds_duration_limit:
            cues.append(current_words)
            current_words = [word]
            continue
        current_words.append(word)

    if current_words:
        cues.append(current_words)
    return cues


def _join_cue_text(words: list[AlignedWord], is_cjk: bool) -> str:
    if is_cjk:
        return "".join(word.text for word in words)
    return " ".join(word.text for word in words)


def _distribute_english_across_cues(
    *,
    english_text: str,
    chinese_cues: list[dict[str, float | str]],
    max_en_words_per_cue: int = 8,
) -> list[dict[str, float | str]]:
    if not english_text.strip() or not chinese_cues:
        return chinese_cues

    en_words = english_text.split()
    if not en_words:
        return chinese_cues

    num_cues = len(chinese_cues)
    total_en_words = len(en_words)
    safe_max = max(1, max_en_words_per_cue)
    min_chunk = 2

    target_chunks = max(1, min(num_cues, total_en_words // min_chunk))
    words_per_chunk = total_en_words / target_chunks

    chunk_assignments: list[tuple[int, int]] = []
    word_offset = 0
    for chunk_idx in range(target_chunks):
        if chunk_idx == target_chunks - 1:
            size = total_en_words - word_offset
        else:
            size = min(safe_max, max(min_chunk, round(words_per_chunk)))
            size = min(size, total_en_words - word_offset)
        chunk_assignments.append((word_offset, size))
        word_offset += size

    cue_en: list[str] = [""] * num_cues
    for ci in range(len(chunk_assignments)):
        if len(chunk_assignments) > 1:
            cue_idx = round(ci * (num_cues - 1) / (len(chunk_assignments) - 1))
        else:
            cue_idx = 0
        cue_idx = max(0, min(cue_idx, num_cues - 1))
        start, size = chunk_assignments[ci]
        cue_en[cue_idx] = " ".join(en_words[start:start + size])

    enriched: list[dict[str, float | str]] = []
    for cue_idx, cue in enumerate(chinese_cues):
        new_cue = dict(cue)
        new_cue["textEn"] = cue_en[cue_idx]
        enriched.append(new_cue)
    return enriched

_ENGLISH_RUN_RE = re.compile(r"[（(][A-Za-z][A-Za-z\s\-''.]*[)）]")
_LATIN_WORD_RE = re.compile(r"\b[A-Za-z]{2,}(?:\s+[A-Za-z]{2,})*\b")


def _extract_cjk_only_text(text: str) -> str:
    """Strip English parentheticals and long Latin runs, keeping CJK + numbers + punctuation."""
    cleaned = _ENGLISH_RUN_RE.sub("", text)
    cleaned = _LATIN_WORD_RE.sub("", cleaned)
    cleaned = re.sub(r"\s+", "", cleaned)
    return cleaned


def build_aligned_caption_cues(
    *,
    script_text: str,
    alignment_payload: dict[str, Any],
    intro_duration_seconds: float,
    words_per_line: int,
    max_duration_seconds: float,
    pause_gap_seconds: float = 0.42,
    max_cue_duration_seconds: float = 2.8,
    min_cue_duration_seconds: float = 0.35,
    max_words_per_cue: int | None = None,
    min_alignment_coverage: float = 0.6,
    english_text: str = "",
    max_en_words_per_cue: int = 8,
) -> list[dict[str, float | str]]:
    characters, starts, ends = _extract_character_timing(alignment_payload)
    alignment_tokens = _build_alignment_tokens(
        characters=characters,
        starts=starts,
        ends=ends,
    )

    is_cjk = _is_cjk_dominant(script_text)

    if is_cjk:
        aligned_words = _build_word_alignments_cjk(
            script_text=script_text,
            alignment_tokens=alignment_tokens,
            min_coverage_ratio=min_alignment_coverage,
        )
        if not aligned_words:
            cjk_only_text = _extract_cjk_only_text(script_text)
            if cjk_only_text and cjk_only_text != script_text:
                LOGGER.info(
                    "Retrying CJK alignment with English stripped (%d -> %d chars)",
                    len(script_text),
                    len(cjk_only_text),
                )
                relaxed_coverage = max(0.30, min_alignment_coverage * 0.5)
                aligned_words = _build_word_alignments_cjk(
                    script_text=cjk_only_text,
                    alignment_tokens=alignment_tokens,
                    min_coverage_ratio=relaxed_coverage,
                )
    else:
        aligned_words = _build_word_alignments_latin(
            script_text=script_text,
            alignment_tokens=alignment_tokens,
            min_coverage_ratio=min_alignment_coverage,
        )

    if not aligned_words:
        return []

    safe_words_per_line = max(1, words_per_line)
    if is_cjk:
        safe_words_per_line = min(safe_words_per_line, 4)
    safe_max_words = max_words_per_cue if max_words_per_cue is not None else safe_words_per_line
    if is_cjk:
        safe_max_words = min(safe_max_words, 6)

    word_groups = _segment_words_into_cues(
        aligned_words=aligned_words,
        pause_gap_seconds=pause_gap_seconds,
        max_cue_duration_seconds=max_cue_duration_seconds,
        max_words_per_cue=safe_max_words,
    )
    if not word_groups:
        return []

    safe_min_cue_duration = max(0.1, min_cue_duration_seconds)
    safe_max_duration = max(0.2, max_duration_seconds)
    safe_intro_offset = max(0.0, min(intro_duration_seconds, safe_max_duration))
    cues: list[dict[str, float | str]] = []
    previous_end = 0.0
    tiny_guard = 0.001

    for phrase_words in word_groups:
        if not phrase_words:
            continue
        cue_start = max(0.0, safe_intro_offset + phrase_words[0].start_sec)
        cue_end = min(safe_max_duration, safe_intro_offset + phrase_words[-1].end_sec)
        cue_start = max(cue_start, previous_end + tiny_guard)
        if cue_end <= cue_start:
            cue_end = min(safe_max_duration, cue_start + safe_min_cue_duration)
        if (cue_end - cue_start) < safe_min_cue_duration:
            cue_end = min(safe_max_duration, cue_start + safe_min_cue_duration)
        if cue_end <= cue_start:
            continue
        cues.append(
            {
                "startSec": round(cue_start, 3),
                "endSec": round(cue_end, 3),
                "text": _join_cue_text(phrase_words, is_cjk),
            }
        )
        previous_end = cue_end

    if english_text:
        cues = _distribute_english_across_cues(
            english_text=english_text,
            chinese_cues=cues,
            max_en_words_per_cue=max_en_words_per_cue,
        )
    return cues
