from __future__ import annotations

import unittest

from pipeline.caption_align import build_aligned_caption_cues


def _build_alignment_payload(*, text: str, char_step: float = 0.05, pauses_after_words: dict[int, float] | None = None) -> dict[str, object]:
    characters: list[str] = []
    starts: list[float] = []
    ends: list[float] = []
    pause_map = pauses_after_words or {}
    current_time = 0.0
    words = text.split(" ")

    for word_index, word in enumerate(words):
        for character in word:
            start_sec = current_time
            end_sec = start_sec + char_step
            characters.append(character)
            starts.append(round(start_sec, 4))
            ends.append(round(end_sec, 4))
            current_time = end_sec
        if word_index < (len(words) - 1):
            pause = max(0.0, pause_map.get(word_index, 0.0))
            start_sec = current_time + pause
            end_sec = start_sec + char_step
            characters.append(" ")
            starts.append(round(start_sec, 4))
            ends.append(round(end_sec, 4))
            current_time = end_sec

    return {
        "characters": characters,
        "character_start_times_seconds": starts,
        "character_end_times_seconds": ends,
    }


class CaptionAlignTests(unittest.TestCase):
    def test_handles_punctuation_normalization_and_keeps_bounds(self) -> None:
        script_text = "OpenAI's newest model, is live!"
        payload = _build_alignment_payload(text="OpenAI’s newest model is live")

        cues = build_aligned_caption_cues(
            script_text=script_text,
            alignment_payload=payload,
            intro_duration_seconds=2.0,
            words_per_line=6,
            max_duration_seconds=20.0,
            min_alignment_coverage=0.6,
        )

        self.assertGreaterEqual(len(cues), 1)
        first_cue = cues[0]
        self.assertGreaterEqual(float(first_cue["startSec"]), 2.0)
        self.assertLessEqual(float(first_cue["endSec"]), 20.0)

    def test_splits_cues_on_pause_gap(self) -> None:
        payload = _build_alignment_payload(
            text="one two three four five six",
            pauses_after_words={2: 0.8},
        )

        cues = build_aligned_caption_cues(
            script_text="one two three four five six",
            alignment_payload=payload,
            intro_duration_seconds=1.5,
            words_per_line=10,
            max_duration_seconds=30.0,
            pause_gap_seconds=0.5,
            max_words_per_cue=10,
        )

        self.assertGreaterEqual(len(cues), 2)
        self.assertIn("one", str(cues[0]["text"]))
        self.assertIn("four", str(cues[1]["text"]))

    def test_returns_empty_when_alignment_coverage_too_low(self) -> None:
        payload = _build_alignment_payload(text="completely different words")

        cues = build_aligned_caption_cues(
            script_text="alpha beta gamma delta epsilon",
            alignment_payload=payload,
            intro_duration_seconds=0.0,
            words_per_line=3,
            max_duration_seconds=10.0,
            min_alignment_coverage=0.9,
        )

        self.assertEqual(cues, [])

    def test_cues_are_monotonic_non_overlapping_and_within_duration(self) -> None:
        payload = _build_alignment_payload(text="a b c d e f g h i j", char_step=0.02)

        cues = build_aligned_caption_cues(
            script_text="a b c d e f g h i j",
            alignment_payload=payload,
            intro_duration_seconds=2.0,
            words_per_line=2,
            max_duration_seconds=4.0,
            max_cue_duration_seconds=0.15,
            min_cue_duration_seconds=0.1,
            max_words_per_cue=2,
        )

        self.assertGreaterEqual(len(cues), 2)
        previous_end = 0.0
        for cue in cues:
            start_sec = float(cue["startSec"])
            end_sec = float(cue["endSec"])
            self.assertGreaterEqual(start_sec, previous_end)
            self.assertGreater(end_sec, start_sec)
            self.assertLessEqual(end_sec, 4.0)
            previous_end = end_sec


class EnglishDistributionTests(unittest.TestCase):
    def _make_chinese_cues(self, texts: list[str]) -> list[dict[str, float | str]]:
        cues: list[dict[str, float | str]] = []
        t = 0.0
        for text in texts:
            cues.append({"startSec": round(t, 3), "endSec": round(t + 1.0, 3), "text": text})
            t += 1.0
        return cues

    def test_all_english_words_are_distributed(self) -> None:
        from pipeline.caption_align import _distribute_english_across_cues

        chinese_cues = self._make_chinese_cues(["美國", "國防部", "宣布了", "新政策", "影響數百萬"])
        english_text = "The Pentagon announced a new policy affecting millions of residents across the country."
        result = _distribute_english_across_cues(english_text=english_text, chinese_cues=chinese_cues)

        all_en_words = " ".join(str(cue.get("textEn", "")) for cue in result).split()
        self.assertEqual(all_en_words, english_text.split())

    def test_no_cue_exceeds_max_en_words(self) -> None:
        from pipeline.caption_align import _distribute_english_across_cues

        chinese_cues = self._make_chinese_cues(["短", "非常長的中文文本在這裡"])
        english_text = "One two three four five six seven eight nine ten eleven twelve"
        result = _distribute_english_across_cues(
            english_text=english_text, chinese_cues=chinese_cues, max_en_words_per_cue=6,
        )

        for cue in result:
            en = str(cue.get("textEn", ""))
            if en:
                self.assertLessEqual(len(en.split()), 6)

    def test_every_cue_gets_english_words(self) -> None:
        from pipeline.caption_align import _distribute_english_across_cues

        chinese_cues = self._make_chinese_cues(["美國", "國防部", "宣布", "新政策", "影響"])
        english_text = "The Pentagon announced a new policy affecting millions of residents across the country."
        result = _distribute_english_across_cues(english_text=english_text, chinese_cues=chinese_cues)

        for cue in result:
            self.assertTrue(str(cue.get("textEn", "")).strip(), f"Cue {cue['text']} has empty English")

    def test_empty_english_returns_cues_unchanged(self) -> None:
        from pipeline.caption_align import _distribute_english_across_cues

        chinese_cues = self._make_chinese_cues(["美國", "國防部"])
        result = _distribute_english_across_cues(english_text="", chinese_cues=chinese_cues)

        self.assertEqual(result, chinese_cues)

    def test_single_cue_gets_all_english(self) -> None:
        from pipeline.caption_align import _distribute_english_across_cues

        chinese_cues = self._make_chinese_cues(["美國國防部宣布新政策"])
        english_text = "The Pentagon announced a new policy."
        result = _distribute_english_across_cues(english_text=english_text, chinese_cues=chinese_cues)

        self.assertEqual(str(result[0]["textEn"]), english_text)


if __name__ == "__main__":
    unittest.main()
