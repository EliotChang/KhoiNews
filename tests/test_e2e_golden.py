"""Golden contract checks for deterministic single-workflow news profile."""

from __future__ import annotations

import unittest

from pipeline.content_gen import (
    EDITORIAL_MAX_WORDS,
    EDITORIAL_MIN_WORDS,
    INSTAGRAM_LIMIT,
    TIKTOK_LIMIT,
    YOUTUBE_LIMIT,
    _extract_sentences,
    validate_script_for_profile,
)


VALID_SCRIPT = (
    "The U.S. Commerce Department announced a new export-control update affecting advanced chip-equipment shipments to three countries, with enforcement starting April 1, 2026. "
    "Officials said the rule names five entities, sets a 30-day licensing review window, and requires updated compliance attestations from U.S. suppliers. "
    "Industry groups said affected firms have paused selected contracts while legal teams review the new definitions and filing requirements in the notice. "
    "The change could affect procurement timelines and quarterly planning for manufacturers that depend on cross-border component approvals."
)


class GoldenProfileTests(unittest.TestCase):
    def test_valid_script_meets_profile_bounds(self) -> None:
        words = len(VALID_SCRIPT.split())
        sentences = _extract_sentences(VALID_SCRIPT, limit=10)

        self.assertGreaterEqual(words, EDITORIAL_MIN_WORDS)
        self.assertLessEqual(words, EDITORIAL_MAX_WORDS)
        self.assertGreaterEqual(len(sentences), 4)
        self.assertLessEqual(len(sentences), 5)

        issues = validate_script_for_profile(
            script_text=VALID_SCRIPT,
            title="Commerce Department issues updated export controls",
            description="The update names five entities and changes licensing review windows effective April 1, 2026.",
            article_url="https://example.com/export-controls",
        )
        self.assertEqual(issues, [])

    def test_short_script_is_rejected(self) -> None:
        issues = validate_script_for_profile(
            script_text="Officials announced a change. Details are pending.",
            title="Officials announced a change",
            description="More details are expected soon.",
            article_url="https://example.com/update",
        )
        self.assertTrue(issues)

    def test_platform_caption_limits(self) -> None:
        self.assertGreater(INSTAGRAM_LIMIT, 0)
        self.assertGreater(TIKTOK_LIMIT, 0)
        self.assertGreater(YOUTUBE_LIMIT, 0)


if __name__ == "__main__":
    unittest.main()
