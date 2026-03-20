from __future__ import annotations

import unittest
from unittest.mock import patch

from pipeline.content_gen import (
    DEFAULT_SCRIPT_MAX_WORDS_BUFFER,
    DEFAULT_SCRIPT_TARGET_WORDS,
    _build_script_policy,
    _extract_sentences,
    generate_content_pack,
    validate_script_for_profile,
)


class _FailingMessageAPI:
    def create(self, **_: object) -> object:
        raise RuntimeError("synthetic llm error")


class _FailingAnthropicClient:
    def __init__(self) -> None:
        self.messages = _FailingMessageAPI()


class ContentGenerationTests(unittest.TestCase):
    def test_script_policy_bounds_match_short_editorial_profile(self) -> None:
        policy = _build_script_policy(
            script_target_seconds=5,
            script_target_words=20,
            script_max_words_buffer=100,
            script_min_words=70,
            script_min_facts=1,
            script_min_sentences=5,
            script_max_sentences=8,
        )

        self.assertEqual(policy.target_seconds, 28)
        self.assertEqual(policy.target_words, 70)
        self.assertEqual(policy.max_words, 170)

    def test_sentence_splitter_preserves_abbreviations(self) -> None:
        text = "U.S. officials met U.K. ministers today. Markets reacted after the briefing."
        sentences = _extract_sentences(text, limit=5)

        self.assertEqual(len(sentences), 2)
        self.assertIn("U.S.", sentences[0])
        self.assertIn("U.K.", sentences[0])

    def test_validate_script_profile_rejects_short_script(self) -> None:
        issues = validate_script_for_profile(
            script_text="Too short.",
            title="Global central bank signals policy shift",
            description="Officials said inflation expectations changed and rate path guidance was updated.",
            article_url="https://example.com/story",
        )
        self.assertTrue(issues)
        self.assertTrue(any("too short" in issue for issue in issues))

    def test_fallback_output_is_still_validated_with_same_profile(self) -> None:
        with patch("pipeline.content_gen.AnthropicBedrock", return_value=_FailingAnthropicClient()):
            result = generate_content_pack(
                aws_access_key_id="fake-key",
                aws_secret_access_key="fake-secret",
                aws_region="us-east-1",
                model_name="fake-model",
                title="Funding round closes",
                description="Series B closes after strong enterprise adoption.",
                article_url="https://example.com/funding",
                script_target_words=DEFAULT_SCRIPT_TARGET_WORDS,
                script_max_words_buffer=DEFAULT_SCRIPT_MAX_WORDS_BUFFER,
            )

        issues = validate_script_for_profile(
            script_text=result.script_10s,
            title="Funding round closes",
            description="Series B closes after strong enterprise adoption.",
            article_url="https://example.com/funding",
        )
        self.assertTrue(issues)


if __name__ == "__main__":
    unittest.main()
