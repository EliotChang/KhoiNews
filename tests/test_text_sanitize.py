from __future__ import annotations

import unittest

from pipeline.text_sanitize import strip_urls_from_text


class TextSanitizeTests(unittest.TestCase):
    def test_preserves_us_abbreviation_and_decimal_values(self) -> None:
        raw = "Iran rejected U.S. demands and unemployment rose to 4.4%."
        self.assertEqual(strip_urls_from_text(raw), raw)

    def test_removes_literal_urls_and_domains(self) -> None:
        raw = "Track updates at https://example.com/live and docs.example.net/reference now."
        cleaned = strip_urls_from_text(raw)
        self.assertNotIn("https://", cleaned.lower())
        self.assertNotIn("example.com", cleaned.lower())
        self.assertNotIn("example.net", cleaned.lower())

    def test_removes_www_urls(self) -> None:
        raw = "Live stream is at www.example.org/watch tonight."
        cleaned = strip_urls_from_text(raw)
        self.assertNotIn("www.", cleaned.lower())
        self.assertNotIn("example.org", cleaned.lower())

    def test_url_only_input_never_collapses_to_single_slash(self) -> None:
        self.assertEqual(strip_urls_from_text("https://example.com/"), "")
        self.assertEqual(strip_urls_from_text("/"), "")


if __name__ == "__main__":
    unittest.main()
