from __future__ import annotations

import unittest
from unittest.mock import patch

import requests

from pipeline.article_media import _infer_media_type, extract_article_context, extract_best_media_from_article


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200

    def raise_for_status(self) -> None:
        return


class ArticleMediaTests(unittest.TestCase):
    def test_infer_media_type_does_not_misclassify_webmanifest_as_video(self) -> None:
        self.assertIsNone(_infer_media_type("https://static.files.bbci.co.uk/site.webmanifest"))

    def test_infer_media_type_supports_video_and_image_extensions(self) -> None:
        self.assertEqual(_infer_media_type("https://cdn.example.com/clip.mp4?token=abc"), "video")
        self.assertEqual(_infer_media_type("https://cdn.example.com/photo.webp?token=abc"), "image")

    @patch("pipeline.article_media.requests.get")
    def test_extract_best_media_ignores_manifest_link_hrefs(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            """
            <html><head>
            <meta property="og:image" content="https://cdn.example.com/story.jpg" />
            <link rel="manifest" href="https://cdn.example.com/site.webmanifest" />
            </head><body></body></html>
            """
        )

        result = extract_best_media_from_article(
            page_url="https://example.com/story",
            timeout_seconds=5,
            rss_entry_payload=None,
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.media_type, "image")
        self.assertEqual(result.media_url, "https://cdn.example.com/story.jpg")
        self.assertTrue(all(candidate.media_url != "https://cdn.example.com/site.webmanifest" for candidate in result.media_candidates))

    @patch("pipeline.article_media.requests.get")
    def test_extract_article_context_prefers_og_description(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            """
            <html><head>
            <meta property="og:description" content="OG context should win over everything else." />
            <meta name="twitter:description" content="Twitter description." />
            <meta name="description" content="Meta description." />
            </head>
            <body><p>Paragraph content with enough words to be considered meaningful.</p></body></html>
            """
        )

        context = extract_article_context(page_url="https://example.com/story", timeout_seconds=5)
        self.assertEqual(context, "OG context should win over everything else.")

    @patch("pipeline.article_media.requests.get")
    def test_extract_article_context_sanitizes_urls_and_whitespace(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            """
            <html><head>
            <meta property="og:description" content="  This   update references https://example.com/source and keeps context.  " />
            </head></html>
            """
        )

        context = extract_article_context(page_url="https://example.com/story", timeout_seconds=5)
        self.assertEqual(context, "This update references and keeps context.")

    @patch("pipeline.article_media.requests.get")
    def test_extract_article_context_uses_first_meaningful_paragraph_when_meta_missing(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            """
            <html><body>
            <p>Short line.</p>
            <p>This paragraph has enough context to pass the enrichment threshold check safely.</p>
            </body></html>
            """
        )

        context = extract_article_context(page_url="https://example.com/story", timeout_seconds=5)
        self.assertEqual(
            context,
            "This paragraph has enough context to pass the enrichment threshold check safely.",
        )

    @patch("pipeline.article_media.requests.get")
    def test_extract_article_context_aggregates_structured_article_paragraphs(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            """
            <html><body>
              <article>
                <p>Officials confirmed the timeline and agency scope after a multi-hour briefing in the capital.</p>
                <p>The policy package includes phased enforcement milestones and formal compliance reporting windows.</p>
                <p>Industry groups said implementation details will affect procurement and staffing plans next quarter.</p>
              </article>
            </body></html>
            """
        )

        context = extract_article_context(page_url="https://example.com/story", timeout_seconds=5)
        self.assertIn("Officials confirmed the timeline", context)
        self.assertIn("compliance reporting windows", context)
        self.assertGreaterEqual(len(context.split()), 30)

    @patch("pipeline.article_media.requests.get")
    def test_extract_article_context_prefers_structured_text_when_metadata_is_thin(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            """
            <html><head>
              <meta property="og:description" content="Short metadata blurb." />
            </head><body>
              <main>
                <p>Authorities released a detailed timeline covering inspections, deadlines, and reporting obligations for affected operators.</p>
                <p>The notice named multiple agencies and outlined phased compliance dates through the next quarter.</p>
                <p>Officials also published implementation notes describing escalation paths, auditing checkpoints, and formal reporting templates for compliance teams.</p>
              </main>
            </body></html>
            """
        )

        context = extract_article_context(page_url="https://example.com/story", timeout_seconds=5)
        self.assertNotEqual(context, "Short metadata blurb.")
        self.assertIn("detailed timeline", context)

    @patch("pipeline.article_media.requests.get")
    def test_extract_article_context_filters_boilerplate_paragraphs(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            """
            <html><body>
              <main>
                <p>Subscribe to our newsletter for updates and privacy policy notices.</p>
                <p>Regulators published a new enforcement calendar with specific deadlines for affected firms.</p>
              </main>
            </body></html>
            """
        )

        context = extract_article_context(page_url="https://example.com/story", timeout_seconds=5)
        self.assertNotIn("Subscribe to our newsletter", context)
        self.assertIn("Regulators published a new enforcement calendar", context)

    @patch("pipeline.article_media.requests.get")
    def test_extract_article_context_returns_empty_string_when_request_fails(self, mock_get) -> None:
        mock_get.side_effect = requests.RequestException("timeout")

        context = extract_article_context(page_url="https://example.com/story", timeout_seconds=5)
        self.assertEqual(context, "")


if __name__ == "__main__":
    unittest.main()
