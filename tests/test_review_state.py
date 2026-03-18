from __future__ import annotations

import unittest

from pipeline.review_state import (
    REVIEW_STATUS_APPROVED,
    REVIEW_STATUS_PENDING,
    REVIEW_STATUS_REGENERATING,
    normalize_review_status,
    review_defaults,
    review_patch_after_regeneration,
    review_patch_for_approval,
    review_patch_for_regeneration_start,
)


class ReviewStateTests(unittest.TestCase):
    def test_review_defaults_include_required_fields(self) -> None:
        defaults = review_defaults()
        self.assertEqual(defaults["approval_status"], REVIEW_STATUS_PENDING)
        self.assertEqual(defaults["approval_by"], "")
        self.assertEqual(defaults["approval_at"], "")
        self.assertEqual(defaults["review_channel_id"], "")
        self.assertEqual(defaults["review_message_id"], "")
        self.assertEqual(defaults["review_thread_id"], "")
        self.assertEqual(defaults["edit_notes"], "")
        self.assertEqual(defaults["regeneration_count"], 0)

    def test_review_patch_for_approval_sets_actor_and_status(self) -> None:
        patch = review_patch_for_approval(actor="eliot (123)", approved=True, now_iso="2026-03-06T18:00:00Z")
        self.assertEqual(patch["approval_status"], REVIEW_STATUS_APPROVED)
        self.assertEqual(patch["approval_by"], "eliot (123)")
        self.assertEqual(patch["approval_at"], "2026-03-06T18:00:00Z")

    def test_review_patch_for_regeneration_start_sets_status_and_notes(self) -> None:
        patch = review_patch_for_regeneration_start(
            actor="eliot (123)",
            edit_notes="Tighten the hook",
            now_iso="2026-03-06T18:00:00Z",
        )
        self.assertEqual(patch["approval_status"], REVIEW_STATUS_REGENERATING)
        self.assertEqual(patch["edit_notes"], "Tighten the hook")

    def test_review_patch_after_regeneration_resets_refs_and_increments_counter(self) -> None:
        patch = review_patch_after_regeneration(
            previous_payload={
                "regeneration_count": 2,
                "review_channel_id": "1",
                "review_message_id": "2",
                "review_thread_id": "3",
            },
            actor="eliot (123)",
            edit_notes="Cut line 3",
            now_iso="2026-03-06T18:00:00Z",
        )
        self.assertEqual(patch["approval_status"], REVIEW_STATUS_PENDING)
        self.assertEqual(patch["regeneration_count"], 3)
        self.assertEqual(patch["edit_notes"], "Cut line 3")
        self.assertEqual(patch["review_channel_id"], "")
        self.assertEqual(patch["review_message_id"], "")
        self.assertEqual(patch["review_thread_id"], "")

    def test_normalize_review_status_defaults_to_pending(self) -> None:
        self.assertEqual(normalize_review_status("approved"), REVIEW_STATUS_APPROVED)
        self.assertEqual(normalize_review_status("invalid-status"), REVIEW_STATUS_PENDING)


if __name__ == "__main__":
    unittest.main()
