from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


REVIEW_STATUS_PENDING = "pending"
REVIEW_STATUS_APPROVED = "approved"
REVIEW_STATUS_REJECTED = "rejected"
REVIEW_STATUS_REGENERATING = "regenerating"
REVIEW_STATUSES = {
    REVIEW_STATUS_PENDING,
    REVIEW_STATUS_APPROVED,
    REVIEW_STATUS_REJECTED,
    REVIEW_STATUS_REGENERATING,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_review_status(raw_status: str) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized in REVIEW_STATUSES:
        return normalized
    return REVIEW_STATUS_PENDING


def review_defaults() -> dict[str, Any]:
    return {
        "approval_status": REVIEW_STATUS_PENDING,
        "approval_by": "",
        "approval_at": "",
        "review_channel_id": "",
        "review_message_id": "",
        "review_thread_id": "",
        "edit_notes": "",
        "regeneration_count": 0,
    }


def review_patch_for_approval(*, actor: str, approved: bool, now_iso: str | None = None) -> dict[str, Any]:
    return {
        "approval_status": REVIEW_STATUS_APPROVED if approved else REVIEW_STATUS_REJECTED,
        "approval_by": actor.strip(),
        "approval_at": now_iso or utc_now_iso(),
    }


def review_patch_for_regeneration_start(*, actor: str, edit_notes: str, now_iso: str | None = None) -> dict[str, Any]:
    return {
        "approval_status": REVIEW_STATUS_REGENERATING,
        "approval_by": actor.strip(),
        "approval_at": now_iso or utc_now_iso(),
        "edit_notes": edit_notes.strip(),
    }


def review_patch_after_regeneration(
    *,
    previous_payload: dict[str, Any],
    actor: str,
    edit_notes: str,
    now_iso: str | None = None,
) -> dict[str, Any]:
    current_count = previous_payload.get("regeneration_count")
    try:
        regen_count = int(current_count)
    except (TypeError, ValueError):
        regen_count = 0
    patch = review_defaults()
    patch["regeneration_count"] = regen_count + 1
    patch["edit_notes"] = edit_notes.strip()
    patch["approval_by"] = actor.strip()
    patch["approval_at"] = now_iso or utc_now_iso()
    return patch


def review_status_from_payload(payload: dict[str, Any]) -> str:
    return normalize_review_status(str(payload.get("approval_status", "")))
