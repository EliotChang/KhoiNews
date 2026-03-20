from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import feedparser
import requests

from pipeline.text_sanitize import strip_urls_from_text
from pipeline.wj_ingest import IngestResult, SourcePostInput


def _source_key_from_url(feed_url: str) -> str:
    parsed = urlparse(feed_url)
    host = (parsed.netloc or "feed").lower()
    path = parsed.path.strip("/").lower()
    combined = f"{host}-{path}" if path else host
    normalized = re.sub(r"[^a-z0-9]+", "_", combined).strip("_")
    return normalized or "feed"


def _canonical_link(link: str) -> str:
    parsed = urlparse(link.strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    normalized = f"{scheme}://{netloc}{path}"
    if parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def _clean_description(raw_description: str) -> str:
    if not raw_description:
        return ""
    text = BeautifulSoup(raw_description, "html.parser").get_text(" ", strip=True)
    return strip_urls_from_text(" ".join(text.split()))


def _parse_published_at(entry: dict[str, Any]) -> datetime | None:
    published = entry.get("published") or entry.get("updated")
    if not published:
        return None
    try:
        parsed = parsedate_to_datetime(published)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_feed_entries(
    *,
    entries: list[dict[str, Any]],
    max_posts: int,
    source: str,
    feed_url: str,
    ingest_source: str,
) -> list[SourcePostInput]:
    normalized_posts: list[SourcePostInput] = []
    for entry in entries[:max_posts]:
        source_guid = str(entry.get("id") or entry.get("guid") or entry.get("link") or "").strip()
        title = str(entry.get("title") or "").strip()
        raw_description = str(entry.get("summary") or entry.get("description") or "").strip()
        description = _clean_description(raw_description)
        link = str(entry.get("link") or "").strip()
        if not source_guid or not title or not link:
            continue
        normalized_posts.append(
            SourcePostInput(
                source=source,
                source_guid=source_guid,
                title=title,
                description=description,
                link=link,
                published_at=_parse_published_at(entry),
                raw_payload={
                    "source": source,
                    "ingest_source": ingest_source,
                    "feed_url": feed_url,
                    "entry": dict(entry),
                },
            )
        )
    return normalized_posts


def fetch_fallback_feed_posts(*, rss_urls: list[str], timeout_seconds: int, max_posts: int) -> IngestResult:
    if not rss_urls or max_posts <= 0:
        return IngestResult(source="trusted_fallback_feed", posts=[])

    deduped_posts: list[SourcePostInput] = []
    seen_links: set[str] = set()
    seen_source_guids: set[tuple[str, str]] = set()

    for feed_url in rss_urls:
        if len(deduped_posts) >= max_posts:
            break
        try:
            response = requests.get(feed_url, timeout=timeout_seconds)
            response.raise_for_status()
        except requests.RequestException:
            continue

        source_key = _source_key_from_url(feed_url)
        source_name = f"fallback_{source_key}"
        parsed = feedparser.parse(response.text)
        entries: list[dict[str, Any]] = parsed.get("entries", [])
        feed_posts = _parse_feed_entries(
            entries=entries,
            max_posts=max_posts,
            source=source_name,
            feed_url=feed_url,
            ingest_source="trusted_fallback_feed",
        )
        for post in feed_posts:
            if len(deduped_posts) >= max_posts:
                break
            canonical = _canonical_link(post.link)
            source_guid_key = (post.source, post.source_guid)
            if canonical in seen_links or source_guid_key in seen_source_guids:
                continue
            seen_links.add(canonical)
            seen_source_guids.add(source_guid_key)
            deduped_posts.append(post)

    return IngestResult(source="trusted_fallback_feed", posts=deduped_posts)
