from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests

from pipeline.text_sanitize import strip_urls_from_text


LOGGER = logging.getLogger("wj_ingest")

WJ_BASE_URL = "https://www.worldjournal.com"

DEFAULT_WJ_CATEGORY_PATHS = [
    "/wj/cate/breaking",
    "/wj/cate/breaking/121006",
    "/wj/cate/breaking/121103",
    "/wj/cate/breaking/121099",
    "/wj/cate/breaking/121102",
    "/wj/cate/breaking/121010",
    "/wj/cate/breaking/121098",
]

_WJ_STORY_URL_RE = re.compile(r"/wj/story/\d+/\d+")

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}


class WJFeedUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourcePostInput:
    source: str
    source_guid: str
    title: str
    description: str
    link: str
    published_at: datetime | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class IngestResult:
    source: str
    posts: list[SourcePostInput]


def _parse_wj_timestamp(raw_timestamp: str) -> datetime | None:
    if not raw_timestamp:
        return None
    cleaned = raw_timestamp.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _clean_description(raw_text: str) -> str:
    if not raw_text:
        return ""
    text = BeautifulSoup(raw_text, "html.parser").get_text(" ", strip=True)
    return strip_urls_from_text(" ".join(text.split()))


def _extract_source_guid_from_url(url: str) -> str:
    match = _WJ_STORY_URL_RE.search(url)
    if match:
        return match.group(0)
    return url


def _scrape_listing_page(
    *,
    url: str,
    timeout_seconds: int,
) -> list[dict[str, str]]:
    try:
        response = requests.get(url, headers=_DEFAULT_HEADERS, timeout=timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("Failed to fetch WJ listing page %s: %s", url, exc)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    articles: list[dict[str, str]] = []

    for link_tag in soup.find_all("a", href=_WJ_STORY_URL_RE):
        href = link_tag.get("href", "").strip()
        if not href:
            continue

        full_url = urljoin(WJ_BASE_URL, href)

        title_tag = link_tag.find(["h2", "h3", "h4"])
        title = ""
        if title_tag:
            title = title_tag.get_text(strip=True)
        if not title:
            title = link_tag.get_text(strip=True)

        if not title or len(title) < 4:
            continue

        description = ""
        desc_candidates = link_tag.find_all("p")
        if not desc_candidates:
            parent = link_tag.parent
            if parent:
                desc_candidates = parent.find_all("p")
        for p_tag in desc_candidates:
            text = p_tag.get_text(strip=True)
            if len(text) > len(description):
                description = text

        raw_timestamp = ""
        time_tag = link_tag.find("time")
        if not time_tag:
            parent = link_tag.parent
            if parent:
                time_tag = parent.find("time")
        if time_tag:
            raw_timestamp = time_tag.get("datetime", "") or time_tag.get_text(strip=True)

        if not raw_timestamp:
            sibling_text = link_tag.find_next(string=re.compile(r"\d{4}-\d{2}-\d{2}"))
            if sibling_text:
                raw_timestamp = sibling_text.strip()

        articles.append({
            "url": full_url,
            "title": title,
            "description": description,
            "timestamp": raw_timestamp,
        })

    return articles


def fetch_wj_posts(
    *,
    base_url: str,
    category_paths: list[str],
    timeout_seconds: int,
    max_posts: int,
    request_delay_seconds: float = 1.0,
) -> IngestResult:
    if not base_url.strip():
        raise WJFeedUnavailableError("WJ_BASE_URL is missing")

    all_articles: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for path in category_paths:
        if len(all_articles) >= max_posts:
            break

        page_url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
        LOGGER.info("Scraping WJ category page: %s", page_url)

        articles = _scrape_listing_page(url=page_url, timeout_seconds=timeout_seconds)

        for article in articles:
            article_url = article["url"]
            if article_url in seen_urls:
                continue
            seen_urls.add(article_url)
            all_articles.append(article)

        if len(category_paths) > 1:
            time.sleep(request_delay_seconds)

    posts: list[SourcePostInput] = []
    for article in all_articles[:max_posts]:
        source_guid = _extract_source_guid_from_url(article["url"])
        title = article["title"].strip()
        description = _clean_description(article.get("description", ""))
        link = article["url"]
        published_at = _parse_wj_timestamp(article.get("timestamp", ""))

        if not title or not link:
            continue

        posts.append(
            SourcePostInput(
                source="world_journal",
                source_guid=source_guid,
                title=title,
                description=description,
                link=link,
                published_at=published_at,
                raw_payload={
                    "source": "world_journal",
                    "ingest_source": "wj_scraper",
                    "category_url": article.get("category_url", ""),
                    "raw_timestamp": article.get("timestamp", ""),
                },
            )
        )

    LOGGER.info("WJ ingest complete: %d posts from %d category pages", len(posts), len(category_paths))
    return IngestResult(source="wj_scraper", posts=posts)
