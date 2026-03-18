from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import requests
from pipeline.text_sanitize import strip_urls_from_text


LOGGER = logging.getLogger("wj_media")

VIDEO_EXTENSIONS = (".mp4", ".webm", ".mov")
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")

_LOW_VALUE_URL_PATTERN = re.compile(
    r"(?:^|[/._\-])(?:logo|icon|avatar|favicon|sprite|placeholder)(?:[/._?#\-]|$)",
    re.IGNORECASE,
)


def is_low_value_image_url(url: str) -> bool:
    return bool(_LOW_VALUE_URL_PATTERN.search(url))

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

_CONTEXT_BOILERPLATE_ATTR_PATTERN = re.compile(
    r"(?:^|[\s_\-])(nav|menu|footer|header|related|recommend|newsletter|subscribe|cookie|comment|share|social|promo|advert)(?:$|[\s_\-])",
    re.IGNORECASE,
)
_CONTEXT_BOILERPLATE_TEXT_PATTERN = re.compile(
    r"(subscribe|sign up|cookie|privacy policy|terms of use|all rights reserved"
    r"|訂閱|隱私權|版權所有|廣告|登入|註冊|更多新聞|工商信息)",
    re.IGNORECASE,
)
_CJK_RANGE_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]")
_CONTEXT_MIN_PARAGRAPH_WORDS = 6
_CONTEXT_MIN_PARAGRAPH_CJK_CHARS = 10
_STRUCTURED_CONTEXT_PREFERRED_MIN_WORDS = 40
_STRUCTURED_CONTEXT_PREFERRED_MIN_CJK_CHARS = 60
_DEFAULT_ARTICLE_CONTEXT_MAX_WORDS = 220
_DEFAULT_ARTICLE_CONTEXT_MAX_CJK_CHARS = 600
_MAX_CONTEXT_PARAGRAPHS = 12


def _normalize_context_text(value: str) -> str:
    normalized = strip_urls_from_text(value or "")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _count_cjk_chars(value: str) -> int:
    return len(_CJK_RANGE_RE.findall(value))


def _is_cjk_dominant(value: str) -> bool:
    if not value:
        return False
    return _count_cjk_chars(value) > len(value.split()) * 0.5


def _truncate_to_max_words(value: str, max_words: int) -> str:
    if _is_cjk_dominant(value):
        max_chars = max(60, max_words * 3)
        if len(value) <= max_chars:
            return value
        return value[:max_chars].rstrip("，。、；：！？ ")
    words = value.split()
    if len(words) <= max_words:
        return value
    return " ".join(words[:max_words]).rstrip(" ,.;:-")


def _clean_context_soup(soup: BeautifulSoup) -> None:
    for tag_name in ("script", "style", "noscript", "nav", "footer", "aside", "form", "button", "svg"):
        for node in soup.find_all(tag_name):
            node.decompose()
    for node in soup.find_all(attrs={"id": _CONTEXT_BOILERPLATE_ATTR_PATTERN}):
        node.decompose()
    for node in soup.find_all(attrs={"class": _CONTEXT_BOILERPLATE_ATTR_PATTERN}):
        node.decompose()


def _metadata_context(soup: BeautifulSoup) -> str:
    for property_name in ("og:description", "twitter:description", "description"):
        value = _meta_content(soup, property_name)
        normalized = _normalize_context_text(value or "")
        if normalized:
            return normalized
    return ""


def _collect_context_paragraphs(scope: BeautifulSoup) -> list[str]:
    paragraphs: list[str] = []
    for paragraph in scope.find_all("p"):
        normalized = _normalize_context_text(paragraph.get_text(" ", strip=True))
        if not normalized:
            continue
        cjk_count = _count_cjk_chars(normalized)
        if cjk_count > 0:
            if cjk_count < _CONTEXT_MIN_PARAGRAPH_CJK_CHARS:
                continue
        elif len(normalized.split()) < _CONTEXT_MIN_PARAGRAPH_WORDS:
            continue
        if _CONTEXT_BOILERPLATE_TEXT_PATTERN.search(normalized):
            continue
        paragraphs.append(normalized)
        if len(paragraphs) >= _MAX_CONTEXT_PARAGRAPHS:
            break
    return paragraphs


def _dedupe_paragraphs(paragraphs: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for paragraph in paragraphs:
        key = re.sub(r"[^\w]+", " ", paragraph.lower()).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(paragraph)
    return deduped


def _structured_context(*, soup: BeautifulSoup, max_words: int) -> str:
    scopes: list[Any] = []
    article_nodes = soup.find_all("article")
    if article_nodes:
        scopes.extend(article_nodes)
    else:
        main_node = soup.find("main")
        if main_node is not None:
            scopes.append(main_node)
        elif soup.body is not None:
            scopes.append(soup.body)

    collected: list[str] = []
    for scope in scopes:
        collected.extend(_collect_context_paragraphs(scope))
        if len(collected) >= _MAX_CONTEXT_PARAGRAPHS:
            break

    deduped = _dedupe_paragraphs(collected)
    if not deduped:
        return ""
    joined = " ".join(deduped)
    return _truncate_to_max_words(joined, max_words=max(40, max_words))


@dataclass(frozen=True)
class MediaCandidate:
    media_type: str
    media_url: str
    selection_reason: str
    priority: int


@dataclass(frozen=True)
class MediaAssetResult:
    media_type: str
    media_url: str
    selection_reason: str
    media_candidates: list[MediaCandidate]
    quality_summary: dict[str, Any] | None = None


def _meta_content(soup: BeautifulSoup, property_name: str) -> str | None:
    node = soup.find("meta", attrs={"property": property_name}) or soup.find("meta", attrs={"name": property_name})
    if not node:
        return None
    content = node.get("content")
    if not content:
        return None
    return str(content).strip() or None


def _infer_media_type(candidate_url: str) -> str | None:
    lower_path = urlparse(candidate_url).path.lower()
    if any(lower_path.endswith(ext) for ext in VIDEO_EXTENSIONS):
        return "video"
    if any(lower_path.endswith(ext) for ext in IMAGE_EXTENSIONS):
        return "image"
    return None


def _add_candidate(
    *,
    candidates: list[MediaCandidate],
    seen_urls: set[str],
    media_type: str,
    media_url: str,
    selection_reason: str,
    priority: int,
) -> None:
    normalized_url = media_url.strip()
    if not normalized_url:
        return
    if normalized_url in seen_urls:
        return
    seen_urls.add(normalized_url)
    candidates.append(
        MediaCandidate(
            media_type=media_type,
            media_url=normalized_url,
            selection_reason=selection_reason,
            priority=priority,
        )
    )


def _collect_video_candidates(soup: BeautifulSoup, page_url: str) -> list[str]:
    candidates: list[str] = []
    for video_tag in soup.find_all("video"):
        source_tag = video_tag.find("source")
        if source_tag and source_tag.get("src"):
            candidates.append(urljoin(page_url, str(source_tag["src"])))
        if video_tag.get("src"):
            candidates.append(urljoin(page_url, str(video_tag["src"])))
    return candidates


def _collect_image_candidates(soup: BeautifulSoup, page_url: str) -> list[str]:
    candidates: list[str] = []
    for image_tag in soup.find_all("img"):
        for key in ("src", "data-src", "data-original"):
            value = image_tag.get(key)
            if value:
                candidates.append(urljoin(page_url, str(value)))
                break
    return candidates


def _sort_media_candidates(candidates: list[MediaCandidate]) -> list[MediaCandidate]:
    return sorted(
        candidates,
        key=lambda candidate: (
            0 if candidate.media_type == "video" else 1,
            candidate.priority,
            candidate.media_url,
        ),
    )


def _is_valid_media_url(candidate_url: str) -> bool:
    lower_url = candidate_url.lower()
    return lower_url.startswith("http://") or lower_url.startswith("https://")


def _normalize_rss_media_type(*, medium: Any, mime_type: Any) -> str | None:
    normalized_medium = str(medium or "").strip().lower()
    if normalized_medium in {"image", "video"}:
        return normalized_medium
    normalized_mime = str(mime_type or "").strip().lower()
    if normalized_mime.startswith("image/"):
        return "image"
    if normalized_mime.startswith("video/"):
        return "video"
    return None


def _collect_rss_media_candidates(*, rss_entry_payload: dict[str, Any] | None) -> list[tuple[str, str, str, int]]:
    if not isinstance(rss_entry_payload, dict):
        return []

    collected: list[tuple[str, str, str, int]] = []

    def add_candidate(
        *,
        raw_url: Any,
        reason: str,
        priority: int,
        media_type: str | None = None,
    ) -> None:
        candidate_url = str(raw_url or "").strip()
        if not candidate_url:
            return
        if candidate_url.startswith("//"):
            candidate_url = f"https:{candidate_url}"
        inferred_type = media_type or _infer_media_type(candidate_url)
        if inferred_type not in {"image", "video"}:
            return
        collected.append((inferred_type, candidate_url, reason, priority))

    media_contents = rss_entry_payload.get("media_content")
    if isinstance(media_contents, dict):
        media_contents = [media_contents]
    if isinstance(media_contents, list):
        for item in media_contents:
            if not isinstance(item, dict):
                continue
            add_candidate(
                raw_url=item.get("url"),
                reason="rss:media_content",
                priority=-3,
                media_type=_normalize_rss_media_type(medium=item.get("medium"), mime_type=item.get("type")),
            )

    media_thumbnails = rss_entry_payload.get("media_thumbnail")
    if isinstance(media_thumbnails, dict):
        media_thumbnails = [media_thumbnails]
    if isinstance(media_thumbnails, list):
        for item in media_thumbnails:
            if not isinstance(item, dict):
                continue
            add_candidate(
                raw_url=item.get("url"),
                reason="rss:media_thumbnail",
                priority=-2,
                media_type="image",
            )

    enclosures = rss_entry_payload.get("enclosures")
    if isinstance(enclosures, dict):
        enclosures = [enclosures]
    if isinstance(enclosures, list):
        for item in enclosures:
            if not isinstance(item, dict):
                continue
            add_candidate(
                raw_url=item.get("href") or item.get("url"),
                reason="rss:enclosure",
                priority=-3,
                media_type=_normalize_rss_media_type(medium=item.get("medium"), mime_type=item.get("type")),
            )

    links = rss_entry_payload.get("links")
    if isinstance(links, dict):
        links = [links]
    if isinstance(links, list):
        for item in links:
            if not isinstance(item, dict):
                continue
            rel = str(item.get("rel") or "").strip().lower()
            if rel not in {"enclosure", "thumbnail"}:
                continue
            add_candidate(
                raw_url=item.get("href") or item.get("url"),
                reason=f"rss:link:{rel}",
                priority=-1 if rel == "thumbnail" else -2,
                media_type=_normalize_rss_media_type(medium=item.get("medium"), mime_type=item.get("type")),
            )

    add_candidate(
        raw_url=rss_entry_payload.get("image"),
        reason="rss:image",
        priority=-2,
        media_type="image",
    )
    add_candidate(
        raw_url=rss_entry_payload.get("thumbnail"),
        reason="rss:thumbnail",
        priority=-2,
        media_type="image",
    )

    return collected


def extract_article_context(
    *,
    page_url: str,
    timeout_seconds: int,
    max_words: int = _DEFAULT_ARTICLE_CONTEXT_MAX_WORDS,
) -> str:
    soup: BeautifulSoup | None = None
    try:
        response = requests.get(page_url, timeout=timeout_seconds, headers=_DEFAULT_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except requests.RequestException as fetch_err:
        status_code = getattr(getattr(fetch_err, "response", None), "status_code", None)
        LOGGER.warning(
            "Article context extraction failed url=%s status=%s",
            page_url,
            status_code or type(fetch_err).__name__,
        )
        return ""

    if soup is None:
        return ""

    _clean_context_soup(soup)
    metadata_context = _metadata_context(soup)
    structured_context = _structured_context(soup=soup, max_words=max_words)

    if metadata_context and structured_context:
        cjk_count = _count_cjk_chars(structured_context)
        if cjk_count > 0:
            if cjk_count < _STRUCTURED_CONTEXT_PREFERRED_MIN_CJK_CHARS:
                return metadata_context
        elif len(structured_context.split()) < _STRUCTURED_CONTEXT_PREFERRED_MIN_WORDS:
            return metadata_context
        return structured_context

    if structured_context:
        return structured_context

    if metadata_context:
        return metadata_context

    return ""


def extract_best_media_from_article(
    *,
    page_url: str,
    timeout_seconds: int,
    rss_entry_payload: dict[str, Any] | None = None,
) -> MediaAssetResult | None:
    soup: BeautifulSoup | None = None
    page_fetch_status = "skipped"
    try:
        response = requests.get(page_url, timeout=timeout_seconds, headers=_DEFAULT_HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        page_fetch_status = f"ok({response.status_code})"
    except requests.RequestException as fetch_err:
        status_code = getattr(getattr(fetch_err, "response", None), "status_code", None)
        page_fetch_status = f"failed({status_code or type(fetch_err).__name__})"
        LOGGER.warning("Article page fetch failed url=%s status=%s", page_url, page_fetch_status)
        soup = None

    candidates: list[MediaCandidate] = []
    seen_urls: set[str] = set()

    rss_raw = _collect_rss_media_candidates(rss_entry_payload=rss_entry_payload)
    rss_candidate_count = len(rss_raw)
    for media_type, media_url, selection_reason, priority in rss_raw:
        _add_candidate(
            candidates=candidates,
            seen_urls=seen_urls,
            media_type=media_type,
            media_url=urljoin(page_url, media_url),
            selection_reason=selection_reason,
            priority=priority,
        )

    og_image_found = False
    twitter_image_found = False
    img_tag_count = 0
    video_tag_count = 0
    link_href_count = 0

    if soup is not None:
        for property_name in ("og:video", "og:video:url", "og:video:secure_url"):
            og_video = _meta_content(soup, property_name)
            if og_video:
                _add_candidate(
                    candidates=candidates,
                    seen_urls=seen_urls,
                    media_type="video",
                    media_url=urljoin(page_url, og_video),
                    selection_reason=property_name,
                    priority=0,
                )

        twitter_video = _meta_content(soup, "twitter:player:stream")
        if twitter_video:
            _add_candidate(
                candidates=candidates,
                seen_urls=seen_urls,
                media_type="video",
                media_url=urljoin(page_url, twitter_video),
                selection_reason="twitter:player:stream",
                priority=1,
            )

        og_image = _meta_content(soup, "og:image")
        if og_image:
            og_image_found = True
            _add_candidate(
                candidates=candidates,
                seen_urls=seen_urls,
                media_type="image",
                media_url=urljoin(page_url, og_image),
                selection_reason="og:image",
                priority=2,
            )

        twitter_image = _meta_content(soup, "twitter:image")
        if twitter_image:
            twitter_image_found = True
            _add_candidate(
                candidates=candidates,
                seen_urls=seen_urls,
                media_type="image",
                media_url=urljoin(page_url, twitter_image),
                selection_reason="twitter:image",
                priority=3,
            )

        video_urls = _collect_video_candidates(soup, page_url)
        video_tag_count = len(video_urls)
        for candidate_url in video_urls:
            _add_candidate(
                candidates=candidates,
                seen_urls=seen_urls,
                media_type="video",
                media_url=candidate_url,
                selection_reason="video_tag",
                priority=4,
            )

        image_urls = _collect_image_candidates(soup, page_url)
        img_tag_count = len(image_urls)
        for candidate_url in image_urls:
            _add_candidate(
                candidates=candidates,
                seen_urls=seen_urls,
                media_type="image",
                media_url=candidate_url,
                selection_reason="img_tag",
                priority=5,
            )

        candidate_links = [link.get("href") for link in soup.find_all("link") if link.get("href")]
        for candidate in candidate_links:
            absolute_url = urljoin(page_url, str(candidate))
            inferred = _infer_media_type(absolute_url)
            if not inferred:
                continue
            link_href_count += 1
            _add_candidate(
                candidates=candidates,
                seen_urls=seen_urls,
                media_type=inferred,
                media_url=absolute_url,
                selection_reason="link_href",
                priority=6,
            )

    sorted_candidates = [candidate for candidate in _sort_media_candidates(candidates) if _is_valid_media_url(candidate.media_url)]

    if not sorted_candidates:
        LOGGER.info(
            "Media extraction url=%s page_fetch=%s rss_candidates=%d og_image=%s twitter_image=%s "
            "img_tags=%d video_tags=%d link_hrefs=%d total_candidates=0 result=no_media",
            page_url, page_fetch_status, rss_candidate_count,
            og_image_found, twitter_image_found, img_tag_count, video_tag_count, link_href_count,
        )
        return None

    video_candidates = [candidate for candidate in sorted_candidates if candidate.media_type == "video"]
    image_candidates = [candidate for candidate in sorted_candidates if candidate.media_type == "image"]
    retained_candidates: list[MediaCandidate] = []
    if video_candidates:
        retained_candidates.append(video_candidates[0])
    retained_candidates.extend(image_candidates[:7 if retained_candidates else 8])
    if not retained_candidates:
        retained_candidates = sorted_candidates[:8]

    primary_candidate = retained_candidates[0]

    LOGGER.info(
        "Media extraction url=%s page_fetch=%s rss_candidates=%d og_image=%s twitter_image=%s "
        "img_tags=%d video_tags=%d link_hrefs=%d total_candidates=%d "
        "retained=%d primary=%s primary_type=%s",
        page_url, page_fetch_status, rss_candidate_count,
        og_image_found, twitter_image_found, img_tag_count, video_tag_count, link_href_count,
        len(sorted_candidates), len(retained_candidates),
        primary_candidate.selection_reason, primary_candidate.media_type,
    )

    return MediaAssetResult(
        media_type=primary_candidate.media_type,
        media_url=primary_candidate.media_url,
        selection_reason=primary_candidate.selection_reason,
        media_candidates=retained_candidates,
    )
