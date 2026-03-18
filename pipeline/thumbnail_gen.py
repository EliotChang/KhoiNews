from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import requests
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline.config import Settings

LOGGER = logging.getLogger("wj_thumbnail")

YOUTUBE_THUMBNAIL_WIDTH = 1280
YOUTUBE_THUMBNAIL_HEIGHT = 720
THUMBNAIL_BUCKET = "video-assets"
_PROVIDER_UNAVAILABLE_UNTIL: dict[str, float] = {}


def _http_session(*, retries: int) -> requests.Session:
    retry = Retry(
        total=max(0, retries),
        connect=max(0, retries),
        read=max(0, retries),
        backoff_factor=0.4,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
    )
    return session


def _provider_available(*, provider: str) -> bool:
    unavailable_until = _PROVIDER_UNAVAILABLE_UNTIL.get(provider, 0.0)
    return time.time() >= unavailable_until


def _mark_provider_unavailable(*, provider: str, cooldown_seconds: int) -> None:
    _PROVIDER_UNAVAILABLE_UNTIL[provider] = time.time() + max(30, cooldown_seconds)


@dataclass(frozen=True)
class ThumbnailResult:
    image_bytes: bytes
    content_type: str
    source: str
    description: str
    url: str | None = None


def generate_thumbnail(
    *,
    settings: Settings,
    strategy: str,
    title: str,
    script: str,
    article_image_url: str | None = None,
) -> ThumbnailResult | None:
    normalized_strategy = _normalize_thumbnail_strategy(strategy)
    handlers: dict[str, Any] = {
        "article-image": _strategy_article_image,
        "web-sourced": _strategy_web_sourced,
        "gemini-generated": _strategy_gemini_generated,
    }
    handler = handlers.get(normalized_strategy)
    if not handler:
        LOGGER.warning("Unknown thumbnail strategy=%s, falling back to article-image", strategy)
        handler = _strategy_article_image

    try:
        return handler(
            settings=settings,
            title=title,
            script=script,
            article_image_url=article_image_url,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Thumbnail generation failed strategy=%s error=%s", normalized_strategy, exc)
        return None


def _normalize_thumbnail_strategy(strategy: str) -> str:
    return strategy.strip().lower()


def _resize_to_thumbnail(image_bytes: bytes) -> bytes:
    img = Image.open(io.BytesIO(image_bytes))
    img = img.convert("RGB")

    src_w, src_h = img.size
    target_ratio = YOUTUBE_THUMBNAIL_WIDTH / YOUTUBE_THUMBNAIL_HEIGHT
    src_ratio = src_w / src_h

    if src_ratio > target_ratio:
        new_w = int(src_h * target_ratio)
        offset = (src_w - new_w) // 2
        img = img.crop((offset, 0, offset + new_w, src_h))
    elif src_ratio < target_ratio:
        new_h = int(src_w / target_ratio)
        offset = (src_h - new_h) // 2
        img = img.crop((0, offset, src_w, offset + new_h))

    img = img.resize((YOUTUBE_THUMBNAIL_WIDTH, YOUTUBE_THUMBNAIL_HEIGHT), Image.LANCZOS)

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()


def _strategy_article_image(
    *,
    settings: Settings,
    title: str,
    script: str,
    article_image_url: str | None,
) -> ThumbnailResult | None:
    if not article_image_url:
        LOGGER.info("No article image URL available for thumbnail")
        return None

    session = _http_session(retries=settings.thumbnail_fetch_retries)
    response = session.get(article_image_url, timeout=settings.request_timeout_seconds)
    response.raise_for_status()
    raw_bytes = response.content
    if len(raw_bytes) < 4000:
        LOGGER.info("Article image too small (%d bytes), skipping", len(raw_bytes))
        return None

    resized = _resize_to_thumbnail(raw_bytes)
    return ThumbnailResult(
        image_bytes=resized,
        content_type="image/jpeg",
        source="article-image",
        description=f"Thumbnail from article image: {article_image_url[:80]}",
    )


def _strategy_web_sourced(
    *,
    settings: Settings,
    title: str,
    script: str,
    article_image_url: str | None,
) -> ThumbnailResult | None:
    if not _provider_available(provider="web-sourced"):
        LOGGER.info("Web image provider cooling down; falling back to article-image")
        return _strategy_article_image(
            settings=settings,
            title=title,
            script=script,
            article_image_url=article_image_url,
        )

    if not settings.google_custom_search_api_key or not settings.google_custom_search_cx:
        LOGGER.info(
            "Google Custom Search not configured (need API key + CX). "
            "falling back to article-image"
        )
        return _strategy_article_image(
            settings=settings,
            title=title,
            script=script,
            article_image_url=article_image_url,
        )

    search_query = _build_image_search_query(settings=settings, title=title)

    params = {
        "key": settings.google_custom_search_api_key,
        "cx": settings.google_custom_search_cx,
        "q": search_query,
        "searchType": "image",
        "imgSize": "xlarge",
        "num": 5,
        "safe": "active",
    }
    session = _http_session(retries=settings.thumbnail_fetch_retries)
    response = session.get(
        "https://www.googleapis.com/customsearch/v1",
        params=params,
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    results = response.json().get("items", [])

    for item in results:
        image_url = item.get("link", "")
        if not image_url:
            continue
        try:
            img_response = session.get(image_url, timeout=settings.request_timeout_seconds)
            img_response.raise_for_status()
            raw_bytes = img_response.content
            if len(raw_bytes) < 4000:
                continue
            resized = _resize_to_thumbnail(raw_bytes)
            return ThumbnailResult(
                image_bytes=resized,
                content_type="image/jpeg",
                source="web-sourced",
                description=f"Thumbnail from web search: {image_url[:80]}",
            )
        except Exception:  # noqa: BLE001
            continue

    LOGGER.info("No suitable web images found, falling back to article-image")
    return _strategy_article_image(
        settings=settings,
        title=title,
        script=script,
        article_image_url=article_image_url,
    )


def _strategy_gemini_generated(
    *,
    settings: Settings,
    title: str,
    script: str,
    article_image_url: str | None,
) -> ThumbnailResult | None:
    if not _provider_available(provider="gemini-generated"):
        LOGGER.info("Gemini provider cooling down; skipping image generation")
        return None
    del article_image_url  # Unused for AI-generated fallback strategy.
    if not settings.gemini_api_key:
        LOGGER.info("Gemini image generation unavailable: GEMINI_API_KEY not configured")
        return None

    prompt = _build_gemini_newsroom_prompt(title=title, script=script)
    for model_name in _ordered_gemini_image_models(settings=settings):
        try:
            image_bytes = _generate_gemini_image_bytes(
                settings=settings,
                model_name=model_name,
                prompt=prompt,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Gemini image generation failed model=%s error=%s",
                model_name,
                exc,
            )
            if "timeout" in str(exc).lower():
                _mark_provider_unavailable(
                    provider="gemini-generated",
                    cooldown_seconds=settings.thumbnail_provider_cooldown_seconds,
                )
            continue
        if not image_bytes:
            LOGGER.info("Gemini image generation returned no bytes model=%s", model_name)
            continue
        resized = _resize_to_thumbnail(image_bytes)
        return ThumbnailResult(
            image_bytes=resized,
            content_type="image/jpeg",
            source="gemini-generated",
            description=f"Thumbnail generated by Gemini model={model_name}",
        )

    return None


def _ordered_gemini_image_models(*, settings: Settings) -> list[str]:
    models: list[str] = []
    primary = settings.gemini_image_model.strip()
    fallback = settings.gemini_image_fallback_model.strip()
    for model in (primary, fallback):
        if model and model not in models:
            models.append(model)
    return models


def _generate_gemini_image_bytes(
    *,
    settings: Settings,
    model_name: str,
    prompt: str,
) -> bytes | None:
    from google import genai
    from google.genai import types

    client = genai.Client(
        api_key=settings.gemini_api_key,
        http_options=types.HttpOptions(timeout=max(1, settings.request_timeout_seconds)),
    )
    response = client.models.generate_images(
        model=model_name,
        prompt=prompt,
        config=types.GenerateImagesConfig(
            number_of_images=1,
        ),
    )
    for generated_image in response.generated_images or []:
        image_payload = generated_image.image
        if image_payload and image_payload.image_bytes:
            return bytes(image_payload.image_bytes)
    return None


def _build_gemini_newsroom_prompt(*, title: str, script: str) -> str:
    script_excerpt = " ".join(script.strip().split())[:220]
    return (
        "Create a photorealistic editorial news photograph suitable for a major TV news website. "
        "Style: neutral newsroom photojournalism, realistic lighting, authentic scene composition, "
        "natural colors, high detail. "
        "Do not include logos, brand marks, text overlays, lower-thirds, captions, or watermarks. "
        f"Story headline context: {title.strip()}. "
        f"Story summary context: {script_excerpt}."
    )


def _build_image_search_query(*, settings: Settings, title: str) -> str:
    from anthropic import Anthropic

    client = Anthropic(api_key=settings.anthropic_api_key)
    response = client.messages.create(
        model=settings.anthropic_model,
        temperature=0.3,
        max_tokens=100,
        system="You generate concise image search queries. Return ONLY the search query, nothing else.",
        messages=[{
            "role": "user",
            "content": (
                f"Generate a Google Image Search query to find a compelling, high-quality photo "
                f"that would work as a YouTube thumbnail for this news story:\n\n"
                f"Title: {title}\n\n"
                f"Return only the search query (5-8 words). Focus on the visual subject, "
                f"not abstract concepts."
            ),
        }],
    )
    return response.content[0].text.strip().strip('"').strip("'")


def upload_thumbnail_to_supabase(
    *,
    settings: Settings,
    post_id: str,
    thumbnail: ThumbnailResult,
) -> str:
    safe_post_id = "".join(c for c in post_id if c.isalnum() or c in {"-", "_"}) or str(uuid4())
    object_path = f"thumbnails/{safe_post_id}.jpg"
    upload_endpoint = (
        f"{settings.supabase_url.rstrip('/')}/storage/v1/object/"
        f"{THUMBNAIL_BUCKET}/{quote(object_path, safe='/')}"
    )
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
        "Content-Type": thumbnail.content_type,
        "x-upsert": "true",
    }
    response = requests.post(
        upload_endpoint,
        headers=headers,
        data=thumbnail.image_bytes,
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    public_url = (
        f"{settings.supabase_url.rstrip('/')}/storage/v1/object/public/"
        f"{THUMBNAIL_BUCKET}/{quote(object_path, safe='/')}"
    )
    LOGGER.info("Thumbnail uploaded post_id=%s url=%s", post_id, public_url)
    return public_url
