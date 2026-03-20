"""Generate a single example MP4 from World Journal, skipping database persistence."""

from __future__ import annotations

import json
import logging
import os
import random
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from pipeline.wj_ingest import fetch_wj_posts, WJ_BASE_URL, DEFAULT_WJ_CATEGORY_PATHS
from pipeline.article_media import extract_article_context, extract_best_media_from_article
from pipeline.content_gen import generate_content_pack
from pipeline.voice_gen import (
    _prepare_tts_text,
    _TTS_TRAILING_FRAGMENT_WORDS,
    VoiceAssetResult,
)
from pipeline.caption_align import build_aligned_caption_cues

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("example_video")

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJECT_DIR = REPO_ROOT / "pipeline" / "video_templates" / "fish_lipsync"


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _download_bytes(url: str, timeout: int = 30) -> bytes:
    import requests
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def _upload_audio_to_supabase(audio_bytes: bytes, post_id: str) -> str:
    import requests

    supabase_url = _env("SUPABASE_URL") or _derive_supabase_url()
    service_key = _env("SUPABASE_SERVICE_ROLE_KEY")
    bucket = _env("SUPABASE_VOICE_BUCKET", "voice-assets")
    object_path = f"wj/{post_id}.mp3"

    upload_url = f"{supabase_url}/storage/v1/object/{bucket}/{object_path}"
    headers = {
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "audio/mpeg",
        "x-upsert": "true",
    }
    resp = requests.post(upload_url, headers=headers, data=audio_bytes, timeout=60)
    resp.raise_for_status()

    return f"{supabase_url}/storage/v1/object/public/{bucket}/{object_path}"


def _derive_supabase_url() -> str:
    from urllib.parse import urlparse
    db_url = _env("SUPABASE_DB_URL")
    parsed = urlparse(db_url)
    username = parsed.username or ""
    if username.startswith("postgres."):
        ref_id = username.split(".", 1)[1]
    else:
        ref_id = (parsed.hostname or "").split(".")[0]
    return f"https://{ref_id}.supabase.co"


def _generate_voice_local(text: str, post_id: str) -> tuple[bytes, dict | None, float | None]:
    """Call ElevenLabs TTS and return (audio_bytes, alignment_dict, duration_sec)."""
    import requests

    api_key = _env("ELEVENLABS_API_KEY")
    voice_id = _env("ELEVENLABS_VOICE_ID", "r6qgCCGI7RWKXCagm158")
    model_id = "eleven_multilingual_v2"

    prepared_text = _prepare_tts_text(text)
    LOGGER.info("TTS text (%d chars): %s", len(prepared_text), prepared_text[:120])

    alignment: dict | None = None
    audio_bytes: bytes | None = None

    ts_url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/with-timestamps"
    ts_headers = {"xi-api-key": api_key, "Content-Type": "application/json"}
    ts_body = {
        "text": prepared_text,
        "model_id": model_id,
        "voice_settings": {"stability": 0.4, "similarity_boost": 0.8},
    }
    try:
        resp = requests.post(ts_url, headers=ts_headers, json=ts_body, timeout=60)
        if resp.status_code == 200:
            payload = resp.json()
            import base64
            audio_bytes = base64.b64decode(payload["audio_base64"])
            alignment = payload.get("normalized_alignment") or payload.get("alignment")
            LOGGER.info("ElevenLabs with-timestamps succeeded, %d bytes", len(audio_bytes))
    except Exception as e:
        LOGGER.warning("with-timestamps call failed: %s", e)

    if audio_bytes is None:
        url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
        headers = {"xi-api-key": api_key, "Content-Type": "application/json"}
        body = {
            "text": prepared_text,
            "model_id": model_id,
            "voice_settings": {"stability": 0.4, "similarity_boost": 0.8},
        }
        resp = requests.post(url, headers=headers, json=body, timeout=60, stream=True)
        resp.raise_for_status()
        audio_bytes = resp.content
        LOGGER.info("ElevenLabs standard TTS succeeded, %d bytes", len(audio_bytes))

    duration: float | None = None
    tmp_path = REPO_ROOT / f"_tmp_audio_{post_id}.mp3"
    try:
        tmp_path.write_bytes(audio_bytes)
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", str(tmp_path)],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            duration = float(result.stdout.strip())
    except Exception:
        pass
    finally:
        tmp_path.unlink(missing_ok=True)

    return audio_bytes, alignment, duration


def _duration_from_ffprobe(path: Path) -> float | None:
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", str(path)],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except Exception:
        pass
    return None


def main() -> None:
    LOGGER.info("=== Step 1: Scrape World Journal ===")
    base_url = _env("WJ_BASE_URL", WJ_BASE_URL)
    cat_raw = _env("WJ_CATEGORY_PATHS", "")
    category_paths = [p.strip() for p in cat_raw.split(",") if p.strip()] if cat_raw else DEFAULT_WJ_CATEGORY_PATHS

    result = fetch_wj_posts(
        base_url=base_url,
        category_paths=category_paths,
        timeout_seconds=20,
        max_posts=5,
    )
    if not result.posts:
        LOGGER.error("No posts scraped from World Journal")
        sys.exit(1)

    post = result.posts[0]
    LOGGER.info("Selected article: %s", post.title)
    LOGGER.info("URL: %s", post.link)

    LOGGER.info("=== Step 2: Extract article context & media ===")
    article_context = extract_article_context(page_url=post.link, timeout_seconds=20)
    LOGGER.info("Article context: %d chars", len(article_context))

    media = extract_best_media_from_article(page_url=post.link, timeout_seconds=20)
    if media:
        LOGGER.info("Media found: %s (%s)", media.media_type, media.media_url[:80])
    else:
        LOGGER.info("No media found for article")

    description = article_context if article_context else post.description

    LOGGER.info("=== Step 3: Generate Mandarin script ===")
    content = generate_content_pack(
        api_key=_env("ANTHROPIC_API_KEY"),
        model_name=_env("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        title=post.title,
        description=description,
        article_url=post.link,
        source_name="世界日報",
        script_target_words=140,
        script_min_words=100,
        script_max_words_buffer=15,
    )
    LOGGER.info("Script (%s, %d chars):\n%s", content.language, len(content.script_10s), content.script_10s)
    LOGGER.info("English translation:\n%s", content.script_10s_en or "(none)")
    LOGGER.info("Video title: %s", content.video_title_short)

    post_id = f"example_{int(datetime.now(tz=timezone.utc).timestamp())}"

    LOGGER.info("=== Step 4: Generate Mandarin TTS audio ===")
    audio_bytes, alignment, audio_duration = _generate_voice_local(content.script_10s, post_id)
    LOGGER.info("Audio: %d bytes, duration=%.2fs, alignment=%s", len(audio_bytes), audio_duration or 0, "yes" if alignment else "no")

    audio_url = _upload_audio_to_supabase(audio_bytes, post_id)
    LOGGER.info("Audio uploaded: %s", audio_url)

    LOGGER.info("=== Step 5: Render video with Remotion ===")
    from pipeline.config import load_settings
    os.environ["VIDEO_SHOW_DEBUG"] = "true"
    settings = load_settings()

    from pipeline.video_gen import generate_fish_lipsync_video
    video_result = generate_fish_lipsync_video(
        settings=settings,
        post_id=post_id,
        audio_url=audio_url,
        post_image_url=media.media_url if media else None,
        audio_duration_sec=audio_duration,
        post_title=content.video_title_short,
        date_label=datetime.now(tz=timezone.utc).strftime("%B %d, %Y"),
        script_text=content.script_10s,
        media_candidates=list(media.media_candidates) if media else [],
        voice_alignment=alignment,
        script_10s_en=content.script_10s_en,
    )

    if video_result.status != "generated":
        LOGGER.error("Video render failed: %s", video_result.error)
        sys.exit(1)

    LOGGER.info("Video generated! URL: %s", video_result.video_url)
    LOGGER.info("Duration: %.2fs", video_result.video_duration_sec or 0)

    output_local = REPO_ROOT / "example_output.mp4"
    if video_result.video_url:
        LOGGER.info("Downloading video to %s ...", output_local)
        video_bytes = _download_bytes(video_result.video_url, timeout=120)
        output_local.write_bytes(video_bytes)
        LOGGER.info("Saved: %s (%.1f MB)", output_local, len(video_bytes) / 1024 / 1024)

    LOGGER.info("=== Done! ===")


if __name__ == "__main__":
    main()
