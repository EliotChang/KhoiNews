from __future__ import annotations

import base64
from dataclasses import dataclass
import logging
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any
from urllib.parse import quote

import requests

from pipeline.text_sanitize import strip_urls_from_text


LOGGER = logging.getLogger("wj_voice")


@dataclass(frozen=True)
class VoiceAssetResult:
    status: str
    audio_url: str | None
    audio_duration_sec: float | None
    alignment: dict[str, Any] | None
    error: str | None


_TTS_TRAILING_FRAGMENT_WORDS = {
    "的",
    "了",
    "和",
    "與",
    "但",
    "因為",
    "所以",
    "在",
    "從",
    "對",
    "將",
    "而",
    "及",
    "或",
    "也",
    "and",
    "or",
    "but",
    "because",
    "so",
    "to",
    "for",
    "of",
    "in",
    "on",
    "at",
    "with",
    "by",
    "from",
}

_CJK_CHAR_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]")


def _prepare_tts_text(raw_text: str) -> str:
    normalized = strip_urls_from_text(raw_text or "")
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    normalized = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", normalized)
    normalized = re.sub(r"(?<!\w)#([\w\u4e00-\u9fff\u3400-\u4dbf]+)", r"\1", normalized)
    normalized = re.sub(r"(?<!\w)@[A-Za-z0-9_]+", "", normalized)
    normalized = normalized.replace("—", "，").replace("–", "，")
    normalized = re.sub(r"\.{3,}", "。", normalized)
    normalized = re.sub(r"[!?！？]{2,}", "。", normalized)
    normalized = re.sub(r"\s*[;；]\s*", "。", normalized)
    normalized = re.sub(r"\s*[：:]\s*", "，", normalized)
    normalized = re.sub(r"\s*[|/]\s*", "，", normalized)
    normalized = re.sub(r"[`*_~]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip(" ,;:-，；：")
    if not normalized:
        return ""
    is_cjk = bool(_CJK_CHAR_RE.search(normalized))
    if not is_cjk:
        trailing_word = normalized.split()[-1].strip(".,!?").lower()
        if trailing_word in _TTS_TRAILING_FRAGMENT_WORDS and len(normalized.split()) > 2:
            normalized = " ".join(normalized.split()[:-1]).rstrip(" ,;:-")
    else:
        last_char = normalized.rstrip("，。、；：！？ ")[-1:] if normalized else ""
        if last_char in _TTS_TRAILING_FRAGMENT_WORDS:
            normalized = normalized.rstrip("，。、；：！？ ")[:-1].rstrip("，。、；：！？ ")
    if normalized and normalized[-1] not in ".!?。！？":
        normalized += "。" if is_cjk else "."
    return normalized


def _clamp_voice_setting(value: float, *, default: float) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = default
    return max(0.0, min(1.0, numeric))


def _duration_from_audio_bytes(audio_bytes: bytes) -> float | None:
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp3") as temp_audio:
            temp_audio.write(audio_bytes)
            temp_audio.flush()
            return _duration_from_ffprobe(media_path=Path(temp_audio.name))
    except OSError:
        return None


def _duration_from_ffprobe(*, media_path: Path) -> float | None:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(media_path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except FileNotFoundError:
        LOGGER.warning("ffprobe binary not found; cannot measure duration for %s", media_path)
        return None
    except subprocess.SubprocessError as exc:
        LOGGER.warning("ffprobe failed for %s: %s", media_path, exc)
        return None

    raw_duration = result.stdout.strip()
    if not raw_duration:
        LOGGER.warning("ffprobe returned empty output for %s", media_path)
        return None
    try:
        return round(float(raw_duration), 2)
    except ValueError:
        LOGGER.warning("ffprobe output not a valid number for %s: %r", media_path, raw_duration)
        return None


def _build_public_audio_url(*, supabase_url: str, bucket_name: str, object_path: str) -> str:
    encoded_object_path = quote(object_path, safe="/")
    return f"{supabase_url.rstrip('/')}/storage/v1/object/public/{bucket_name}/{encoded_object_path}"


def _upload_audio_to_supabase_storage(
    *,
    supabase_url: str,
    supabase_service_role_key: str,
    bucket_name: str,
    object_path: str,
    audio_bytes: bytes,
    timeout_seconds: int,
) -> str:
    upload_endpoint = (
        f"{supabase_url.rstrip('/')}/storage/v1/object/"
        f"{bucket_name}/{quote(object_path, safe='/')}"
    )
    headers = {
        "Authorization": f"Bearer {supabase_service_role_key}",
        "apikey": supabase_service_role_key,
        "Content-Type": "audio/mpeg",
        "x-upsert": "true",
    }
    response = requests.post(upload_endpoint, headers=headers, data=audio_bytes, timeout=timeout_seconds)
    response.raise_for_status()
    return _build_public_audio_url(
        supabase_url=supabase_url,
        bucket_name=bucket_name,
        object_path=object_path,
    )


def generate_elevenlabs_voice(
    *,
    api_key: str,
    voice_id: str,
    text: str,
    post_id: str,
    supabase_url: str,
    supabase_service_role_key: str,
    supabase_voice_bucket: str,
    timeout_seconds: int,
    model_id: str = "eleven_multilingual_v2",
    voice_stability: float = 0.4,
    voice_similarity_boost: float = 0.8,
    apply_text_normalization: bool = True,
) -> VoiceAssetResult:
    prepared_text = _prepare_tts_text(text)
    if not prepared_text:
        return VoiceAssetResult(
            status="failed",
            audio_url=None,
            audio_duration_sec=None,
            alignment=None,
            error="Script text was empty after TTS preprocessing",
        )
    stability = _clamp_voice_setting(voice_stability, default=0.4)
    similarity_boost = _clamp_voice_setting(voice_similarity_boost, default=0.8)
    apply_text_normalization_value = "on" if apply_text_normalization else "off"
    LOGGER.info(
        "ElevenLabs request post_id=%s model=%s stability=%.2f similarity_boost=%.2f "
        "apply_text_normalization=%s words=%s chars=%s",
        post_id,
        model_id,
        stability,
        similarity_boost,
        apply_text_normalization_value,
        len(prepared_text.split()),
        len(prepared_text),
    )
    endpoint_with_timestamps = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/with-timestamps"
    endpoint = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {
        "xi-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "text": prepared_text,
        "model_id": model_id,
        "apply_text_normalization": apply_text_normalization_value,
        "voice_settings": {"stability": stability, "similarity_boost": similarity_boost},
    }

    response_content: bytes | None = None
    alignment_payload: dict[str, Any] | None = None
    try:
        response = requests.post(endpoint_with_timestamps, json=payload, headers=headers, timeout=timeout_seconds)
        response.raise_for_status()
        response_payload = response.json()
        audio_base64 = response_payload.get("audio_base64")
        if isinstance(audio_base64, str) and audio_base64:
            response_content = base64.b64decode(audio_base64)
        raw_alignment_payload = response_payload.get("normalized_alignment") or response_payload.get("alignment")
        if isinstance(raw_alignment_payload, dict):
            alignment_payload = raw_alignment_payload
    except (requests.RequestException, ValueError, base64.binascii.Error) as ts_err:
        LOGGER.warning(
            "ElevenLabs timestamps endpoint failed for post_id=%s: %s; falling back to standard endpoint",
            post_id,
            ts_err,
        )
        response_content = None
        alignment_payload = None

    if response_content is None:
        fallback_headers = {
            "xi-api-key": api_key,
            "Content-Type": "application/json",
            "Accept": "audio/mpeg",
        }
        try:
            response = requests.post(endpoint, json=payload, headers=fallback_headers, timeout=timeout_seconds)
            response.raise_for_status()
            response_content = response.content
        except requests.RequestException as exc:
            return VoiceAssetResult(
                status="failed",
                audio_url=None,
                audio_duration_sec=None,
                alignment=None,
                error=str(exc),
            )

    if not response_content:
        return VoiceAssetResult(
            status="failed",
            audio_url=None,
            audio_duration_sec=None,
            alignment=None,
            error="ElevenLabs returned an empty audio payload",
        )

    object_path = f"wj/{post_id}.mp3"
    try:
        audio_url = _upload_audio_to_supabase_storage(
            supabase_url=supabase_url,
            supabase_service_role_key=supabase_service_role_key,
            bucket_name=supabase_voice_bucket,
            object_path=object_path,
            audio_bytes=response_content,
            timeout_seconds=timeout_seconds,
        )
    except requests.RequestException as exc:
        return VoiceAssetResult(
            status="failed",
            audio_url=None,
            audio_duration_sec=None,
            alignment=None,
            error=f"Supabase upload failed: {exc}",
        )

    duration = _duration_from_audio_bytes(response_content)
    if duration is None:
        LOGGER.warning("Could not detect MP3 duration for post_id=%s; downstream video may use fallback duration", post_id)
    return VoiceAssetResult(
        status="generated",
        audio_url=audio_url,
        audio_duration_sec=duration,
        alignment=alignment_payload,
        error=None,
    )
