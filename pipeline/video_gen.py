from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
from typing import Any
from urllib.parse import quote, urlparse
from uuid import uuid4

import requests

from pipeline.article_media import MediaCandidate, is_low_value_image_url
from pipeline.caption_align import build_aligned_caption_cues, _distribute_english_across_cues, _extract_character_timing
from pipeline.config import Settings


LOGGER = logging.getLogger("wj_video")
MIN_IMAGE_PAYLOAD_BYTES = 4_000
MAX_RENDER_TITLE_CHARS = 80
DEFAULT_CHROME_EXECUTABLE = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
REQUIRED_RENDER_NODE_MAJOR = 20
_REMOTION_PREFLIGHT_CACHE: set[tuple[str, str, str | None]] = set()


def _node_version_tuple(raw_version: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", raw_version)
    if not parts:
        return (0,)
    return tuple(int(part) for part in parts)


def _resolve_node_tool(tool_name: str, *, configured_executable: str = "") -> str:
    configured = configured_executable.strip()
    if not configured:
        env_key = f"VIDEO_{tool_name.upper()}_EXECUTABLE"
        configured = os.getenv(env_key, "").strip()
    if configured:
        return configured

    nvm_versions_dir = Path.home() / ".nvm" / "versions" / "node"
    if not nvm_versions_dir.exists():
        return tool_name

    tool_candidates: list[tuple[tuple[int, ...], Path]] = []
    for version_dir in nvm_versions_dir.glob("v20*/"):
        candidate = version_dir / "bin" / tool_name
        if not candidate.exists():
            continue
        tool_candidates.append((_node_version_tuple(version_dir.name), candidate))
    if not tool_candidates:
        return tool_name
    best = max(tool_candidates, key=lambda item: item[0])[1]
    return str(best)


def _resolve_node_binary_from_npx(npx_executable: str) -> str:
    npx_path = Path(npx_executable)
    if npx_path.is_absolute():
        sibling_node = npx_path.with_name("node")
        if sibling_node.exists():
            return str(sibling_node)
    return "node"


def _assert_executable_available(executable: str, *, label: str) -> None:
    executable_path = Path(executable)
    if executable_path.is_absolute():
        if executable_path.exists():
            return
        raise ValueError(f"{label} executable not found at: {executable}")
    if shutil.which(executable):
        return
    raise ValueError(f"{label} executable not found in PATH: {executable}")


def _node_major_version(*, node_executable: str) -> int:
    result = subprocess.run(
        [node_executable, "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    raw = result.stdout.strip() or result.stderr.strip()
    parsed = _node_version_tuple(raw)
    return parsed[0] if parsed else 0


def _assert_node20_toolchain(*, npx_executable: str) -> None:
    _assert_executable_available(npx_executable, label="npx")
    node_executable = _resolve_node_binary_from_npx(npx_executable)
    try:
        major_version = _node_major_version(node_executable=node_executable)
    except (subprocess.SubprocessError, FileNotFoundError) as exc:
        raise ValueError(
            f"Unable to resolve Node runtime for Remotion from npx='{npx_executable}'. "
            "Set VIDEO_NPX_EXECUTABLE and VIDEO_NPM_EXECUTABLE to your Node 20 bin paths."
        ) from exc
    if major_version != REQUIRED_RENDER_NODE_MAJOR:
        raise ValueError(
            f"Remotion render requires Node {REQUIRED_RENDER_NODE_MAJOR}. "
            f"Resolved Node {major_version} via '{node_executable}'. "
            "Set VIDEO_NPX_EXECUTABLE and VIDEO_NPM_EXECUTABLE to Node 20 toolchain paths."
        )


def _resolve_browser_executable(*, settings: Settings) -> str | None:
    configured_browser = settings.video_browser_executable.strip() or os.getenv("VIDEO_BROWSER_EXECUTABLE", "").strip()
    if configured_browser:
        if Path(configured_browser).exists():
            return configured_browser
        raise ValueError(f"Configured browser executable does not exist: {configured_browser}")
    if Path(DEFAULT_CHROME_EXECUTABLE).exists():
        return DEFAULT_CHROME_EXECUTABLE
    raise ValueError(
        "No browser executable available for Remotion render. "
        "Set VIDEO_BROWSER_EXECUTABLE to your Chrome binary path."
    )


def _run_remotion_preflight_once(
    *,
    project_dir: Path,
    npx_executable: str,
    browser_executable: str | None,
) -> None:
    cache_key = (str(project_dir), npx_executable, browser_executable)
    if cache_key in _REMOTION_PREFLIGHT_CACHE:
        return

    version_command = [npx_executable, "remotion", "versions", "--log=error"]
    if browser_executable:
        version_command.extend(["--browser-executable", browser_executable])
    subprocess.run(
        version_command,
        cwd=str(project_dir),
        check=True,
        capture_output=True,
        text=True,
        timeout=45,
    )
    _REMOTION_PREFLIGHT_CACHE.add(cache_key)


def _is_transient_render_failure(*, stderr: str, stdout: str) -> bool:
    combined = f"{stderr}\n{stdout}".lower()
    return "got no response" in combined or "net::err_" in combined


@dataclass(frozen=True)
class RuntimeMediaResult:
    video_relative_path: str | None
    image_relative_paths: list[str]
    image_candidates_attempted: int
    image_download_failures: int
    image_usability_rejects: int
    fallback_used: bool
    reject_reasons: list[str]


@dataclass(frozen=True)
class VideoAssetResult:
    status: str
    video_url: str | None
    subtitle_url: str | None
    video_duration_sec: float | None
    error: str | None


@dataclass(frozen=True)
class PreparedAudioTrackResult:
    source_path: Path
    relative_path: str
    duration_seconds: float


@dataclass(frozen=True)
class PreparedOverlayAudioAssets:
    intro_music: PreparedAudioTrackResult | None
    outro: PreparedAudioTrackResult | None


def _resolve_path(raw_path: str, *, repo_root: Path) -> Path:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    return (repo_root / candidate).resolve()


def _download_bytes(*, url: str, timeout_seconds: int) -> bytes:
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def _image_extension_from_url(url: str) -> str:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"}:
        return suffix
    return ".png"


def _video_extension_from_url(url: str) -> str:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".mp4", ".webm", ".mov"}:
        return suffix
    return ".mp4"


def _is_likely_supported_image_bytes(payload_bytes: bytes) -> bool:
    if not payload_bytes:
        return False
    prefix = payload_bytes[:64].lower()
    if prefix.startswith(b"<!doctype html") or prefix.startswith(b"<html"):
        return False
    if payload_bytes.startswith(b"\xff\xd8\xff"):  # jpeg
        return True
    if payload_bytes.startswith(b"\x89PNG\r\n\x1a\n"):  # png
        return True
    if payload_bytes.startswith((b"GIF87a", b"GIF89a")):  # gif
        return True
    if payload_bytes[:4] == b"RIFF" and payload_bytes[8:12] == b"WEBP":  # webp
        return True
    if b"ftypavif" in payload_bytes[:32] or b"ftypavis" in payload_bytes[:32]:  # avif
        return True
    return False


def _is_likely_supported_video_bytes(payload_bytes: bytes) -> bool:
    if not payload_bytes:
        return False
    prefix = payload_bytes[:256].lower().lstrip()
    if (
        prefix.startswith(b"<!doctype html")
        or prefix.startswith(b"<html")
        or prefix.startswith(b"<?xml")
        or prefix.startswith(b"{")
        or prefix.startswith(b"[")
    ):
        return False
    if len(payload_bytes) >= 12 and payload_bytes[4:8] == b"ftyp":
        return True
    if payload_bytes.startswith(b"\x1a\x45\xdf\xa3"):
        return True
    return False


def _is_usable_image_candidate(*, media_url: str, payload_bytes: bytes) -> bool:
    if is_low_value_image_url(media_url):
        return False
    if len(payload_bytes) < MIN_IMAGE_PAYLOAD_BYTES:
        return False
    return _is_likely_supported_image_bytes(payload_bytes)


def _build_public_video_url(*, supabase_url: str, bucket_name: str, object_path: str) -> str:
    encoded_object_path = quote(object_path, safe="/")
    return f"{supabase_url.rstrip('/')}/storage/v1/object/public/{bucket_name}/{encoded_object_path}"


def _upload_bytes_to_supabase_storage(
    *,
    supabase_url: str,
    supabase_service_role_key: str,
    bucket_name: str,
    object_path: str,
    payload_bytes: bytes,
    content_type: str,
    timeout_seconds: int,
) -> str:
    upload_endpoint = f"{supabase_url.rstrip('/')}/storage/v1/object/{bucket_name}/{quote(object_path, safe='/')}"
    headers = {
        "Authorization": f"Bearer {supabase_service_role_key}",
        "apikey": supabase_service_role_key,
        "Content-Type": content_type,
        "x-upsert": "true",
    }
    response = requests.post(upload_endpoint, headers=headers, data=payload_bytes, timeout=timeout_seconds)
    response.raise_for_status()
    return _build_public_video_url(
        supabase_url=supabase_url,
        bucket_name=bucket_name,
        object_path=object_path,
    )


def _safe_post_id(post_id: str) -> str:
    return "".join(char for char in post_id if char.isalnum() or char in {"-", "_"}) or str(uuid4())


def _ensure_static_assets(*, settings: Settings) -> Path:
    repo_root = Path(__file__).resolve().parent.parent
    project_dir = _resolve_path(settings.remotion_project_dir, repo_root=repo_root)
    mouth_source_dir = _resolve_path(settings.fish_mouth_frames_dir, repo_root=repo_root)
    background_source_path = _resolve_path(settings.fish_background_image_path, repo_root=repo_root)

    if not project_dir.exists():
        raise ValueError(f"Remotion project directory does not exist: {project_dir}")
    if not (project_dir / "package.json").exists():
        raise ValueError(f"Remotion project is missing package.json: {project_dir}")
    if not mouth_source_dir.exists():
        raise ValueError(f"Fish mouth frames directory does not exist: {mouth_source_dir}")
    if not background_source_path.exists():
        raise ValueError(f"Fish background image does not exist: {background_source_path}")

    public_dir = project_dir / "public"
    public_dir.mkdir(parents=True, exist_ok=True)
    mouth_dest = public_dir / "mouth"
    if mouth_source_dir.resolve() != mouth_dest.resolve():
        shutil.copytree(mouth_source_dir, mouth_dest, dirs_exist_ok=True)
    bg_dest = public_dir / "background.png"
    if background_source_path.resolve() != bg_dest.resolve():
        shutil.copy2(background_source_path, bg_dest)
    return project_dir


def _ensure_node_modules(*, project_dir: Path, npm_executable: str) -> None:
    node_modules_dir = project_dir / "node_modules"
    if node_modules_dir.exists():
        return
    LOGGER.info("Installing Remotion dependencies in %s", project_dir)
    _assert_executable_available(npm_executable, label="npm")
    subprocess.run(
        [npm_executable, "install"],
        cwd=str(project_dir),
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    )


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
                "json",
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

    try:
        payload = json.loads(result.stdout)
        raw_duration = payload.get("format", {}).get("duration")
        if raw_duration is None:
            LOGGER.warning("ffprobe returned no duration field for %s; stdout=%s", media_path, result.stdout[:200])
            return None
        return round(float(raw_duration), 2)
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        LOGGER.warning("ffprobe output parse error for %s: %s; stdout=%s", media_path, exc, result.stdout[:200])
        return None


def _prepare_overlay_audio_track(
    *,
    configured_path: str,
    runtime_dir: Path,
    runtime_token: str,
    repo_root: Path,
    post_id: str,
    track_label: str,
    runtime_filename_base: str,
    default_extension: str,
    required: bool,
) -> PreparedAudioTrackResult | None:
    configured_value = configured_path.strip()
    if not configured_value:
        if required:
            raise ValueError(f"{track_label} audio path is required when VIDEO_REQUIRE_INTRO_AND_BREAKING_AUDIO=true")
        return None

    source_path = _resolve_path(configured_value, repo_root=repo_root)
    if not source_path.exists():
        if required:
            raise ValueError(f"{track_label} audio path does not exist: {source_path}")
        LOGGER.warning("%s audio path does not exist for post_id=%s path=%s", track_label, post_id, source_path)
        return None
    if not source_path.is_file():
        if required:
            raise ValueError(f"{track_label} audio path is not a file: {source_path}")
        LOGGER.warning("%s audio path is not a file for post_id=%s path=%s", track_label, post_id, source_path)
        return None

    extension = source_path.suffix or default_extension
    runtime_filename = f"{runtime_filename_base}{extension}"
    runtime_track_path = runtime_dir / runtime_filename
    shutil.copy2(source_path, runtime_track_path)
    measured_duration = _duration_from_ffprobe(media_path=runtime_track_path)
    if not measured_duration or measured_duration <= 0:
        runtime_track_path.unlink(missing_ok=True)
        if required:
            raise ValueError(f"{track_label} audio duration probe failed: {source_path}")
        LOGGER.warning(
            "%s duration probe failed for post_id=%s path=%s; skipping track",
            track_label,
            post_id,
            runtime_track_path,
        )
        return None

    return PreparedAudioTrackResult(
        source_path=source_path.resolve(),
        relative_path=f"runtime/{runtime_token}/{runtime_filename}",
        duration_seconds=measured_duration,
    )


def _prepare_overlay_audio_assets(
    *,
    settings: Settings,
    runtime_dir: Path,
    runtime_token: str,
    repo_root: Path,
    post_id: str,
) -> PreparedOverlayAudioAssets:
    intro_music_track = _prepare_overlay_audio_track(
        configured_path=settings.video_intro_music_path,
        runtime_dir=runtime_dir,
        runtime_token=runtime_token,
        repo_root=repo_root,
        post_id=post_id,
        track_label="Intro music",
        runtime_filename_base="intro-music",
        default_extension=".mp3",
        required=False,
    )

    outro_track = _prepare_overlay_audio_track(
        configured_path=settings.video_outro_audio_path,
        runtime_dir=runtime_dir,
        runtime_token=runtime_token,
        repo_root=repo_root,
        post_id=post_id,
        track_label="Outro",
        runtime_filename_base="outro",
        default_extension=".wav",
        required=False,
    )

    return PreparedOverlayAudioAssets(
        intro_music=intro_music_track,
        outro=outro_track,
    )


def _video_stream_info_from_ffprobe(*, media_path: Path) -> dict[str, str] | None:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name,pix_fmt,color_range,avg_frame_rate,r_frame_rate",
                "-of",
                "json",
                str(media_path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        return None
    try:
        payload = json.loads(result.stdout)
        stream = payload.get("streams", [{}])[0]
        return {
            "codec_name": str(stream.get("codec_name", "")),
            "pix_fmt": str(stream.get("pix_fmt", "")),
            "color_range": str(stream.get("color_range", "")),
            "avg_frame_rate": str(stream.get("avg_frame_rate", "")),
            "r_frame_rate": str(stream.get("r_frame_rate", "")),
        }
    except (json.JSONDecodeError, IndexError, TypeError, ValueError):
        return None


def _frame_rate_to_float(raw_frame_rate: str) -> float | None:
    normalized = raw_frame_rate.strip()
    if not normalized:
        return None
    if "/" in normalized:
        numerator_raw, denominator_raw = normalized.split("/", maxsplit=1)
        try:
            numerator = float(numerator_raw)
            denominator = float(denominator_raw)
        except ValueError:
            return None
        if denominator == 0:
            return None
        return numerator / denominator
    try:
        return float(normalized)
    except ValueError:
        return None


def _normalize_pixel_format(
    *,
    media_path: Path,
    target_pixel_format: str,
    target_color_range: str,
    crf: int,
) -> bool:
    corrected_path = media_path.with_suffix(".corrected.mp4")
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(media_path),
                "-c:v",
                "libx264",
                "-crf",
                str(crf),
                "-vf",
                f"format={target_pixel_format}",
                "-pix_fmt",
                target_pixel_format,
                "-color_range",
                target_color_range,
                "-movflags",
                "+faststart",
                "-c:a",
                "copy",
                str(corrected_path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (subprocess.SubprocessError, FileNotFoundError) as exc:
        LOGGER.warning("Pixel format normalization failed: %s", exc)
        corrected_path.unlink(missing_ok=True)
        return False

    if not corrected_path.exists() or corrected_path.stat().st_size < 1000:
        LOGGER.warning("Pixel format normalization produced empty or missing output")
        corrected_path.unlink(missing_ok=True)
        return False

    media_path.unlink(missing_ok=True)
    corrected_path.rename(media_path)
    LOGGER.info(
        "Normalized video stream to pix_fmt=%s color_range=%s for %s",
        target_pixel_format,
        target_color_range,
        media_path.name,
    )
    return True


def _normalize_runtime_video_asset(
    *,
    input_path: Path,
    output_path: Path,
    crf: int,
    target_fps: int = 30,
) -> bool:
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(input_path),
                "-vf",
                f"fps={target_fps},scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p",
                "-c:v",
                "libx264",
                "-crf",
                str(crf),
                "-pix_fmt",
                "yuv420p",
                "-color_range",
                "tv",
                "-movflags",
                "+faststart",
                "-an",
                str(output_path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (subprocess.SubprocessError, FileNotFoundError) as exc:
        LOGGER.warning("Runtime source video normalization failed input=%s error=%s", input_path, exc)
        output_path.unlink(missing_ok=True)
        return False

    if not output_path.exists() or output_path.stat().st_size < 1000:
        LOGGER.warning("Runtime source video normalization produced empty output input=%s", input_path)
        output_path.unlink(missing_ok=True)
        return False
    return True


def _is_render_stream_compliant(
    *,
    stream_info: dict[str, str] | None,
    target_pixel_format: str,
    target_color_range: str,
) -> tuple[bool, list[str]]:
    if not stream_info:
        return False, ["stream_missing"]

    reasons: list[str] = []
    actual_pixel_format = str(stream_info.get("pix_fmt") or "").strip().lower()
    actual_color_range = str(stream_info.get("color_range") or "").strip().lower()
    if actual_pixel_format != target_pixel_format.strip().lower():
        reasons.append(f"pix_fmt={actual_pixel_format or 'missing'}")
    color_range_ok = (
        actual_color_range == target_color_range.strip().lower()
        or (not actual_color_range and actual_pixel_format == "yuv420p")
    )
    if not color_range_ok:
        reasons.append(f"color_range={actual_color_range or 'missing'}")
    return len(reasons) == 0, reasons


def _enforce_render_stream_compliance(
    *,
    media_path: Path,
    post_id: str,
    target_pixel_format: str,
    target_color_range: str,
    crf: int,
) -> tuple[dict[str, str] | None, str | None]:
    stream_info = _video_stream_info_from_ffprobe(media_path=media_path)
    LOGGER.info(
        "Rendered stream probe post_id=%s pix_fmt=%s color_range=%s codec=%s avg_frame_rate=%s",
        post_id,
        (stream_info or {}).get("pix_fmt", "missing"),
        (stream_info or {}).get("color_range", "missing"),
        (stream_info or {}).get("codec_name", "missing"),
        (stream_info or {}).get("avg_frame_rate", "missing"),
    )
    stream_compliant, mismatch_reasons = _is_render_stream_compliant(
        stream_info=stream_info,
        target_pixel_format=target_pixel_format,
        target_color_range=target_color_range,
    )
    if stream_compliant:
        return stream_info, None

    LOGGER.warning(
        "Rendered stream mismatch for post_id=%s expected_pix_fmt=%s expected_color_range=%s reasons=%s; attempting normalization",
        post_id,
        target_pixel_format,
        target_color_range,
        ",".join(mismatch_reasons) or "unknown",
    )
    normalized = _normalize_pixel_format(
        media_path=media_path,
        target_pixel_format=target_pixel_format,
        target_color_range=target_color_range,
        crf=crf,
    )
    if not normalized:
        return stream_info, "Rendered video stream normalization failed"

    normalized_stream_info = _video_stream_info_from_ffprobe(media_path=media_path)
    LOGGER.info(
        "Post-normalization stream probe post_id=%s pix_fmt=%s color_range=%s codec=%s avg_frame_rate=%s",
        post_id,
        (normalized_stream_info or {}).get("pix_fmt", "missing"),
        (normalized_stream_info or {}).get("color_range", "missing"),
        (normalized_stream_info or {}).get("codec_name", "missing"),
        (normalized_stream_info or {}).get("avg_frame_rate", "missing"),
    )
    normalized_ok, normalized_reasons = _is_render_stream_compliant(
        stream_info=normalized_stream_info,
        target_pixel_format=target_pixel_format,
        target_color_range=target_color_range,
    )
    if (
        not normalized_ok
        and normalized_reasons
        and set(normalized_reasons) == {"color_range=missing"}
        and str((normalized_stream_info or {}).get("pix_fmt", "")).strip().lower() == target_pixel_format
    ):
        LOGGER.warning(
            "Treating missing color_range as acceptable for post_id=%s because pix_fmt=%s is compliant",
            post_id,
            target_pixel_format,
        )
        normalized_ok = True
    if not normalized_ok:
        return normalized_stream_info, (
            "Rendered video stream non-compliant after normalization: "
            + ",".join(normalized_reasons)
        )
    return normalized_stream_info, None


def _media_candidate_fields(candidate: MediaCandidate | dict[str, Any]) -> tuple[str, str]:
    if isinstance(candidate, dict):
        return str(candidate.get("media_type", "")), str(candidate.get("media_url", ""))
    return candidate.media_type, candidate.media_url


def _prepare_render_post_title(post_title: str, *, max_chars: int = MAX_RENDER_TITLE_CHARS) -> str:
    normalized = re.sub(r"\s+", " ", post_title).strip(" \t\r\n-:;,.")
    if not normalized:
        return "News Update"
    if len(normalized) <= max_chars:
        return normalized
    return f"{normalized[: max(0, max_chars - 3)].rstrip()}..."


def _prepare_runtime_media(
    *,
    runtime_dir: Path,
    runtime_token: str,
    media_candidates: list[MediaCandidate] | list[dict[str, Any]],
    post_image_url: str | None,
    timeout_seconds: int,
    max_images: int,
    video_crf: int,
) -> RuntimeMediaResult:
    video_relative_path: str | None = None
    image_relative_paths: list[str] = []
    normalized_candidates: list[tuple[str, str]] = []
    reject_reasons: list[str] = []

    for candidate in media_candidates:
        media_type, media_url = _media_candidate_fields(candidate)
        if not media_url.startswith(("http://", "https://")):
            continue
        normalized_candidates.append((media_type, media_url))

    image_candidates_attempted = 0
    image_download_failures = 0
    image_usability_rejects = 0

    for media_type, media_url in normalized_candidates:
        if media_type != "image":
            continue
        if len(image_relative_paths) >= max_images:
            continue
        image_candidates_attempted += 1
        try:
            payload_bytes = _download_bytes(url=media_url, timeout_seconds=timeout_seconds)
        except requests.RequestException as dl_err:
            image_download_failures += 1
            reject_detail = f"download_failed:{type(dl_err).__name__}"
            reject_reasons.append(reject_detail)
            LOGGER.warning(
                "Runtime image download failed token=%s url=%s error=%s",
                runtime_token, media_url, type(dl_err).__name__,
            )
            continue
        if not _is_usable_image_candidate(media_url=media_url, payload_bytes=payload_bytes):
            image_usability_rejects += 1
            reject_detail = "low_value_url" if is_low_value_image_url(media_url) else f"too_small({len(payload_bytes)})" if len(payload_bytes) < MIN_IMAGE_PAYLOAD_BYTES else "unsupported_format"
            reject_reasons.append(reject_detail)
            LOGGER.info(
                "Runtime image rejected token=%s url=%s reason=%s bytes=%d",
                runtime_token, media_url, reject_detail, len(payload_bytes),
            )
            continue

        extension = _image_extension_from_url(media_url)
        filename = f"source-image-{len(image_relative_paths) + 1}{extension}"
        (runtime_dir / filename).write_bytes(payload_bytes)
        image_relative_paths.append(f"runtime/{runtime_token}/{filename}")

    for media_type, media_url in normalized_candidates:
        if media_type != "video" or video_relative_path is not None:
            continue
        try:
            payload_bytes = _download_bytes(url=media_url, timeout_seconds=timeout_seconds)
        except requests.RequestException:
            continue
        if not _is_likely_supported_video_bytes(payload_bytes):
            continue

        extension = _video_extension_from_url(media_url)
        raw_filename = f"source-video-raw{extension}"
        raw_path = runtime_dir / raw_filename
        raw_path.write_bytes(payload_bytes)
        normalized_filename = "source-video.mp4"
        normalized_path = runtime_dir / normalized_filename
        normalized_ok = _normalize_runtime_video_asset(
            input_path=raw_path,
            output_path=normalized_path,
            crf=video_crf if video_crf > 0 else 18,
            target_fps=30,
        )
        raw_path.unlink(missing_ok=True)
        if not normalized_ok:
            reject_reasons.append("video_normalization_failed")
            continue
        video_relative_path = f"runtime/{runtime_token}/{normalized_filename}"
        break

    fallback_used = False
    if not image_relative_paths and post_image_url:
        try:
            fallback_image_bytes = _download_bytes(url=post_image_url, timeout_seconds=timeout_seconds)
            if not _is_likely_supported_image_bytes(fallback_image_bytes):
                reject_reasons.append(f"fallback_unsupported_format({len(fallback_image_bytes)})")
                LOGGER.warning(
                    "Runtime post-image fallback unsupported format token=%s url=%s bytes=%d",
                    runtime_token, post_image_url, len(fallback_image_bytes),
                )
            else:
                fallback_extension = _image_extension_from_url(post_image_url)
                fallback_filename = f"post-image{fallback_extension}"
                (runtime_dir / fallback_filename).write_bytes(fallback_image_bytes)
                image_relative_paths.append(f"runtime/{runtime_token}/{fallback_filename}")
                fallback_used = True
        except requests.RequestException as fb_err:
            reject_reasons.append(f"fallback_download_failed:{type(fb_err).__name__}")
            LOGGER.warning(
                "Runtime post-image fallback download failed token=%s url=%s error=%s",
                runtime_token, post_image_url, type(fb_err).__name__,
            )

    has_visuals = bool(image_relative_paths) or bool(video_relative_path)
    log_fn = LOGGER.info if has_visuals else LOGGER.warning
    log_fn(
        "Runtime media summary token=%s images=%d video=%s fallback_used=%s "
        "attempted=%d download_failures=%d usability_rejects=%d has_visuals=%s reject_reasons=%s",
        runtime_token, len(image_relative_paths), video_relative_path is not None,
        fallback_used, image_candidates_attempted, image_download_failures,
        image_usability_rejects, has_visuals, ",".join(reject_reasons) or "none",
    )
    return RuntimeMediaResult(
        video_relative_path=video_relative_path,
        image_relative_paths=image_relative_paths,
        image_candidates_attempted=image_candidates_attempted,
        image_download_failures=image_download_failures,
        image_usability_rejects=image_usability_rejects,
        fallback_used=fallback_used,
        reject_reasons=reject_reasons,
    )


def _build_caption_cues(
    *,
    script_text: str,
    duration_seconds: float,
    intro_duration_seconds: float,
    words_per_line: int,
) -> list[dict[str, float | str]]:
    words = [word for word in script_text.strip().split() if word]
    safe_words_per_line = max(1, words_per_line)
    if not words:
        return []
    lines = [
        " ".join(words[start_idx : start_idx + safe_words_per_line])
        for start_idx in range(0, len(words), safe_words_per_line)
    ]
    spoken_window_seconds = max(0.6, duration_seconds - max(0.0, intro_duration_seconds))
    segment_seconds = spoken_window_seconds / max(1, len(lines))
    cues: list[dict[str, float | str]] = []
    for idx, line in enumerate(lines):
        start_sec = intro_duration_seconds + (idx * segment_seconds)
        end_sec = min(duration_seconds, intro_duration_seconds + ((idx + 1) * segment_seconds))
        cues.append(
            {
                "startSec": round(start_sec, 3),
                "endSec": round(max(start_sec + 0.25, end_sec), 3),
                "text": line,
            }
        )
    return cues


_MOUTH_CUE_MERGE_GAP_SECONDS = 0.08
_CJK_VOICED_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf\w]")
_CJK_CHAR_RE_VIDEO = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]")
_SYLLABLE_OPEN_RATIO = 0.72
_MIN_SYLLABLE_OPEN_SECONDS = 0.08
_MIN_CUE_HOLD_SECONDS = 0.10
_MIN_CLOSED_GAP_SECONDS = 0.07


def _build_mouth_cues_from_alignment(
    *,
    alignment_payload: dict[str, Any],
    voice_start_seconds: float,
) -> list[dict[str, float]]:
    characters, starts, ends = _extract_character_timing(alignment_payload)
    if not characters:
        return []

    voiced_intervals: list[tuple[float, float]] = []
    is_cjk_dominant = sum(1 for c in characters if _CJK_CHAR_RE_VIDEO.match(c)) > len(characters) * 0.3

    for idx, char in enumerate(characters):
        if not _CJK_VOICED_RE.match(char):
            continue
        start = starts[idx]
        end = ends[idx]
        if start is None or end is None or end <= start:
            continue
        voiced_intervals.append((start, end))

    if not voiced_intervals:
        return []

    voiced_intervals.sort(key=lambda interval: interval[0])

    if is_cjk_dominant:
        offset = voice_start_seconds
        raw_cues: list[tuple[float, float]] = []
        for start, end in voiced_intervals:
            duration = end - start
            open_duration = max(_MIN_SYLLABLE_OPEN_SECONDS, duration * _SYLLABLE_OPEN_RATIO)
            cue_end = start + min(open_duration, duration)
            raw_cues.append((round(start + offset, 3), round(cue_end + offset, 3)))

        if not raw_cues:
            return []
        merged_cjk: list[tuple[float, float]] = [raw_cues[0]]
        for cue_start, cue_end in raw_cues[1:]:
            prev_start, prev_end = merged_cjk[-1]
            if cue_start - prev_end <= _MIN_CLOSED_GAP_SECONDS:
                merged_cjk[-1] = (prev_start, max(prev_end, cue_end))
            else:
                merged_cjk.append((cue_start, cue_end))

        return [
            {"startSec": s, "endSec": e}
            for s, e in merged_cjk
            if e - s >= _MIN_CUE_HOLD_SECONDS
        ]

    merged: list[tuple[float, float]] = [voiced_intervals[0]]
    for start, end in voiced_intervals[1:]:
        prev_start, prev_end = merged[-1]
        if start - prev_end <= _MOUTH_CUE_MERGE_GAP_SECONDS:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))

    offset = voice_start_seconds
    return [
        {"startSec": round(start + offset, 3), "endSec": round(end + offset, 3)}
        for start, end in merged
    ]


def _compute_render_timeline(
    *,
    audio_track_duration_seconds: float,
    configured_intro_duration_seconds: float,
    intro_music_duration_seconds: float | None,
    outro_duration_seconds: float | None,
) -> tuple[float, float, float, float]:
    safe_audio_track_seconds = max(1.0, round(audio_track_duration_seconds, 2))
    safe_outro_seconds = max(0.0, round(outro_duration_seconds or 0.0, 3))

    safe_intro_music_seconds = max(0.0, round(intro_music_duration_seconds or 0.0, 3))
    if safe_intro_music_seconds > 0:
        intro_duration_seconds = safe_intro_music_seconds
        voice_start_seconds = intro_duration_seconds
    else:
        intro_duration_seconds = max(0.0, round(configured_intro_duration_seconds, 3))
        voice_start_seconds = 0.0
    voice_end_seconds = round(voice_start_seconds + safe_audio_track_seconds, 3)
    outro_start_seconds = voice_end_seconds
    render_duration_seconds = round(max(1.0, outro_start_seconds + safe_outro_seconds), 2)
    return intro_duration_seconds, voice_start_seconds, outro_start_seconds, render_duration_seconds


def _srt_timestamp(raw_seconds: float) -> str:
    safe_seconds = max(0.0, raw_seconds)
    total_ms = int(round(safe_seconds * 1000))
    hours = total_ms // 3_600_000
    minutes = (total_ms % 3_600_000) // 60_000
    seconds = (total_ms % 60_000) // 1000
    milliseconds = total_ms % 1000
    return f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"


def _caption_cues_to_srt(cues: list[dict[str, float | str]]) -> str:
    rows: list[str] = []
    for idx, cue in enumerate(cues, start=1):
        start_sec = float(cue["startSec"])
        end_sec = float(cue["endSec"])
        text = str(cue["text"]).strip()
        if not text:
            continue
        text_en = str(cue.get("textEn", "")).strip()
        rows.append(str(idx))
        rows.append(f"{_srt_timestamp(start_sec)} --> {_srt_timestamp(end_sec)}")
        rows.append(text)
        if text_en:
            rows.append(text_en)
        rows.append("")
    return "\n".join(rows).strip() + "\n"


def generate_fish_lipsync_video(
    *,
    settings: Settings,
    post_id: str,
    audio_url: str | None,
    post_image_url: str | None,
    audio_duration_sec: float | None,
    post_title: str,
    date_label: str,
    script_text: str,
    media_candidates: list[MediaCandidate] | list[dict[str, Any]],
    voice_alignment: dict[str, Any] | None = None,
    script_10s_en: str = "",
) -> VideoAssetResult:
    if not settings.enable_video_render:
        return VideoAssetResult(
            status="skipped",
            video_url=None,
            subtitle_url=None,
            video_duration_sec=None,
            error="Video render disabled",
        )
    if not audio_url:
        return VideoAssetResult(
            status="failed",
            video_url=None,
            subtitle_url=None,
            video_duration_sec=None,
            error="Audio URL missing",
        )

    runtime_dir: Path | None = None
    output_path: Path | None = None
    try:
        repo_root = Path(__file__).resolve().parent.parent
        npm_executable = _resolve_node_tool("npm", configured_executable=settings.video_npm_executable)
        npx_executable = _resolve_node_tool("npx", configured_executable=settings.video_npx_executable)
        _assert_node20_toolchain(npx_executable=npx_executable)
        project_dir = _ensure_static_assets(settings=settings)
        _ensure_node_modules(project_dir=project_dir, npm_executable=npm_executable)
        browser_executable = _resolve_browser_executable(settings=settings)
        _run_remotion_preflight_once(
            project_dir=project_dir,
            npx_executable=npx_executable,
            browser_executable=browser_executable,
        )

        runtime_token = _safe_post_id(post_id)
        runtime_dir = project_dir / "public" / "runtime" / runtime_token
        runtime_dir.mkdir(parents=True, exist_ok=True)

        audio_bytes = _download_bytes(url=audio_url, timeout_seconds=settings.request_timeout_seconds)
        audio_relative_path = f"runtime/{runtime_token}/audio.mp3"
        runtime_audio_path = runtime_dir / "audio.mp3"
        runtime_audio_path.write_bytes(audio_bytes)

        media_result = _prepare_runtime_media(
            runtime_dir=runtime_dir,
            runtime_token=runtime_token,
            media_candidates=media_candidates,
            post_image_url=post_image_url,
            timeout_seconds=settings.request_timeout_seconds,
            max_images=max(1, settings.video_media_max_images),
            video_crf=settings.video_crf,
        )
        media_video_path = media_result.video_relative_path
        media_image_paths = media_result.image_relative_paths

        has_any_visual = bool(media_image_paths) or bool(media_video_path)
        if settings.video_require_image_media and not has_any_visual:
            LOGGER.warning(
                "Aborting render for post_id=%s reason=no_usable_visuals_after_download "
                "candidates=%d post_image_url=%s attempted=%d download_failures=%d "
                "usability_rejects=%d reject_reasons=%s",
                post_id,
                len(media_candidates),
                post_image_url is not None,
                media_result.image_candidates_attempted,
                media_result.image_download_failures,
                media_result.image_usability_rejects,
                ",".join(media_result.reject_reasons) or "none",
            )
            return VideoAssetResult(
                status="failed",
                video_url=None,
                subtitle_url=None,
                video_duration_sec=None,
                error="No usable image or video media survived download and filtering",
            )

        overlay_audio_assets = _prepare_overlay_audio_assets(
            settings=settings,
            runtime_dir=runtime_dir,
            runtime_token=runtime_token,
            repo_root=repo_root,
            post_id=post_id,
        )
        intro_music_relative_path = (
            overlay_audio_assets.intro_music.relative_path if overlay_audio_assets.intro_music else None
        )
        intro_music_duration_seconds = (
            overlay_audio_assets.intro_music.duration_seconds if overlay_audio_assets.intro_music else None
        )
        outro_audio_relative_path = (
            overlay_audio_assets.outro.relative_path if overlay_audio_assets.outro else None
        )
        outro_duration_seconds = (
            overlay_audio_assets.outro.duration_seconds if overlay_audio_assets.outro else 0.0
        )

        output_dir = project_dir / "out"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{runtime_token}.mp4"

        measured_audio_duration = _duration_from_ffprobe(media_path=runtime_audio_path)
        if measured_audio_duration and measured_audio_duration > 0:
            audio_track_duration_seconds = measured_audio_duration
        elif audio_duration_sec and audio_duration_sec > 0:
            audio_track_duration_seconds = audio_duration_sec
            LOGGER.warning(
                "Using upstream audio duration fallback for post_id=%s because runtime audio probe failed",
                post_id,
            )
        else:
            audio_track_duration_seconds = 17.0
            LOGGER.warning(
                "Using default fallback duration for post_id=%s because all duration probes failed",
                post_id,
            )
        (
            intro_duration_seconds,
            voice_start_seconds,
            outro_start_seconds,
            render_duration_seconds,
        ) = _compute_render_timeline(
            audio_track_duration_seconds=audio_track_duration_seconds,
            configured_intro_duration_seconds=settings.video_intro_duration_seconds,
            intro_music_duration_seconds=intro_music_duration_seconds,
            outro_duration_seconds=outro_duration_seconds,
        )
        LOGGER.info(
            "Render audio timeline post_id=%s intro_path=%s intro_duration=%.3fs "
            "voice_start=%.3fs outro_path=%s outro_duration=%.3fs outro_start=%.3fs "
            "intro_volume=%.3f outro_volume=%.3f",
            post_id,
            overlay_audio_assets.intro_music.source_path if overlay_audio_assets.intro_music else "none",
            intro_music_duration_seconds or 0.0,
            voice_start_seconds,
            overlay_audio_assets.outro.source_path if overlay_audio_assets.outro else "none",
            outro_duration_seconds,
            outro_start_seconds,
            settings.video_intro_music_volume,
            settings.video_outro_volume,
        )

        script_word_count = len(script_text.strip().split()) if script_text.strip() else 0
        if script_word_count > 0:
            expected_min_duration = script_word_count / 3.5
            if audio_track_duration_seconds < expected_min_duration * 0.6:
                LOGGER.warning(
                    "Audio duration %.2fs is suspiciously short for %d-word script "
                    "(expected >=%.1fs at ~3.5 words/sec) for post_id=%s; "
                    "TTS may have truncated the audio",
                    audio_track_duration_seconds,
                    script_word_count,
                    expected_min_duration,
                    post_id,
                )

        caption_offset_seconds = max(-2.0, min(settings.video_caption_offset_seconds, audio_track_duration_seconds * 0.5))
        story_caption_offset_seconds = round(voice_start_seconds + caption_offset_seconds, 3)
        caption_cues: list[dict[str, float | str]] = []
        if settings.video_caption_alignment_enabled and settings.video_caption_alignment_provider == "elevenlabs":
            if voice_alignment:
                caption_cues = build_aligned_caption_cues(
                    script_text=script_text,
                    alignment_payload=voice_alignment,
                    intro_duration_seconds=story_caption_offset_seconds,
                    words_per_line=settings.video_caption_words_per_line,
                    max_duration_seconds=render_duration_seconds,
                    pause_gap_seconds=settings.video_caption_pause_gap_seconds,
                    max_cue_duration_seconds=settings.video_caption_max_cue_duration_seconds,
                    min_cue_duration_seconds=settings.video_caption_min_cue_duration_seconds,
                    max_words_per_cue=settings.video_caption_max_words_per_cue,
                    min_alignment_coverage=settings.video_caption_min_alignment_coverage,
                    english_text=script_10s_en,
                    max_en_words_per_cue=settings.video_caption_max_en_words_per_cue,
                )
                if caption_cues and script_word_count > 0:
                    caption_word_count = sum(len(str(cue["text"]).split()) for cue in caption_cues)
                    coverage_ratio = caption_word_count / script_word_count
                    if coverage_ratio < 0.9:
                        LOGGER.warning(
                            "Aligned captions cover only %d/%d words (%.0f%%) for post_id=%s; "
                            "falling back to heuristic timing to ensure full script is shown",
                            caption_word_count,
                            script_word_count,
                            coverage_ratio * 100,
                            post_id,
                        )
                        caption_cues = []
                if not caption_cues:
                    LOGGER.warning(
                        "Aligned caption build returned no cues for post_id=%s; falling back to heuristic timing",
                        post_id,
                    )
            else:
                LOGGER.warning("Caption alignment enabled but no alignment payload for post_id=%s", post_id            )
        if not caption_cues:
            caption_cues = _build_caption_cues(
                script_text=script_text,
                duration_seconds=render_duration_seconds,
                intro_duration_seconds=story_caption_offset_seconds,
                words_per_line=settings.video_caption_words_per_line,
            )
            if script_10s_en and caption_cues:
                caption_cues = _distribute_english_across_cues(
                    english_text=script_10s_en,
                    chinese_cues=caption_cues,
                    max_en_words_per_cue=settings.video_caption_max_en_words_per_cue,
                )

        backgrounds_dir = project_dir / "public" / "backgrounds"
        background_count = 0
        if backgrounds_dir.exists():
            background_count = len([f for f in backgrounds_dir.iterdir() if f.suffix.lower() in {".png", ".jpg", ".jpeg"}])
        background_index = random.randint(0, max(0, background_count - 1)) if background_count > 0 else 0
        LOGGER.info("Selected background index %d/%d for post_id=%s", background_index, background_count, post_id)

        mouth_cues: list[dict[str, float]] = []
        if voice_alignment:
            mouth_cues = _build_mouth_cues_from_alignment(
                alignment_payload=voice_alignment,
                voice_start_seconds=voice_start_seconds,
            )
            LOGGER.info(
                "Built %d mouth cues from alignment for post_id=%s",
                len(mouth_cues),
                post_id,
            )

        render_props = {
            "audioPath": audio_relative_path,
            "backgroundIndex": background_index,
            "fishFlipped": random.choice([True, False]),
            "postImagePath": media_image_paths[0] if media_image_paths else None,
            "durationInSeconds": render_duration_seconds,
            "sensitivity": settings.video_sensitivity,
            "freqStart": settings.video_freq_start,
            "freqEnd": settings.video_freq_end,
            "fishX": settings.video_fish_x,
            "fishY": settings.video_fish_y,
            "fishScale": settings.video_fish_scale,
            "bgX": settings.video_bg_x,
            "bgY": settings.video_bg_y,
            "bgScale": settings.video_bg_scale,
            "postImageY": settings.video_post_image_y,
            "postImageScale": settings.video_post_image_scale,
            "showDebug": settings.video_show_debug,
            "postTitle": _prepare_render_post_title(post_title),
            "dateLabel": date_label,
            "scriptText": script_text,
            "introDurationSeconds": intro_duration_seconds,
            "voiceStartSeconds": voice_start_seconds,
            "introMusicPath": intro_music_relative_path,
            "introMusicVolume": settings.video_intro_music_volume,
            "outroAudioPath": outro_audio_relative_path,
            "outroStartSeconds": outro_start_seconds,
            "outroVolume": settings.video_outro_volume,
            "captionY": settings.video_caption_y,
            "captionsEnabled": settings.video_captions_enabled,
            "captionCues": caption_cues,
            "mediaVideoPath": media_video_path,
            "mediaImagePaths": media_image_paths,
            "mediaDisplaySeconds": settings.video_media_display_seconds,
            "mouthCues": mouth_cues,
        }
        base_render_command = [
            npx_executable,
            "remotion",
            "render",
            "FishLipSync",
            str(output_path),
            "--props",
            json.dumps(render_props),
            "--codec",
            settings.video_codec,
            "--pixel-format",
            settings.video_pixel_format,
            "--audio-bitrate",
            settings.video_audio_bitrate,
        ]
        if settings.video_crf > 0:
            base_render_command.extend(["--crf", str(settings.video_crf)])
        elif settings.video_bitrate.strip():
            base_render_command.extend(["--video-bitrate", settings.video_bitrate])
        configured_render_concurrency = settings.video_render_concurrency if settings.video_render_concurrency > 0 else None
        attempt_specs: list[dict[str, Any]] = [
            {
                "port": None,
                "browser_executable": browser_executable,
                "concurrency": configured_render_concurrency,
            },
        ]
        if browser_executable and browser_executable != DEFAULT_CHROME_EXECUTABLE and Path(DEFAULT_CHROME_EXECUTABLE).exists():
            attempt_specs.append(
                {
                    "port": None,
                    "browser_executable": DEFAULT_CHROME_EXECUTABLE,
                    "concurrency": configured_render_concurrency,
                }
            )
        base_port = 3200 + (sum(ord(character) for character in post_id) % 2000)
        attempt_specs.append(
            {
                "port": base_port,
                "browser_executable": browser_executable,
                "concurrency": 1,
            }
        )
        if browser_executable and browser_executable != DEFAULT_CHROME_EXECUTABLE and Path(DEFAULT_CHROME_EXECUTABLE).exists():
            attempt_specs.append(
                {
                    "port": base_port + 1,
                    "browser_executable": DEFAULT_CHROME_EXECUTABLE,
                    "concurrency": 1,
                }
            )

        last_render_error: subprocess.CalledProcessError | None = None
        for attempt_index, attempt_spec in enumerate(attempt_specs, start=1):
            render_command = [*base_render_command]
            configured_port = attempt_spec["port"]
            if configured_port is not None:
                render_command.extend(["--port", str(configured_port)])
            configured_browser = attempt_spec["browser_executable"]
            configured_concurrency = attempt_spec["concurrency"]
            if configured_browser:
                render_command.extend(["--browser-executable", configured_browser])
            if configured_concurrency:
                render_command.extend(["--concurrency", str(configured_concurrency)])
            try:
                subprocess.run(
                    render_command,
                    cwd=str(project_dir),
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=600,
                )
                last_render_error = None
                break
            except subprocess.CalledProcessError as process_error:
                last_render_error = process_error
                stderr_tail = (process_error.stderr or "").strip()[-1200:]
                stdout_tail = (process_error.stdout or "").strip()[-1200:]
                transient_failure = _is_transient_render_failure(stderr=stderr_tail, stdout=stdout_tail)
                has_more_attempts = attempt_index < len(attempt_specs)
                LOGGER.warning(
                    "Remotion render attempt %s/%s failed for post_id=%s transient=%s browser=%s concurrency=%s port=%s stderr_tail=%s",
                    attempt_index,
                    len(attempt_specs),
                    post_id,
                    transient_failure,
                    configured_browser or "auto",
                    configured_concurrency or "auto",
                    configured_port if configured_port is not None else "auto",
                    stderr_tail.replace("\n", " ")[:240],
                )
                if transient_failure and has_more_attempts:
                    continue
                raise
        if last_render_error is not None:
            raise last_render_error

        if not output_path.exists():
            return VideoAssetResult(
                status="failed",
                video_url=None,
                subtitle_url=None,
                video_duration_sec=None,
                error="Remotion render finished without output file",
            )

        stream_info, compliance_error = _enforce_render_stream_compliance(
            media_path=output_path,
            post_id=post_id,
            target_pixel_format=settings.video_pixel_format,
            target_color_range="tv",
            crf=settings.video_crf if settings.video_crf > 0 else 18,
        )
        if compliance_error:
            return VideoAssetResult(
                status="failed",
                video_url=None,
                subtitle_url=None,
                video_duration_sec=None,
                error=compliance_error,
            )
        if stream_info:
            fps_value = _frame_rate_to_float(stream_info.get("avg_frame_rate", ""))
            if stream_info.get("codec_name") and stream_info["codec_name"] != settings.video_codec:
                LOGGER.warning(
                    "Rendered codec mismatch for post_id=%s expected=%s actual=%s",
                    post_id,
                    settings.video_codec,
                    stream_info["codec_name"],
                )
            if fps_value and abs(fps_value - 30.0) > 0.1:
                LOGGER.warning(
                    "Rendered frame rate drift for post_id=%s expected=30 actual=%.3f (avg_frame_rate=%s)",
                    post_id,
                    fps_value,
                    stream_info.get("avg_frame_rate", ""),
                )

        video_bytes = output_path.read_bytes()
        object_path = f"wj/{post_id}.mp4"
        video_url = _upload_bytes_to_supabase_storage(
            supabase_url=settings.supabase_url,
            supabase_service_role_key=settings.supabase_service_role_key,
            bucket_name=settings.supabase_video_bucket,
            object_path=object_path,
            payload_bytes=video_bytes,
            content_type="video/mp4",
            timeout_seconds=settings.request_timeout_seconds,
        )

        subtitle_url: str | None = None
        if caption_cues:
            subtitle_object_path = f"wj/{post_id}.srt"
            subtitle_url = _upload_bytes_to_supabase_storage(
                supabase_url=settings.supabase_url,
                supabase_service_role_key=settings.supabase_service_role_key,
                bucket_name=settings.supabase_video_bucket,
                object_path=subtitle_object_path,
                payload_bytes=_caption_cues_to_srt(caption_cues).encode("utf-8"),
                content_type="application/x-subrip",
                timeout_seconds=settings.request_timeout_seconds,
            )

        detected_duration = _duration_from_ffprobe(media_path=output_path)
        if detected_duration is None:
            detected_duration = round(render_duration_seconds, 2)
        else:
            duration_diff = round(abs(detected_duration - render_duration_seconds), 2)
            if duration_diff > 0.1:
                LOGGER.warning(
                    "Render duration mismatch for post_id=%s expected=%.2fs video=%.2fs diff=%.2fs",
                    post_id,
                    render_duration_seconds,
                    detected_duration,
                    duration_diff,
                )
        return VideoAssetResult(
            status="generated",
            video_url=video_url,
            subtitle_url=subtitle_url,
            video_duration_sec=detected_duration,
            error=None,
        )
    except requests.RequestException as request_error:
        return VideoAssetResult(
            status="failed",
            video_url=None,
            subtitle_url=None,
            video_duration_sec=None,
            error=f"Network error during video generation: {request_error}",
        )
    except subprocess.CalledProcessError as process_error:
        stderr_tail = (process_error.stderr or "").strip()[-1200:]
        stdout_tail = (process_error.stdout or "").strip()[-1200:]
        render_details = stderr_tail or stdout_tail or str(process_error)
        return VideoAssetResult(
            status="failed",
            video_url=None,
            subtitle_url=None,
            video_duration_sec=None,
            error=f"Render process error: {render_details}",
        )
    except (subprocess.SubprocessError, FileNotFoundError) as process_error:
        return VideoAssetResult(
            status="failed",
            video_url=None,
            subtitle_url=None,
            video_duration_sec=None,
            error=f"Render process error: {process_error}",
        )
    except ValueError as config_error:
        return VideoAssetResult(
            status="failed",
            video_url=None,
            subtitle_url=None,
            video_duration_sec=None,
            error=f"Render configuration error: {config_error}",
        )
    except Exception as unknown_error:  # noqa: BLE001
        return VideoAssetResult(
            status="failed",
            video_url=None,
            subtitle_url=None,
            video_duration_sec=None,
            error=f"Unexpected video generation error: {unknown_error}",
        )
    finally:
        if runtime_dir and runtime_dir.exists():
            shutil.rmtree(runtime_dir, ignore_errors=True)
        if output_path and output_path.exists():
            output_path.unlink(missing_ok=True)
