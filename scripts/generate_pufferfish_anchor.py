#!/usr/bin/env python3
"""Generate koi fish news anchor mouth frames using Gemini image generation.

Uses Gemini 3 Pro Image Preview (Nano Banana Pro) to create 2 mouth-state PNGs
(closed / open) for the lip-sync video template, with reference-image chaining
for consistency.  Outputs transparent-background PNGs via green-screen removal.
"""

from __future__ import annotations

import io
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv
from google import genai
from google.genai import types
from PIL import Image

MOUTH_DIR = Path("pipeline/video_templates/fish_lipsync/public/mouth")
BACKUP_DIR = MOUTH_DIR / "mouth_backup_v2"
TARGET_SIZE = (1300, 1042)
MODEL = "gemini-3-pro-image-preview"
NUM_MOUTH_STATES = 2

MOUTH_STATES = {
    0: "completely closed, jaw shut tight in profile view",
    1: "slightly open, jaw dropped slightly showing a small gap on the side",
    2: "moderately open, jaw clearly dropped with a visible opening on the side",
    3: "wide open, jaw dropped wide showing the interior of the mouth from the side",
    4: "very wide open, jaw gaping from the side",
    5: "fully open, jaw dropped as far as possible in profile",
}

BASE_PROMPT = (
    "Generate ONLY a SINGLE koi fish (錦鯉) news anchor character — do NOT "
    "include the reference image in the output, do NOT place two figures "
    "side by side. Output ONE character CENTERED in the frame. "
    "The fish head is shown in SIDE PROFILE — only ONE eye visible, facing "
    "LEFT. The head is tilted FAR BACK so the mouth points STRAIGHT UP "
    "toward the ceiling / sky — like a fish looking directly upward. The "
    "mouth is at the VERY TOP of the composition. This exaggerated upward "
    "tilt gives the character a deadpan serious-but-absurd news anchor vibe. "
    "PROPORTIONS — THIS IS CRITICAL: The fish HEAD is HUGE — it takes up "
    "55-60% of the total character height. The body below is VERY SHORT — "
    "ONLY the shoulders and upper chest are visible, like a bust or a "
    "news anchor behind a desk. Do NOT draw the waist, hips, or lower "
    "torso. The body is just a SHORT WIDE STUMP of shoulders + upper chest. "
    "The shoulders are BROAD, filling about 55-60% of the frame width. The overall "
    "silhouette is TOP-HEAVY and COMPACT — a big lively fish head sitting "
    "on a short squat pair of broad shoulders. Think of the SpongeBob "
    "'Realistic Fish Head' news anchor proportions. "
    "CRITICAL — the koi fish HEAD must look like a REAL PHOTOGRAPH of a "
    "GORGEOUS, VIBRANT, SHOW-QUALITY koi fish. PHOTOREALISTIC, not "
    "illustrated, not painterly, not digital art. The coloring must be "
    "RICH and SATURATED — deep warm GOLDEN-ORANGE and fiery RED-ORANGE "
    "patches with clean bright PEARL-WHITE areas. NOT pale, NOT washed "
    "out, NOT pinkish-grey. The orange should POP — think a prize-winning "
    "Kohaku koi in crystal-clear water under sunlight. The scales should "
    "SHIMMER with iridescent light reflections. The eye must look BRIGHT, "
    "ALERT, and full of LIFE — a glossy jet-black pupil with a sharp "
    "specular highlight, giving the fish PERSONALITY and charm. The skin "
    "has a healthy wet SHEEN. Overall the fish should look ALIVE, VIBRANT, "
    "and BEAUTIFUL — the kind of fish that makes people stop scrolling. "
    "NO makeup, NO blush, NO eyelashes, NO humanization — just a "
    "stunningly beautiful living koi. "
    "The OUTFIT is 2D flat cartoon style — a modern WOMEN'S fitted blazer "
    "in COBALT BLUE (vivid saturated blue, NOT grey, NOT muted). The blazer "
    "is sleek and contemporary: clean lines, slim fit, single-button closure, "
    "structured but NOT boxy. Underneath is a simple black round-neck top. "
    "A thin delicate gold chain necklace adds a modern feminine accent. "
    "Drawn with solid flat colors, clean outlines, and minimal shading. "
    "NO fabric texture, NO realistic folds, NO painterly brushwork on the suit "
    "— just clean, simple, flat 2D shapes with solid colors. "
    "The CONTRAST between the photorealistic fish head and the flat 2D "
    "cartoon suit is INTENTIONAL and must be preserved — do NOT blend the "
    "styles together. "
    "The background MUST be a single solid bright green (#00FF00) color — "
    "NO checkerboard, NO gradient, NO pattern, just pure flat green. "
    "IMPORTANT: ONE character ONLY, CENTERED, BIG HEAD on SHORT WIDE body, "
    "side-profile fish head with mouth UP, ALWAYS facing LEFT (eye on the "
    "RIGHT side of the image). The fish must face the SAME direction in "
    "EVERY frame — NEVER mirror or flip. Keep the body, suit, and overall "
    "composition IDENTICAL across all frames — only the mouth opening changes."
)


def _load_image_bytes(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _crop_right_subject(img: Image.Image) -> Image.Image:
    """Remove reference-image bleed on the left if present, then center the subject.

    Only crops the left portion if it detects a non-green-screen region there
    (the Gemini reference photo leak). If the left side is clean green-screen,
    the full image is preserved so we don't cut off the character's shoulders.
    """
    import numpy as np

    arr = np.array(img.convert("RGBA"))
    h, w = arr.shape[:2]
    r, g, b = arr[:, :, 0].astype(float), arr[:, :, 1].astype(float), arr[:, :, 2].astype(float)

    green_bg = (g > 180) & (r < 180) & (b < 180) & (g > r + 30) & (g > b + 30)

    left_strip = green_bg[:, :int(w * 0.15)]
    left_green_ratio = left_strip.sum() / max(left_strip.size, 1)

    if left_green_ratio < 0.4:
        crop_x = int(w * 0.35)
        print(f"    Detected reference bleed on left ({left_green_ratio:.0%} green), cropping left {crop_x}px")
        arr = arr[:, crop_x:]
    else:
        print(f"    Left side is clean green-screen ({left_green_ratio:.0%} green), no crop needed")

    return Image.fromarray(arr, "RGBA")


def _remove_green_screen(img: Image.Image, tolerance: int = 80) -> Image.Image:
    """Replace green-screen background (#00FF00 ± tolerance) with true alpha."""
    import numpy as np

    arr = np.array(img.convert("RGBA"), dtype=np.float32)
    r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]

    green_mask = (g > 180) & (r < 100 + tolerance) & (b < 100 + tolerance) & (g > r + 30) & (g > b + 30)

    arr[green_mask, 3] = 0

    kernel = np.ones((3, 3), dtype=bool)
    h, w = green_mask.shape
    padded = np.pad(green_mask, 1, mode="constant", constant_values=False)
    dilated = np.zeros_like(green_mask)
    for dy in range(3):
        for dx in range(3):
            dilated |= padded[dy:dy + h, dx:dx + w]
    edge_zone = dilated & ~green_mask

    edge_ys, edge_xs = np.where(edge_zone)
    for y, x in zip(edge_ys, edge_xs):
        total = r[y, x] + g[y, x] + b[y, x]
        green_ratio = g[y, x] / max(total, 1)
        if green_ratio > 0.45:
            arr[y, x, 3] = max(0, arr[y, x, 3] * (1 - green_ratio))
            arr[y, x, 1] = arr[y, x, 1] * 0.5

    return Image.fromarray(arr.clip(0, 255).astype(np.uint8), "RGBA")


def _extract_image_from_response(response) -> Image.Image | None:
    """Pull the first image from a Gemini response."""
    for part in response.candidates[0].content.parts:
        if part.inline_data and part.inline_data.mime_type.startswith("image/"):
            return Image.open(io.BytesIO(part.inline_data.data)).convert("RGBA")
    return None


def _make_reference_part(img_bytes: bytes, mime: str = "image/png") -> types.Part:
    return types.Part.from_bytes(data=img_bytes, mime_type=mime)


def _find_eye_side(img: Image.Image) -> str:
    """Determine which side of the image the fish eye is on.

    Looks for the darkest pixel cluster in the upper 60% of the image
    (where the head is). Returns 'left' or 'right'.
    """
    import numpy as np

    arr = np.array(img.convert("RGBA"))
    h, w = arr.shape[:2]
    upper = arr[:int(h * 0.6), :, :]

    alpha = upper[:, :, 3]
    has_content = alpha > 30

    r, g, b = upper[:, :, 0].astype(float), upper[:, :, 1].astype(float), upper[:, :, 2].astype(float)
    darkness = 255 * 3 - (r + g + b)
    darkness[~has_content] = 0

    threshold = np.percentile(darkness[has_content], 95) if has_content.any() else 500
    dark_pixels = (darkness >= threshold) & has_content

    if not dark_pixels.any():
        return "left"

    dark_xs = np.where(dark_pixels)[1]
    content_xs = np.where(has_content)[1]
    if len(content_xs) == 0:
        return "left"

    content_center = (content_xs.min() + content_xs.max()) / 2
    avg_dark_x = dark_xs.mean()

    return "left" if avg_dark_x < content_center else "right"


def _ensure_consistent_direction(frames: dict[int, Image.Image]) -> dict[int, Image.Image]:
    """Flip any frames whose eye is on the opposite side from mouth_0."""
    if 0 not in frames:
        return frames

    ref_side = _find_eye_side(frames[0])
    print(f"\n  mouth_0 eye detected on: {ref_side}")

    for idx in sorted(frames.keys()):
        if idx == 0:
            continue
        side = _find_eye_side(frames[idx])
        if side != ref_side:
            print(f"  mouth_{idx} eye on {side} (expected {ref_side}) — flipping horizontally")
            frames[idx] = frames[idx].transpose(Image.FLIP_LEFT_RIGHT)
        else:
            print(f"  mouth_{idx} eye on {side} — consistent, no flip needed")

    return frames


def _composite_mouth_region(base: Image.Image, open_mouth: Image.Image, feather_px: int = 4) -> Image.Image:
    """Create an open-mouth frame by blending only the mouth pixels onto the base.

    Uses a tight pixel-level diff mask (not a bounding rectangle) so only the
    actual mouth-opening pixels come from open_mouth.  A small Gaussian feather
    softens the boundary.  Everything outside the mask is pixel-identical to base.
    """
    import numpy as np
    from PIL import ImageFilter

    base_arr = np.array(base.convert("RGBA"), dtype=np.float32)
    open_arr = np.array(open_mouth.convert("RGBA"), dtype=np.float32)

    if base_arr.shape != open_arr.shape:
        open_mouth_resized = open_mouth.resize(base.size, Image.LANCZOS)
        open_arr = np.array(open_mouth_resized.convert("RGBA"), dtype=np.float32)

    diff = np.abs(base_arr[:, :, :3] - open_arr[:, :, :3]).mean(axis=2)

    both_visible = (base_arr[:, :, 3] > 30) & (open_arr[:, :, 3] > 30)
    diff[~both_visible] = 0

    h, w = diff.shape

    threshold = max(np.percentile(diff[both_visible], 92), 30) if both_visible.any() else 30
    significant = diff > threshold

    top_region = np.zeros_like(significant)
    top_region[:int(h * 0.35), :] = True
    mouth_pixels = significant & top_region

    if mouth_pixels.sum() < 50:
        mouth_pixels = significant.copy()
        mouth_pixels[int(h * 0.5):, :] = False

    if mouth_pixels.sum() < 10:
        print("    WARNING: No significant mouth region found, returning open_mouth as-is")
        return open_mouth

    mask_arr = mouth_pixels.astype(np.uint8) * 255
    mask_img = Image.fromarray(mask_arr, "L")
    mask_img = mask_img.filter(ImageFilter.GaussianBlur(radius=feather_px))
    mask_f = np.array(mask_img, dtype=np.float32) / 255.0

    result = base_arr.copy()
    for c in range(4):
        result[:, :, c] = base_arr[:, :, c] * (1 - mask_f) + open_arr[:, :, c] * mask_f

    affected = (mask_f > 0.01).sum()
    print(f"    Mouth composite: {mouth_pixels.sum()} diff pixels, "
          f"{affected} blended pixels ({affected / max(both_visible.sum(), 1) * 100:.1f}% of visible), "
          f"feather={feather_px}px")

    return Image.fromarray(result.clip(0, 255).astype(np.uint8), "RGBA")


def backup_existing_assets():
    """Copy current mouth frames to a backup directory."""
    if BACKUP_DIR.exists():
        print(f"Backup already exists at {BACKUP_DIR}, skipping backup.")
        return
    BACKUP_DIR.mkdir(parents=True)
    for i in range(6):
        src = MOUTH_DIR / f"mouth_{i}.png"
        if src.exists():
            shutil.copy2(src, BACKUP_DIR / f"mouth_{i}.png")
            print(f"  Backed up {src.name}")
    print(f"Backup complete -> {BACKUP_DIR}")


def generate_mouth_frames():
    load_dotenv()
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GEMINI_API_KEY not set in environment or .env")
        sys.exit(1)

    client = genai.Client(api_key=api_key)

    ref_fish_bytes = _load_image_bytes(Path(__file__).parent / "puffernews_reference.png")
    ref_fish_part = _make_reference_part(ref_fish_bytes)

    generated_frames: dict[int, Image.Image] = {}

    for mouth_idx, mouth_desc in MOUTH_STATES.items():
        print(f"\n--- Generating mouth_{mouth_idx} ({mouth_desc}) ---")

        prompt = (
            f"{BASE_PROMPT}\n\n"
            f"The mouth is {mouth_desc}.\n"
        )

        content_parts: list[types.Part] = [types.Part.from_text(text=prompt)]

        content_parts.append(
            types.Part.from_text(
                text="Reference image for PROPORTIONS and STYLE ONLY — match this character's BIG HEAD, SHORT WIDE BODY proportions and vibrant fish coloring. Generate a NEW koi fish character with cobalt blue blazer, side-profile head with mouth pointing UP. Do NOT copy the background, desk, or text from this reference:"
            )
        )
        content_parts.append(ref_fish_part)

        if mouth_idx > 0 and 0 in generated_frames:
            mouth0_buf = io.BytesIO()
            generated_frames[0].save(mouth0_buf, format="PNG")
            mouth0_bytes = mouth0_buf.getvalue()
            content_parts.append(
                types.Part.from_text(
                    text="This is mouth_0 (closed). Generate the EXACT SAME character facing the EXACT SAME direction (LEFT) with the IDENTICAL body, suit, and pose — ONLY change the mouth opening. Do NOT mirror or flip the character:"
                )
            )
            content_parts.append(_make_reference_part(mouth0_bytes))

        contents = [types.Content(role="user", parts=content_parts)]
        config = types.GenerateContentConfig(
            response_modalities=["IMAGE"],
            temperature=0.4,
        )

        response = client.models.generate_content(
            model=MODEL,
            contents=contents,
            config=config,
        )

        img = _extract_image_from_response(response)
        if img is None:
            print(f"  WARNING: No image returned for mouth_{mouth_idx}, retrying with higher temperature...")
            config = types.GenerateContentConfig(
                response_modalities=["IMAGE"],
                temperature=0.8,
            )
            response = client.models.generate_content(
                model=MODEL,
                contents=contents,
                config=config,
            )
            img = _extract_image_from_response(response)

        if img is None:
            print(f"  ERROR: Failed to generate mouth_{mouth_idx} after retry. Skipping.")
            continue

        print(f"  Cropping right subject & removing green-screen...")
        img = _crop_right_subject(img)
        img = _remove_green_screen(img)
        img_resized = img.resize(TARGET_SIZE, Image.LANCZOS)
        generated_frames[mouth_idx] = img_resized

        out_path = MOUTH_DIR / f"mouth_{mouth_idx}.png"
        img_resized.save(out_path, "PNG")
        print(f"  Saved {out_path} ({img_resized.size})")

    print(f"\nGeneration complete. {len(generated_frames)}/{NUM_MOUTH_STATES} frames generated.")

    print("\nStep 3: Ensuring consistent facing direction...")
    generated_frames = _ensure_consistent_direction(generated_frames)

    if 0 in generated_frames:
        print("\nStep 4: Compositing mouth regions onto base frame...")
        for idx in sorted(generated_frames.keys()):
            if idx == 0:
                continue
            print(f"  Compositing mouth_{idx} onto mouth_0 base...")
            generated_frames[idx] = _composite_mouth_region(
                base=generated_frames[0],
                open_mouth=generated_frames[idx],
            )

    for idx, img in generated_frames.items():
        out_path = MOUTH_DIR / f"mouth_{idx}.png"
        img.save(out_path, "PNG")
        print(f"  Saved {out_path} (final)")

    if len(generated_frames) < NUM_MOUTH_STATES:
        missing = [i for i in range(NUM_MOUTH_STATES) if i not in generated_frames]
        print(f"  Missing frames: {missing}")
        print("  Re-run the script or manually create the missing frames.")


def main():
    os.chdir(Path(__file__).resolve().parent.parent)
    print("=== Koi Fish Anchor Generation ===\n")

    print("Step 1: Backing up existing mouth frames...")
    backup_existing_assets()

    print("\nStep 2: Generating koi fish mouth frames via Gemini...")
    generate_mouth_frames()

    print("\nDone! Check the mouth frames at:")
    print(f"  {MOUTH_DIR.resolve()}")
    print(f"  Backups at: {BACKUP_DIR.resolve()}")


if __name__ == "__main__":
    main()
