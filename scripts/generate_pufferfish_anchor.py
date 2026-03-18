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
    0: "completely closed, lips pressed tightly together",
    1: "barely open, a thin slit visible between the upper and lower lip",
    2: "slightly open, a small gap showing between upper and lower lip",
    3: "moderately open, mouth clearly open with a visible gap",
    4: "wide open, mouth opened wide",
    5: "fully open, mouth gaping as wide as possible",
}

BASE_PROMPT = (
    "A SINGLE koi fish (錦鯉) news anchor character, WIDE and ROUND body "
    "proportions — NOT tall and narrow. The character should be WIDER than it "
    "is tall, similar to how the realistic fish head news anchor in SpongeBob "
    "SquarePants looks — a big wide rounded fish head facing the camera with "
    "a short stout body. "
    "The fish faces the camera HEAD-ON. The mouth is at the TOP of the head "
    "and opens UPWARD (upper jaw hinges up, like a fish gulping at the "
    "surface). This is key — the mouth opens vertically at the top, NOT at "
    "the front. "
    "The koi HEAD and BODY are hyper-realistic and detailed — beautiful "
    "red-orange and white coloring, shimmering scales, large expressive eyes "
    "looking directly at the viewer. The body is WIDE and ROUND, taking up "
    "most of the frame width. "
    "NO makeup, NO blush, NO eyelashes, NO feminine accessories — just a "
    "natural, beautiful koi fish rendered photo-realistically. "
    "The SUIT is 2D flat cartoon style — a brown suit jacket with a white "
    "dress shirt collar and a red necktie, drawn with solid flat colors, clean "
    "outlines, and minimal shading. The suit is at the bottom, like a simple "
    "2D cartoon drawing pasted onto the character. "
    "NO fabric texture, NO realistic folds, NO painterly brushwork on the suit "
    "— just clean, simple, flat 2D shapes with solid colors. "
    "The background MUST be a single solid bright green (#00FF00) color — "
    "NO checkerboard, NO gradient, NO pattern, just pure flat green. "
    "IMPORTANT: ONE character only, centered, WIDE proportions. Keep the body, "
    "suit, tie, eyes, and overall composition IDENTICAL across all frames — "
    "only the mouth opening at the TOP changes."
)


def _load_image_bytes(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


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

    ref_fish_bytes = _load_image_bytes(MOUTH_DIR / "mouth_backup_fish" / "mouth_0.png")
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
                text="Reference image (original fish anchor — match this pose, framing, and suit style but use a KOI FISH instead of a pufferfish):"
            )
        )
        content_parts.append(ref_fish_part)

        if mouth_idx > 0 and 0 in generated_frames:
            mouth0_buf = io.BytesIO()
            generated_frames[0].save(mouth0_buf, format="PNG")
            mouth0_bytes = mouth0_buf.getvalue()
            content_parts.append(
                types.Part.from_text(
                    text="Generated koi fish mouth_0 (closed) — keep the character IDENTICAL, only change the mouth opening:"
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

        print(f"  Removing green-screen background...")
        img = _remove_green_screen(img)
        img_resized = img.resize(TARGET_SIZE, Image.LANCZOS)
        generated_frames[mouth_idx] = img_resized

        out_path = MOUTH_DIR / f"mouth_{mouth_idx}.png"
        img_resized.save(out_path, "PNG")
        print(f"  Saved {out_path} ({img_resized.size})")

    print(f"\nGeneration complete. {len(generated_frames)}/{NUM_MOUTH_STATES} frames generated.")
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
