## Learned User Preferences

- Prefers single static article image per video -- no cycling or hard-cut image transitions
- Wants hyper-realistic / photorealistic character rendering with transparent backgrounds (not cartoon or stylized)
- Uses Gemini (`gemini-3-pro-image-preview`) via `GEMINI_API_KEY` for image asset generation; requires green-screen workaround (`#00FF00` bg + chroma-key removal) since Gemini cannot output true alpha
- Iterates on voice and visual assets before committing; expects to test multiple options
- For local testing, prefers standalone scripts that skip Supabase and output files directly to the repo root
- Plans should be presented before implementation; confirm before executing
- When adapting an existing pipeline, copy the structure first, then modify in place
- Prefers 2-state (open/closed) mouth animation over multi-frame lip-sync -- simpler is better for the pufferfish character
- Wants bilingual captions: Chinese (large) on top, English translation (smaller) below, positioned between the article image and the fish character to avoid overlap
- English captions should appear as short readable phrases (2-3 words per chunk) cycling in rhythm with Chinese cues; large unreadable blocks or missing English are both unacceptable
- Wants viseme-based lip-sync using ElevenLabs alignment data; pure amplitude-based approaches (max, RMS, EMA) were tried extensively and deemed insufficient for convincing mouth movement
- Cross-fading / opacity blending between mouth PNGs is unacceptable -- creates flickering/ghosting artifacts; use discrete frame switching only
- Test renders must include full Chinese content -- English placeholder text is unacceptable even for quick previews

## Learned Workspace Facts

- Pipeline flow: World Journal scraper -> Claude script generation -> ElevenLabs TTS -> jieba caption alignment -> Remotion 4 video render -> social publish
- Content language is Traditional Mandarin Chinese (zh-TW), targeting overseas Chinese / Taiwanese audience
- Source project was cloned and adapted from `/Users/eliotchang/Local/Github/Figment/news`; when lip-sync or defaults diverge, the source project's values are usually correct
- Anchor character is a pufferfish (fugu); old fish assets backed up to `mouth_backup_fish/`, previous v2 to `mouth_backup_v2/`
- Video template uses a layered composite: 2 mouth PNGs (mouth_0=closed, mouth_1=open) + desk overlay + randomly selected background
- Remotion renders each frame independently in parallel during SSR -- all animation/lip-sync logic must be purely stateless (no `useRef` for cross-frame accumulation; it silently resets every frame)
- ElevenLabs TTS provides character-level alignment data (via `/with-timestamps` endpoint); Chinese characters have zero gaps between consecutive characters -- only punctuation creates measurable pauses
- Mouth timing pipeline: ElevenLabs TTS -> `voice_gen.py` (alignment payload) -> `video_gen.py` (`_build_mouth_cues_from_alignment`) -> Remotion `mouthCues` prop -> `FishLipSync.tsx`; each Chinese character = one syllable (~200-400ms) requiring per-character open/close cycles
- Current ElevenLabs voice: Martin Li (`WuLq5z7nEcrhppO0ZQJw`), middle-aged male, deep serious Chinese (replaced Stella Gu for lacking depth)
- Node 20 is required for Remotion renders; `npx`/`npm` resolved via nvm or env vars; Remotion intermediate frames use PNG (not JPEG) for transparency fidelity
- Supabase handles DB and storage; all secrets loaded from `.env` in the project root
- Default `VIDEO_MEDIA_MAX_IMAGES` is 1 (single image per video)
- Script generation targets ~35s / ~140 words (`EDITORIAL_MIN_WORDS=100`, `EDITORIAL_MAX_WORDS=160`); three-part structure: Hook (0-5s, ~15w) -> Easy Explanation (5-30s, ~70w) -> Twist (30-35s, ~15w); 5 rotating hook formulas
