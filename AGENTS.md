## Learned User Preferences

- Prefers single static article image per video -- no cycling or hard-cut image transitions
- Wants hyper-realistic / photorealistic character rendering with transparent backgrounds (not cartoon or stylized); clothes are 2D flat cartoon style with clean outlines
- Uses Gemini (`gemini-3-pro-image-preview`) via `GEMINI_API_KEY` for image asset generation; requires green-screen workaround (`#00FF00` bg + chroma-key removal) since Gemini cannot output true alpha
- Iterates on voice and visual assets before committing; expects rapid regeneration cycles with blunt feedback and course-correction
- For local testing, prefers standalone scripts that skip Supabase and output files directly to the repo root
- Plans should be presented before implementation; confirm before executing
- Prefers 2-state (open/closed) mouth animation over multi-frame lip-sync -- simpler is better
- Wants bilingual captions: Chinese (large) on top, English translation (smaller) below, positioned between the article image and the fish character to avoid overlap
- English captions should appear as short readable phrases (2-3 words per chunk) cycling in rhythm with Chinese cues; large unreadable blocks or missing English are both unacceptable
- Wants viseme-based lip-sync using ElevenLabs alignment data; pure amplitude-based approaches (max, RMS, EMA) were tried extensively and deemed insufficient for convincing mouth movement
- Cross-fading / opacity blending between mouth PNGs is unacceptable -- creates flickering/ghosting artifacts; use `display` switching (not `opacity`) for discrete frame swaps only
- Test renders must include full Chinese content -- English placeholder text is unacceptable even for quick previews

## Learned Workspace Facts

- Pipeline flow: World Journal scraper -> Claude script generation -> ElevenLabs TTS -> jieba caption alignment -> Remotion 4 video render -> social publish
- Content language is Traditional Mandarin Chinese (zh-TW), targeting overseas Chinese / Taiwanese audience
- GitHub repo: `EliotChang/KhoiNews` (private); GitHub username `EliotChang`
- Anchor character is a koi fish (錦鯉), upright SpongeBob-style orientation facing camera head-on with mouth at top; old pufferfish assets backed up to `mouth_backup_fish/`, previous v2 to `mouth_backup_v2/`
- Video template uses a layered composite: 2 mouth PNGs (mouth_0=closed, mouth_1=open) + desk overlay + randomly selected background
- Remotion renders each frame independently in parallel during SSR -- all animation/lip-sync logic must be purely stateless (no `useRef` for cross-frame accumulation; it silently resets every frame)
- ElevenLabs TTS provides character-level alignment data (via `/with-timestamps` endpoint); Chinese characters have zero gaps between consecutive characters -- only punctuation creates measurable pauses
- Mouth timing pipeline: ElevenLabs TTS -> `voice_gen.py` (alignment payload) -> `video_gen.py` (`_build_mouth_cues_from_alignment`) -> Remotion `mouthCues` prop -> `FishLipSync.tsx`; each Chinese character = one syllable (~200-400ms) requiring per-character open/close cycles; anti-flicker post-processing merges gaps <70ms and drops cues <100ms
- Current ElevenLabs voice: Anna Su / Trustworthy (`r6qgCCGI7RWKXCagm158`), Taiwanese Mandarin female (replaced Martin Li for lacking naturalness)
- Node 20 is required for Remotion renders; `npx`/`npm` resolved via nvm or env vars; Remotion intermediate frames use PNG (not JPEG) for transparency fidelity
- Supabase handles DB and storage; all secrets loaded from `.env` in the project root
- Script generation targets ~35s / ~130 characters; three-part structure: Hook (0-5s, ~15字) -> Easy Explanation (5-30s, ~85字, 3-5句) -> Twist (30-35s, ~15字); 5 rotating hook formulas; GHA overrides in `wj-pipeline.yml` control production budgets
