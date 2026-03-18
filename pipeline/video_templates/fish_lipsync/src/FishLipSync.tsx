import React from "react"
import {AbsoluteFill, Audio, Img, OffthreadVideo, Sequence, staticFile, useCurrentFrame, useVideoConfig} from "remotion"
import {useAudioData, visualizeAudio} from "@remotion/media-utils"
import {z} from "zod"

const mouthCueSchema = z.object({
  startSec: z.number(),
  endSec: z.number(),
})

const captionCueSchema = z.object({
  startSec: z.number(),
  endSec: z.number(),
  text: z.string(),
  textEn: z.string().optional(),
})

const BACKGROUNDS = [
  "backgrounds/bg_original.png",
  "backgrounds/bg_00.png",
  "backgrounds/bg_01.png",
  "backgrounds/bg_02.png",
  "backgrounds/bg_03.png",
  "backgrounds/bg_04.png",
  "backgrounds/bg_05.png",
]

export const fishLipSyncSchema = z.object({
  audioPath: z.string().default("audio/audio.mp3"),
  backgroundIndex: z.number().min(0).max(6).step(1).default(0),
  postImagePath: z.string().optional(),
  postTitle: z.string().default("News Update"),
  dateLabel: z.string().default(""),
  scriptText: z.string().default(""),
  introDurationSeconds: z.number().min(0).max(120).default(2),
  voiceStartSeconds: z.number().min(0).max(120).default(3),
  introMusicPath: z.string().optional(),
  introMusicVolume: z.number().min(0).max(1).default(0.5),
  outroAudioPath: z.string().optional(),
  outroStartSeconds: z.number().min(0).max(600).default(0),
  outroVolume: z.number().min(0).max(2).default(1.0),
  captionsEnabled: z.boolean().default(true),
  captionCues: z.array(captionCueSchema).default([]),
  mediaVideoPath: z.string().optional(),
  mediaImagePaths: z.array(z.string()).default([]),
  mediaDisplaySeconds: z.number().min(0.5).max(10).default(2.5),
  sensitivity: z.number().min(0.01).max(1).step(0.01).default(0.04),
  freqStart: z.number().min(0).max(128).step(1).default(1),
  freqEnd: z.number().min(1).max(256).step(1).default(40),
  fishX: z.number().min(-100).max(100).step(1).default(0),
  fishY: z.number().min(-100).max(100).step(0.1).default(14.3),
  fishScale: z.number().min(10).max(200).step(1).default(70),
  fishFlipped: z.boolean().default(false),
  bgX: z.number().min(-100).max(100).step(1).default(0),
  bgY: z.number().min(-100).max(100).step(1).default(0),
  bgScale: z.number().min(50).max(300).step(1).default(100),
  deskX: z.number().min(-100).max(100).step(1).default(0),
  deskY: z.number().min(-100).max(100).step(1).default(1),
  deskScale: z.number().min(50).max(300).step(1).default(110),
  captionY: z.number().min(-100).max(100).step(1).default(-15),
  postImageY: z.number().min(-100).max(100).step(1).default(-24),
  postImageScale: z.number().min(10).max(100).step(1).default(56),
  showDebug: z.boolean().default(false),
  durationInSeconds: z.number().min(1).max(120).default(17),
  mouthCues: z.array(mouthCueSchema).default([]),
})

export type FishLipSyncProps = z.infer<typeof fishLipSyncSchema>

const MOUTH_FRAMES = 2
const SILENCE_THRESHOLD = 0.003
const OPEN_THRESHOLD = 0.15
const SAMPLE_RADIUS = 3

function getAmplitudeAtFrame({
  audioData,
  fps,
  frame,
  freqStart,
  freqEnd,
}: {
  audioData: NonNullable<ReturnType<typeof useAudioData>>
  fps: number
  frame: number
  freqStart: number
  freqEnd: number
}): number {
  const visualization = visualizeAudio({
    fps,
    frame,
    audioData,
    numberOfSamples: 256,
  })
  const band = visualization.slice(freqStart, freqEnd)
  if (band.length === 0) return 0
  return Math.max(...band)
}

export function FishLipSync({
  audioPath,
  backgroundIndex,
  postImagePath,
  dateLabel,
  scriptText,
  introDurationSeconds,
  voiceStartSeconds,
  introMusicPath,
  introMusicVolume,
  outroAudioPath,
  outroStartSeconds,
  outroVolume,
  captionsEnabled,
  captionCues,
  mediaVideoPath,
  mediaImagePaths,
  mediaDisplaySeconds,
  sensitivity,
  freqStart,
  freqEnd,
  fishX,
  fishY,
  fishScale,
  fishFlipped,
  bgX,
  bgY,
  bgScale,
  deskX,
  deskY,
  deskScale,
  captionY,
  postImageY,
  postImageScale,
  showDebug,
  mouthCues,
}: FishLipSyncProps): React.ReactElement {
  const frame = useCurrentFrame()
  const {fps} = useVideoConfig()

  const safeIndex = typeof backgroundIndex === "number" ? backgroundIndex : 0
  const backgroundPath = BACKGROUNDS[safeIndex % BACKGROUNDS.length]
  const audioSrc = staticFile(audioPath)
  const audioData = useAudioData(audioSrc)
  const introFrames = Math.floor(introDurationSeconds * fps)
  const voiceStartFrames = Math.max(0, Math.floor(voiceStartSeconds * fps))
  const outroStartFrames = Math.max(0, Math.floor(outroStartSeconds * fps))
  const introMusicFrames = Math.max(1, introFrames)
  const introMusicFadeFrames = Math.max(1, Math.min(introMusicFrames, Math.floor(0.35 * fps)))

  let mouthIndex = 0
  let amplitude = 0
  let normalized = 0
  const currentTimeSec = frame / fps

  if (mouthCues.length > 0) {
    mouthIndex = mouthCues.some(
      (cue) => currentTimeSec >= cue.startSec && currentTimeSec <= cue.endSec
    ) ? 1 : 0
  } else {
    const isInVoicePhase = frame >= voiceStartFrames
    if (isInVoicePhase && audioData) {
      const localFrame = frame - voiceStartFrames
      let ampSum = 0
      const count = SAMPLE_RADIUS * 2 + 1
      for (let offset = -SAMPLE_RADIUS; offset <= SAMPLE_RADIUS; offset++) {
        ampSum += getAmplitudeAtFrame({audioData, fps, frame: Math.max(0, localFrame + offset), freqStart, freqEnd})
      }
      amplitude = ampSum / count

      if (amplitude >= SILENCE_THRESHOLD) {
        normalized = Math.min(amplitude / sensitivity, 1)
        mouthIndex = normalized >= OPEN_THRESHOLD ? 1 : 0
      }
    }
  }

  const mergedImagePaths = [...mediaImagePaths]
  if (typeof postImagePath === "string" && postImagePath.length > 0 && mergedImagePaths.length === 0) {
    mergedImagePaths.push(postImagePath)
  }
  const hasMediaVideo = typeof mediaVideoPath === "string" && mediaVideoPath.length > 0
  const hasMediaImages = mergedImagePaths.length > 0
  const elapsedFrames = Math.max(0, frame - introFrames)
  const mediaDisplayFrames = Math.max(1, Math.round(mediaDisplaySeconds * fps))
  const imageIndex =
    hasMediaImages
      ? Math.floor(elapsedFrames / mediaDisplayFrames) % mergedImagePaths.length
      : 0
  const currentImagePath = hasMediaImages ? mergedImagePaths[imageIndex] : undefined
  const activeCaption = captionsEnabled
    ? captionCues.find((cue) => {
        const cueStart = cue.startSec * fps
        const cueEnd = cue.endSec * fps
        return frame >= cueStart && frame <= cueEnd
      })
    : undefined

  return (
    <AbsoluteFill style={{backgroundColor: "#000"}}>
      <Img
        src={staticFile(backgroundPath)}
        style={{
          position: "absolute",
          width: `${bgScale}%`,
          height: `${bgScale}%`,
          objectFit: "cover",
          left: `${50 + bgX}%`,
          top: `${50 + bgY}%`,
          transform: "translate(-50%, -50%)",
        }}
      />

      {hasMediaVideo ? (
        <OffthreadVideo
          key={`media-video-${mediaVideoPath}`}
          src={staticFile(mediaVideoPath)}
          muted
          style={{
            position: "absolute",
            width: `${postImageScale}%`,
            height: "56%",
            left: "50%",
            top: `${50 + postImageY}%`,
            transform: "translate(-50%, -50%)",
            objectFit: "cover",
            borderRadius: 20,
            overflow: "hidden",
          }}
        />
      ) : null}

      {!hasMediaVideo && currentImagePath ? (
        <Img
          src={staticFile(currentImagePath)}
          style={{
            position: "absolute",
            width: `${postImageScale}%`,
            maxHeight: "56%",
            left: "50%",
            top: `${50 + postImageY}%`,
            transform: "translate(-50%, -50%)",
            objectFit: "contain",
            borderRadius: 16,
          }}
        />
      ) : null}

      {Array.from({length: MOUTH_FRAMES}, (_, i) => (
        <Img
          key={`mouth-${i}`}
          src={staticFile(`mouth/mouth_${i}.png`)}
          style={{
            position: "absolute",
            width: `${fishScale}%`,
            left: `${50 + fishX}%`,
            bottom: `${5 + fishY}%`,
            transform: `translateX(-50%)${fishFlipped ? " scaleX(-1)" : ""}`,
            display: i === mouthIndex ? "block" : "none",
          }}
        />
      ))}

      <Img
        src={staticFile("desk-only.png")}
        style={{
          position: "absolute",
          width: `${deskScale}%`,
          height: `${deskScale}%`,
          objectFit: "cover",
          left: `${50 + deskX}%`,
          top: `${50 + deskY}%`,
          transform: "translate(-50%, -50%)",
        }}
      />

      {dateLabel ? (
        <div
          style={{
            position: "absolute",
            left: 64,
            bottom: 390,
            color: "rgba(255,255,255,0.78)",
            fontSize: 24,
            fontWeight: 600,
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            fontFamily: "Inter, Avenir Next, Helvetica Neue, Arial, sans-serif",
            textShadow: "0 2px 8px rgba(0,0,0,0.45)",
          }}
        >
          {dateLabel}
        </div>
      ) : null}

      <div
        style={{
          position: "absolute",
          right: 64,
          bottom: 390,
          color: "rgba(255,255,255,0.78)",
          fontSize: 24,
          fontWeight: 600,
          letterSpacing: "0.06em",
          fontFamily: "Inter, Avenir Next, Helvetica Neue, Arial, sans-serif",
          textShadow: "0 2px 8px rgba(0,0,0,0.45)",
        }}
      >
        @PufferNews
      </div>

      {activeCaption ? (
        <div
          style={{
            position: "absolute",
            left: "50%",
            top: `${50 + captionY}%`,
            transform: "translate(-50%, -50%)",
            width: "88%",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: 8,
          }}
        >
          <div
            style={{
              color: "#fff",
              fontSize: 56,
              textAlign: "center",
              fontWeight: 900,
              lineHeight: 1.1,
              letterSpacing: "0.01em",
              fontFamily: "Inter, Avenir Next, Helvetica Neue, Arial, sans-serif",
              textShadow:
                "0 4px 0 rgba(0,0,0,0.96), 0 -4px 0 rgba(0,0,0,0.96), 4px 0 0 rgba(0,0,0,0.96), -4px 0 0 rgba(0,0,0,0.96), 3px 3px 0 rgba(0,0,0,0.96), -3px 3px 0 rgba(0,0,0,0.96), 0 8px 18px rgba(0,0,0,0.7)",
            }}
          >
            {activeCaption.text}
          </div>
          {activeCaption.textEn ? (
            <div
              style={{
                color: "rgba(255,255,255,0.88)",
                fontSize: 28,
                textAlign: "center",
                fontWeight: 600,
                lineHeight: 1.2,
                letterSpacing: "0.02em",
                fontFamily: "Inter, Avenir Next, Helvetica Neue, Arial, sans-serif",
                textShadow:
                  "0 2px 0 rgba(0,0,0,0.92), 0 -2px 0 rgba(0,0,0,0.92), 2px 0 0 rgba(0,0,0,0.92), -2px 0 0 rgba(0,0,0,0.92), 0 4px 12px rgba(0,0,0,0.6)",
              }}
            >
              {activeCaption.textEn}
            </div>
          ) : null}
        </div>
      ) : null}

      {showDebug ? (
        <div
          style={{
            position: "absolute",
            top: 20,
            left: 20,
            padding: "12px 16px",
            background: "rgba(0,0,0,0.7)",
            color: "#0f0",
            fontFamily: "monospace",
            fontSize: 24,
            lineHeight: 1.6,
            borderRadius: 8,
          }}
        >
          <div>amp: {amplitude.toFixed(4)}</div>
          <div>norm: {normalized.toFixed(3)}</div>
          <div>mouth: {mouthIndex}/1 {mouthCues.length > 0 ? "align" : "amp"}</div>
          <div>frame: {frame}</div>
        </div>
      ) : null}

      {introMusicPath ? (
        <Audio
          src={staticFile(introMusicPath)}
          endAt={introMusicFrames}
          volume={(f) => {
            if (introMusicFadeFrames <= 0) return introMusicVolume
            const fadeStartFrame = Math.max(0, introMusicFrames - introMusicFadeFrames)
            if (f < fadeStartFrame) return introMusicVolume
            const framesIntoFade = f - fadeStartFrame
            const fadeRatio = Math.max(0, 1 - framesIntoFade / introMusicFadeFrames)
            return introMusicVolume * fadeRatio
          }}
        />
      ) : null}
      <Sequence from={voiceStartFrames}>
        <Audio src={audioSrc} />
      </Sequence>
      {outroAudioPath ? (
        <Sequence from={outroStartFrames}>
          <Audio src={staticFile(outroAudioPath)} volume={outroVolume} />
        </Sequence>
      ) : null}
    </AbsoluteFill>
  )
}
