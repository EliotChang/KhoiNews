import React from "react"
import {Composition} from "remotion"
import {FishLipSync, fishLipSyncSchema} from "./FishLipSync"

const FPS = 30
const FALLBACK_DURATION_SECONDS = 17

export function RemotionRoot(): React.ReactElement {
  return (
    <Composition
      id="FishLipSync"
      component={FishLipSync}
      schema={fishLipSyncSchema}
      fps={FPS}
      width={1080}
      height={1920}
      durationInFrames={Math.ceil(FALLBACK_DURATION_SECONDS * FPS)}
      calculateMetadata={({props}) => {
        const safeDuration =
          typeof props.durationInSeconds === "number" && props.durationInSeconds > 0
            ? props.durationInSeconds
            : FALLBACK_DURATION_SECONDS
        return {
          durationInFrames: Math.max(1, Math.ceil(safeDuration * FPS)),
        }
      }}
      defaultProps={{
        audioPath: "audio/audio.mp3",
        backgroundIndex: 0,
        postImagePath: undefined,
        postTitle: "News Update",
        dateLabel: "",
        scriptText: "",
        introDurationSeconds: 2,
        voiceStartSeconds: 0,
        introMusicPath: undefined,
        introMusicVolume: 0.5,
        outroAudioPath: "audio/breaking_news_outro.wav",
        outroStartSeconds: 14,
        outroVolume: 1.0,
        captionsEnabled: true,
        captionCues: [],
        mediaVideoPath: undefined,
        mediaImagePaths: [],
        mediaDisplaySeconds: 2.5,
        sensitivity: 0.04,
        freqStart: 1,
        freqEnd: 40,
        fishX: 0,
        fishY: 14.3,
        fishScale: 70,
        fishFlipped: false,
        bgX: 0,
        bgY: 0,
        bgScale: 100,
        deskX: 0,
        deskY: 1,
        deskScale: 110,
        captionY: -5,
        postImageY: -24,
        postImageScale: 62,
        showDebug: false,
        durationInSeconds: FALLBACK_DURATION_SECONDS,
        mouthCues: [],
      }}
    />
  )
}
