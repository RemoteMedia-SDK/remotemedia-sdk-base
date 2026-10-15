## ADDED Requirements

### Requirement: AudioRecorder provides low-latency microphone capture
`AudioRecorder` SHALL capture audio from the device microphone using Oboe with configurable sample rate, channel count, and frame size, delivering PCM16 data via a Kotlin callback/flow.

#### Scenario: Recorder starts and delivers audio frames
- **WHEN** `recorder.start()` is called after construction with a `Context`
- **THEN** `onAudioData` callback receives non-empty `ByteArray` PCM16 frames at the configured rate

#### Scenario: Recorder handles 16kHz output for STT
- **WHEN** constructed with `sampleRate = 16000`, `channels = 1`
- **THEN** delivered frames are 16kHz mono PCM16 suitable for Whisper STT input

#### Scenario: Recorder reports state changes
- **WHEN** recorder transitions between `IDLE`, `RECORDING`, `ERROR` states
- **THEN** `onStateChange` callback is invoked with the new state

#### Scenario: Recorder reports errors without crashing
- **WHEN** an Oboe error occurs (permission denied, device busy)
- **THEN** `onError` callback receives error message and recorder transitions to `ERROR` state

### Requirement: AudioPlayer provides low-latency audio playback
`AudioPlayer` SHALL play PCM16 audio via Oboe with configurable sample rate, handling resampling from pipeline output rates (16kHz, 24kHz) to device output rate (48kHz).

#### Scenario: Player starts and accepts audio queue
- **WHEN** `player.start(24000)` is called with Kokoro TTS sample rate
- **THEN** player enters `PLAYING` state and `enqueue(pcmData)` accepts audio frames

#### Scenario: Player handles underrun gracefully
- **WHEN** audio queue empties during playback
- **THEN** `onUnderrun` callback fires and playback continues silently until new data arrives

#### Scenario: Player reports state and errors
- **WHEN** player state changes or Oboe error occurs
- **THEN** respective callbacks fire with appropriate state/error info

### Requirement: Audio components lifecycle-managed and resource-safe
Both `AudioRecorder` and `AudioPlayer` SHALL implement `AutoCloseable` / provide `destroy()` to release Oboe streams and native resources.

#### Scenario: Resources released on destroy
- **WHEN** `recorder.destroy()` or `player.destroy()` is called
- **THEN** Oboe streams are closed, callbacks cleared, no native memory leaks

### Requirement: Audio format utilities for pipeline interop
The library SHALL provide extension functions or utilities to convert between common audio formats (PCM16 byte arrays, float arrays, sample rate conversion hints) for seamless pipeline integration.

#### Scenario: PipelineManager.sendAudio accepts AudioRecorder output directly
- **WHEN** `recorder.onAudioData = { pcm -> pipelineManager.sendAudio(pcm) }`
- **THEN** no manual format conversion required; pipeline receives correctly formatted audio