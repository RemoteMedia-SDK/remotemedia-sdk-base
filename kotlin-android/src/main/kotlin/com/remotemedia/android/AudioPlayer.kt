package com.remotemedia.android

import android.content.Context
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioTrack
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

/**
 * Low-latency audio player using AudioTrack.
 * Plays PCM16 audio with configurable sample rate, handles resampling from pipeline output rates.
 */
class AudioPlayer(private val context: Context) {

    companion object {
        private const val TAG = "AudioPlayer"
        private const val DEFAULT_BUFFER_FRAMES = 5
        private const val DEFAULT_OUTPUT_SAMPLE_RATE = 48000
    }

    enum class State {
        IDLE,
        PLAYING,
        ERROR
    }

    // Configuration
    private var inputSampleRate = 24000 // Default for Kokoro TTS
    private var outputSampleRate = DEFAULT_OUTPUT_SAMPLE_RATE
    private var channels = 1

    // AudioTrack
    private var audioTrack: AudioTrack? = null

    // Audio queue
    private val audioQueue = Channel<ByteArray>(DEFAULT_BUFFER_FRAMES * 2)

    // Callbacks
    var onError: ((String) -> Unit)? = null
    var onStateChange: ((State) -> Unit)? = null
    var onUnderrun: (() -> Unit)? = null

    // Internal state
    private var currentState = State.IDLE
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var isPlaying = false

    /**
     * Configure audio format (call before start)
     * @param inputSampleRate Sample rate of incoming audio (e.g., 16000 for Whisper, 24000 for Kokoro)
     */
    fun setInputSampleRate(inputSampleRate: Int) {
        require(inputSampleRate in 8000..48000) { "Input sample rate must be 8000-48000 Hz" }
        this.inputSampleRate = inputSampleRate
    }

    /**
     * Start playback
     * @param inputSampleRate Sample rate of audio that will be enqueued
     */
    fun start(inputSampleRate: Int = this.inputSampleRate) {
        if (currentState == State.PLAYING) {
            Log.w(TAG, "Already playing")
            return
        }

        this.inputSampleRate = inputSampleRate

        scope.launch {
            try {
                updateState(State.PLAYING)

                val bufferSize = AudioTrack.getMinBufferSize(outputSampleRate, channelConfig, AudioFormat.ENCODING_PCM_16BIT)
                if (bufferSize == AudioTrack.ERROR || bufferSize == AudioTrack.ERROR_BAD_VALUE) {
                    throw IllegalStateException("Invalid audio parameters")
                }

                val audioAttributes = AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_MEDIA)
                    .setContentType(AudioAttributes.CONTENT_TYPE_MUSIC)
                    .build()

                val audioFormat = AudioFormat.Builder()
                    .setEncoding(AudioFormat.ENCODING_PCM_16BIT)
                    .setSampleRate(outputSampleRate)
                    .setChannelMask(channelConfig)
                    .build()

                audioTrack = AudioTrack.Builder()
                    .setAudioAttributes(audioAttributes)
                    .setAudioFormat(audioFormat)
                    .setBufferSizeInBytes(bufferSize)
                    .setTransferMode(AudioTrack.MODE_STREAM)
                    .build()

                if (audioTrack!!.state != AudioTrack.STATE_INITIALIZED) {
                    throw IllegalStateException("AudioTrack initialization failed")
                }

                audioTrack!!.play()
                isPlaying = true

                // Use buffer size divided by frame size for frames per callback
                val framesPerCallback = bufferSize / (channels * 2)
                val bufferSizeBytes = framesPerCallback * channels * 2
                val buffer = ByteArray(bufferSizeBytes)
                val silenceBuffer = ByteArray(bufferSizeBytes)

                while (isPlaying) {
                    var filled = 0

                    // Try to fill buffer from queue
                    while (filled < bufferSizeBytes) {
                        val remaining = bufferSizeBytes - filled
                        val chunk = audioQueue.tryReceive()

                        when {
                            chunk.isSuccess -> {
                                val audioData = chunk.getOrNull()
                                if (audioData != null) {
                                    val copySize = minOf(audioData.size, remaining)
                                    audioData.copyInto(buffer, filled, 0, copySize)
                                    filled += copySize

                                    // If chunk was larger than remaining, put remainder back
                                    if (audioData.size > copySize) {
                                        val remainder = audioData.copyOfRange(copySize, audioData.size)
                                        scope.launch { audioQueue.send(remainder) }
                                    }
                                }
                            }
                            chunk.isClosed -> {
                                // Channel closed
                                break
                            }
                            else -> {
                                // Queue empty - fill with silence (underrun)
                                if (filled == 0) {
                                    withContext(Dispatchers.Main) {
                                        onUnderrun?.invoke()
                                    }
                                }
                                silenceBuffer.copyInto(buffer, filled, 0, remaining)
                                filled = bufferSizeBytes
                            }
                        }
                    }

                    // Write to AudioTrack
                    val result = audioTrack!!.write(buffer, 0, bufferSizeBytes, AudioTrack.WRITE_BLOCKING)
                    if (result < 0) {
                        Log.e(TAG, "AudioTrack write error: $result")
                        break
                    }
                }

            } catch (e: Exception) {
                Log.e(TAG, "Playback error", e)
                withContext(Dispatchers.Main) {
                    onError?.invoke(e.message ?: "Playback failed")
                    updateState(State.ERROR)
                }
            }
        }
    }

    /**
     * Enqueue PCM16 audio data for playback
     */
    fun enqueue(pcmData: ByteArray) {
        scope.launch {
            audioQueue.send(pcmData)
        }
    }

    /**
     * Stop playback and clear queue
     */
    fun stop() {
        isPlaying = false
        scope.launch {
            try {
                audioTrack?.stop()
                audioTrack?.release()
                audioTrack = null
            } catch (e: Exception) {
                Log.e(TAG, "Error stopping player", e)
            }
            // Clear queue
            while (true) {
                val result = audioQueue.tryReceive()
                if (result.isClosed || !result.isSuccess) {
                    break
                }
            }
            updateState(State.IDLE)
        }
    }

    /**
     * Release all resources
     */
    fun destroy() {
        stop()
        scope.coroutineContext.cancelChildren()
        onError = null
        onStateChange = null
        onUnderrun = null
    }

    private val channelConfig: Int
        get() = if (channels == 1) AudioFormat.CHANNEL_OUT_MONO else AudioFormat.CHANNEL_OUT_STEREO

    private fun updateState(newState: State) {
        if (currentState != newState) {
            currentState = newState
            scope.launch(Dispatchers.Main) {
                onStateChange?.invoke(newState)
            }
        }
    }

    fun getCurrentState(): State = currentState
    fun isActive(): Boolean = currentState == State.PLAYING
}