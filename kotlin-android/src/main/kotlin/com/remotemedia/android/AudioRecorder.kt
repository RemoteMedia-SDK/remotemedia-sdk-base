package com.remotemedia.android

import android.content.Context
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import android.util.Log
import kotlinx.coroutines.*

/**
 * Low-latency audio recorder using AudioRecord.
 * Captures 48kHz mono PCM16 and lets pipeline handle resampling.
 */
class AudioRecorder(private val context: Context) {

    companion object {
        private const val TAG = "AudioRecorder"
        private const val DEFAULT_SAMPLE_RATE = 48000
        private const val DEFAULT_CHANNELS = 1
        private const val DEFAULT_ENCODING = AudioFormat.ENCODING_PCM_16BIT
    }

    enum class State {
        IDLE,
        RECORDING,
        ERROR
    }

    // Configuration
    private var sampleRate = DEFAULT_SAMPLE_RATE
    private var channels = DEFAULT_CHANNELS
    private var encoding = DEFAULT_ENCODING

    // AudioRecord
    private var audioRecord: AudioRecord? = null

    // Callbacks
    var onAudioData: ((ByteArray) -> Unit)? = null
    var onError: ((String) -> Unit)? = null
    var onStateChange: ((State) -> Unit)? = null

    // Internal state
    private var currentState = State.IDLE
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var isRecording = false

    /**
     * Configure audio format (call before start)
     */
    fun setFormat(sampleRate: Int, channels: Int = 1, encoding: Int = DEFAULT_ENCODING) {
        require(sampleRate in 8000..192000) { "Sample rate must be 8000-192000 Hz" }
        require(channels in 1..2) { "Channels must be 1 or 2" }
        this.sampleRate = sampleRate
        this.channels = channels
        this.encoding = encoding
    }

    /**
     * Start recording audio from microphone
     */
    fun start() {
        if (currentState == State.RECORDING) {
            Log.w(TAG, "Already recording")
            return
        }

        scope.launch {
            try {
                updateState(State.RECORDING)

                val bufferSize = AudioRecord.getMinBufferSize(sampleRate, channelConfig, encoding)
                if (bufferSize == AudioRecord.ERROR || bufferSize == AudioRecord.ERROR_BAD_VALUE) {
                    throw IllegalStateException("Invalid audio parameters: bufferSize=$bufferSize")
                }

                audioRecord = AudioRecord.Builder()
                    .setAudioSource(MediaRecorder.AudioSource.MIC)
                    .setAudioFormat(
                        AudioFormat.Builder()
                            .setEncoding(encoding)
                            .setSampleRate(sampleRate)
                            .setChannelMask(channelConfig)
                            .build()
                    )
                    .setBufferSizeInBytes(bufferSize * 2)
                    .build()

                if (audioRecord!!.state != AudioRecord.STATE_INITIALIZED) {
                    throw IllegalStateException("AudioRecord initialization failed")
                }

                audioRecord!!.startRecording()
                isRecording = true

                val buffer = ByteArray(bufferSize)

                while (isRecording) {
                    val readResult = audioRecord!!.read(buffer, 0, buffer.size)
                    if (readResult > 0) {
                        val audioData = buffer.copyOf(readResult)
                        withContext(Dispatchers.Main) {
                            onAudioData?.invoke(audioData)
                        }
                    } else if (readResult < 0) {
                        Log.e(TAG, "AudioRecord read error: $readResult")
                        break
                    }
                }

            } catch (e: Exception) {
                Log.e(TAG, "Recording error", e)
                withContext(Dispatchers.Main) {
                    onError?.invoke(e.message ?: "Recording failed")
                    updateState(State.ERROR)
                }
            }
        }
    }

    /**
     * Stop recording
     */
    fun stop() {
        isRecording = false
        scope.launch {
            try {
                audioRecord?.stop()
                audioRecord?.release()
                audioRecord = null
            } catch (e: Exception) {
                Log.e(TAG, "Error stopping recorder", e)
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
        onAudioData = null
        onError = null
        onStateChange = null
    }

    private val channelConfig: Int
        get() = if (channels == 1) AudioFormat.CHANNEL_IN_MONO else AudioFormat.CHANNEL_IN_STEREO

    private fun updateState(newState: State) {
        if (currentState != newState) {
            currentState = newState
            scope.launch(Dispatchers.Main) {
                onStateChange?.invoke(newState)
            }
        }
    }

    fun getCurrentState(): State = currentState
    fun isActive(): Boolean = currentState == State.RECORDING
}