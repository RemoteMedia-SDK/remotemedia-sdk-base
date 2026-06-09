package com.remotemedia.android

import android.content.Context
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Low-latency audio recorder using AudioRecord.
 * Captures 48kHz mono PCM16 and resamples to 16kHz for the pipeline.
 */
class AudioRecorder(private val context: Context) {

    companion object {
        private const val TAG = "AudioRecorder"
    }

    // Audio configuration
    private val sourceSampleRate = 48000  // Device capture rate
    private val targetSampleRate = 16000  // Pipeline processing rate
    private val channelConfig = AudioFormat.CHANNEL_IN_MONO
    private val audioFormat = AudioFormat.ENCODING_PCM_16BIT
    private val frameSizeMs = 20  // 20ms frames
    private val sourceFrameSize = sourceSampleRate * frameSizeMs / 1000  // 960 samples
    private val targetFrameSize = targetSampleRate * frameSizeMs / 1000  // 320 samples

    // State
    private var audioRecord: AudioRecord? = null
    private var isRecording = false
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var recordJob: Job? = null

    // Output channel for captured audio (target rate 16kHz)
    private var audioChannel = Channel<ByteArray>(capacity = 100)

    // Callbacks
    var onAudioData: ((ByteArray) -> Unit)? = null
    var onError: ((String) -> Unit)? = null
    var onStateChange: ((RecordingState) -> Unit)? = null

    enum class RecordingState {
        IDLE,
        STARTING,
        RECORDING,
        STOPPING,
        STOPPED,
        ERROR
    }

    /**
     * Initialize and start recording
     */
    fun start(): Boolean {
        if (isRecording) {
            Log.w(TAG, "Already recording")
            return true
        }

        audioChannel = Channel(capacity = 100)
        updateState(RecordingState.STARTING)

        return try {
            // Calculate buffer size
            val minBufferSize = AudioRecord.getMinBufferSize(
                sourceSampleRate,
                channelConfig,
                audioFormat
            )

            if (minBufferSize == AudioRecord.ERROR || minBufferSize == AudioRecord.ERROR_BAD_VALUE) {
                throw IllegalStateException("Invalid audio parameters")
            }

            // Use larger buffer for stability
            val bufferSize = maxOf(minBufferSize, sourceFrameSize * 4 * 2) // 2 bytes per sample

            audioRecord = AudioRecord.Builder()
                .setAudioSource(MediaRecorder.AudioSource.MIC)
                .setAudioFormat(
                    android.media.AudioFormat.Builder()
                        .setEncoding(audioFormat)
                        .setSampleRate(sourceSampleRate)
                        .setChannelMask(channelConfig)
                        .build()
                )
                .setBufferSizeInBytes(bufferSize)
                .build()

            if (audioRecord?.state != AudioRecord.STATE_INITIALIZED) {
                throw IllegalStateException("AudioRecord initialization failed")
            }

            audioRecord?.startRecording()

            if (audioRecord?.recordingState != AudioRecord.RECORDSTATE_RECORDING) {
                throw IllegalStateException("Failed to start recording")
            }

            isRecording = true
            updateState(RecordingState.RECORDING)

            // Start recording loop
            recordJob = scope.launch {
                recordingLoop()
            }

            Log.i(TAG, "Recording started at ${sourceSampleRate}Hz")
            true
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start recording", e)
            updateState(RecordingState.ERROR)
            onError?.invoke("Failed to start recording: ${e.message}")
            cleanup()
            false
        }
    }

    /**
     * Stop recording
     */
    fun stop() {
        if (!isRecording) return

        updateState(RecordingState.STOPPING)
        isRecording = false

        recordJob?.cancel()
        recordJob = null

        cleanup()

        updateState(RecordingState.STOPPED)
        Log.i(TAG, "Recording stopped")
    }

    /**
     * Recording loop - reads from AudioRecord and resamples
     */
    private suspend fun recordingLoop() {
        val buffer = ByteArray(sourceFrameSize * 2) // 16-bit = 2 bytes
        val tempBuffer = ShortArray(sourceFrameSize)

        while (isRecording && scope.coroutineContext.isActive) {
            try {
                val readResult = audioRecord?.read(buffer, 0, buffer.size, AudioRecord.READ_BLOCKING)

                if (readResult != null && readResult > 0) {
                    // Convert bytes to shorts
                    val byteBuffer = ByteBuffer.wrap(buffer, 0, readResult)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .asShortBuffer()
                    byteBuffer.get(tempBuffer, 0, readResult / 2)

                    // Resample from 48kHz to 16kHz (simple decimation for 3x ratio)
                    // In production, use rubato or similar high-quality resampler
                    val resampled = resample3x(tempBuffer, readResult / 2)

                    // Convert back to bytes
                    val outputBuffer = ByteArray(resampled.size * 2)
                    val outByteBuffer = ByteBuffer.wrap(outputBuffer).order(ByteOrder.LITTLE_ENDIAN)
                    for (sample in resampled) {
                        outByteBuffer.putShort(sample)
                    }

                    // Send to channel (non-blocking offer)
                    audioChannel.trySend(outputBuffer) ?: run {
                        Log.w(TAG, "Audio channel full, dropping frame")
                    }

                    // Also call callback directly for low latency
                    onAudioData?.invoke(outputBuffer)
                } else if (readResult == AudioRecord.ERROR_INVALID_OPERATION) {
                    Log.e(TAG, "Invalid operation reading audio")
                } else if (readResult == AudioRecord.ERROR_BAD_VALUE) {
                    Log.e(TAG, "Bad value reading audio")
                }
            } catch (e: Exception) {
                if (isRecording) {
                    Log.e(TAG, "Recording loop error", e)
                    onError?.invoke("Recording error: ${e.message}")
                }
                break
            }
        }
    }

    /**
     * Simple 3x decimation resampler (48kHz -> 16kHz)
     * For production, replace with rubato or similar
     */
    private fun resample3x(input: ShortArray, inputLen: Int): ShortArray {
        val outputSize = inputLen / 3
        val output = ShortArray(outputSize)

        // Simple box filter decimation (average of 3 samples)
        for (i in 0 until outputSize) {
            var sum = 0L
            for (j in 0..2) {
                sum += input[i * 3 + j].toLong()
            }
            output[i] = (sum / 3).toShort()
        }

        return output
    }

    /**
     * Get the receive channel for audio data
     */
    fun getAudioChannel(): ReceiveChannel<ByteArray> = audioChannel

    private fun cleanup() {
        audioRecord?.stop()
        audioRecord?.release()
        audioRecord = null
        audioChannel.close()
    }

    private fun updateState(state: RecordingState) {
        // Post to main thread for UI updates
        scope.launch(Dispatchers.Main) {
            onStateChange?.invoke(state)
        }
    }

    fun isRecording(): Boolean = isRecording

    fun destroy() {
        stop()
        scope.coroutineContext.cancelChildren()
    }
}