package com.remotemedia.inprocess

import android.content.Context
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioManager
import android.media.AudioTrack
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import java.nio.ByteBuffer
import java.nio.ByteOrder

/** Low-latency audio player using AudioTrack.
 * Plays 48kHz mono PCM16, handles resampling from 24kHz (Kokoro) or 16kHz.
 */
class AudioPlayer(private val context: Context) {
    
    companion object {
        private const val TAG = "AudioPlayer"
    }
    
    // Audio configuration
    private val outputSampleRate = 48000  // Device playback rate
    private val channelConfig = AudioFormat.CHANNEL_OUT_MONO
    private val audioFormat = AudioFormat.ENCODING_PCM_16BIT
    private val frameSizeMs = 20  // 20ms frames
    private val outputFrameSize = outputSampleRate * frameSizeMs / 1000  // 960 samples
    
    // State
    private var audioTrack: AudioTrack? = null
    private var isPlaying = false
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var playJob: Job? = null
    
    // Input channel for audio data to play
    private var audioChannel = Channel<ByteArray>(capacity = 100)
    
    // Resample ratios
    private var inputSampleRate = 24000  // Default Kokoro sample rate
    
    // Callbacks
    var onError: ((String) -> Unit)? = null
    var onStateChange: ((PlaybackState) -> Unit)? = null
    var onUnderrun: (() -> Unit)? = null
    
    enum class PlaybackState {
        IDLE,
        STARTING,
        PLAYING,
        STOPPING,
        STOPPED,
        ERROR
    }
    
    /**
     * Initialize and start playback
     */
    fun start(inputSampleRate: Int = 24000): Boolean {
        if (isPlaying) {
            Log.w(TAG, "Already playing")
            return true
        }
        
        audioChannel = Channel(capacity = 100)
        this.inputSampleRate = inputSampleRate
        updateState(PlaybackState.STARTING)
        
        return try {
            // Calculate buffer size
            val minBufferSize = AudioTrack.getMinBufferSize(
                outputSampleRate,
                channelConfig,
                audioFormat
            )
            
            if (minBufferSize == AudioTrack.ERROR || minBufferSize == AudioTrack.ERROR_BAD_VALUE) {
                throw IllegalStateException("Invalid audio parameters")
            }
            
            // Use larger buffer for stability (3-5 frames ahead)
            val bufferSize = maxOf(minBufferSize, outputFrameSize * 5 * 2)
            
            val audioAttributes = AudioAttributes.Builder()
                .setUsage(AudioAttributes.USAGE_MEDIA)
                .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                .setFlags(AudioAttributes.FLAG_LOW_LATENCY)
                .build()
            
            val audioFormat = android.media.AudioFormat.Builder()
                .setEncoding(this.audioFormat)
                .setSampleRate(outputSampleRate)
                .setChannelMask(channelConfig)
                .build()
            
            audioTrack = AudioTrack.Builder()
                .setAudioAttributes(audioAttributes)
                .setAudioFormat(audioFormat)
                .setBufferSizeInBytes(bufferSize)
                .setTransferMode(AudioTrack.MODE_STREAM)
                .build()
            
            if (audioTrack?.state != AudioTrack.STATE_INITIALIZED) {
                throw IllegalStateException("AudioTrack initialization failed")
            }
            
            audioTrack?.play()
            
            isPlaying = true
            updateState(PlaybackState.PLAYING)
            
            // Start playback loop
            playJob = scope.launch {
                playbackLoop()
            }
            
            Log.i(TAG, "Playback started at ${outputSampleRate}Hz (input: ${inputSampleRate}Hz)")
            true
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start playback", e)
            updateState(PlaybackState.ERROR)
            onError?.invoke("Failed to start playback: ${e.message}")
            cleanup()
            false
        }
    }
    
    /**
     * Stop playback
     */
    fun stop() {
        if (!isPlaying) return
        
        updateState(PlaybackState.STOPPING)
        isPlaying = false
        
        playJob?.cancel()
        playJob = null
        
        // Wait a bit for buffer to drain
        scope.launch {
            delay(100)
            cleanup()
            updateState(PlaybackState.STOPPED)
            Log.i(TAG, "Playback stopped")
        }
    }
    
    /**
     * Queue audio data for playback (resamples from inputSampleRate to outputSampleRate)
     * @param pcmData PCM16 audio data at inputSampleRate
     * @return true if queued successfully
     */
    fun queueAudio(pcmData: ByteArray): Boolean {
        if (!isPlaying) return false
        
        try {
            // Resample if needed
            val outputData = if (inputSampleRate != outputSampleRate) {
                resample(pcmData, inputSampleRate, outputSampleRate)
            } else {
                pcmData
            }
            
            // Non-blocking offer
            return audioChannel.trySend(outputData).isSuccess
        } catch (e: Exception) {
            Log.e(TAG, "Failed to queue audio", e)
            onError?.invoke("Queue audio error: ${e.message}")
            return false
        }
    }
    
    /**
     * Send channel for external producers
     */
    fun getAudioChannel(): SendChannel<ByteArray> = audioChannel
    
    /**
     * Playback loop - reads from channel and writes to AudioTrack
     */
    private suspend fun playbackLoop() {
        val writeBuffer = ByteArray(outputFrameSize * 2)
        
        while (isPlaying && scope.coroutineContext.isActive) {
            try {
                // Try to get data from channel with timeout
                val data = withTimeoutOrNull(50) {
                    audioChannel.receive()
                }
                
                if (data != null) {
                    val bytesWritten = audioTrack?.write(data, 0, data.size, AudioTrack.WRITE_BLOCKING)
                    
                    if (bytesWritten != null && bytesWritten < 0) {
                        Log.e(TAG, "AudioTrack write error: $bytesWritten")
                        if (bytesWritten == AudioTrack.ERROR_INVALID_OPERATION) {
                            onUnderrun?.invoke()
                        }
                    }
                } else {
                    // No data available - write silence to prevent underrun
                    audioTrack?.write(writeBuffer, 0, writeBuffer.size, AudioTrack.WRITE_NON_BLOCKING)
                }
            } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
                // Channel receive timeout - write silence
                audioTrack?.write(writeBuffer, 0, writeBuffer.size, AudioTrack.WRITE_NON_BLOCKING)
            } catch (e: Exception) {
                if (isPlaying) {
                    Log.e(TAG, "Playback loop error", e)
                    onError?.invoke("Playback error: ${e.message}")
                }
                break
            }
        }
    }
    
    /**
     * Simple linear interpolation resampler
     * For production, use rubato or similar high-quality resampler
     */
    private fun resample(input: ByteArray, inRate: Int, outRate: Int): ByteArray {
        val inBuffer = ByteBuffer.wrap(input).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()
        val inSamples = ShortArray(inBuffer.remaining())
        inBuffer.get(inSamples)
        val inLen = input.size / 2
        
        val ratio = outRate.toDouble() / inRate
        val outLen = (inLen * ratio).toInt()
        val outSamples = ShortArray(outLen)
        
        // Linear interpolation
        for (i in 0 until outLen) {
            val srcPos = i / ratio
            val srcIndex = srcPos.toInt()
            val frac = srcPos - srcIndex
            
            if (srcIndex + 1 < inLen) {
                val s0 = inSamples[srcIndex].toDouble()
                val s1 = inSamples[srcIndex + 1].toDouble()
                outSamples[i] = (s0 + (s1 - s0) * frac).toInt().toShort()
            } else if (srcIndex < inLen) {
                outSamples[i] = inSamples[srcIndex]
            }
        }
        
        // Convert to bytes
        val output = ByteArray(outLen * 2)
        val outBuffer = ByteBuffer.wrap(output).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer()
        outBuffer.put(outSamples)
        
        return output
    }
    
    private fun cleanup() {
        audioTrack?.stop()
        audioTrack?.release()
        audioTrack = null
        audioChannel.close()
    }
    
    private fun updateState(state: PlaybackState) {
        scope.launch(Dispatchers.Main) {
            onStateChange?.invoke(state)
        }
    }
    
    fun isPlaying(): Boolean = isPlaying
    
    fun destroy() {
        stop()
        scope.coroutineContext.cancelChildren()
    }
}