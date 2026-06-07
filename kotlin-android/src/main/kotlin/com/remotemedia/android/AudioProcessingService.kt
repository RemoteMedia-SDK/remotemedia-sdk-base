package com.remotemedia.android

import android.app.Service
import android.content.Intent
import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.AudioTrack
import android.media.MediaRecorder
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

/**
 * Foreground service for continuous audio processing.
 * Keeps microphone capture and audio playback alive even when app is backgrounded.
 * Use this for always-on voice assistants or background audio processing.
 *
 * Requires FOREGROUND_SERVICE_MICROPHONE permission (Android 14+)
 */
class AudioProcessingService : Service() {

    companion object {
        private const val TAG = "AudioProcessingService"
        private const val NOTIFICATION_ID = 1001
        private const val CHANNEL_ID = "RemoteMediaAudioProcessing"

        // Action intents
        const val ACTION_START = "com.remotemedia.android.ACTION_START_PROCESSING"
        const val ACTION_STOP = "com.remotemedia.android.ACTION_STOP_PROCESSING"
        const val ACTION_SEND_AUDIO = "com.remotemedia.android.ACTION_SEND_AUDIO"
    }

    private val binder = LocalBinder()

    // Audio components
    private lateinit var recorder: AudioRecorder
    private lateinit var player: AudioPlayer
    private lateinit var pipelineManager: PipelineManager

    // Configuration
    private var sampleRate = 16000
    private var outputSampleRate = 24000
    private var manifestName: String? = null

    // State
    private var isProcessing = false
    private val processingScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Audio queue for sending to pipeline
    private val audioSendQueue = Channel<ByteArray>(10)

    inner class LocalBinder : Binder() {
        fun getService(): AudioProcessingService = this@AudioProcessingService
    }

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
    }

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        intent?.action?.let { action ->
            when (action) {
                ACTION_START -> startProcessing()
                ACTION_STOP -> stopProcessing()
                ACTION_SEND_AUDIO -> {
                    // Could handle external audio injection here
                }
            }
        }
        return START_STICKY
    }

    /**
     * Start audio processing pipeline
     */
    fun startProcessing(manifest: String = "voice-assistant-mobile.yaml") {
        if (isProcessing) {
            Log.w(TAG, "Already processing")
            return
        }

        this.manifestName = manifest

        processingScope.launch {
            try {
                isProcessing = true

                // Initialize components
                pipelineManager = PipelineManager(this@AudioProcessingService)
                val initialized = pipelineManager.initializeBlocking()
                if (!initialized) {
                    throw IllegalStateException("PipelineManager initialization failed")
                }

                // Load manifest
                if (!pipelineManager.loadManifest(manifest)) {
                    throw IllegalStateException("Failed to load manifest: $manifest")
                }

                // Configure audio
                recorder = AudioRecorder(this@AudioProcessingService)
                recorder.setFormat(sampleRate, 1, 20)

                player = AudioPlayer(this@AudioProcessingService)
                player.setInputSampleRate(outputSampleRate)

                // Set up pipeline callbacks
                pipelineManager.onOutput = { outputJson ->
                    // Handle pipeline output (TTS audio, text, etc.)
                    // For voice assistant, PipelineManager handles audio internally
                    Log.d(TAG, "Pipeline output: $outputJson")
                }

                pipelineManager.onError = { error ->
                    Log.e(TAG, "Pipeline error: $error")
                    stopForeground(true)
                    stopSelf()
                }

                pipelineManager.onStateChange = { state ->
                    Log.i(TAG, "Pipeline state: $state")
                }

                // Start streaming
                val streamingStarted = pipelineManager.startStreamingBlocking()
                if (!streamingStarted) {
                    throw IllegalStateException("Failed to start streaming")
                }

                // Start audio I/O
                recorder.onAudioData = { pcmData ->
                    // Send to pipeline
                    pipelineManager.sendAudioBlocking(pcmData, sampleRate, 1)
                }

                recorder.onError = { error ->
                    Log.e(TAG, "Recorder error: $error")
                }

                recorder.onStateChange = { state ->
                    Log.i(TAG, "Recorder state: $state")
                }

                player.onError = { error ->
                    Log.e(TAG, "Player error: $error")
                }

                player.onStateChange = { state ->
                    Log.i(TAG, "Player state: $state")
                }

                player.onUnderrun = {
                    Log.w(TAG, "Audio underrun")
                }

                recorder.start()
                player.start(outputSampleRate)

                // Start foreground service
                val notification = buildNotification("RemoteMedia: Voice Assistant Active")
                startForeground(NOTIFICATION_ID, notification)

                Log.i(TAG, "Audio processing started")

            } catch (e: Exception) {
                Log.e(TAG, "Failed to start processing", e)
                isProcessing = false
                stopForeground(true)
                stopSelf()
            }
        }
    }

    /**
     * Stop audio processing
     */
    fun stopProcessing() {
        if (!isProcessing) return

        processingScope.launch {
            isProcessing = false

            recorder.stop()
            player.stop()
            pipelineManager.stopStreamingBlocking()
            pipelineManager.destroyBlocking()

            recorder.destroy()
            player.destroy()

            stopForeground(true)
            stopSelf()

            Log.i(TAG, "Audio processing stopped")
        }
    }

    /**
     * Send text to pipeline (for TTS-only or text input)
     */
    fun sendText(text: String) {
        processingScope.launch {
            pipelineManager.sendTextBlocking(text)
        }
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = android.app.NotificationChannel(
                CHANNEL_ID,
                "RemoteMedia Audio Processing",
                android.app.NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Foreground service for voice assistant audio processing"
                setShowBadge(false)
            }
            val manager = getSystemService(android.app.NotificationManager::class.java)
            manager.createNotificationChannel(channel)
        }
    }

    private fun buildNotification(content: String): android.app.Notification {
        return android.app.Notification.Builder(this, CHANNEL_ID)
            .setContentTitle("RemoteMedia")
            .setContentText(content)
            .setSmallIcon(android.R.drawable.ic_media_play)
            .setOngoing(true)
            .setShowWhen(false)
            .build()
    }

    override fun onDestroy() {
        stopProcessing()
        processingScope.coroutineContext.cancelChildren()
        super.onDestroy()
    }
}