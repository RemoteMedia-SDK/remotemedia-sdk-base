package com.remotemedia.android

import android.content.Context
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Manages RemoteMedia pipeline lifecycle and execution.
 * Handles both unary and streaming modes with idiomatic Kotlin coroutines API.
 */
class PipelineManager(private val context: Context) {

    companion object {
        private const val TAG = "PipelineManager"
        private var pluginLoaded = false
    }

    // State
    private var executorHandle: Long = 0
    private var sessionHandle: Long = 0
    private val isRunning = AtomicBoolean(false)
    private val isStreaming = AtomicBoolean(false)

    // Coroutine scope for background operations
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Audio configuration
    private val sampleRate = 16000
    private val channels = 1

    // Callbacks (for backward compatibility and simple UI integration)
    var onOutput: ((String) -> Unit)? = null
    var onError: ((String) -> Unit)? = null
    var onStateChange: ((PipelineState) -> Unit)? = null

    // Pipeline configuration
    private var currentManifest: String? = null

    /**
     * Pipeline execution state
     */
    enum class PipelineState {
        IDLE,
        INITIALIZING,
        READY,
        RUNNING,
        STREAMING,
        ERROR,
        DESTROYED
    }

    /**
     * Initialize the pipeline executor.
     * Extracts native plugins from assets and creates the native executor.
     */
    suspend fun initialize(): Boolean = withContext(Dispatchers.IO) {
        try {
            updateState(PipelineState.INITIALIZING)

            // Ensure native loadable plugins are extracted to files directory
            if (!pluginLoaded) {
                loadNativePlugins()
            }

            executorHandle = NativeInterface.nativeCreateExecutor()
            if (executorHandle == 0L) {
                throw NativeException("Failed to create executor")
            }

            Log.i(TAG, "Executor created: $executorHandle")
            updateState(PipelineState.READY)
            true
        } catch (e: Exception) {
            Log.e(TAG, "Initialization failed", e)
            updateState(PipelineState.ERROR)
            onError?.invoke(e.message ?: "Unknown error")
            false
        }
    }

    /**
     * Initialize the pipeline executor (blocking version for backward compatibility).
     */
    fun initializeBlocking(): Boolean = runBlocking { initialize() }

    /**
     * Load native plugins from assets to private files directory
     */
    private fun loadNativePlugins() {
        loadNativePlugin("plugins/libsilero_vad_loadable_plugin.so", "libsilero_vad_loadable_plugin.so")
        loadNativePlugin("plugins/libwhisper_loadable_plugin.so", "libwhisper_loadable_plugin.so")
        loadNativePlugin("plugins/liblitert_lm_loadable_plugin.so", "liblitert_lm_loadable_plugin.so")
        loadNativePlugin("plugins/libmisaki_g2p_plugin.so", "libmisaki_g2p_plugin.so")
        loadNativePlugin("plugins/libkokoro_onnx_plugin.so", "libkokoro_onnx_plugin.so")
        pluginLoaded = true
    }

    private fun loadNativePlugin(assetPath: String, fileName: String) {
        try {
            val pluginFile = File(context.filesDir, fileName)
            Log.i(TAG, "Extracting native plugin from assets: $assetPath")
            context.assets.open(assetPath).use { inputStream ->
                FileOutputStream(pluginFile).use { outputStream ->
                    inputStream.copyTo(outputStream)
                }
            }
            Log.i(TAG, "Plugin extracted to: ${pluginFile.absolutePath}")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to extract native plugin: $assetPath", e)
        }
    }

    /**
     * Load a pipeline manifest from assets
     */
    fun loadManifest(manifestName: String): Boolean {
        return try {
            val inputStream = context.assets.open("manifests/$manifestName")
            val manifestJson = inputStream.readBytes().decodeToString()
            currentManifest = manifestJson
            Log.i(TAG, "Loaded manifest: $manifestName")
            true
        } catch (e: Exception) {
            Log.e(TAG, "Failed to load manifest: $manifestName", e)
            onError?.invoke("Failed to load manifest: ${e.message}")
            false
        }
    }

    /**
     * Load a pipeline manifest from a JSON string
     */
    fun loadManifestJson(manifestJson: String): Boolean {
        currentManifest = manifestJson
        return true
    }

    /**
     * Execute a unary (single request/response) pipeline
     */
    suspend fun executeUnary(inputText: String): String? = withContext(Dispatchers.IO) {
        if (executorHandle == 0L) {
            onError?.invoke("Executor not initialized")
            return@withContext null
        }

        if (currentManifest == null) {
            onError?.invoke("No manifest loaded")
            return@withContext null
        }

        updateState(PipelineState.RUNNING)

        try {
            val result = NativeInterface.nativeRunPipeline(executorHandle, currentManifest!!)
            updateState(PipelineState.READY)
            result
        } catch (e: Exception) {
            Log.e(TAG, "Unary execution failed", e)
            updateState(PipelineState.ERROR)
            onError?.invoke(e.message ?: "Execution failed")
            null
        }
    }

    /**
     * Execute a unary pipeline (blocking version for backward compatibility).
     */
    fun executeUnaryBlocking(inputText: String): String? = runBlocking { executeUnary(inputText) }

    /**
     * Start a streaming session
     */
    suspend fun startStreaming(): Boolean = withContext(Dispatchers.IO) {
        if (executorHandle == 0L) {
            onError?.invoke("Executor not initialized")
            return@withContext false
        }

        if (currentManifest == null) {
            onError?.invoke("No manifest loaded")
            return@withContext false
        }

        if (isStreaming.get()) {
            Log.w(TAG, "Already streaming")
            return@withContext true
        }

        try {
            updateState(PipelineState.STREAMING)

            sessionHandle = NativeInterface.nativeCreateSession(executorHandle, currentManifest!!)
            if (sessionHandle == 0L) {
                throw NativeException("Failed to create session")
            }

            isStreaming.set(true)
            isRunning.set(true)

            Log.i(TAG, "Streaming session started: $sessionHandle")
            updateState(PipelineState.STREAMING)

            // Start output receiver coroutine
            scope.launch {
                receiveLoop()
            }

            true
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start streaming", e)
            isStreaming.set(false)
            updateState(PipelineState.ERROR)
            onError?.invoke(e.message ?: "Failed to start streaming")
            false
        }
    }

    /**
     * Start a streaming session (blocking version for backward compatibility).
     */
    fun startStreamingBlocking(): Boolean = runBlocking { startStreaming() }

    /**
     * Send text input to the streaming session
     */
    suspend fun sendText(text: String): Boolean = withContext(Dispatchers.IO) {
        if (!isStreaming.get() || sessionHandle == 0L) {
            onError?.invoke("Not streaming")
            return@withContext false
        }
        NativeInterface.nativeSendInputText(sessionHandle, text)
    }

    /**
     * Send text input (blocking version for backward compatibility).
     */
    fun sendTextBlocking(text: String): Boolean = runBlocking { sendText(text) }

    /**
     * Send audio input (PCM 16-bit) to the streaming session
     */
    suspend fun sendAudio(
        pcmData: ByteArray,
        sampleRate: Int = this.sampleRate,
        channels: Int = this.channels
    ): Boolean = withContext(Dispatchers.IO) {
        if (!isStreaming.get() || sessionHandle == 0L) {
            onError?.invoke("Not streaming")
            return@withContext false
        }
        NativeInterface.nativeSendInputAudio(sessionHandle, pcmData, sampleRate, channels)
    }

    /**
     * Send audio input (blocking version for backward compatibility).
     */
    fun sendAudioBlocking(
        pcmData: ByteArray,
        sampleRate: Int = this.sampleRate,
        channels: Int = this.channels
    ): Boolean = runBlocking { sendAudio(pcmData, sampleRate, channels) }

    /**
     * Receive loop for streaming output
     */
    private suspend fun receiveLoop() {
        while (isStreaming.get()) {
            try {
                val output = NativeInterface.nativeRecvOutput(sessionHandle)

                if (output.isEmpty()) {
                    // End of stream
                    Log.i(TAG, "End of stream received")
                    break
                }

                // Callback on main thread
                withContext(Dispatchers.Main) {
                    onOutput?.invoke(output)
                }
            } catch (e: Exception) {
                Log.e(TAG, "Receive error", e)
                withContext(Dispatchers.Main) {
                    onError?.invoke("Receive error: ${e.message}")
                }
                break
            }
        }

        isStreaming.set(false)
        isRunning.set(false)
        withContext(Dispatchers.Main) {
            updateState(PipelineState.READY)
        }
    }

    /**
     * Stop the streaming session
     */
    suspend fun stopStreaming() = withContext(Dispatchers.IO) {
        if (!isStreaming.get()) {
            return@withContext
        }

        isStreaming.set(false)
        isRunning.set(false)

        if (sessionHandle != 0L) {
            NativeInterface.nativeCloseSession(sessionHandle)
            sessionHandle = 0
        }

        Log.i(TAG, "Streaming stopped")
        updateState(PipelineState.READY)
    }

    /**
     * Stop the streaming session (blocking version for backward compatibility).
     */
    fun stopStreamingBlocking() = runBlocking { stopStreaming() }

    /**
     * Get available nodes for UI
     */
    suspend fun getAvailableNodes(): List<NodeInfo> = withContext(Dispatchers.IO) {
        try {
            val json = NativeInterface.nativeGetAvailableNodes()
            parseAvailableNodes(json)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to get available nodes", e)
            emptyList()
        }
    }

    /**
     * Get available nodes (blocking version for backward compatibility).
     */
    fun getAvailableNodesBlocking(): List<NodeInfo> = runBlocking { getAvailableNodes() }

    /**
     * Clean up all resources
     */
    suspend fun destroy() = withContext(Dispatchers.IO) {
        stopStreaming()

        if (executorHandle != 0L) {
            NativeInterface.nativeDestroyExecutor(executorHandle)
            executorHandle = 0
        }

        scope.coroutineContext.cancelChildren()
        updateState(PipelineState.DESTROYED)

        Log.i(TAG, "Pipeline manager destroyed")
    }

    /**
     * Clean up all resources (blocking version for backward compatibility).
     */
    fun destroyBlocking() = runBlocking { destroy() }

    /**
     * Check if currently streaming
     */
    fun isStreamingActive(): Boolean = isStreaming.get()

    /**
     * Check if executor is initialized
     */
    fun isInitialized(): Boolean = executorHandle != 0L

    private fun updateState(state: PipelineState) {
        scope.launch(Dispatchers.Main) {
            onStateChange?.invoke(state)
        }
    }
}