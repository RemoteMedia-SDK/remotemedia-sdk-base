package com.remotemedia.inprocess

import android.content.Context
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import java.io.File
import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Manages RemoteMedia pipeline lifecycle and execution.
 * Handles both unary and streaming modes.
 */
class PipelineManager(private val context: Context) {
    
    private const val TAG = "PipelineManager"
    
    // State
    private var executorHandle: NativeInterface.ExecutorHandle = 0
    private var sessionHandle: NativeInterface.SessionHandle = 0
    private val isRunning = AtomicBoolean(false)
    private val isStreaming = AtomicBoolean(false)
    
    // Coroutine scope for background operations
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    
    // Audio configuration
    private val sampleRate = 16000
    private val channels = 1
    
    // Callbacks
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
     * Initialize the pipeline executor
     */
    fun initialize(): Boolean = scope.await {
        try {
            updateState(PipelineState.INITIALIZING)
            
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
        true
    }
    
    /**
     * Execute a unary (single request/response) pipeline
     */
    fun executeUnary(inputText: String): String? {
        return scope.await {
            if (executorHandle == 0L) {
                onError?.invoke("Executor not initialized")
                return@await null
            }
            
            if (currentManifest == null) {
                onError?.invoke("No manifest loaded")
                return@await null
            }
            
            updateState(PipelineState.RUNNING)
            
            try {
                // Create a temporary manifest with the input embedded
                // or just run with the manifest and pass input via JNI
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
    }
    
    /**
     * Start a streaming session
     */
    fun startStreaming(): Boolean = scope.await {
        if (executorHandle == 0L) {
            onError?.invoke("Executor not initialized")
            return@await false
        }
        
        if (currentManifest == null) {
            onError?.invoke("No manifest loaded")
            return@await false
        }
        
        if (isStreaming.get()) {
            Log.w(TAG, "Already streaming")
            return@await true
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
     * Send text input to the streaming session
     */
    fun sendText(text: String): Boolean {
        if (!isStreaming.get() || sessionHandle == 0L) {
            onError?.invoke("Not streaming")
            return false
        }
        
        return NativeInterface.nativeSendInputText(sessionHandle, text)
    }
    
    /**
     * Send audio input (PCM 16-bit) to the streaming session
     */
    fun sendAudio(pcmData: ByteArray, sampleRate: Int = this.sampleRate, channels: Int = this.channels): Boolean {
        if (!isStreaming.get() || sessionHandle == 0L) {
            onError?.invoke("Not streaming")
            return false
        }
        
        return NativeInterface.nativeSendInputAudio(sessionHandle, pcmData, sampleRate, channels)
    }
    
    /**
     * Receive loop for streaming output
     */
    private fun receiveLoop() {
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
    fun stopStreaming() = scope.await {
        if (!isStreaming.get()) {
            return@await
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
     * Get available nodes for UI
     */
    fun getAvailableNodes(): List<NodeInfo> = scope.await {
        try {
            val json = NativeInterface.nativeGetAvailableNodes()
            parseAvailableNodes(json)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to get available nodes", e)
            emptyList()
        }
    }
    
    /**
     * Clean up all resources
     */
    fun destroy() = scope.await {
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
     * Check if currently streaming
     */
    fun isStreamingActive(): Boolean = isStreaming.get()
    
    /**
     * Check if executor is initialized
     */
    fun isInitialized(): Boolean = executorHandle != 0L
    
    private fun updateState(state: PipelineState) {
        withContext(Dispatchers.Main) {
            onStateChange?.invoke(state)
        }
    }
}