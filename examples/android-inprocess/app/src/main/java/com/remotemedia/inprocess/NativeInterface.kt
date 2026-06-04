package com.remotemedia.inprocess

import android.util.Log

/**
 * JNI interface for RemoteMedia native library.
 * All methods correspond to native functions in lib.rs
 */
object NativeInterface {
    
    private const val TAG = "NativeInterface"
    
    // Opaque handle type for executor
    typealias ExecutorHandle = Long
    typealias SessionHandle = Long
    
    // Initialize Android logger
    @JvmStatic
    external fun initLogger()
    
    // Create a pipeline executor (unary)
    @JvmStatic
    external fun nativeCreateExecutor(): ExecutorHandle
    
    // Execute a simple pipeline (unary)
    @JvmStatic
    external fun nativeRunPipeline(handle: ExecutorHandle, manifestJson: String): String
    
    // Destroy executor
    @JvmStatic
    external fun nativeDestroyExecutor(handle: ExecutorHandle)
    
    // Test Python node directly
    @JvmStatic
    external fun nativeTestPythonNode(): String
    
    // Create a streaming session
    @JvmStatic
    external fun nativeCreateSession(handle: ExecutorHandle, manifestJson: String): SessionHandle
    
    // Send text input to session
    @JvmStatic
    external fun nativeSendInputText(session: SessionHandle, text: String): Boolean
    
    // Send audio samples (PCM 16-bit) to session
    @JvmStatic
    external fun nativeSendInputAudio(
        session: SessionHandle,
        pcmData: ByteArray,
        sampleRate: Int,
        channels: Int
    ): Boolean
    
    // Receive output from session (blocking)
    @JvmStatic
    external fun nativeRecvOutput(session: SessionHandle): String
    
    // Close and destroy session
    @JvmStatic
    external fun nativeCloseSession(session: SessionHandle)
    
    // Get available nodes for UI
    @JvmStatic
    external fun nativeGetAvailableNodes(): String
    
    // Start streaming mode
    @JvmStatic
    external fun nativeStartStreaming(handle: ExecutorHandle): Boolean
    
    // Stop streaming
    @JvmStatic
    external fun nativeStopStreaming(handle: ExecutorHandle): Boolean

    companion object {
        private const val LIB_NAME = "remotemedia_android_inprocess"
        
        init {
            // Library is loaded in Application.onCreate()
        }
    }
}

/**
 * Exception thrown by native operations
 */
class NativeException(message: String) : RuntimeException(message)

/**
 * Pipeline execution modes
 */
enum class PipelineMode {
    UNARY,
    STREAMING
}

/**
 * Pipeline node information for UI
 */
data class NodeInfo(
    val name: String,
    val description: String,
    val category: String, // "STT", "LLM", "TTS", "VAD", "UTILITY"
    val inputTypes: List<String>,
    val outputTypes: List<String>,
    val parameters: Map<String, Any>
)

/**
 * Parses node list from native JSON
 */
fun parseAvailableNodes(json: String): List<NodeInfo> {
    return try {
        kotlinx.serialization.json.Json { ignoreUnknownKeys = true }
            .decodeFromString(
                kotlinx.serialization.json.JsonElement.serializer().listType(),
                json
            ).map { element ->
                val obj = element.jsonObject
                NodeInfo(
                    name = obj["name"]?.jsonPrimitive?.content ?: "",
                    description = obj["description"]?.jsonPrimitive?.content ?: "",
                    category = obj["category"]?.jsonPrimitive?.content ?: "UNKNOWN",
                    inputTypes = (obj["input_types"]?.jsonArray?.map { it.jsonPrimitive?.content ?: "" } ?: emptyList()),
                    outputTypes = (obj["output_types"]?.jsonArray?.map { it.jsonPrimitive?.content ?: "" } ?: emptyList()),
                    parameters = obj["parameters"]?.jsonObject?.mapValues { (_, v) -> v.jsonPrimitive?.content ?: "" } ?: emptyMap()
                )
            }
    } catch (e: Exception) {
        Log.e(TAG, "Failed to parse available nodes: ${e.message}")
        emptyList()
    }
}