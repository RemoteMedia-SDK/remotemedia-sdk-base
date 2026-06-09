package com.remotemedia.android

import android.util.Log
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonArray

/** JNI interface for RemoteMedia native library.
 * All methods correspond to native functions in lib.rs
 */
object NativeInterface {

    private const val TAG = "NativeInterface"
    private const val LIB_NAME = "remotemedia_android_inprocess"

    // Initialize Android logger
    @JvmStatic
    external fun initLogger()

    // Create a pipeline executor (unary)
    @JvmStatic
    external fun nativeCreateExecutor(): Long

    // Execute a simple pipeline (unary)
    @JvmStatic
    external fun nativeRunPipeline(handle: Long, manifestJson: String): String

    // Destroy executor
    @JvmStatic
    external fun nativeDestroyExecutor(handle: Long)

    // Test Python node directly
    @JvmStatic
    external fun nativeTestPythonNode(): String

    // Create a streaming session
    @JvmStatic
    external fun nativeCreateSession(handle: Long, manifestJson: String): Long

    // Send text input to session
    @JvmStatic
    external fun nativeSendInputText(session: Long, text: String): Boolean

    // Send audio samples (PCM 16-bit) to session
    @JvmStatic
    external fun nativeSendInputAudio(
        session: Long,
        pcmData: ByteArray,
        sampleRate: Int,
        channels: Int
    ): Boolean

    // Receive output from session (blocking)
    @JvmStatic
    external fun nativeRecvOutput(session: Long): String

    // Close and destroy session
    @JvmStatic
    external fun nativeCloseSession(session: Long)

    // Get available nodes for UI
    @JvmStatic
    external fun nativeGetAvailableNodes(): String

    // Get full node schema (including model sources) for a specific node type
    @JvmStatic
    external fun nativeGetNodeSchema(nodeType: String): String

    // Start streaming mode
    @JvmStatic
    external fun nativeStartStreaming(handle: Long): Boolean

    // Stop streaming
    @JvmStatic
    external fun nativeStopStreaming(handle: Long): Boolean

    // LiteRT-LM Kotlin node bridge
    @JvmStatic
    external fun nativeCreateLiteRtNode(
        executorHandle: Long,
        nodeId: String,
        modelPath: String,
        backend: String,
        maxNumTokens: Int,
        systemPrompt: String?,
    ): Long

    @JvmStatic
    external fun nativeDestroyLiteRtNode(executorHandle: Long, nodeHandle: Long)

    @JvmStatic
    external fun nativeGenerateLiteRtNode(
        executorHandle: Long,
        nodeHandle: Long,
        sessionId: String,
        text: String,
    ): Boolean
}

/** Parses node list from native JSON */
fun parseAvailableNodes(json: String): List<NodeInfo> {
    return try {
        Json { ignoreUnknownKeys = true }
            .decodeFromString(
                JsonArray.serializer(),
                json
            ).map { element ->
                val obj = element as JsonObject
                NodeInfo(
                    name = obj["name"]?.let { (it as JsonPrimitive).content } ?: "",
                    description = obj["description"]?.let { (it as JsonPrimitive).content } ?: "",
                    category = obj["category"]?.let { (it as JsonPrimitive).content } ?: "UNKNOWN",
                    inputTypes = (obj["input_types"]?.let { it as JsonArray }?.map { (it as JsonPrimitive).content ?: "" } ?: emptyList()),
                    outputTypes = (obj["output_types"]?.let { it as JsonArray }?.map { (it as JsonPrimitive).content ?: "" } ?: emptyList()),
                    parameters = obj["parameters"]?.let { it as JsonObject }?.mapValues { (_, v) -> (v as JsonPrimitive).content ?: "" } ?: emptyMap()
                )
            }
    } catch (e: Exception) {
        Log.e("NativeInterface", "Failed to parse available nodes: ${e.message}")
        emptyList()
    }
}

/** Exception thrown by native operations */
class NativeException(message: String) : RuntimeException(message)

/** Pipeline node information for UI */
data class NodeInfo(
    val name: String,
    val description: String,
    val category: String, // "STT", "LLM", "TTS", "VAD", "UTILITY"
    val inputTypes: List<String>,
    val outputTypes: List<String>,
    val parameters: Map<String, Any>
)