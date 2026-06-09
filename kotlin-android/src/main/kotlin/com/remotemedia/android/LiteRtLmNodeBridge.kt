package com.remotemedia.android

import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn

/**
 * LiteRT-LM Kotlin node bridge.
 * Requires Google LiteRT-LM library as a dependency:
 * // build.gradle.kts:
 * // api("com.google.ai.edge.litert:litert-lm:<version>")
 *
 * If the dependency is not available, this class will throw UnsupportedOperationException.
 */
class LiteRtLmNodeBridge(
    private val modelPath: String,
    private val backend: String = "gpu",
    private val maxNumTokens: Int = 512,
    private val systemPrompt: String? = null,
) {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var engine: Any? = null
    private val conversations = mutableMapOf<String, Any>()

    private val liteRtLmAvailable: Boolean

    companion object {
        private const val TAG = "LiteRtLmNodeBridge"
    }

    init {
        // Check if LiteRT-LM classes are available at runtime
        liteRtLmAvailable = try {
            Class.forName("com.google.ai.edge.litertlm.Engine")
            Class.forName("com.google.ai.edge.litertlm.Conversation")
            Class.forName("com.google.ai.edge.litertlm.EngineConfig")
            Class.forName("com.google.ai.edge.litertlm.Backend")
            Class.forName("com.google.ai.edge.litertlm.MessageCallback")
            Class.forName("com.google.ai.edge.litertlm.Role")
            true
        } catch (e: ClassNotFoundException) {
            Log.w(TAG, "LiteRT-LM dependency not found. LiteRtLmNodeBridge will not be functional.")
            false
        }
    }

    fun start() {
        if (!liteRtLmAvailable) {
            throw UnsupportedOperationException("LiteRT-LM dependency not available. Add 'com.google.ai.edge.litert:litert-lm' to your build.gradle.kts")
        }

        // Use reflection to avoid compile-time dependency
        try {
            val engineConfigClass = Class.forName("com.google.ai.edge.litertlm.EngineConfig")
            val engineClass = Class.forName("com.google.ai.edge.litertlm.Engine")

            val config = engineConfigClass.getDeclaredConstructor().newInstance()
            engineConfigClass.getField("modelPath").set(config, modelPath)
            val backendEnum = Class.forName("com.google.ai.edge.litertlm.Backend")
            val enumConstants = backendEnum.enumConstants
            val backendValue = enumConstants.first { (it as Enum<*>).name.lowercase() == backend.lowercase() }
            engineConfigClass.getField("backend").set(config, backendValue)
            engineConfigClass.getField("maxNumTokens").set(config, maxNumTokens)

            val engineInstance = engineClass.getDeclaredConstructor(engineConfigClass).newInstance(config)
            engineClass.getMethod("initialize").invoke(engineInstance)

            engine = engineInstance
            Log.i(TAG, "LiteRT-LM engine initialized modelPath=$modelPath backend=$backend")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to initialize LiteRT-LM engine", e)
            throw RuntimeException("Failed to initialize LiteRT-LM engine", e)
        }
    }

    fun stop() {
        runBlocking { shutdown() }
    }

    private suspend fun shutdown() {
        conversations.values.forEach { conv ->
            try {
                conv.javaClass.getMethod("close").invoke(conv)
            } catch (_: Exception) {}
        }
        conversations.clear()
        engine?.let { eng ->
            try {
                eng.javaClass.getMethod("close").invoke(eng)
            } catch (_: Exception) {}
        }
        engine = null
        scope.cancel()
    }

    fun generate(sessionId: String, userText: String): Flow<String> = channelFlow {
        if (!liteRtLmAvailable) {
            close(UnsupportedOperationException("LiteRT-LM dependency not available"))
            return@channelFlow
        }

        val conversation = conversations.getOrPut(sessionId) {
            try {
                val conversationClass = Class.forName("com.google.ai.edge.litertlm.Conversation")
                val conversationConfigClass = Class.forName("com.google.ai.edge.litertlm.ConversationConfig")
                val config = conversationConfigClass.getDeclaredConstructor().newInstance()
                systemPrompt?.let { config.javaClass.getField("systemInstruction").set(config, it) }
                val eng = engine!!
                val createConv = eng.javaClass.getMethod("createConversation", conversationConfigClass)
                createConv.invoke(eng, config)
            } catch (e: Exception) {
                Log.e(TAG, "Failed to create conversation", e)
                close(e)
                return@channelFlow
            }
        }

        val callback = object : java.lang.Object() {
            @Suppress("UNUSED_PARAMETER")
            fun onMessage(message: Any) {
                try {
                    val text = message.javaClass.getMethod("contentAsStringOrNull").invoke(message) as String?
                    text?.let { trySendBlocking(it) }
                } catch (_: Exception) {}
            }

            @Suppress("UNUSED_PARAMETER")
            fun onDone() {
                close()
            }

            @Suppress("UNUSED_PARAMETER")
            fun onError(throwable: Throwable) {
                Log.e(TAG, "LiteRT-LM stream error: ${throwable.message}", throwable)
                close(throwable)
            }
        }

        try {
            val sendMessage = conversation.javaClass.getMethod("sendMessageAsync", String::class.java, Class.forName("com.google.ai.edge.litertlm.MessageCallback"))
            sendMessage.invoke(conversation, userText, callback)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to send message", e)
            close(e)
        }

        awaitClose {
            Log.d(TAG, "Flow closed for session=$sessionId")
        }
    }.flowOn(Dispatchers.IO)

    fun shutdownSession(sessionId: String) {
        conversations.remove(sessionId)?.let { conv ->
            try {
                conv.javaClass.getMethod("close").invoke(conv)
            } catch (_: Exception) {}
        }
    }

    fun isAvailable(): Boolean = liteRtLmAvailable
}