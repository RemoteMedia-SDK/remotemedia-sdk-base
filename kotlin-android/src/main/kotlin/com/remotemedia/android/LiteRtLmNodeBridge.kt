package com.remotemedia.android

import android.util.Log
import com.google.ai.edge.litertlm.Backend
import com.google.ai.edge.litertlm.Conversation
import com.google.ai.edge.litertlm.ConversationConfig
import com.google.ai.edge.litertlm.Engine
import com.google.ai.edge.litertlm.EngineConfig
import com.google.ai.edge.litertlm.MessageCallback
import com.google.ai.edge.litertlm.Role
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn

class LiteRtLmNodeBridge(
    private val modelPath: String,
    private val backend: String = "gpu",
    private val maxNumTokens: Int = 512,
    private val systemPrompt: String? = null,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var engine: Engine? = null
    private val conversations = mutableMapOf<String, Conversation>()

    fun start() {
        val config = EngineConfig(
            modelPath = modelPath,
            backend = Backend.valueOf(backend.uppercase()),
            maxNumTokens = maxNumTokens,
        )
        engine = Engine(config).also { it.initialize() }
        Log.i(TAG, "LiteRT-LM engine initialized modelPath=$modelPath backend=$backend")
    }

    fun stop() {
        runBlocking { shutdown() }
    }

    private suspend fun shutdown() {
        conversations.values.forEach { it.close() }
        conversations.clear()
        engine?.close()
        engine = null
        scope.cancel()
    }

    fun generate(sessionId: String, userText: String): Flow<String> = channelFlow {
        val conversation = conversations.getOrPut(sessionId) {
            ConversationConfig().also { cfg ->
                systemPrompt?.let { cfg.systemInstruction = it }
            }.let { cfg -> engine!!.createConversation(cfg) }
        }

        val callback = object : MessageCallback {
            override fun onMessage(message: com.google.ai.edge.litertlm.Message) {
                val text = message.contentAsStringOrNull()
                if (text != null) {
                    trySendBlocking(text)
                }
            }

            override fun onDone() {
                close()
            }

            override fun onError(throwable: Throwable) {
                Log.e(TAG, "LiteRT-LM stream error: ${throwable.message}", throwable)
                close(throwable)
            }
        }

        conversation.sendMessageAsync(userText, callback)

        awaitClose {
            Log.d(TAG, "Flow closed for session=$sessionId")
        }
    }.flowOn(Dispatchers.IO)

    fun shutdownSession(sessionId: String) {
        conversations.remove(sessionId)?.close()
    }

    companion object {
        private const val TAG = "LiteRtLmNodeBridge"
    }
}
