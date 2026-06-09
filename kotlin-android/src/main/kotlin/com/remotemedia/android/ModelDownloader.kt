package com.remotemedia.android

import android.content.Context
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

/** Fetches model sources from node schemas via native FFI and downloads them. */
class ModelDownloader(private val context: Context) {

    companion object {
        private const val TAG = "ModelDownloader"
        private const val DEFAULT_TIMEOUT_MS = 300000
        private const val CHUNK_SIZE = 8192

        // JSON config matching Rust's snake_case serialization
        private val snakeCaseJson = Json { ignoreUnknownKeys = true }
    }

    private val cacheDir: File = File(context.filesDir, "cache/models").apply { mkdirs() }

    // Cache of node schemas to avoid repeated native calls
    private val schemaCache = ConcurrentHashMap<String, NodeSchema>()

    /** Model source declaration from node schema. */
    @Serializable
    data class ModelSourceFile(
        @SerialName("path") val path: String,
        @SerialName("filename") val filename: String,
        @SerialName("url") val url: String,
        @SerialName("expected_size") val expectedSize: Long? = null,
        @SerialName("required") val required: Boolean = true
    )

    /** Node schema with model sources. */
    @Serializable
    data class NodeSchema(
        @SerialName("node_type") val nodeType: String,
        @SerialName("description") val description: String? = null,
        @SerialName("category") val category: String? = null,
        @SerialName("accepts") val accepts: List<String> = emptyList(),
        @SerialName("produces") val produces: List<String> = emptyList(),
        @SerialName("config_schema") val configSchema: JsonElement? = null,
        @SerialName("config_defaults") val configDefaults: JsonElement? = null,
        @SerialName("is_python") val isPython: Boolean = false,
        @SerialName("streaming") val streaming: Boolean = true,
        @SerialName("multi_output") val multiOutput: Boolean = false,
        @SerialName("model_sources") val modelSources: ModelSources? = null
    )

    @Serializable
    data class ModelSources(
        @SerialName("files") val files: List<ModelSourceFile> = emptyList()
    )

    interface DownloadProgressListener {
        fun onProgress(modelName: String, downloadedBytes: Long, totalBytes: Long, percent: Double)
        fun onCompleted(modelName: String, localPath: String)
        fun onError(modelName: String, error: String)
    }

    /** Fetch node schema from native registry (cached). */
    suspend fun getNodeSchema(nodeType: String): NodeSchema? = withContext(Dispatchers.IO) {
        schemaCache.getOrPut(nodeType) {
            try {
                val json = NativeInterface.nativeGetNodeSchema(nodeType)
                val nodeSchema = snakeCaseJson.decodeFromString(NodeSchema.serializer(), json)
                if (nodeSchema.nodeType.isEmpty()) null else nodeSchema
            } catch (e: Exception) {
                Log.w(TAG, "Failed to fetch schema for $nodeType: ${e.message}")
                null
            }
        }
    }

    /** Get all model sources for a given node type from its schema. */
    suspend fun getModelSources(nodeType: String): List<ModelSourceFile> = withContext(Dispatchers.IO) {
        getNodeSchema(nodeType)?.modelSources?.files ?: emptyList()
    }

    /** Get model source by filename across all known node schemas. */
    suspend fun getModelSource(nodeType: String, filename: String): ModelSourceFile? = withContext(Dispatchers.IO) {
        getModelSources(nodeType).find { it.filename == filename }
    }

    fun isModelCached(modelName: String): Boolean {
        val modelFile = File(cacheDir, modelName)
        return modelFile.exists() && modelFile.length() > 0
    }

    fun getModelPath(modelName: String): String {
        return File(cacheDir, modelName).absolutePath
    }

    /** Ensure all models required by a node type are downloaded. */
    suspend fun ensureModelsForNode(
        nodeType: String,
        listener: DownloadProgressListener? = null
    ): Result<List<String>> = withContext(Dispatchers.IO) {
        val sources = getModelSources(nodeType)
        if (sources.isEmpty()) {
            Log.i(TAG, "No model sources declared for node: $nodeType")
            return@withContext Result.success(emptyList())
        }

        val results = mutableListOf<Result<String>>()
        for (source in sources) {
            val result = ensureModelDownloaded(source, listener)
            results.add(result)
            // If required model failed, stop and propagate error
            if (result.isFailure() && source.required) {
                return@withContext Result.failure(result.getExceptionOrNull() ?: IllegalStateException("Required model download failed: ${source.filename}"))
            }
        }
        Result.success(results.mapNotNull { it.getOrNull() })
    }

    /** Download a single model from its source declaration. */
    suspend fun ensureModelDownloaded(
        source: ModelSourceFile,
        listener: DownloadProgressListener? = null
    ): Result<String> = withContext(Dispatchers.IO) {
        try {
            if (isModelCached(source.filename)) {
                val path = getModelPath(source.filename)
                Log.i(TAG, "Model already cached: ${source.filename} at $path")
                listener?.onCompleted(source.filename, path)
                return@withContext Result.success(path)
            }

            val path = downloadModel(source, listener)
            Result.success(path)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to ensure model downloaded: ${source.filename}", e)
            listener?.onError(source.filename, e.message ?: "Unknown error")
            Result.failure(e)
        }
    }

    /** Legacy overload: resolve by filename only (looks up in all schemas). */
    suspend fun ensureModelDownloaded(
        filename: String,
        listener: DownloadProgressListener? = null
    ): Result<String> = withContext(Dispatchers.IO) {
        // Try to find the model in common node types
        val commonNodeTypes = listOf(
            "SileroVAD", "KokoroTTSNode", "WhisperSTTNode", "LiteRtLmGenerationNode",
            "SileroVADNode", "WhisperNode", "MisakiG2PNode"
        )

        for (nodeType in commonNodeTypes) {
            val source = getModelSource(nodeType, filename)
            if (source != null) {
                return@withContext ensureModelDownloaded(source, listener)
            }
        }

        Result.failure(IllegalArgumentException("Model not found in any node schema: $filename"))
    }

    private suspend fun downloadModel(
        source: ModelSourceFile,
        listener: DownloadProgressListener?
    ): String {
        val modelFile = File(cacheDir, source.filename)
        val tempFile = File(modelFile.parentFile, "${modelFile.name}.downloading")

        Log.i(TAG, "Starting download: ${source.filename} from ${source.url}")

        try {
            val url = URL(source.url)
            val connection = url.openConnection()
            connection.connectTimeout = 30000
            connection.readTimeout = DEFAULT_TIMEOUT_MS

            val contentLength = connection.contentLengthLong
            val totalBytes = if (contentLength > 0) contentLength else source.expectedSize ?: 0L

            connection.getInputStream().use { input ->
                FileOutputStream(tempFile).use { output ->
                    val buffer = ByteArray(CHUNK_SIZE)
                    var downloaded = 0L

                    while (true) {
                        val read = input.read(buffer)
                        if (read == -1) break

                        output.write(buffer, 0, read)
                        downloaded += read

                        if (totalBytes > 0) {
                            val percent = (downloaded.toDouble() / totalBytes) * 100
                            listener?.onProgress(source.filename, downloaded, totalBytes, percent)
                        }
                    }
                }
            }

            if (tempFile.length() == 0L) {
                throw IllegalStateException("Downloaded file is empty")
            }

            if (!tempFile.renameTo(modelFile)) {
                throw IllegalStateException("Failed to move downloaded file to cache")
            }

            Log.i(TAG, "Download completed: ${source.filename} -> ${modelFile.absolutePath}")
            listener?.onCompleted(source.filename, modelFile.absolutePath)
            return modelFile.absolutePath
        } catch (e: Exception) {
            tempFile.delete()
            throw e
        }
    }

    fun clearModelCache(modelName: String): Boolean {
        return File(cacheDir, modelName).delete()
    }

    fun clearAllCache() {
        cacheDir.listFiles()?.forEach { it.delete() }
    }

    fun getCacheSize(): Long {
        return cacheDir.listFiles()?.sumOf { it.length() } ?: 0L
    }
}

sealed class Result<out T> {
    data class Success<T>(val value: T) : Result<T>()
    data class Failure(val exception: Exception) : Result<Nothing>()

    companion object {
        fun <T> success(value: T): Result<T> = Success(value)
        fun <T> failure(exception: Exception): Result<T> = Failure(exception)
    }

    fun isSuccess(): Boolean = this is Success
    fun isFailure(): Boolean = this is Failure

    fun getOrNull(): T? = if (this is Success) value else null
    fun getExceptionOrNull(): Exception? = if (this is Failure) exception else null
}