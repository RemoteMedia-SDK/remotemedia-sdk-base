package com.remotemedia.android

import android.content.Context
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.copyTo
import kotlin.io.path.deleteRecursively

/**
 * Manages RemoteMedia pipeline lifecycle and execution.
 * Handles both unary and streaming modes with idiomatic Kotlin coroutines API.
 */
class PipelineManager(private val context: Context) {

    companion object {
        private const val TAG = "PipelineManager"
        private var pluginLoaded = false
    }

    // Use application context for asset access (merged assets)
    private val appAssets = context.applicationContext.assets
    private var executorHandle: Long = 0
    private var sessionHandle: Long = 0
    private val isRunning = AtomicBoolean(false)
    private val isStreaming = AtomicBoolean(false)

    // Coroutine scope for background operations
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Audio configuration
    private val sampleRate = 16000
    private val channels = 1

    // Model downloader for fetching models not in APK
    private val modelDownloader = ModelDownloader(context)

    // Sproot container lifecycle controller
    private val sprootController by lazy { SprootController(context) }

    // Cache of node type to schema for this manifest resolution
    private val nodeSchemaCache = mutableMapOf<String, ModelDownloader.NodeSchema>()

    // Callbacks (for backward compatibility and simple UI integration)
    var onOutput: ((String) -> Unit)? = null
    var onError: ((String) -> Unit)? = null
    var onStateChange: ((PipelineState) -> Unit)? = null
    var onModelDownloadProgress: ((String, Long, Long, Double) -> Unit)? = null

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

            // Set app files directory for native code (must be before nativeCreateExecutor)
            NativeInterface.nativeSetAppFilesDir(context.filesDir.absolutePath)

            // Ensure native loadable plugins are extracted to files directory
            extractRuntimeAssets()

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
    private fun extractRuntimeAssets() {
        if (pluginLoaded) {
            Log.i(TAG, "Runtime assets already extracted")
            return
        }

        val homeDir = File(context.filesDir, ".hermes")
        val marker = File(homeDir, ".extracted")

        if (marker.exists()) {
            Log.i(TAG, "Runtime assets already extracted to ${homeDir.absolutePath}")
            pluginLoaded = true
            return
        }

        // Native loadable plugins: assets/plugins/*.so -> files/*.so
        extractAssetFile(
            assetPath = "plugins/libsilero_vad_loadable_plugin.so",
            destFile = File(context.filesDir, "libsilero_vad_loadable_plugin.so")
        )
        extractAssetFile(
            assetPath = "plugins/libwhisper_loadable_plugin.so",
            destFile = File(context.filesDir, "libwhisper_loadable_plugin.so")
        )
        extractAssetFile(
            assetPath = "plugins/liblitert_lm_loadable_plugin.so",
            destFile = File(context.filesDir, "liblitert_lm_loadable_plugin.so")
        )
        extractAssetFile(
            assetPath = "plugins/libmisaki_g2p_plugin.so",
            destFile = File(context.filesDir, "libmisaki_g2p_plugin.so")
        )
        extractAssetFile(
            assetPath = "plugins/libkokoro_onnx_plugin.so",
            destFile = File(context.filesDir, "libkokoro_onnx_plugin.so")
        )

        // Python runtime: assets/python-runtimes/hermes/... -> files/python/...
        extractAssetTree(
            assetPath = "python-runtimes/hermes/bundle",
            destDir = File(context.filesDir, "python/bundle")
        )
        extractAssetTree(
            assetPath = "python-runtimes/hermes/src",
            destDir = File(context.filesDir, "python/src")
        )

        // Small/model support assets: assets/models/... -> files/models/...
        extractAssetTree(
            assetPath = "models/silero-vad",
            destDir = File(context.filesDir, "models/silero-vad")
        )
        extractAssetTree(
            assetPath = "models/whisper",
            destDir = File(context.filesDir, "models/whisper")
        )
        extractAssetTree(
            assetPath = "models/kokoro",
            destDir = File(context.filesDir, "models/kokoro")
        )
        extractAssetTree(
            assetPath = "models/misaki-g2p",
            destDir = File(context.filesDir, "models/misaki-g2p")
        )

        File(context.filesDir, "cache/litert-lm").mkdirs()

        // Create ~/.hermes/ directory if it doesn't exist (profile will be loaded/created at runtime)
        homeDir.mkdirs()

        pluginLoaded = true
        Log.i(TAG, "Runtime assets extracted")
    }

    private fun extractAssetFile(assetPath: String, destFile: File) {
        try {
            destFile.parentFile?.mkdirs()

            Log.i(TAG, "Extracting asset file: $assetPath -> ${destFile.absolutePath}")
            appAssets.open(assetPath).use { inputStream ->
                FileOutputStream(destFile).use { outputStream ->
                    inputStream.copyTo(outputStream)
                }
            }

            // Plugin .so files loaded via dlopen need executable/readable mode.
            destFile.setReadable(true, true)
            destFile.setExecutable(true, true)

            Log.i(TAG, "Extracted asset file: ${destFile.absolutePath}")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to extract asset file: $assetPath", e)
            throw e
        }
    }

    private fun extractAssetTree(assetPath: String, destDir: File) {
        val children = try {
            appAssets.list(assetPath)?.toList().orEmpty()
        } catch (e: Exception) {
            Log.e(TAG, "Failed to list asset directory: $assetPath", e)
            throw e
        }

        if (children.isEmpty()) {
            // AssetManager returns an empty list for files.
            extractAssetFile(assetPath, destDir)
            return
        }

        destDir.mkdirs()

        for (child in children) {
            val childAssetPath = "$assetPath/$child"
            val childDest = File(destDir, child)

            val grandChildren = try {
                appAssets.list(childAssetPath)?.toList().orEmpty()
            } catch (_: Exception) {
                emptyList()
            }

            if (grandChildren.isEmpty()) {
                extractAssetFile(childAssetPath, childDest)
            } else {
                extractAssetTree(childAssetPath, childDest)
            }
        }
    }

    /** Legacy alias for backwards compatibility */
    @Deprecated("Use extractRuntimeAssets() instead", ReplaceWith("extractRuntimeAssets()"))
    private fun loadNativePlugins() {
        extractRuntimeAssets()
    }

    private fun assetExists(assetPath: String): Boolean {
        return try {
            val entries = appAssets.list(assetPath)
            entries != null && entries.isNotEmpty()
        } catch (_: Exception) {
            false
        }
    }

    private fun extractTar(tarPath: String, destDir: File) {
        destDir.mkdirs()
        try {
            val process = Runtime.getRuntime().exec("tar -xf $tarPath -C ${destDir.absolutePath}")
            val result = process.waitFor()
            if (result == 0) {
                Log.i(TAG, "Extracted tar: $tarPath -> ${destDir.absolutePath}")
            } else {
                Log.e(TAG, "Failed to extract tar: $tarPath, exit code: $result")
            }
        } catch (e: Exception) {
            Log.e(TAG, "Exception extracting tar: $tarPath", e)
        }
    }

    /**
     * Load a Hermes profile from a directory containing auth.json and other profile files.
     * Copies the profile to ~/.hermes/ for runtime access.
     */
    suspend fun loadHermesProfile(profileDir: File): Boolean = withContext(Dispatchers.IO) {
        try {
            val homeDir = File(context.filesDir, ".hermes")
            if (homeDir.exists()) {
                // Clear existing profile
                homeDir.listFiles()?.forEach { it.deleteRecursively() }
            }
            homeDir.mkdirs()

            val authJson = File(profileDir, "auth.json")
            if (!authJson.exists()) {
                Log.e(TAG, "Profile directory missing auth.json: $profileDir")
                return@withContext false
            }

            // Copy auth.json
            val destAuth = File(homeDir, "auth.json")
            authJson.copyTo(destAuth, overwrite = true)

            // Copy any other profile files
            profileDir.listFiles()?.forEach { file ->
                if (file.name != "auth.json" && file.isFile) {
                    val dest = File(homeDir, file.name)
                    file.copyTo(dest, overwrite = true)
                }
            }

            val marker = File(homeDir, ".extracted")
            marker.createNewFile()

            Log.i(TAG, "Loaded Hermes profile from: $profileDir")
            true
        } catch (e: Exception) {
            Log.e(TAG, "Failed to load Hermes profile", e)
            false
        }
    }

    /**
     * Create a new Hermes profile using the hermes CLI tools.
     * This runs the profile creation process and saves it to ~/.hermes/
     */
    suspend fun createHermesProfile(profileName: String): Boolean = withContext(Dispatchers.IO) {
        try {
            val homeDir = File(context.filesDir, ".hermes")
            homeDir.mkdirs()

            // Run Python-based profile creation using the Hermes CLI tools via JNI
            // This will create auth.json and default config
            val success = NativeInterface.nativeCreateHermesProfile(profileName, homeDir.absolutePath)

            if (success) {
                val marker = File(homeDir, ".extracted")
                marker.createNewFile()
                Log.i(TAG, "Created Hermes profile: $profileName")
            }
            success
        } catch (e: Exception) {
            Log.e(TAG, "Failed to create Hermes profile", e)
            false
        }
    }

    /**
     * Check if a valid Hermes profile exists in ~/.hermes/
     */
    fun hasHermesProfile(): Boolean {
        val authJson = File(context.filesDir, ".hermes/default/auth.json")
        return authJson.exists() && authJson.length() > 0
    }

    /**
     * Resolve model paths in the manifest, downloading any models that aren't cached locally.
     * This fetches each node's schema (which declares model_sources) and ensures
     * all required models are downloaded, rewriting paths in the manifest.
     */
    private suspend fun resolveAndDownloadModels(manifestJson: String): String {
        return try {
            val json = Json { ignoreUnknownKeys = true }
                .decodeFromString(JsonElement.serializer(), manifestJson)

            val jsonObject = json as? JsonObject ?: return manifestJson

            val nodesElement = jsonObject["nodes"]
            if (nodesElement !is JsonArray) return manifestJson

            val updatedNodes = mutableListOf<JsonElement>()
            nodeSchemaCache.clear()

            for (nodeElement in nodesElement) {
                val nodeObj = nodeElement as? JsonObject
                if (nodeObj == null) {
                    updatedNodes.add(nodeElement)
                    continue
                }

                val nodeType = nodeObj["node_type"]?.let { (it as JsonPrimitive).content }
                val nodeId = nodeObj["id"]?.let { (it as JsonPrimitive).content } ?: "unknown"

                val paramsElement = nodeObj["params"]
                if (paramsElement !is JsonObject) {
                    updatedNodes.add(nodeElement)
                    continue
                }

                val paramsObj = paramsElement
                val updatedParams = paramsObj.toMutableMap()

                // Fetch node schema if we have a node_type
                nodeType?.let { type ->
                    if (!nodeSchemaCache.containsKey(type)) {
                        val schema = runBlocking {
                            modelDownloader.getNodeSchema(type)
                        }
                        schema?.let { nodeSchemaCache[type] = it }
                    }
                }

                // Download all models declared in the node's schema
                nodeType?.let { type ->
                    nodeSchemaCache[type]?.modelSources?.files?.forEach { source ->
                        if (!source.required) return@forEach // skip optional for now, they're handled by model_sources in manifest

                        val localPath = File(context.filesDir, "models/${source.path}").absolutePath
                        val expectedPath = File(context.filesDir, "models/${source.path}")

                        // If model already exists at expected path, use it
                        if (expectedPath.exists() && expectedPath.length() > 0) {
                            // Rewrite any param field matching the declared path
                            updatedParams.forEach { (key, value) ->
                                val s = (value as? JsonPrimitive)?.content ?: return@forEach
                                if (s.contains(source.path) || s.contains(source.filename)) {
                                    updatedParams[key] = JsonPrimitive(localPath)
                                }
                            }
                            return@forEach
                        }

                        // Download the model
                        updateState(PipelineState.INITIALIZING)
                        val listener = object : ModelDownloader.DownloadProgressListener {
                            override fun onProgress(modelName: String, downloadedBytes: Long, totalBytes: Long, percent: Double) {
                                scope.launch(Dispatchers.Main) {
                                    onModelDownloadProgress?.invoke(modelName, downloadedBytes, totalBytes, percent)
                                }
                            }
                            override fun onCompleted(modelName: String, localPath: String) {}
                            override fun onError(modelName: String, error: String) {}
                        }

                        val result = runBlocking {
                            modelDownloader.ensureModelDownloaded(source, listener)
                        }

                        when (result) {
                            is Result.Success -> {
                                val downloadedPath = result.getOrNull()!!
                                Log.i(TAG, "Model resolved to local path: $downloadedPath")
                                // Rewrite any param field matching the declared path
                                updatedParams.forEach { (key, value) ->
                                    val s = (value as? JsonPrimitive)?.content ?: return@forEach
                                    if (s.contains(source.path) || s.contains(source.filename)) {
                                        updatedParams[key] = JsonPrimitive(downloadedPath)
                                    }
                                }
                            }
                            is Result.Failure -> {
                                val error = result.getExceptionOrNull()?.message ?: "Unknown download error"
                                Log.e(TAG, "Failed to download model: ${source.filename} - $error")
                                if (source.required) {
                                    throw IllegalStateException("Required model download failed: ${source.filename} - $error")
                                }
                            }
                        }
                    }
                }

                // Also check for model_path parameter (legacy support)
                paramsObj["model_path"]?.let { modelPathElement ->
                    val modelPath = (modelPathElement as? JsonPrimitive)?.content
                    if (modelPath != null && modelPath.isNotEmpty()) {
                        val fileName = File(modelPath).name
                        // Check if we already downloaded it via schema
                        val alreadyHandled = nodeType?.let { type ->
                            nodeSchemaCache[type]?.modelSources?.files?.any { it.filename == fileName } ?: false
                        } ?: false

                        if (!alreadyHandled) {
                            // Try legacy lookup by filename
                            val listener = object : ModelDownloader.DownloadProgressListener {
                                override fun onProgress(modelName: String, downloadedBytes: Long, totalBytes: Long, percent: Double) {
                                    scope.launch(Dispatchers.Main) {
                                        onModelDownloadProgress?.invoke(modelName, downloadedBytes, totalBytes, percent)
                                    }
                                }
                                override fun onCompleted(modelName: String, localPath: String) {}
                                override fun onError(modelName: String, error: String) {}
                            }

                            val result = runBlocking {
                                modelDownloader.ensureModelDownloaded(fileName, listener)
                            }

                            when (result) {
                                is Result.Success -> {
                                    val localPath = result.getOrNull()!!
                                    Log.i(TAG, "Model resolved to local path: $localPath")
                                    updatedParams["model_path"] = JsonPrimitive(localPath)
                                }
                                is Result.Failure -> {
                                    val error = result.getExceptionOrNull()?.message ?: "Unknown download error"
                                    Log.e(TAG, "Failed to download model: $fileName - $error")
                                    throw IllegalStateException("Model download failed: $fileName - $error")
                                }
                            }
                        }
                    }
                }

                // Also check for cache_dir that might need updating
                paramsObj["cache_dir"]?.let { cacheDirElement ->
                    val cacheDir = (cacheDirElement as? JsonPrimitive)?.content
                    if (cacheDir != null && cacheDir.contains("cache/litert-lm")) {
                        // Ensure cache directory exists
                        File(cacheDir).mkdirs()
                    }
                }

                // Metadata-driven resolution: inspect params["model_sources"] and
                // download/rewrite declared files when they are missing locally.
                paramsObj["model_sources"]?.let { modelSourcesElement ->
                    val filesElement = (modelSourcesElement as? JsonObject)?.get("files")
                    if (filesElement is JsonArray) {
                        for (fileElement in filesElement) {
                            val fileObj = fileElement as? JsonObject ?: continue
                            val pathStr = (fileObj.get("path") as? JsonPrimitive)?.content ?: continue
                            if (pathStr.isEmpty()) continue

                            val filename = (fileObj.get("filename") as? JsonPrimitive)?.content ?: File(pathStr).name
                            val required = (fileObj.get("required") as? JsonPrimitive)?.content?.toBooleanStrictOrNull() ?: true

                            // Already present: nothing to do.
                            if (File(pathStr).exists()) continue

                            // Try to find in schema
                            val schemaSource = nodeType?.let { type ->
                                nodeSchemaCache[type]?.modelSources?.files?.find { it.filename == filename }
                            } ?: null

                            if (schemaSource != null) {
                                val listener = object : ModelDownloader.DownloadProgressListener {
                                    override fun onProgress(modelName: String, downloadedBytes: Long, totalBytes: Long, percent: Double) {
                                        scope.launch(Dispatchers.Main) {
                                            onModelDownloadProgress?.invoke(modelName, downloadedBytes, totalBytes, percent)
                                        }
                                    }
                                    override fun onCompleted(modelName: String, localPath: String) {}
                                    override fun onError(modelName: String, error: String) {}
                                }

                                val result = runBlocking {
                                    modelDownloader.ensureModelDownloaded(schemaSource, listener)
                                }

                                when (result) {
                                    is Result.Success -> {
                                        val localPath = result.getOrNull()!!
                                        Log.i(TAG, "Resolved model_sources file '$filename' to local path: $localPath")

                                        // Rewrite any param field whose string value matches the declared path.
                                        for ((key, value) in updatedParams) {
                                            val s = (value as? JsonPrimitive)?.content ?: continue
                                            if (s == pathStr) {
                                                updatedParams[key] = JsonPrimitive(localPath)
                                            }
                                        }
                                    }
                                    is Result.Failure -> {
                                        val error = result.getExceptionOrNull()?.message ?: "Unknown download error"
                                        if (required) {
                                            Log.e(TAG, "Failed to download required model: $filename - $error")
                                            throw IllegalStateException("Model download failed: $filename - $error")
                                        } else {
                                            Log.w(TAG, "Optional model missing, skipping: $filename - $error")
                                        }
                                    }
                                }
                            } else if (required) {
                                Log.w(TAG, "Required model has no registered downloader: $filename at $pathStr")
                            }
                        }
                    }
                }

                // Rebuild node with updated params using JsonObject builder
                val updatedNode = nodeObj.toMutableMap().apply {
                    put("params", JsonObject(updatedParams))
                }
                updatedNodes.add(JsonObject(updatedNode))
            }

            // Rebuild manifest with updated nodes
            val updatedManifest = (jsonObject as JsonObject).toMutableMap().apply {
                put("nodes", JsonArray(updatedNodes))
            }

            return Json { prettyPrint = true }.encodeToString(JsonElement.serializer(), JsonObject(updatedManifest))
        } catch (e: Exception) {
            Log.e(TAG, "Failed to resolve model paths", e)
            throw e
        }
    }

    /**
     * Ensure all models in the current manifest are downloaded and paths resolved.
     * Call this after loading a manifest and before executing.
     */
    suspend fun ensureModelsReady(): Boolean {
        if (currentManifest == null) {
            onError?.invoke("No manifest loaded")
            return false
        }

        try {
            currentManifest = resolveAndDownloadModels(currentManifest!!)
            return true
        } catch (e: Exception) {
            Log.e(TAG, "Failed to ensure models ready", e)
            onError?.invoke("Model preparation failed: ${e.message}")
            return false
        }
    }

    /**
     * Load a pipeline manifest from assets
     */
    fun loadManifest(manifestName: String): Boolean {
        return try {
            val inputStream = appAssets.open("manifests/$manifestName")
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

        // Ensure models are ready before execution
        if (!ensureModelsReady()) {
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

        // Ensure models are ready before starting streaming
        if (!ensureModelsReady()) {
            return@withContext false
        }

        try {
            updateState(PipelineState.STREAMING)

            // Determine if we need to start the Debian Sproot runner container
            if (requiresSproot(currentManifest!!)) {
                Log.i(TAG, "Manifest requires glibc runtime. Starting Sproot container...")
                val socketPath = sprootController.start()
                if (socketPath == null) {
                    throw NativeException("Failed to boot guest Debian Sproot runner container")
                }
                NativeInterface.nativeSetSprootSocketPath(socketPath)
            } else {
                // Ensure no lingering socket path is configured in JNI for in-process runs
                Log.i(TAG, "Manifest doesn't require glibc runtime, unsetting nativeSprocketSocketPath.")
                NativeInterface.nativeSetSprootSocketPath("")
            }

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
                val output = NativeInterface.nativeRecvOutput(sessionHandle) ?: break

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

        // Stop Sproot container if running
        sprootController.stop()

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

    /**
     * Checks if the manifest specifies the glibc Python runtime either globally
     * or at the node parameter level.
     */
    private fun requiresSproot(manifestJson: String): Boolean {
        return try {
            val json = Json { ignoreUnknownKeys = true }
                .decodeFromString(JsonElement.serializer(), manifestJson)
            val jsonObject = json as? JsonObject ?: return false

            // Check default_python_runtime in metadata
            val metadata = jsonObject["metadata"] as? JsonObject
            val defaultRuntime = (metadata?.get("default_python_runtime") as? JsonPrimitive)?.content
            if (defaultRuntime == "glibc") {
                return true
            }

            // Check if any individual node requires glibc
            val nodes = jsonObject["nodes"] as? JsonArray ?: return false
            for (nodeElement in nodes) {
                val nodeObj = nodeElement as? JsonObject ?: continue
                val params = nodeObj["params"] as? JsonObject ?: continue
                val pythonRuntime = (params["python_runtime"] as? JsonPrimitive)?.content
                if (pythonRuntime == "glibc") {
                    return true
                }
            }
            false
        } catch (e: Exception) {
            Log.e(TAG, "Failed to parse manifest to check for glibc/sproot requirements", e)
            false
        }
    }
}