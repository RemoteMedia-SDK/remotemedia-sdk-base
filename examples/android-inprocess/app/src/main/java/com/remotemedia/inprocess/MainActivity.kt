package com.remotemedia.inprocess

import android.Manifest
import android.content.pm.PackageManager
import android.content.res.ColorStateList
import android.media.AudioManager
import android.os.Bundle
import android.util.Log
import android.view.View
import android.widget.AdapterView
import android.widget.ArrayAdapter
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.widget.AppCompatSpinner
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.lifecycleScope
import com.remotemedia.inprocess.databinding.ActivityMainBinding
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonArray
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlinx.coroutines.delay

class MainActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "MainActivity"
        private const val REQUEST_AUDIO_PERMISSION = 1001
    }

    private var binding: ActivityMainBinding? = null
    private val pipelineManager by lazy { PipelineManager(this) }
    private val audioRecorder by lazy { AudioRecorder(this) }
    private val audioPlayer by lazy { AudioPlayer(this) }
    private var currentPipeline = "voice-assistant-mobile.json"
    private var pipelineInitialized = false
    private var autoStartRequested = false
    private var autoStartConsumed = false
    private var simulateSpeechRequested = false
    private var audioFramesSeen = 0
    private var isTransitioning = false
    private var lastSpeaker: String? = null
    private var vadSpeechActive = false
    private var vadBargeInActiveUntilMs = 0L
    private var assistantActivityUntilMs = 0L

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding?.root)
        autoStartRequested = intent.getBooleanExtra("auto_start", false)
        simulateSpeechRequested = intent.getBooleanExtra("simulate_speech", false)
        intent.getStringExtra("pipeline")?.takeIf { it.isNotBlank() }?.let {
            currentPipeline = it
        }
        Log.i(TAG, "MainActivity created: autoStartRequested=$autoStartRequested, simulateSpeechRequested=$simulateSpeechRequested, currentPipeline=$currentPipeline")

        setupToolbar()
        setupCallbacks()
        setupPipelineSpinner()
        checkAudioPermission()
    }

    private fun setupToolbar() {
        supportActionBar?.title = "RemoteMedia Voice Assistant"
        supportActionBar?.subtitle = "Offline • On-Device"
    }

    private fun checkAudioPermission() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO)
            != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(
                this,
                arrayOf(Manifest.permission.RECORD_AUDIO),
                REQUEST_AUDIO_PERMISSION
            )
        } else {
            initializePipeline()
        }
    }

    override fun onRequestPermissionsResult(
        requestCode: Int,
        permissions: Array<out String>,
        grantResults: IntArray
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == REQUEST_AUDIO_PERMISSION) {
            if (grantResults.isNotEmpty() && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                initializePipeline()
            } else {
                Toast.makeText(this, "Microphone permission required", Toast.LENGTH_LONG).show()
                finish()
            }
        }
    }

    private fun initializePipeline() {
        lifecycleScope.launch {
            binding?.progressBar?.visibility = View.VISIBLE
            binding?.statusText?.text = "Initializing..."

            val success = pipelineManager.initialize()

            if (success) {
                pipelineInitialized = true
                Log.i(TAG, "Pipeline executor initialized; loading selected manifest")
                loadSelectedPipeline()
            } else {
                binding?.progressBar?.visibility = View.GONE
                binding?.statusText?.text = "Initialization failed"
                showError("Failed to initialize pipeline")
            }
        }
    }

    private fun setupPipelineSpinner() {
        val pipelines = listOf(
            "voice-assistant-mobile.json" to "Voice Assistant (VAD→STT→LLM→TTS)",
            "llm-mobile.json" to "LiteRT LLM (debug)",
            "transcribe-mobile.json" to "Transcribe Only (STT)",
            "tts-mobile.json" to "Text-to-Speech (LLM→TTS)"
        )

        binding?.pipelineSpinner?.adapter = ArrayAdapter(
            this,
            android.R.layout.simple_spinner_dropdown_item,
            pipelines.map { it.second }.toTypedArray()
        )

        binding?.pipelineSpinner?.onItemSelectedListener = object : AdapterView.OnItemSelectedListener {
            override fun onItemSelected(parent: AdapterView<*>?, view: View?, position: Int, id: Long) {
                currentPipeline = pipelines[position].first
                loadSelectedPipeline()
            }

            override fun onNothingSelected(parent: AdapterView<*>?) {}
        }

        val selectedIndex = pipelines.indexOfFirst { it.first == currentPipeline }
        if (selectedIndex >= 0) {
            binding?.pipelineSpinner?.setSelection(selectedIndex, false)
        }
    }

    private fun loadSelectedPipeline() {
        lifecycleScope.launch {
            binding?.progressBar?.visibility = View.VISIBLE
            binding?.statusText?.text = "Loading pipeline..."

            val success = pipelineManager.loadManifest(currentPipeline)

            if (success) {
                Log.i(TAG, "Pipeline manifest loaded: $currentPipeline")
                binding?.statusText?.text = "Ready - Tap mic to start"
                binding?.progressBar?.visibility = View.GONE
                updateUIForState(PipelineManager.PipelineState.READY)
                maybeAutoStart()
            } else {
                binding?.progressBar?.visibility = View.GONE
                binding?.statusText?.text = "Failed to load pipeline"
            }
        }
    }

    private fun setupCallbacks() {
        // Pipeline state changes
        pipelineManager.onStateChange = { state ->
            runOnUiThread {
                updateUIForState(state)
            }
        }

        pipelineManager.onOutput = { outputJson ->
            runOnUiThread {
                handlePipelineOutput(outputJson)
            }
        }

        pipelineManager.onError = { error ->
            runOnUiThread {
                showError(error)
            }
        }

        // Audio recorder callbacks
        audioRecorder.onAudioData = { pcmData ->
            audioFramesSeen += 1
            if (audioFramesSeen % 50 == 0) {
                Log.i(TAG, "Forwarding audio frame $audioFramesSeen (${pcmData.size} bytes)")
            }
            pipelineManager.sendAudio(pcmData)
        }

        audioRecorder.onError = { error ->
            runOnUiThread {
                showError("Recorder: $error")
            }
        }

        audioRecorder.onStateChange = { state ->
            runOnUiThread {
                updateRecorderUI(state)
            }
        }

        // Audio player callbacks
        audioPlayer.onError = { error ->
            runOnUiThread {
                showError("Player: $error")
            }
        }

        audioPlayer.onStateChange = { state ->
            runOnUiThread {
                updatePlayerUI(state)
            }
        }

        audioPlayer.onUnderrun = {
            runOnUiThread {
                Log.w(TAG, "Audio underrun")
            }
        }

        // Mic button
        binding?.micButton?.setOnClickListener {
            toggleListening()
        }

        // Stop button
        binding?.stopButton?.setOnClickListener {
            stopAll()
        }

        // Test button
        binding?.testButton?.setOnClickListener {
            simulateSpeech()
        }
    }

    private fun toggleListening() {
        if (!pipelineManager.isStreamingActive()) {
            startListening()
        } else {
            stopListening()
        }
    }

    private fun startListening() {
        if (isTransitioning) return
        isTransitioning = true
        Log.i(TAG, "Starting listening flow")
        lastSpeaker = null

        binding?.micButton?.isEnabled = false
        binding?.stopButton?.isEnabled = false
        binding?.micButton?.text = "Starting..."
        binding?.statusText?.text = "Starting..."
        binding?.progressBar?.visibility = View.VISIBLE

        lifecycleScope.launch {
            try {
                val started = pipelineManager.startStreaming()
                if (started) {
                    Log.i(TAG, "Pipeline streaming started; starting recorder/player")
                    val recording = audioRecorder.start()
                    if (recording) {
                        val playing = audioPlayer.start(24000) // Kokoro default sample rate
                        if (!playing) {
                            showError("Failed to start audio playback")
                            audioRecorder.stop()
                            pipelineManager.stopStreaming()
                        }
                    } else {
                        Log.e(TAG, "Failed to start audio recording")
                        pipelineManager.stopStreaming()
                    }
                } else {
                    Log.e(TAG, "Pipeline streaming failed to start")
                    binding?.statusText?.text = "Failed to start"
                }
            } catch (e: Exception) {
                Log.e(TAG, "Error starting listening", e)
                showError("Start failed: ${e.message}")
            } finally {
                binding?.progressBar?.visibility = View.GONE
                isTransitioning = false
                val isStreaming = pipelineManager.isStreamingActive()
                updateUIForState(if (isStreaming) PipelineManager.PipelineState.STREAMING else PipelineManager.PipelineState.READY)
            }
        }
    }

    private fun maybeAutoStart() {
        if (!autoStartRequested || autoStartConsumed) {
            return
        }

        if (!pipelineInitialized) {
            Log.i(TAG, "Auto-start requested but executor is not initialized yet")
            return
        }

        autoStartConsumed = true
        if (simulateSpeechRequested) {
            Log.i(TAG, "Auto-start requested with simulate_speech; starting simulation")
            simulateSpeech()
        } else {
            Log.i(TAG, "Auto-start requested; starting pipeline")
            startListening()
        }
    }

    private fun stopListening() {
        if (isTransitioning) return
        isTransitioning = true
        Log.i(TAG, "Stopping listening flow")

        binding?.micButton?.isEnabled = false
        binding?.stopButton?.isEnabled = false
        binding?.micButton?.text = "Stopping..."
        binding?.statusText?.text = "Stopping..."
        binding?.progressBar?.visibility = View.VISIBLE

        lifecycleScope.launch {
            try {
                audioRecorder.stop()
                audioPlayer.stop()
                if (pipelineManager.isStreamingActive()) {
                    flushTrailingSilence()
                }
                pipelineManager.stopStreaming()
            } catch (e: Exception) {
                Log.e(TAG, "Error stopping listening", e)
            } finally {
                binding?.progressBar?.visibility = View.GONE
                isTransitioning = false
                val isStreaming = pipelineManager.isStreamingActive()
                updateUIForState(if (isStreaming) PipelineManager.PipelineState.STREAMING else PipelineManager.PipelineState.READY)
            }
        }
    }

    private suspend fun flushTrailingSilence(durationMs: Int = 400) {
        val frameSamples = 320 // 20ms at 16kHz mono
        val silentFrame = ByteArray(frameSamples * 2)
        val frameCount = maxOf(1, durationMs / 20)
        Log.i(TAG, "Flushing $frameCount trailing silence frame(s) before shutdown")
        repeat(frameCount) {
            if (!pipelineManager.isStreamingActive()) return
            pipelineManager.sendAudio(silentFrame)
            delay(20)
        }
    }

    private fun stopAll() {
        stopListening()
    }

    private fun simulateSpeech() {
        lastSpeaker = null
        lifecycleScope.launch {
            binding?.progressBar?.visibility = View.VISIBLE
            binding?.statusText?.text = "Simulating speech..."

            try {
                // 1. Ensure pipeline is streaming
                if (!pipelineManager.isStreamingActive()) {
                    Log.i(TAG, "Starting pipeline for speech simulation")
                    val started = pipelineManager.startStreaming()
                    if (!started) {
                        showError("Failed to start pipeline for simulation")
                        return@launch
                    }
                    audioPlayer.start(24000)
                    updateUIForState(PipelineManager.PipelineState.STREAMING)
                } else {
                    // Stop the physical mic recorder so they don't fight
                    audioRecorder.stop()
                }

                if (currentPipeline == "tts-mobile.json") {
                    val text = "Have a wonderful day."
                    Log.i(TAG, "Sending simulated TTS text input: $text")
                    runOnUiThread {
                        binding?.progressBar?.visibility = View.GONE
                        appendUserTranscript(text)
                    }
                    if (!pipelineManager.sendText(text)) {
                        showError("Failed to send simulated TTS text")
                        return@launch
                    }
                    Toast.makeText(this@MainActivity, "TTS simulation submitted!", Toast.LENGTH_SHORT).show()
                    return@launch
                }

                // 2. Load WAV file from assets
                val inputStream = assets.open("have_a_wonderful_day.wav")
                val allBytes = inputStream.readBytes()
                inputStream.close()

                if (allBytes.size <= 44) {
                    showError("WAV file is too small or invalid")
                    return@launch
                }

                // Discard 44-byte WAV header
                val pcmBytes = allBytes.copyOfRange(44, allBytes.size)
                val numSamples = pcmBytes.size / 2
                val inputSamples = ShortArray(numSamples)
                ByteBuffer.wrap(pcmBytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().get(inputSamples)

                // 3. Resample from 24kHz to 16kHz
                val targetRatio = 1.5
                val targetSize = (numSamples / targetRatio).toInt()
                val resampledSamples = ShortArray(targetSize)
                for (i in 0 until targetSize) {
                    val inputIdx = i * targetRatio
                    val floorIdx = inputIdx.toInt()
                    val frac = inputIdx - floorIdx
                    if (floorIdx + 1 < numSamples) {
                        resampledSamples[i] = ((1.0 - frac) * inputSamples[floorIdx] + frac * inputSamples[floorIdx + 1]).toInt().toShort()
                    } else if (floorIdx < numSamples) {
                        resampledSamples[i] = inputSamples[floorIdx]
                    }
                }

                // 4. Stream chunks of 20ms (320 samples = 640 bytes)
                val chunkSize = 320 // samples
                var offset = 0
                Log.i(
                    TAG,
                    "Starting to stream simulated audio chunks: total ${resampledSamples.size} samples " +
                        "(20ms chunks, no pre-trim)"
                )

                runOnUiThread {
                    binding?.progressBar?.visibility = View.GONE
                }

                while (offset < resampledSamples.size && pipelineManager.isStreamingActive()) {
                    val actualChunkSize = minOf(chunkSize, resampledSamples.size - offset)
                    val chunk = ShortArray(chunkSize)
                    System.arraycopy(resampledSamples, offset, chunk, 0, actualChunkSize)

                    // Convert ShortArray to ByteArray (little endian)
                    val byteBuffer = ByteBuffer.allocate(chunkSize * 2).order(ByteOrder.LITTLE_ENDIAN)
                    for (sample in chunk) {
                        byteBuffer.putShort(sample)
                    }

                    pipelineManager.sendAudio(byteBuffer.array())
                    offset += actualChunkSize

                    // 20ms delay
                    delay(20)
                }

                flushTrailingSilence(durationMs = 900)

                Log.i(TAG, "Finished streaming simulated audio")
                runOnUiThread {
                    Toast.makeText(this@MainActivity, "Simulation completed!", Toast.LENGTH_SHORT).show()
                }

            } catch (e: Exception) {
                Log.e(TAG, "Simulation failed", e)
                showError("Simulation failed: ${e.message}")
            } finally {
                runOnUiThread {
                    binding?.progressBar?.visibility = View.GONE
                }
            }
        }
    }

    private fun handlePipelineOutput(outputJson: String) {
        if (outputJson.isEmpty()) return
        Log.i(TAG, "Received pipeline output JSON: $outputJson")

        try {
            val jsonElement = Json { ignoreUnknownKeys = true }.parseToJsonElement(outputJson)
            val obj = jsonElement as? JsonObject ?: return
            val source = obj["source"]?.let { (it as? JsonPrimitive)?.content }
            val dataObj = obj["data"] as? JsonObject
            if (source != null && dataObj != null) {
                handleRuntimeDataOutput(dataObj, source)
                return
            }

            handleRuntimeDataOutput(obj, null)

        } catch (e: Exception) {
            Log.w(TAG, "Failed to parse output: ${e.message}")
        }
    }

    private fun handleRuntimeDataOutput(obj: JsonObject, source: String?) {
        if (source == "vad") {
            val vadJson = (obj["Json"] ?: obj["json"]) as? JsonObject
            if (vadJson != null) {
                updateVadDebug(vadJson)
            }
            return
        }

        // 1. Handle Audio variant (e.g. RuntimeData::Audio)
        val audioObj = (obj["Audio"] ?: obj["audio"]) as? JsonObject
        if (audioObj != null) {
            playAudioFromJsonObject(audioObj)
            return
        }

        // 2. Handle Json variant (e.g. RuntimeData::Json)
        val jsonVal = obj["Json"] ?: obj["json"]
        if (jsonVal != null) {
            if (jsonVal is JsonObject) {
                val dataType = jsonVal["data_type"]?.let { (it as? JsonPrimitive)?.content }
                if (dataType == "audio") {
                    playAudioFromJsonObject(jsonVal)
                    return
                }

                val textVal = jsonVal["text"]?.let { (it as? JsonPrimitive)?.content }
                if (textVal != null && textVal.isNotEmpty()) {
                    appendTextForSource(source, textVal)
                    return
                }
            }

            // Generic JSON output (e.g. VAD or other data)
            if (source != "stt" && source != "llm") {
                appendTranscript("[${source ?: "JSON"}]: $jsonVal")
            }
            return
        }

        // 3. Handle Text variant (e.g. RuntimeData::Text)
        val textVal = (obj["Text"] ?: obj["text"])?.let { (it as? JsonPrimitive)?.content }
        if (textVal != null) {
            if (textVal.isNotEmpty()) {
                appendTextForSource(source, textVal)
            }

            val tokensArray = (obj["tokens"] ?: obj["Tokens"]) as? JsonArray
            val tokens = tokensArray?.mapNotNull { (it as? JsonPrimitive)?.content }
            tokens?.takeIf { it.isNotEmpty() }?.let {
                updateStreamingTokens(it)
            }
            return
        }

        // Fallback for generic untagged objects
        appendTranscript("[${source ?: "Output"}]: $obj")
    }

    private fun appendTextForSource(source: String?, text: String) {
        when (source) {
            "stt" -> appendUserTranscript(text)
            "llm" -> {
                markAssistantActive(3000)
                appendAssistantTranscript(text)
            }
            "data" -> {
                if (currentPipeline == "transcribe-mobile.json") {
                    appendUserTranscript(text)
                } else {
                    markAssistantActive(2000)
                    appendAssistantTranscript(text)
                }
            }
            else -> appendAssistantTranscript(text)
        }
    }

    private fun appendUserTranscript(text: String) {
        if (lastSpeaker != "User") {
            lastSpeaker = "User"
            val current = binding?.transcriptText?.text ?: ""
            binding?.transcriptText?.text = if (current.isEmpty()) "User: $text" else "$current\nUser: $text"
        } else {
            val needsSpace = binding?.transcriptText?.text?.lastOrNull()?.isWhitespace() == false
            binding?.transcriptText?.append(if (needsSpace) " $text" else text)
        }
        binding?.transcriptScroll?.post {
            binding?.transcriptScroll?.fullScroll(View.FOCUS_DOWN)
        }
    }

    private fun appendAssistantTranscript(text: String) {
        if (lastSpeaker != "Assistant") {
            lastSpeaker = "Assistant"
            val current = binding?.transcriptText?.text ?: ""
            binding?.transcriptText?.text = if (current.isEmpty()) "Assistant: $text" else "$current\nAssistant: $text"
        } else {
            binding?.transcriptText?.append(text)
        }
        binding?.transcriptScroll?.post {
            binding?.transcriptScroll?.fullScroll(View.FOCUS_DOWN)
        }
    }

    private fun playAudioFromJsonObject(obj: JsonObject) {
        try {
            val samplesArray = obj["samples"] as? JsonArray ?: return
            val sampleRate = obj["sample_rate"]?.let { (it as? JsonPrimitive)?.content?.toIntOrNull() } ?: 24000
            
            // Extract floats
            val floatSamples = FloatArray(samplesArray.size)
            var peakAbs = 0.0f
            var sumSquares = 0.0
            var leadingSilenceSamples = 0
            var inLeadingSilence = true
            for (i in 0 until samplesArray.size) {
                val sample = (samplesArray[i] as? JsonPrimitive)?.content?.toFloatOrNull() ?: 0.0f
                floatSamples[i] = sample
                val abs = kotlin.math.abs(sample)
                peakAbs = maxOf(peakAbs, abs)
                sumSquares += (sample * sample).toDouble()
                if (inLeadingSilence) {
                    if (abs <= 1.0e-4f) {
                        leadingSilenceSamples += 1
                    } else {
                        inLeadingSilence = false
                    }
                }
            }
            val rms = if (floatSamples.isEmpty()) 0.0 else kotlin.math.sqrt(sumSquares / floatSamples.size)
            val durationMs = if (sampleRate > 0) {
                ((floatSamples.size.toDouble() / sampleRate.toDouble()) * 1000.0).toLong()
            } else {
                0L
            }
            markAssistantActive(durationMs + 1000)
            
            // Convert float PCM [-1.0, 1.0] to short PCM16 little-endian bytes
            val byteBuffer = ByteBuffer.allocate(floatSamples.size * 2).order(ByteOrder.LITTLE_ENDIAN)
            for (fSample in floatSamples) {
                val clamped = maxOf(-1.0f, minOf(1.0f, fSample))
                val shortVal = (clamped * 32767.0f).toInt().toShort()
                byteBuffer.putShort(shortVal)
            }
            val pcmBytes = byteBuffer.array()
            
            // Start audio player if not playing
            if (!audioPlayer.isPlaying()) {
                audioPlayer.start(sampleRate)
            }
            
            val queued = audioPlayer.queueAudio(pcmBytes)
            val metadata = obj["metadata"]?.toString()
            Log.i(
                TAG,
                "Audio playback buffer samples=${floatSamples.size} rate=${sampleRate}Hz " +
                    "peakAbs=$peakAbs rms=$rms leadingSilenceSamples=$leadingSilenceSamples queued=$queued " +
                    "metadata=$metadata"
            )
        } catch (e: Exception) {
            Log.e(TAG, "Failed to parse/play audio from JSON: ${e.message}")
        }
    }

    private fun updateVadDebug(vadJson: JsonObject) {
        val hasSpeech = vadJson.booleanValue("has_speech")
        val isSpeechStart = vadJson.booleanValue("is_speech_start")
        val isSpeechEnd = vadJson.booleanValue("is_speech_end")
        val probability = vadJson.doubleValue("speech_probability")
        val rms = vadJson.doubleValue("rms")
        val peak = vadJson.doubleValue("peak")
        val samples = vadJson.intValue("samples")
        val sampleRate = vadJson.intValue("sample_rate")
        val now = System.currentTimeMillis()

        if (isSpeechStart) {
            vadSpeechActive = true
            if (now < assistantActivityUntilMs) {
                vadBargeInActiveUntilMs = now + 2500
                Log.i(TAG, "VAD barge-in detected: user speech started while assistant output was active")
            }
        } else if (isSpeechEnd) {
            vadSpeechActive = false
        } else if (!hasSpeech && !vadSpeechActive) {
            vadSpeechActive = false
        }

        val stateLabel = when {
            now < vadBargeInActiveUntilMs -> "VAD: barge-in"
            isSpeechStart -> "VAD: speech start"
            isSpeechEnd -> "VAD: speech end"
            vadSpeechActive || hasSpeech -> "VAD: speech"
            else -> "VAD: idle"
        }
        val stateColor = when {
            now < vadBargeInActiveUntilMs -> R.color.vad_barge_in
            vadSpeechActive || hasSpeech -> R.color.vad_speech
            else -> R.color.vad_idle
        }
        val bargeText = if (now < vadBargeInActiveUntilMs) {
            "Barge-in: detected"
        } else if (now < assistantActivityUntilMs) {
            "Barge-in: armed"
        } else {
            "Barge-in: clear"
        }
        val bargeColor = if (now < vadBargeInActiveUntilMs) R.color.vad_barge_in else R.color.secondary_text

        binding?.vadStateText?.text = stateLabel
        binding?.vadStateText?.setTextColor(ContextCompat.getColor(this, stateColor))
        binding?.vadBargeInText?.text = bargeText
        binding?.vadBargeInText?.setTextColor(ContextCompat.getColor(this, bargeColor))
        binding?.vadProbabilityBar?.progress = (probability.coerceIn(0.0, 1.0) * 100.0).toInt()
        binding?.vadMetricsText?.text =
            "p ${probability.format(2)} · rms ${rms.format(4)} · peak ${peak.format(4)} · samples $samples · ${sampleRate}Hz"
    }

    private fun markAssistantActive(durationMs: Long) {
        val until = System.currentTimeMillis() + durationMs.coerceAtLeast(0L)
        if (until > assistantActivityUntilMs) {
            assistantActivityUntilMs = until
        }
    }

    private fun JsonObject.booleanValue(key: String): Boolean =
        (this[key] as? JsonPrimitive)?.content?.toBooleanStrictOrNull() ?: false

    private fun JsonObject.doubleValue(key: String): Double =
        (this[key] as? JsonPrimitive)?.content?.toDoubleOrNull() ?: 0.0

    private fun JsonObject.intValue(key: String): Int =
        (this[key] as? JsonPrimitive)?.content?.toIntOrNull() ?: 0

    private fun Double.format(decimals: Int): String =
        "%.${decimals}f".format(java.util.Locale.US, this)

    private fun updateUIForState(state: PipelineManager.PipelineState) {
        val button = binding?.micButton ?: return
        val stopBtn = binding?.stopButton
        when (state) {
            PipelineManager.PipelineState.IDLE -> {
                binding?.statusText?.text = "Idle"
                button.isEnabled = false
                button.text = "Idle"
                button.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(this, R.color.disabled_text))
                stopBtn?.isEnabled = false
            }
            PipelineManager.PipelineState.INITIALIZING -> {
                binding?.statusText?.text = "Initializing..."
                binding?.progressBar?.visibility = View.VISIBLE
                button.isEnabled = false
                button.text = "Starting..."
                button.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(this, R.color.disabled_text))
                stopBtn?.isEnabled = false
            }
            PipelineManager.PipelineState.READY -> {
                binding?.statusText?.text = "Ready - Tap mic to start"
                binding?.progressBar?.visibility = View.GONE
                button.isEnabled = true
                button.text = "Tap to Listen"
                button.icon = ContextCompat.getDrawable(this, android.R.drawable.ic_media_play)
                button.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(this, R.color.mic_active))
                stopBtn?.isEnabled = false
            }
            PipelineManager.PipelineState.RUNNING -> {
                binding?.statusText?.text = "Processing..."
                button.isEnabled = false
                button.text = "Processing..."
                button.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(this, R.color.disabled_text))
                stopBtn?.isEnabled = false
            }
            PipelineManager.PipelineState.STREAMING -> {
                binding?.statusText?.text = "Listening... Speak now"
                binding?.progressBar?.visibility = View.GONE
                button.isEnabled = true
                button.text = "Stop"
                button.icon = ContextCompat.getDrawable(this, android.R.drawable.ic_media_pause)
                button.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(this, R.color.mic_inactive))
                stopBtn?.isEnabled = true
            }
            PipelineManager.PipelineState.ERROR -> {
                binding?.statusText?.text = "Error"
                binding?.progressBar?.visibility = View.GONE
                button.isEnabled = true
                button.text = "Tap to Listen"
                button.icon = ContextCompat.getDrawable(this, android.R.drawable.ic_media_play)
                button.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(this, R.color.mic_active))
                stopBtn?.isEnabled = false
            }
            PipelineManager.PipelineState.DESTROYED -> {
                binding?.statusText?.text = "Destroyed"
                button.isEnabled = false
                button.text = "Destroyed"
                button.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(this, R.color.disabled_text))
                stopBtn?.isEnabled = false
            }
        }
    }

    private fun updateRecorderUI(state: AudioRecorder.RecordingState) {
        // Update recording indicator
    }

    private fun updatePlayerUI(state: AudioPlayer.PlaybackState) {
        // Update playback indicator
    }

    private fun appendTranscript(text: String) {
        val current = binding?.transcriptText?.text ?: ""
        binding?.transcriptText?.text = if (current.isEmpty()) text else "$current\n$text"
        // Scroll to bottom after layout pass
        binding?.transcriptScroll?.post {
            binding?.transcriptScroll?.fullScroll(View.FOCUS_DOWN)
        }
    }

    private fun updateStreamingTokens(tokens: List<String>) {
        // Update streaming token display if needed
        val text = tokens.joinToString(" ")
        binding?.streamingText?.text = text
    }

    private fun showError(message: String) {
        binding?.statusText?.text = "Error: $message"
        Toast.makeText(this, message, Toast.LENGTH_LONG).show()
        Log.e(TAG, message)
    }

    override fun onDestroy() {
        super.onDestroy()
        pipelineManager.destroy()
        audioRecorder.destroy()
        audioPlayer.destroy()
        binding = null
    }

    override fun onPause() {
        super.onPause()
        stopListening()
    }
}
