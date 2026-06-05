package com.remotemedia.inprocess

import android.Manifest
import android.content.pm.PackageManager
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
    private var audioFramesSeen = 0

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding?.root)
        autoStartRequested = intent.getBooleanExtra("auto_start", false)
        intent.getStringExtra("pipeline")?.takeIf { it.isNotBlank() }?.let {
            currentPipeline = it
        }
        Log.i(TAG, "MainActivity created: autoStartRequested=$autoStartRequested, currentPipeline=$currentPipeline")

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
                binding?.micButton?.isEnabled = true
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
                pipelineManager.sendAudio(pcmData)
            }
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
            testPythonNode()
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
        Log.i(TAG, "Starting listening flow")
        lifecycleScope.launch {
            binding?.statusText?.text = "Starting..."

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
        Log.i(TAG, "Auto-start requested; starting pipeline")
        startListening()
    }

    private fun stopListening() {
        audioRecorder.stop()
        audioPlayer.stop()
        pipelineManager.stopStreaming()
    }

    private fun stopAll() {
        stopListening()
        binding?.statusText?.text = "Stopped"
        binding?.micButton?.icon = ContextCompat.getDrawable(this, android.R.drawable.ic_media_play)
    }

    private fun testPythonNode() {
        lifecycleScope.launch {
            binding?.progressBar?.visibility = View.VISIBLE
            binding?.statusText?.text = "Testing Python node..."

            try {
                val result = NativeInterface.nativeTestPythonNode()
                Log.i(TAG, "Python test result: $result")
                runOnUiThread {
                    binding?.progressBar?.visibility = View.GONE
                    binding?.statusText?.text = "Python node test passed"
                    Toast.makeText(this@MainActivity, "Python node working!", Toast.LENGTH_SHORT).show()
                }
            } catch (e: Exception) {
                Log.e(TAG, "Python test failed", e)
                runOnUiThread {
                    binding?.progressBar?.visibility = View.GONE
                    showError("Python test failed: ${e.message}")
                }
            }
        }
    }

    private fun handlePipelineOutput(outputJson: String) {
        if (outputJson.isEmpty()) return

        try {
            val jsonElement = Json { ignoreUnknownKeys = true }.parseToJsonElement(outputJson)
            val obj = jsonElement as JsonObject

            // Handle different output types
            if (obj.containsKey("text")) {
                val text = obj["text"]?.let { (it as JsonPrimitive).content } ?: ""
                appendTranscript(text)
            } else if (obj.containsKey("audio")) {
                // Audio output will be handled by AudioPlayer callback
                // This is for metadata
            } else if (obj.containsKey("tokens")) {
                // Handle streaming tokens
                val tokens = obj["tokens"]?.let { it as JsonArray }?.map { (it as JsonPrimitive).content ?: "" } ?: emptyList()
                updateStreamingTokens(tokens)
            }
        } catch (e: Exception) {
            Log.w(TAG, "Failed to parse output: ${e.message}")
        }
    }

    private fun updateUIForState(state: PipelineManager.PipelineState) {
        when (state) {
            PipelineManager.PipelineState.IDLE -> {
                binding?.statusText?.text = "Idle"
                binding?.micButton?.isEnabled = false
            }
            PipelineManager.PipelineState.INITIALIZING -> {
                binding?.statusText?.text = "Initializing..."
                binding?.progressBar?.visibility = View.VISIBLE
            }
            PipelineManager.PipelineState.READY -> {
                binding?.statusText?.text = "Ready - Tap mic to start"
                binding?.progressBar?.visibility = View.GONE
                binding?.micButton?.isEnabled = true
            }
            PipelineManager.PipelineState.RUNNING -> {
                binding?.statusText?.text = "Processing..."
            }
            PipelineManager.PipelineState.STREAMING -> {
                binding?.statusText?.text = "Listening... Speak now"
                binding?.micButton?.icon = ContextCompat.getDrawable(this, android.R.drawable.ic_media_pause)
            }
            PipelineManager.PipelineState.ERROR -> {
                binding?.statusText?.text = "Error"
                binding?.progressBar?.visibility = View.GONE
                binding?.micButton?.isEnabled = true
                binding?.micButton?.icon = ContextCompat.getDrawable(this, android.R.drawable.ic_media_play)
            }
            PipelineManager.PipelineState.DESTROYED -> {
                binding?.statusText?.text = "Destroyed"
                binding?.micButton?.isEnabled = false
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
        binding?.transcriptText?.text = "$current\n$text"
        // Scroll to bottom
        binding?.transcriptScroll?.fullScroll(View.FOCUS_DOWN)
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
