package com.remotemedia.inprocess

import android.os.Bundle
import android.util.Log
import android.view.View
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.remotemedia.inprocess.databinding.ActivityHermesProfileBinding
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File

class HermesProfileActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "HermesProfileActivity"
        private const val ANDROID_HERMES_HOME = "/data/data/com.remotemedia.inprocess/files/hermes_home"
        private const val DEFAULT_PROFILE_NAME = "android-imported"
    }

    private var binding: ActivityHermesProfileBinding? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityHermesProfileBinding.inflate(layoutInflater)
        setContentView(binding?.root)

        supportActionBar?.title = getString(R.string.hermes_profile_title)
        supportActionBar?.setDisplayHomeAsUpEnabled(true)

        binding?.refreshBtn?.setOnClickListener { loadProfileInfo() }
        loadProfileInfo()
    }

    private fun loadProfileInfo() {
        lifecycleScope.launch {
            val profileInfo = readProfileInfo()
            updateUI(profileInfo)
        }
    }

    private suspend fun readProfileInfo(): ProfileInfo = withContext(Dispatchers.IO) {
        val hermesHome = System.getenv("HERMES_HOME") ?: ANDROID_HERMES_HOME
        val profileName = System.getenv("HERMES_PROFILE") ?: DEFAULT_PROFILE_NAME

        // Read profile directory
        val profileDir = File(hermesHome, "profiles/$profileName")
        var configExists = false
        var configContent = ""
        var soulExists = false
        var soulContent = ""
        var modelConfig: ModelConfig? = null

        if (profileDir.exists() && profileDir.isDirectory) {
            val configFile = File(profileDir, "config.yaml")
            configExists = configFile.exists()
            if (configExists) {
                try {
                    configContent = configFile.readText()
                    modelConfig = parseConfigYaml(configContent)
                } catch (e: Exception) {
                    Log.w(TAG, "Failed to read config.yaml: ${e.message}")
                    configContent = "Error reading file: ${e.message}"
                }
            }

            val soulFile = File(profileDir, "SOUL.md")
            soulExists = soulFile.exists()
            if (soulExists) {
                try {
                    soulContent = soulFile.readText()
                } catch (e: Exception) {
                    Log.w(TAG, "Failed to read SOUL.md: ${e.message}")
                    soulContent = "Error reading file: ${e.message}"
                }
            }
        } else {
            // Profile not imported yet, use default paths
        }

        ProfileInfo(
            hermesHome = hermesHome,
            profileName = profileName,
            configExists = configExists,
            configContent = configContent,
            soulExists = soulExists,
            soulContent = soulContent,
            modelConfig = modelConfig
        )
    }

    private data class ProfileInfo(
        val hermesHome: String,
        val profileName: String,
        val configExists: Boolean,
        val configContent: String,
        val soulExists: Boolean,
        val soulContent: String,
        val modelConfig: ModelConfig?
    )

    private data class ModelConfig(
        val baseUrl: String,
        val model: String,
        val provider: String
    )

    private fun parseConfigYaml(content: String): ModelConfig? {
        try {
            // Parse YAML for model section
            var baseUrl = "https://inference-api.nousresearch.com/v1"
            var model = "nvidia/nemotron-3-ultra:free"
            var provider = "nous"

            val lines = content.lines()
            var inModelSection = false
            for (line in lines) {
                val trimmed = line.trim()
                if (trimmed.startsWith("model:")) {
                    inModelSection = true
                    continue
                }
                if (inModelSection) {
                    if (trimmed.startsWith("- ") || trimmed.isEmpty()) {
                        continue
                    }
                    if (trimmed.contains(":")) {
                        val parts = trimmed.split(":", limit = 2)
                        val key = parts[0].trim()
                        val value = parts[1].trim().replace("^\"", "").replace("\"$", "")
                        when (key) {
                            "base_url" -> baseUrl = value
                            "default" -> model = value
                            "provider" -> provider = value
                        }
                    }
                    // Stop at next top-level section
                    if (trimmed.startsWith(" ") || trimmed.startsWith("\t") || !trimmed.contains(":")) {
                        // continue
                    } else if (!trimmed.startsWith("model:") && trimmed.matches(Regex("^[a-z_]+:.*"))) {
                        break
                    }
                }
            }
            return ModelConfig(baseUrl, model, provider)
        } catch (e: Exception) {
            Log.w(TAG, "Failed to parse config.yaml: ${e.message}")
            return null
        }
    }

    private fun updateUI(info: ProfileInfo) {
        binding?.hermesHomePath?.text = info.hermesHome
        binding?.profileNameValue?.text = info.profileName

        // Update profile status
        val statusText = if (info.configExists && info.soulExists) {
            getString(R.string.hermes_profile_status_loaded)
        } else if (info.configExists || info.soulExists) {
            getString(R.string.hermes_profile_status_error, "Partial profile")
        } else {
            getString(R.string.hermes_profile_status_not_loaded)
        }
        binding?.profileStatusLabel?.text = statusText
        binding?.profileStatusLabel?.setTextColor(
            androidx.core.content.ContextCompat.getColor(this,
                if (info.configExists && info.soulExists) R.color.success else R.color.warning))

        // Update model config from parsed YAML
        info.modelConfig?.let { config ->
            binding?.baseUrlValue?.text = config.baseUrl
            binding?.modelValue?.text = config.model
            binding?.providerValue?.text = config.provider
        } ?: run {
            binding?.baseUrlValue?.text = getString(R.string.error_prefix) + " unavailable"
            binding?.modelValue?.text = getString(R.string.error_prefix) + " unavailable"
            binding?.providerValue?.text = getString(R.string.error_prefix) + " unavailable"
        }

        // Show actual file contents
        binding?.configContent?.text = if (info.configContent.isNotEmpty()) info.configContent else getString(R.string.error_prefix) + " not available"
        binding?.soulContent?.text = if (info.soulContent.isNotEmpty()) info.soulContent else getString(R.string.error_prefix) + " not available"
    }

    override fun onSupportNavigateUp(): Boolean {
        onBackPressed()
        return true
    }
}