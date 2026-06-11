package com.remotemedia.inprocess

import android.app.Activity
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.util.Log
import android.view.View
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.google.android.material.button.MaterialButton
import com.remotemedia.android.NativeInterface
import com.remotemedia.inprocess.databinding.ActivityHermesProfileBinding
import com.remotemedia.android.PipelineManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File

class HermesProfileActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "HermesProfileActivity"
        private const val ANDROID_HERMES_HOME = "/data/data/com.remotemedia.inprocess/files/hermes_home"
        private const val DEFAULT_PROFILE_NAME = "default"
        private const val REQUEST_SELECT_PROFILE_FILE = 1001
    }

    private var binding: ActivityHermesProfileBinding? = null
    private lateinit var pipelineManager: PipelineManager

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityHermesProfileBinding.inflate(layoutInflater)
        setContentView(binding?.root)

        pipelineManager = PipelineManager(this)

        supportActionBar?.title = getString(R.string.hermes_profile_title)
        supportActionBar?.setDisplayHomeAsUpEnabled(true)

        binding?.refreshBtn?.setOnClickListener { loadProfileInfo() }
        binding?.selectProfileBtn?.setOnClickListener { openProfilePicker() }
        binding?.createProfileBtn?.setOnClickListener { showCreateProfileDialog() }

        loadProfileInfo()
    }

    private fun loadProfileInfo() {
        lifecycleScope.launch {
            val hasProfile = withContext(Dispatchers.IO) {
                pipelineManager.hasHermesProfile()
            }
            val profileInfo = readProfileInfo(hasProfile)
            updateUI(profileInfo)
        }
    }

    private fun openProfilePicker() {
        val intent = Intent(Intent.ACTION_OPEN_DOCUMENT).apply {
            addCategory(Intent.CATEGORY_OPENABLE)
            type = "application/gzip"
            putExtra(Intent.EXTRA_MIME_TYPES, arrayOf("application/gzip", "application/x-tar", "application/octet-stream"))
            addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION)
        }
        startActivityForResult(intent, REQUEST_SELECT_PROFILE_FILE)
    }

    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        if (requestCode == REQUEST_SELECT_PROFILE_FILE && resultCode == Activity.RESULT_OK) {
            data?.data?.let { uri ->
                loadSelectedProfileFile(uri)
            }
        }
    }

    private fun loadSelectedProfileFile(fileUri: Uri) {
        lifecycleScope.launch {
            val success = withContext(Dispatchers.IO) {
                try {
                    val activity = this@HermesProfileActivity
                    val homeDir = File(activity.filesDir, ".hermes")
                    homeDir.mkdirs()
                    // Clear existing profile
                    homeDir.listFiles()?.forEach { it.deleteRecursively() }

                    // Copy the selected file to a temporary file
                    val tempFile = File(activity.cacheDir, "selected_profile_${System.currentTimeMillis()}.tar.gz")
                    contentResolver.openInputStream(fileUri)?.use { input ->
                        tempFile.outputStream().use { output ->
                            input.copyTo(output)
                        }
                    } ?: run {
                        Log.e(TAG, "Failed to open input stream for selected file")
                        return@withContext false
                    }

                    // Extract tar.gz using tar command
                    try {
                        val process = Runtime.getRuntime().exec("tar -xzf ${tempFile.absolutePath} -C ${homeDir.absolutePath}")
                        val result = process.waitFor()
                        tempFile.delete()
                        if (result == 0) {
                            val marker = File(homeDir, ".extracted")
                            marker.createNewFile()
                            true
                        } else {
                            Log.e(TAG, "tar command exited with code: $result")
                            false
                        }
                    } catch (e: Exception) {
                        Log.e(TAG, "Failed to extract tar.gz", e)
                        tempFile.delete()
                        false
                    }
                } catch (e: Exception) {
                    Log.e(TAG, "Failed to extract/load profile", e)
                    false
                }
            }
            runOnUiThread {
                if (success) {
                    Toast.makeText(this@HermesProfileActivity, R.string.profile_loaded, Toast.LENGTH_SHORT).show()
                    loadProfileInfo()
                } else {
                    Toast.makeText(this@HermesProfileActivity, R.string.profile_failed, Toast.LENGTH_LONG).show()
                }
            }
        }
    }

    private fun showCreateProfileDialog() {
        val dialogView = android.widget.EditText(this).apply {
            hint = getString(R.string.profile_name_hint)
            setText(DEFAULT_PROFILE_NAME)
            selectAll()
        }

        AlertDialog.Builder(this)
            .setTitle(R.string.create_profile)
            .setMessage(R.string.select_profile_dir)
            .setView(dialogView)
            .setPositiveButton(android.R.string.ok) { _, _ ->
                val profileName = dialogView.text.toString().trim()
                if (profileName.isNullOrBlank()) {
                    Toast.makeText(this@HermesProfileActivity, R.string.profile_name_hint, Toast.LENGTH_SHORT).show()
                    return@setPositiveButton
                }
                createProfile(profileName)
            }
            .setNegativeButton(android.R.string.cancel, null)
            .show()
    }

    private fun createProfile(profileName: String) {
        lifecycleScope.launch {
            val success = withContext(Dispatchers.IO) {
                pipelineManager.createHermesProfile(profileName)
            }
            runOnUiThread {
                if (success) {
                    Toast.makeText(this@HermesProfileActivity, R.string.profile_created, Toast.LENGTH_SHORT).show()
                    loadProfileInfo()
                } else {
                    Toast.makeText(this@HermesProfileActivity, R.string.profile_failed, Toast.LENGTH_LONG).show()
                }
            }
        }
    }

    private suspend fun readProfileInfo(hasProfile: Boolean): ProfileInfo = withContext(Dispatchers.IO) {
        val activity = this@HermesProfileActivity
        val hermesHome = System.getenv("HERMES_HOME") ?: activity.filesDir.absolutePath + "/.hermes"
        val profileName = System.getenv("HERMES_PROFILE") ?: DEFAULT_PROFILE_NAME

        // Read profile directory - profile is extracted directly to .hermes/default/
        val profileDir = File(hermesHome, profileName)
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
            Log.i(TAG, "SOUL.md exists: $soulExists at path: ${soulFile.absolutePath}")
            if (soulExists) {
                try {
                    soulContent = soulFile.readText()
                    Log.i(TAG, "SOUL.md content length: ${soulContent.length}")
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
            hasProfile = hasProfile,
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
        val hasProfile: Boolean,
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
                        val value = parts[1].trim().replace("^\\\"", "").replace("\\\"$", "")
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

        // Update action buttons visibility
        val actionsVisible = !info.hasProfile
        binding?.actionButtonsContainer?.visibility = if (actionsVisible) View.VISIBLE else View.GONE

        // Update profile status
        val statusText = if (info.hasProfile) {
            getString(R.string.hermes_profile_status_loaded)
        } else {
            getString(R.string.hermes_profile_status_not_loaded)
        }
        binding?.profileStatusLabel?.text = statusText
        binding?.profileStatusLabel?.setTextColor(
            androidx.core.content.ContextCompat.getColor(this,
                if (info.hasProfile) R.color.success else R.color.warning))

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