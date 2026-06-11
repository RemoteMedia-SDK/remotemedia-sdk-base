package com.remotemedia.inprocess

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.os.Bundle
import android.util.Log
import android.view.View
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import com.remotemedia.android.MicrodroidController
import com.remotemedia.inprocess.databinding.ActivityMicrodroidRunnerManagementBinding
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class MicrodroidRunnerManagementActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "MicrodroidRunnerMgmt"
    }

    private var binding: ActivityMicrodroidRunnerManagementBinding? = null
    private var microdroidController: MicrodroidController? = null
    private var isVmRunning = false
    private var isVsockConnected = false
    private val logBuffer = StringBuilder()
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault())

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMicrodroidRunnerManagementBinding.inflate(layoutInflater)
        setContentView(binding?.root)

        supportActionBar?.title = getString(R.string.microdroid_runners_title)
        supportActionBar?.setDisplayHomeAsUpEnabled(true)

        // Initialize controller
        microdroidController = MicrodroidController(this)

        setupButtons()
        checkAvfSupport()
    }

    private fun setupButtons() {
        binding?.deployRunnerBtn?.setOnClickListener { deployRunner() }
        binding?.removeRunnerBtn?.setOnClickListener { removeRunner() }
        binding?.connectVsockBtn?.setOnClickListener { connectVsock() }
        binding?.refreshStatusBtn?.setOnClickListener { refreshStatus() }
        binding?.copyLogBtn?.setOnClickListener { copyLogToClipboard() }
    }

    private fun checkAvfSupport() {
        log("Checking AVF support...")
        val supported = microdroidController?.isSupported() ?: false
        
        runOnUiThread {
            binding?.avfSupportValue?.text = if (supported) {
                getString(R.string.avf_supported)
            } else {
                getString(R.string.avf_not_supported)
            }
            binding?.avfSupportValue?.setTextColor(
                androidx.core.content.ContextCompat.getColor(
                    this,
                    if (supported) R.color.success else R.color.warning
                )
            )
            
            if (!supported) {
                binding?.deployRunnerBtn?.isEnabled = false
                showError(getString(R.string.avf_check_failed))
            }
            
            log("AVF Support: ${if (supported) "Supported" else "Not Supported"}")
            refreshStatus()
        }
    }

    private fun refreshStatus() {
        lifecycleScope.launch {
            // Check if VM is running by trying to stop (will fail if not running)
            // We track state internally since MicrodroidController doesn't expose status
            val running = isVmRunning
            
            runOnUiThread {
                binding?.runnerStatusValue?.text = when {
                    running -> getString(R.string.runner_status_running)
                    else -> getString(R.string.runner_status_stopped)
                }
                binding?.runnerStatusValue?.setTextColor(
                    androidx.core.content.ContextCompat.getColor(
                        this@MicrodroidRunnerManagementActivity,
                        if (running) R.color.success else R.color.disabled_text
                    )
                )
                
                // Update button states
                binding?.deployRunnerBtn?.isEnabled = !running
                binding?.removeRunnerBtn?.isEnabled = running
                binding?.connectVsockBtn?.isEnabled = running && !isVsockConnected
                
                // vsock status
                binding?.vsockStatusValue?.text = if (isVsockConnected) {
                    getString(R.string.vsock_status_connected)
                } else {
                    getString(R.string.vsock_status_disconnected)
                }
                binding?.vsockStatusValue?.setTextColor(
                    androidx.core.content.ContextCompat.getColor(
                        this@MicrodroidRunnerManagementActivity,
                        if (isVsockConnected) R.color.success else R.color.disabled_text
                    )
                )
            }
        }
    }

    private fun deployRunner() {
        if (isVmRunning) {
            log("Runner already running")
            return
        }

        log("Deploying runner...")
        setProgressVisible(true)
        
        binding?.runnerStatusValue?.text = getString(R.string.runner_status_starting)
        binding?.runnerStatusValue?.setTextColor(
            androidx.core.content.ContextCompat.getColor(this, R.color.warning)
        )

        lifecycleScope.launch {
            try {
                microdroidController?.startVm()
                isVmRunning = true
                log("Runner deployed successfully")
                
                runOnUiThread {
                    setProgressVisible(false)
                    showToast(getString(R.string.runner_deployed_success))
                    refreshStatus()
                }
            } catch (t: Throwable) {
                isVmRunning = false
                log("Failed to deploy runner: ${t.message}")
                
                runOnUiThread {
                    setProgressVisible(false)
                    showError(getString(R.string.runner_deploy_failed, t.message ?: "Unknown error"))
                    refreshStatus()
                }
            }
        }
    }

    private fun removeRunner() {
        if (!isVmRunning) {
            log("Runner not running")
            return
        }

        log("Removing runner...")
        setProgressVisible(true)
        
        binding?.runnerStatusValue?.text = getString(R.string.runner_status_stopping)
        binding?.runnerStatusValue?.setTextColor(
            androidx.core.content.ContextCompat.getColor(this, R.color.warning)
        )

        lifecycleScope.launch {
            try {
                microdroidController?.stopVm()
                isVmRunning = false
                isVsockConnected = false
                log("Runner removed successfully")
                
                runOnUiThread {
                    setProgressVisible(false)
                    showToast(getString(R.string.runner_removed_success))
                    refreshStatus()
                }
            } catch (t: Throwable) {
                log("Failed to remove runner: ${t.message}")
                
                runOnUiThread {
                    setProgressVisible(false)
                    showError(getString(R.string.runner_remove_failed, t.message ?: "Unknown error"))
                    refreshStatus()
                }
            }
        }
    }

    private fun connectVsock() {
        if (!isVmRunning) {
            log("Cannot connect vsock: runner not running")
            return
        }

        if (isVsockConnected) {
            log("vsock already connected")
            return
        }

        log("Connecting vsock...")
        binding?.vsockStatusValue?.text = getString(R.string.vsock_status_connecting)
        binding?.vsockStatusValue?.setTextColor(
            androidx.core.content.ContextCompat.getColor(this, R.color.warning)
        )
        binding?.connectVsockBtn?.isEnabled = false

        lifecycleScope.launch(Dispatchers.IO) {
            try {
                val pfd = microdroidController?.connectVsock()
                if (pfd != null) {
                    // Pass the FD to native code
                    val fd = pfd.detachFd()
                    val success = com.remotemedia.android.NativeInterface.nativeSetVsockFd(fd)
                    
                    if (success) {
                        isVsockConnected = true
                        log("vsock connected on port 5555, FD handed to native")
                    } else {
                        log("Failed to hand FD to native code")
                    }
                    
                    runOnUiThread {
                        if (success) {
                            showToast(getString(R.string.vsock_connected_success))
                        } else {
                            showError(getString(R.string.vsock_connected_failed, "Native handoff failed"))
                        }
                        refreshStatus()
                    }
                } else {
                    throw IllegalStateException("connectVsock returned null")
                }
            } catch (t: Throwable) {
                log("vsock connection failed: ${t.message}")
                isVsockConnected = false
                
                runOnUiThread {
                    showError(getString(R.string.vsock_connected_failed, t.message ?: "Unknown error"))
                    refreshStatus()
                }
            }
        }
    }

    private fun setProgressVisible(visible: Boolean) {
        binding?.progressBar?.visibility = if (visible) View.VISIBLE else View.GONE
    }

    private fun log(message: String) {
        val timestamp = dateFormat.format(Date())
        val logEntry = "[$timestamp] $message\n"
        logBuffer.append(logEntry)
        
        runOnUiThread {
            binding?.logText?.append(logEntry)
            // Auto-scroll to bottom
            binding?.logScroll?.fullScroll(View.FOCUS_DOWN)
        }
    }

    private fun copyLogToClipboard() {
        val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
        val clip = ClipData.newPlainText("Microdroid Runner Log", logBuffer.toString())
        clipboard.setPrimaryClip(clip)
        showToast("Log copied to clipboard")
    }

    private fun showToast(message: String) {
        Toast.makeText(this, message, Toast.LENGTH_SHORT).show()
    }

    private fun showError(message: String) {
        Toast.makeText(this, message, Toast.LENGTH_LONG).show()
        log("ERROR: $message")
    }

    override fun onSupportNavigateUp(): Boolean {
        finish()
        return true
    }

    override fun onDestroy() {
        super.onDestroy()
        // Clean up controller
        microdroidController?.close()
        microdroidController = null
    }
}