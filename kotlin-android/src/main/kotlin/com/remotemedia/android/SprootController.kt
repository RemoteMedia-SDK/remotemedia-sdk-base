package com.remotemedia.android

import android.content.Context
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Manages the lifecycle of the Debian Sproot user-space container on Android.
 * Extracts the required proot binary, rootfs, and runner executable to the app's files directory
 * and launches the guest process with custom bind mounts.
 */
class SprootController(private val context: Context) {

    companion object {
        private const val TAG = "SprootController"
        private const val BOOT_TIMEOUT_SECONDS = 30L
    }

    private val sprootDir = File(context.filesDir, "sproot")
    private val runDir = File(sprootDir, "run")
    private val rootfsDir = File(sprootDir, "rootfs")
    
    private val prootExe = File(context.applicationInfo.nativeLibraryDir, "libproot.so")
    private val socketFile = File(runDir, "runner.sock")
    private val rootfsRunnerExe = File(context.applicationInfo.nativeLibraryDir, "libremotemedia_sproot_runner.so")

    private var containerProcess: Process? = null
    private val isRunning = AtomicBoolean(false)

    /**
     * Start the Sproot container. Unpacks assets if necessary, sets up bind mounts,
     * and spawns the container process.
     * 
     * @return The absolute path to the UDS socket if startup was successful, null otherwise.
     */
    suspend fun start(): String? = withContext(Dispatchers.IO) {
        if (isRunning.get()) {
            Log.w(TAG, "Sproot container is already running")
            return@withContext socketFile.absolutePath
        }

        try {
            Log.i(TAG, "Starting Sproot container initialization...")
            prepareDirectories()
            extractRequiredBinaries()
            
            // Clean up stale socket from previous run if it exists
            if (socketFile.exists()) {
                socketFile.delete()
            }

            val prootCommand = buildProotCommand()
            Log.i(TAG, "Launching PRoot: ${prootCommand.joinToString(" ")}")

            val processBuilder = ProcessBuilder(prootCommand)
                .directory(sprootDir)
                .redirectErrorStream(true)

            // Setup environment variables in the container process
            val env = processBuilder.environment()
            env["PROOT_TMP_DIR"] = context.cacheDir.absolutePath
            env["LD_LIBRARY_PATH"] = context.applicationInfo.nativeLibraryDir
            env["PROOT_NO_SECCOMP"] = "1"
            env.remove("LD_PRELOAD")

            val process = processBuilder.start()
            containerProcess = process
            isRunning.set(true)

            // Spawn log consumer thread to forward logs to logcat
            Thread {
                try {
                    process.inputStream.bufferedReader().use { reader ->
                        var line: String?
                        while (reader.readLine().also { line = it } != null) {
                            Log.d("SprootGuest", line ?: "")
                        }
                    }
                } catch (e: IOException) {
                    Log.e(TAG, "Error reading container process output", e)
                } finally {
                    Log.i(TAG, "Container stdout/stderr reader terminated")
                }
            }.start()

            // Wait for the Unix Domain Socket file to appear
            Log.i(TAG, "Waiting for UDS socket to appear at ${socketFile.absolutePath}...")
            val startTime = System.currentTimeMillis()
            var socketCreated = false
            
            while (System.currentTimeMillis() - startTime < BOOT_TIMEOUT_SECONDS * 1000) {
                if (!process.isAlive) {
                    val exitCode = try { process.exitValue() } catch (e: Exception) { -1 }
                    throw IOException("PRoot container exited prematurely with exit code: $exitCode")
                }
                if (socketFile.exists()) {
                    socketCreated = true
                    break
                }
                Thread.sleep(200)
            }

            if (!socketCreated) {
                throw IOException("Timeout waiting for Sproot runner socket to be created")
            }

            Log.i(TAG, "Sproot container runner successfully booted and listening on UDS socket.")
            socketFile.absolutePath
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start Sproot container", e)
            stop()
            null
        }
    }

    /**
     * Stop the container process tree and delete the socket file.
     */
    fun stop() {
        if (!isRunning.get() && containerProcess == null) {
            return
        }
        
        Log.i(TAG, "Stopping Sproot container...")
        isRunning.set(false)

        containerProcess?.let { process ->
            try {
                // Try to terminate gracefully (SIGINT equivalent on Process)
                process.destroy()
                if (!process.waitFor(3, TimeUnit.SECONDS)) {
                    Log.w(TAG, "Container process did not exit gracefully; destroying forcibly...")
                    process.destroyForcibly()
                }
                Unit
            } catch (e: Exception) {
                Log.e(TAG, "Error stopping container process", e)
            }
        }
        containerProcess = null

        if (socketFile.exists()) {
            socketFile.delete()
        }
        Log.i(TAG, "Sproot container stopped.")
    }

    private fun prepareDirectories() {
        runDir.mkdirs()
        rootfsDir.mkdirs()
        // Ensure models, plugins, and python/src folders exist in Context files dir so bind mounts succeed
        File(context.filesDir, "models").mkdirs()
        File(context.filesDir, "plugins").mkdirs()
        File(context.filesDir, "python/src").mkdirs()

        // Ensure guest mount points exist inside rootfs
        File(rootfsDir, "system").mkdirs()
        File(rootfsDir, "apex").mkdirs()
        File(rootfsDir, "linkerconfig").mkdirs()
        File(rootfsDir, "system_ext").mkdirs()
        File(rootfsDir, "product").mkdirs()
        File(rootfsDir, "vendor").mkdirs()
        File(rootfsDir, "odm").mkdirs()
    }

    private fun extractRequiredBinaries() {
        // 1. Extract rootfs
        val rootfsMarker = File(rootfsDir, ".rootfs_extracted")
        if (!rootfsMarker.exists()) {
            Log.i(TAG, "Extracting and decompressing rootfs archive from assets...")
            val rootfsTar = File(sprootDir, "rootfs.tar")
            try {
                java.util.zip.GZIPInputStream(context.assets.open("sproot/rootfs.targz")).use { gzipInputStream ->
                    FileOutputStream(rootfsTar).use { outputStream ->
                        gzipInputStream.copyTo(outputStream)
                    }
                }
                Log.i(TAG, "Extracting rootfs tarball...")
                extractTar(rootfsTar.absolutePath, rootfsDir)
            } finally {
                if (rootfsTar.exists()) {
                    rootfsTar.delete()
                }
            }
            rootfsMarker.createNewFile()
        }

        // 2. Create placeholder for guest runner binary bind mount (under both bin/ and usr/bin/ for usr-merge compatibility)
        val runnerPlaceholder1 = File(rootfsDir, "bin/remotemedia-sproot-runner")
        if (!runnerPlaceholder1.exists()) {
            runnerPlaceholder1.parentFile?.mkdirs()
            runnerPlaceholder1.createNewFile()
        }
        val runnerPlaceholder2 = File(rootfsDir, "usr/bin/remotemedia-sproot-runner")
        if (!runnerPlaceholder2.exists()) {
            runnerPlaceholder2.parentFile?.mkdirs()
            runnerPlaceholder2.createNewFile()
        }
    }

    private fun extractAssetFile(assetPath: String, destFile: File) {
        try {
            destFile.parentFile?.mkdirs()
            context.assets.open(assetPath).use { inputStream ->
                FileOutputStream(destFile).use { outputStream ->
                    inputStream.copyTo(outputStream)
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Failed to extract asset $assetPath to ${destFile.absolutePath}", e)
            throw e
        }
    }

    private fun extractTar(tarPath: String, destDir: File) {
        try {
            val process = Runtime.getRuntime().exec(arrayOf("tar", "-xof", tarPath, "-C", destDir.absolutePath))
            val errorReader = process.errorStream.bufferedReader()
            val outputReader = process.inputStream.bufferedReader()
            
            val stderr = StringBuilder()
            val stdout = StringBuilder()
            
            val errThread = Thread {
                try {
                    var line: String?
                    while (errorReader.readLine().also { line = it } != null) {
                        stderr.append(line).append("\n")
                    }
                } catch (_: Exception) {}
            }
            errThread.start()
            
            val outThread = Thread {
                try {
                    var line: String?
                    while (outputReader.readLine().also { line = it } != null) {
                        stdout.append(line).append("\n")
                    }
                } catch (_: Exception) {}
            }
            outThread.start()
            
            val result = process.waitFor()
            errThread.join(1000)
            outThread.join(1000)
            
            if (result != 0) {
                val errStr = stderr.toString().trim()
                val outStr = stdout.toString().trim()
                Log.e(TAG, "tar stdout: $outStr")
                Log.e(TAG, "tar stderr: $errStr")
                throw IOException("tar extraction exited with code $result. stderr: $errStr")
            }
        } catch (e: Exception) {
            Log.e(TAG, "Exception extracting tar: $tarPath", e)
            throw e
        }
    }

    private fun buildProotCommand(): List<String> {
        val modelsHostPath = File(context.filesDir, "models").absolutePath
        val pluginsHostPath = File(context.filesDir, "plugins").absolutePath
        val pythonHostPath = File(context.filesDir, "python/src").absolutePath
        val runHostPath = runDir.absolutePath

        return listOf(
            prootExe.absolutePath,
            "-0",
            "-r", rootfsDir.absolutePath,
            "-w", "/",
            "-b", "/sys:/sys",
            "-b", "/proc:/proc",
            "-b", "/dev:/dev",
            "-b", "/system",
            "-b", "/apex",
            "-b", "/system_ext",
            "-b", "/product",
            "-b", "/vendor",
            "-b", "/odm",
            "-b", "/linkerconfig/ld.config.txt:/linkerconfig/ld.config.txt",
            "-b", "$modelsHostPath:/mnt/models",
            "-b", "$pluginsHostPath:/mnt/plugins",
            "-b", "$pythonHostPath:/mnt/python_src",
            "-b", "$runHostPath:/mnt/run",
            "-b", "${rootfsRunnerExe.absolutePath}:/bin/remotemedia-sproot-runner",
            "-b", "${rootfsRunnerExe.absolutePath}:/usr/bin/remotemedia-sproot-runner",
            "/bin/remotemedia-sproot-runner",
            "--socket-path", "/mnt/run/runner.sock"
        )
    }
}
