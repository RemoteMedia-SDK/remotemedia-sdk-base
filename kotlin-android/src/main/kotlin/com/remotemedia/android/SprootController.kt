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
    private val runDir = File(context.filesDir, "sproot/run")
    private val rootfsDir = File(sprootDir, "rootfs")
    
    private val prootExe = File(context.applicationInfo.nativeLibraryDir, "libproot.so")
    private val socketFile = File(runDir, "runner.sock")

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
            return@withContext socketFile.canonicalPath
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

            // Shared env for both smoke-test and real launch
            val prootEnv = mutableMapOf<String, String>().also { env ->
                env["PROOT_TMP_DIR"] = context.cacheDir.canonicalPath
                env["LD_LIBRARY_PATH"] = context.applicationInfo.nativeLibraryDir
                env["PROOT_NO_SECCOMP"] = "1"
                env["PROOT_LOADER"] = File(context.applicationInfo.nativeLibraryDir, "libproot_loader.so").canonicalPath
            }

            val glibcHostPath = File(context.filesDir, "sproot-glibc").canonicalPath
            val nativeHostPath = context.applicationInfo.nativeLibraryDir

            logPathDiagnostics("glibc_ld", File(nativeHostPath, "libglibc_ld.so"))
            logPathDiagnostics("runner", File(nativeHostPath, "libremotemedia_sproot_runner.so"))

            // PRoot smoke-test: run the glibc runner through the APK-packaged glibc loader
            // to confirm PRoot can execute the glibc entrypoint before the real runner boot.
            val smokeCommand = listOf(
                prootExe.canonicalPath,
                "-v", "9",
                "-0",
                "-r", rootfsDir.canonicalPath,
                "-w", "/",

                "-b", "/sys:/sys",
                "-b", "/proc:/proc",
                "-b", "/dev:/dev",
                "-b", "${File(sprootDir, "shm").canonicalPath}:/dev/shm",

                "-b", "/system",
                "-b", "/apex",
                "-b", "/system_ext",
                "-b", "/product",
                "-b", "/vendor",
                "-b", "/odm",
                "-b", "/linkerconfig/ld.config.txt:/linkerconfig/ld.config.txt",

                "-b", "$nativeHostPath:/apk-native",
                "-b", "$glibcHostPath:/apk-native-glibc",
                "-b", "${runDir.canonicalPath}:/mnt/run",
                "-b", "${context.cacheDir.canonicalPath}:${context.cacheDir.canonicalPath}",

                "/apk-native/libglibc_ld.so",
                "--library-path",
                "/apk-native-glibc:/apk-native:/lib:/lib/aarch64-linux-gnu:/usr/lib:/usr/lib/aarch64-linux-gnu",
                "/apk-native/libremotemedia_sproot_runner.so",
                "--version"
            )
            try {
                Log.i(TAG, "PRoot smoke-test: ${smokeCommand.joinToString(" ")}")
                val smokeProc = ProcessBuilder(smokeCommand)
                    .directory(sprootDir)
                    .redirectErrorStream(true)
                    .also { it.environment().putAll(prootEnv) }
                    .start()
                val smokeOut = smokeProc.inputStream.bufferedReader().use { it.readText() }
                val smokeExit = smokeProc.waitFor()
                Log.i(TAG, "PRoot smoke-test exit=$smokeExit out=${smokeOut.trim()}")
            } catch (t: Throwable) {
                Log.w(TAG, "PRoot smoke-test threw: ${t.message}")
            }

            Log.i(TAG, "Launching PRoot: ${prootCommand.joinToString(" ")}")

            val processBuilder = ProcessBuilder(prootCommand)
                .directory(sprootDir)
                .redirectErrorStream(true)
                .also { it.environment().putAll(prootEnv) }

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
            Log.i(TAG, "Waiting for UDS socket to appear at ${socketFile.canonicalPath}...")
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
            socketFile.canonicalPath
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

        // Ensure host and guest shm folders exist
        File(sprootDir, "shm").mkdirs()
        File(rootfsDir, "dev/shm").mkdirs()

        // Ensure guest bind-mount target folders exist inside rootfs
        File(rootfsDir, "apk-native").mkdirs()
        File(rootfsDir, "apk-native-glibc").mkdirs()
        File(rootfsDir, "mnt/run").mkdirs()
        File(rootfsDir, "mnt/models").mkdirs()
        File(rootfsDir, "mnt/plugins").mkdirs()
        File(rootfsDir, "mnt/python_src").mkdirs()
        File(rootfsDir, context.cacheDir.canonicalPath.removePrefix("/")).mkdirs()

        // Create symlinks for versioned glibc libraries
        setupGlibcSymlinks()
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
                extractTar(rootfsTar.canonicalPath, rootfsDir)

                // Explicitly validate required paths after extraction
                require(File(rootfsDir, "lib/aarch64-linux-gnu").exists()) { "Rootfs extraction missing lib/aarch64-linux-gnu" }
                require(File(rootfsDir, "usr/lib").exists()) { "Rootfs extraction missing usr/lib" }
                require(File(rootfsDir, "tmp").exists()) { "Rootfs extraction missing tmp" }
            } finally {
                if (rootfsTar.exists()) {
                    rootfsTar.delete()
                }
            }
            rootfsMarker.createNewFile()
        }

        // Runner binary and glibc loader are now packaged as APK native libraries (jniLibs).
        // No extraction into rootfs needed. The glibc ld-linux and runner .so files
        // are invoked directly from the APK native library directory via --library-path.
    }

    private fun extractTar(tarPath: String, destDir: File) {
        try {
            val process = Runtime.getRuntime().exec(arrayOf("tar", "-xof", tarPath, "-C", destDir.canonicalPath))
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

            // On Android FUSE, tar always emits symlink warnings (can't link, Permission denied).
            // These are non-critical - the files are extracted, just symlinks aren't created.
            // Always treat extraction as success since the actual file contents are there.
            val errStr = stderr.toString().trim()
            val outStr = stdout.toString().trim()
            Log.i(TAG, "tar exit code: $result")
            if (errStr.isNotBlank()) {
                Log.i(TAG, "tar stderr (non-critical symlink warnings): ${errStr.lines().count()} lines")
            }
            if (outStr.isNotBlank()) {
                Log.i(TAG, "tar stdout: $outStr")
            }
            // Always continue - rootfs files are extracted despite symlink warnings
        } catch (e: Exception) {
            Log.e(TAG, "Exception extracting tar: $tarPath", e)
            throw e
        }
    }

    private fun setupGlibcSymlinks() {
        val glibcDir = File(context.filesDir, "sproot-glibc")
        glibcDir.mkdirs()

        val nativeHostPath = context.applicationInfo.nativeLibraryDir

        val mappings = mapOf(
            "libc.so.6" to "libglibc_libc.so",
            "libm.so.6" to "libglibc_libm.so",
            "libdl.so.2" to "libglibc_libdl.so",
            "libpthread.so.0" to "libglibc_libpthread.so",
            "libresolv.so.2" to "libglibc_libresolv.so",
            "libutil.so.1" to "libglibc_libutil.so",
            "librt.so.1" to "libglibc_librt.so",
            "libnsl.so.2" to "libglibc_libnsl.so",
            "libgcc_s.so.1" to "libglibc_libgcc_s.so",
            "libstdc++.so.6" to "libglibc_libstdcxx.so",
            "libz.so.1" to "libglibc_libz.so",
            "libffi.so.8" to "libglibc_libffi.so",
            "libssl.so.3" to "libglibc_libssl.so",
            "libcrypto.so.3" to "libglibc_libcrypto.so",
            "libpython3.11.so.1.0" to "libglibc_libpython3_11.so"
        )

        for ((symlinkName, targetName) in mappings) {
            val symlinkFile = File(glibcDir, symlinkName)
            val targetFile = File(nativeHostPath, targetName)
            
            // Re-create symlink to ensure correctness
            if (symlinkFile.exists() || java.nio.file.Files.isSymbolicLink(symlinkFile.toPath())) {
                symlinkFile.delete()
            }
            
            try {
                java.nio.file.Files.createSymbolicLink(
                    symlinkFile.toPath(),
                    java.nio.file.Paths.get(targetFile.canonicalPath)
                )
                Log.i(TAG, "Created glibc symlink: ${symlinkFile.name} -> ${targetFile.name}")
            } catch (e: Exception) {
                Log.e(TAG, "Failed to create glibc symlink: ${symlinkFile.name}", e)
            }
        }
    }

    private fun buildProotCommand(): List<String> {
        val modelsHostPath = File(context.filesDir, "models").canonicalPath
        val pluginsHostPath = File(context.filesDir, "plugins").canonicalPath
        val pythonHostPath = File(context.filesDir, "python/src").canonicalPath
        val runHostPath = runDir.canonicalPath
        val glibcHostPath = File(context.filesDir, "sproot-glibc").canonicalPath
        val nativeHostPath = context.applicationInfo.nativeLibraryDir

        return listOf(
            prootExe.canonicalPath,
            "-0",
            "-r", rootfsDir.canonicalPath,
            "-w", "/",

            "-b", "/sys:/sys",
            "-b", "/proc:/proc",
            "-b", "/dev:/dev",
            "-b", "${File(sprootDir, "shm").canonicalPath}:/dev/shm",

            "-b", "/system",
            "-b", "/apex",
            "-b", "/system_ext",
            "-b", "/product",
            "-b", "/vendor",
            "-b", "/odm",
            "-b", "/linkerconfig/ld.config.txt:/linkerconfig/ld.config.txt",

            "-b", "$nativeHostPath:/apk-native",
            "-b", "$glibcHostPath:/apk-native-glibc",
            "-b", "$modelsHostPath:/mnt/models",
            "-b", "$pluginsHostPath:/mnt/plugins",
            "-b", "$pythonHostPath:/mnt/python_src",
            "-b", "$runHostPath:/mnt/run",
            "-b", "${context.cacheDir.canonicalPath}:${context.cacheDir.canonicalPath}",

            "/apk-native/libglibc_ld.so",
            "--library-path",
            "/apk-native-glibc:/apk-native:/lib:/lib/aarch64-linux-gnu:/usr/lib:/usr/lib/aarch64-linux-gnu",
            "/apk-native/libremotemedia_sproot_runner.so",
            "--socket-path", "/mnt/run/runner.sock"
        )
    }

    private fun logPathDiagnostics(label: String, f: File) {
        try {
            val exists = f.exists()
            val isFile = f.isFile
            val isDir = f.isDirectory
            val isSymlink = java.nio.file.Files.isSymbolicLink(f.toPath())
            val length = if (exists && isFile && !isSymlink) f.length() else -1
            val canonical = if (exists) f.canonicalPath else "n/a"
            Log.i(TAG, "FS[$label] path=${f.absolutePath} exists=$exists file=$isFile dir=$isDir symlink=$isSymlink len=$length canonical=$canonical")
        } catch (t: Throwable) {
            Log.w(TAG, "FS[$label] path=${f.absolutePath} threw: ${t.message}")
        }
    }
}
