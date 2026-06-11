package com.remotemedia.android

import android.app.Application
import android.content.Context
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.Dispatchers

/**
 * Base Application class for RemoteMedia integration.
 * Automatically initializes native library and logger.
 * Extend this in your app, or call init manually.
 */
open class RemoteMediaApplication : Application() {

    // Coroutine exception handler for uncaught exceptions in library scopes
    private val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        android.util.Log.e("RemoteMedia", "Uncaught exception in RemoteMedia coroutine", throwable)
    }

    override fun onCreate() {
        super.onCreate()
        initRemoteMedia()
    }

    /**
     * Initialize RemoteMedia native library and logger.
     * Can be called manually if not extending this class.
     */
    fun initRemoteMedia() {
        // Load native library
        try {
            System.loadLibrary("remotemedia_android")
            android.util.Log.i("RemoteMedia", "Loaded libremotemedia_android.so")
        } catch (e: UnsatisfiedLinkError) {
            android.util.Log.e("RemoteMedia", "Failed to load libremotemedia_android.so", e)
            throw RuntimeException("RemoteMedia native library not found. Ensure the AAR is included.", e)
        }

        // Initialize JNI logger
        NativeInterface.initLogger()
        android.util.Log.i("RemoteMedia", "Native logger initialized")
    }

    /**
     * Get the app's cache directory for temporary files.
     * Useful for model storage and plugin extraction.
     */
    companion object {
        @JvmStatic
        fun getCacheDir(context: Context): java.io.File = context.cacheDir

        @JvmStatic
        fun getFilesDir(context: Context): java.io.File = context.filesDir
    }

    /**
     * Default coroutine exception handler for library use.
     */
    val defaultExceptionHandler: CoroutineExceptionHandler = exceptionHandler
}