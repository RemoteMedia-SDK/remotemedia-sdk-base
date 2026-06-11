package com.remotemedia.inprocess

import android.app.Application
import android.content.Context
import com.remotemedia.android.NativeInterface
import com.remotemedia.inprocess.BuildConfig
import timber.log.Timber

class RemoteMediaApplication : Application() {

    override fun onCreate() {
        super.onCreate()

        // Initialize Timber for logging
        if (BuildConfig.DEBUG) {
            Timber.plant(Timber.DebugTree())
        }

        // Initialize native library
        runCatching {
            System.loadLibrary("GemmaModelConstraintProvider")
        }.onFailure {
            Timber.w(it, "LiteRT-LM constraint provider library not available")
        }
        runCatching {
            System.loadLibrary("litert_lm")
        }.onFailure {
            Timber.w(it, "LiteRT-LM native library not available")
        }
        try {
            System.loadLibrary("remotemedia_android_inprocess")
            Timber.i("Loaded libremotemedia_android_inprocess.so")
        } catch (e: UnsatisfiedLinkError) {
            try {
                System.loadLibrary("remotemedia_android")
                Timber.i("Loaded libremotemedia_android.so")
            } catch (e2: UnsatisfiedLinkError) {
                Timber.e(e2, "Failed to load JNI libraries")
                throw RuntimeException("RemoteMedia native library not found.", e2)
            }
        }

        // Set app files directory for native code (must be after loadLibrary for JNI to be available)
        NativeInterface.nativeSetAppFilesDir(filesDir.absolutePath)

        // Initialize logger from JNI
        NativeInterface.initLogger()
    }

    companion object {
        @JvmStatic
        fun getCacheDir(context: Context): java.io.File {
            return context.cacheDir
        }
    }
}
