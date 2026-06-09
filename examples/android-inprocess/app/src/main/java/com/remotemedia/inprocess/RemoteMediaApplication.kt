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
        System.loadLibrary("remotemedia_android_inprocess")

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
