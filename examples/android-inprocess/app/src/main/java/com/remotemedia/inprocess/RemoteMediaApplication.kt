package com.remotemedia.inprocess

import android.app.Application
import android.content.Context
import timber.log.Timber

class RemoteMediaApplication : Application() {

    override fun onCreate() {
        super.onCreate()
        
        // Initialize Timber for logging
        if (BuildConfig.DEBUG) {
            Timber.plant(Timber.DebugTree())
        }
        
        // Initialize native library
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