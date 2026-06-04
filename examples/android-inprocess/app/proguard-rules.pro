# RemoteMedia ProGuard Rules

# Keep JNI native methods
-keepclassmembers class com.remotemedia.inprocess.NativeInterface {
    native <methods>;
}

# Keep model classes for serialization
-keepclassmembers class com.remotemedia.inprocess.** {
    <fields>;
    <methods>;
}

# Keep Kotlin coroutines
-keepclassmembers class kotlinx.coroutines.** { *; }

# Keep Kotlin serialization
-keepclassmembers class kotlinx.serialization.** { *; }

# Keep Timber
-keepclassmembers class timber.log.** { *; }

# Keep AudioRecord/AudioTrack
-keepclassmembers class android.media.AudioRecord { *; }
-keepclassmembers class android.media.AudioTrack { *; }

# Keep Oboe (if used via JNI)
-keepclassmembers class com.google.oboe.** { *; }

# Prevent obfuscation of native library loading
-keep class com.remotemedia.inprocess.RemoteMediaApplication { *; }