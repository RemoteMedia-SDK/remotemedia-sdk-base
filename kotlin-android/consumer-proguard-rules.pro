# RemoteMedia Android Library ProGuard/R8 Rules
# These rules are packaged into the AAR and applied to consumer projects

# Keep JNI native methods
-keepclassmembers class com.remotemedia.android.NativeInterface {
    native <methods>;
}

# Keep Kotlin coroutines
-keep class kotlinx.coroutines.** { *; }
-keep class kotlinx.coroutines.internal.** { *; }

# Keep Kotlin serialization
-keep class kotlinx.serialization.** { *; }
-keepclassmembers class * {
    @kotlinx.serialization.Serializable *;
}

# Keep Oboe classes
-keep class com.google.oboe.** { *; }

# Keep pipeline data classes
-keep class com.remotemedia.android.PipelineState { *; }
-keep class com.remotemedia.android.NodeInfo { *; }
-keep class com.remotemedia.android.PipelineMode { *; }
-keep class com.remotemedia.android.NativeException { *; }

# Keep PipelineManager public API
-keep class com.remotemedia.android.PipelineManager { *; }
-keep class com.remotemedia.android.AudioRecorder { *; }
-keep class com.remotemedia.android.AudioPlayer { *; }
-keep class com.remotemedia.android.RemoteMediaApplication { *; }

# Keep enums
-keepclassmembers enum * {
    **[] $VALUES;
    public *;
}

# Don't strip debug symbols from native libraries (handled by doNotStrip in build.gradle.kts)
# But ensure JNI_OnLoad is kept
-keep class com.remotemedia.android.NativeInterface {
    public static void initLogger();
    public static long nativeCreateExecutor();
    public static java.lang.String nativeRunPipeline(long, java.lang.String);
    public static void nativeDestroyExecutor(long);
    public static java.lang.String nativeTestPythonNode();
    public static long nativeCreateSession(long, java.lang.String);
    public static boolean nativeSendInputText(long, java.lang.String);
    public static boolean nativeSendInputAudio(long, byte[], int, int);
    public static java.lang.String nativeRecvOutput(long);
    public static void nativeCloseSession(long);
    public static java.lang.String nativeGetAvailableNodes();
    public static boolean nativeStartStreaming(long);
    public static boolean nativeStopStreaming(long);
}