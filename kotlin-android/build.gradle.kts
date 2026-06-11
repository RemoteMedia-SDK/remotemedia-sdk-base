plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.serialization")
}

android {
    namespace = "com.remotemedia.android"
    compileSdk = 34
    ndkVersion = "25.2.9519653"

    defaultConfig {
        minSdk = 24
        targetSdk = 34
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    kotlinOptions {
        jvmTarget = "1.8"
        freeCompilerArgs += listOf(
            "-Xopt-in=kotlin.RequiresOptIn",
            "-Xjvm-default=all"
        )
    }

    buildFeatures {
        buildConfig = true
    }

    packagingOptions {
        jniLibs {
            useLegacyPackaging = true
        }
        doNotStrip.add("**/libremotemedia_android.so")
    }

    externalNativeBuild {
        cmake {
            path("CMakeLists.txt")
        }
    }
}

dependencies {
    // Coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    // Serialization
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")

    // AndroidX
    api("androidx.core:core-ktx:1.12.0")
    api("androidx.annotation:annotation:1.7.0")

    // LiteRT-LM (optional - for LiteRtLmNodeBridge)
    // Requires Google LiteRT-LM library from maven.google.com or local AAR
    // api("com.google.ai.edge.litert:litert-lm:0.0.1-nightly")

    // Testing
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test.ext:junit:1.1.5")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.5.1")
    androidTestImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
}