plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.serialization")
}

android {
    namespace = "com.remotemedia.inprocess"
    compileSdk = 34
    ndkVersion = "25.2.9519653"

    defaultConfig {
        applicationId = "com.remotemedia.inprocess"
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "1.0.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        externalNativeBuild {
            cmake {
                arguments("-DANDROID_STL=c++_shared")
                abiFilters.addAll(listOf("arm64-v8a", "x86_64"))
            }
        }

        ndk {
            abiFilters.addAll(listOf("arm64-v8a", "x86_64"))
        }

        // Default pipeline manifest, overridable via -PdefaultPipeline=...
        val defaultPipeline = project.findProperty("defaultPipeline")?.toString() ?: "hermes-agent-test.json"
        buildConfigField("String", "DEFAULT_PIPELINE", "\"$defaultPipeline\"")
    }

    buildTypes {
        release {
            isDebuggable = true
            isMinifyEnabled = false
            isShrinkResources = false
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            signingConfig = signingConfigs.getByName("debug")
        }
        debug {
            isDebuggable = true
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("debug")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }

    kotlinOptions {
        jvmTarget = "1.8"
    }

    buildFeatures {
        viewBinding = true
        buildConfig = true
    }

    packagingOptions {
        jniLibs {
            useLegacyPackaging = true
        }
        // Native libs are now provided by kotlin-android library
        // doNotStrip.add("**/libremotemedia_android_inprocess.so")
        // doNotStrip.add("**/libpython3.11.so")
    }

    // Keep underscore-prefixed Python package paths (for example `httpx/_transports`)
    // in APK assets. The default ignore pattern can strip them.
    aaptOptions {
        ignoreAssetsPattern = "!.svn:!.git:!*.scc:!CVS:!thumbs.db:!picasa.ini:!*~"
    }

    externalNativeBuild {
        cmake {
            path("../CMakeLists.txt")
        }
    }
}

dependencies {
    // AndroidX
    implementation("androidx.core:core-ktx:1.12.0")
    implementation("androidx.appcompat:appcompat:1.6.1")
    implementation("com.google.android.material:material:1.11.0")
    implementation("androidx.constraintlayout:constraintlayout:2.1.4")
    implementation("androidx.lifecycle:lifecycle-viewmodel-ktx:2.7.0")
    implementation("androidx.lifecycle:lifecycle-livedata-ktx:2.7.0")
    implementation("androidx.activity:activity-ktx:1.8.2")

    // Oboe for low-latency audio
    implementation("com.google.oboe:oboe:1.8.0")

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")

    // Serialization for manifests
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")

    // Logging
    implementation("com.jakewharton.timber:timber:5.0.1")

    // RemoteMedia Kotlin Android library (local dependency)
    implementation(project(":kotlin-android"))

    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test.ext:junit:1.1.5")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.5.1")
}