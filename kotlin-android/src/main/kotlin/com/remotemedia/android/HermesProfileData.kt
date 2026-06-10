package com.remotemedia.android

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * Data classes for Hermes Agent profile data
 */
@Serializable
data class HermesProfileData(
    val active_profile: String,
    val profiles: List<HermesProfile>,
    val models: List<HermesModel>,
    val tools: List<HermesTool>,
    val hermes_home: String
)

@Serializable
data class HermesProfile(
    val name: String,
    val path: String,
    val active: Boolean,
    val error: String? = null
)

@Serializable
data class HermesModel(
    val id: String,
    val base_url: String,
    val model: String,
    val temperature: Double = 0.7,
    val max_tokens: Int = 2048
)

@Serializable
data class HermesTool(
    val name: String,
    val enabled: Boolean = true
)

/**
 * Wrapper for parsing Hermes profile data from native JSON
 */
object HermesProfileParser {
    fun parse(json: String): HermesProfileData? {
        return try {
            // Use Json directly with decodeFromString (available in kotlinx-serialization-json 1.6.3)
            Json { ignoreUnknownKeys = true }.decodeFromString(HermesProfileData.serializer(), json)
        } catch (e: Exception) {
            null
        }
    }
}

/**
 * Extension function to get Hermes profile data
 */
fun NativeInterface.getHermesProfileData(): HermesProfileData? {
    val json = nativeGetHermesProfileData()
    return HermesProfileParser.parse(json)
}