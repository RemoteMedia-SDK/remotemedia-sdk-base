package com.remotemedia.inprocess

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.media.AudioManager
import android.os.Build
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat

class AudioProcessingService : Service() {
    
    private const val TAG = "AudioProcessingService"
    private const val NOTIFICATION_ID = 1
    private const val CHANNEL_ID = "RemoteMediaAudioChannel"
    
    private var audioManager: AudioManager? = null
    private var focusListener: AudioManager.OnAudioFocusChangeListener? = null
    
    override fun onCreate() {
        super.onCreate()
        
        // Create notification channel
        createNotificationChannel()
        
        // Start foreground service
        val notification = createNotification()
        startForeground(NOTIFICATION_ID, notification)
        
        // Request audio focus
        requestAudioFocus()
    }
    
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        return START_STICKY
    }
    
    override fun onBind(intent: Intent?): IBinder? = null
    
    override fun onDestroy() {
        super.onDestroy()
        abandonAudioFocus()
        stopForeground(true)
    }
    
    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "RemoteMedia Audio Processing",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Active voice assistant session"
                enableVibration(false)
                setSound(null, null)
            }
            
            val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            manager.createNotificationChannel(channel)
        }
    }
    
    private fun createNotification(): Notification {
        val intent = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        val pendingIntent = PendingIntent.getActivity(
            this, 0, intent,
            PendingIntent.FLAG_IMMUTABLE
        )
        
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("RemoteMedia Voice Assistant")
            .setContentText("Listening for voice input...")
            .setSmallIcon(R.drawable.ic_mic)
            .setContentIntent(pendingIntent)
            .setOngoing(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setVisibility(NotificationCompat.VISIBILITY_PUBLIC)
            .build()
    }
    
    private fun requestAudioFocus() {
        audioManager = getSystemService(Context.AUDIO_SERVICE) as AudioManager
        
        focusListener = AudioManager.OnAudioFocusChangeListener { focusChange ->
            when (focusChange) {
                AudioManager.AUDIOFOCUS_LOSS -> {
                    Log.i(TAG, "Audio focus lost - pausing")
                    // Pause pipeline
                }
                AudioManager.AUDIOFOCUS_LOSS_TRANSIENT -> {
                    Log.i(TAG, "Audio focus lost transient - pausing")
                    // Pause pipeline
                }
                AudioManager.AUDIOFOCUS_LOSS_TRANSIENT_CAN_DUCK -> {
                    Log.i(TAG, "Audio focus duck - lowering volume")
                    // Lower volume
                }
                AudioManager.AUDIOFOCUS_GAIN -> {
                    Log.i(TAG, "Audio focus gained - resuming")
                    // Resume pipeline
                }
            }
        }
        
        val result = audioManager?.requestAudioFocus(
            focusListener!!,
            AudioManager.STREAM_VOICE_CALL,
            AudioManager.AUDIOFOCUS_GAIN
        )
        
        if (result != AudioManager.AUDIOFOCUS_REQUEST_GRANTED) {
            Log.w(TAG, "Audio focus not granted")
        }
    }
    
    private fun abandonAudioFocus() {
        audioManager?.abandonAudioFocus(focusListener!!)
        focusListener = null
    }
    
    companion object {
        fun startService(context: Context) {
            val intent = Intent(context, AudioProcessingService::class.java)
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        }
        
        fun stopService(context: Context) {
            val intent = Intent(context, AudioProcessingService::class.java)
            context.stopService(intent)
        }
    }
}