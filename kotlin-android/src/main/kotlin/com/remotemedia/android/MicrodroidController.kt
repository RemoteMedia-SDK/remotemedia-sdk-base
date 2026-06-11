package com.remotemedia.android

import android.content.Context
import android.os.ParcelFileDescriptor
import android.util.Log
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.Closeable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Controller for Microdroid pVM (protected Virtual Machine) lifecycle.
 * Manages VM creation, state transitions, callbacks, and vsock connection handoff.
 * Uses reflection to avoid compile-time dependencies on system/hidden AVF APIs.
 */
class MicrodroidController(
    private val context: Context
) : Closeable {

    companion object {
        private const val TAG = "MicrodroidController"
        private const val VM_NAME = "remote-media-microdroid-runner"
        private const val VSOCK_PORT = 5555
    }

    private val executor: ExecutorService = Executors.newSingleThreadExecutor()
    private var vm: Any? = null

    // Load framework classes dynamically
    private val vmManagerClass: Class<*> by lazy { Class.forName("android.system.virtualmachine.VirtualMachineManager") }
    private val vmClass: Class<*> by lazy { Class.forName("android.system.virtualmachine.VirtualMachine") }
    private val vmConfigClass: Class<*> by lazy { Class.forName("android.system.virtualmachine.VirtualMachineConfig") }
    private val builderClass: Class<*> by lazy { Class.forName("android.system.virtualmachine.VirtualMachineConfig\$Builder") }
    private val callbackClass: Class<*> by lazy { Class.forName("android.system.virtualmachine.VirtualMachineCallback") }

    /**
     * Checks if the Android Virtualization Framework (AVF) is supported and enabled on the device.
     */
    fun isSupported(): Boolean {
        return try {
            val manager = context.getSystemService(vmManagerClass) ?: return false
            val capabilitiesMethod = vmManagerClass.getMethod("getCapabilities")
            val capabilities = capabilitiesMethod.invoke(manager) as Int
            capabilities != 0
        } catch (t: Throwable) {
            Log.w(TAG, "Failed to check virtualization capabilities: ${t.message}")
            false
        }
    }

    /**
     * Helper to retrieve or create the VirtualMachine instance via reflection.
     */
    private fun getOrCreateVm(manager: Any): Any {
        val getMethod = vmManagerClass.getMethod("get", String::class.java)
        val existing = runCatching {
            getMethod.invoke(manager, VM_NAME)
        }.getOrNull()

        if (existing != null) {
            vm = existing
            return existing
        }

        Log.i(TAG, "Creating new pVM instance: $VM_NAME")

        // Build config for non-protected or protected mode using reflection
        val builderConstructor = builderClass.getConstructor(Context::class.java)
        val builder = builderConstructor.newInstance(context)

        builderClass.getMethod("setPayloadBinaryName", String::class.java).invoke(builder, "remotemedia-microdroid-runner")
        builderClass.getMethod("setProtectedVm", Boolean::class.java).invoke(builder, false)
        builderClass.getMethod("setMemoryBytes", Long::class.java).invoke(builder, 512L * 1024L * 1024L)

        // cpuTopologyOneCpu = VirtualMachineConfig.CPU_TOPOLOGY_ONE_CPU
        val cpuTopologyField = vmConfigClass.getField("CPU_TOPOLOGY_ONE_CPU")
        val cpuTopologyOneCpu = cpuTopologyField.get(null) as Int
        builderClass.getMethod("setCpuTopology", Int::class.java).invoke(builder, cpuTopologyOneCpu)

        val config = builderClass.getMethod("build").invoke(builder)

        val createMethod = vmManagerClass.getMethod("create", String::class.java, vmConfigClass)
        val created = createMethod.invoke(manager, VM_NAME, config) ?: throw IllegalStateException("Failed to create VirtualMachine")

        vm = created
        return created
    }

    /**
     * Starts the Microdroid VM. Blocks/suspends until the payload signals ready.
     */
    suspend fun startVm(): Unit = withContext(Dispatchers.IO) {
        if (!isSupported()) {
            throw UnsupportedOperationException("Android Virtualization Framework (AVF) is not supported on this device")
        }

        val manager = context.getSystemService(vmManagerClass)
            ?: throw IllegalStateException("Failed to obtain VirtualMachineManager")

        val targetVm = getOrCreateVm(manager)

        // Use a CompletableDeferred to wait for the onPayloadReady callback asynchronously
        val readyDeferred = CompletableDeferred<Unit>()

        // Create a dynamic proxy to implement the VirtualMachineCallback interface
        val callbackProxy = java.lang.reflect.Proxy.newProxyInstance(
            callbackClass.classLoader,
            arrayOf(callbackClass),
            java.lang.reflect.InvocationHandler { _, method, args ->
                when (method.name) {
                    "onPayloadStarted" -> {
                        Log.i(TAG, "Microdroid VM payload started")
                    }
                    "onPayloadReady" -> {
                        Log.i(TAG, "Microdroid VM payload ready for connection")
                        readyDeferred.complete(Unit)
                    }
                    "onPayloadFinished" -> {
                        val exitCode = args?.get(1) as? Int ?: -1
                        Log.i(TAG, "Microdroid VM payload finished with exitCode: $exitCode")
                        readyDeferred.completeExceptionally(IllegalStateException("VM finished early with exitCode $exitCode"))
                    }
                    "onError" -> {
                        val errorCode = args?.get(1) as? Int ?: -1
                        val message = args?.get(2) as? String ?: "Unknown error"
                        Log.e(TAG, "Microdroid VM encountered error $errorCode: $message")
                        readyDeferred.completeExceptionally(RuntimeException("VM Error ($errorCode): $message"))
                    }
                    "onStopped" -> {
                        val reason = args?.get(1) as? Int ?: -1
                        Log.i(TAG, "Microdroid VM stopped. Reason: $reason")
                        readyDeferred.completeExceptionally(IllegalStateException("VM stopped with reason $reason"))
                    }
                }
                null
            }
        )

        val setCallbackMethod = vmClass.getMethod("setCallback", ExecutorService::class.java, callbackClass)
        setCallbackMethod.invoke(targetVm, executor, callbackProxy)

        Log.i(TAG, "Starting Microdroid VM...")
        try {
            val runMethod = vmClass.getMethod("run")
            runMethod.invoke(targetVm)
            readyDeferred.await()
        } catch (t: Throwable) {
            Log.e(TAG, "Failed to launch or initialize Microdroid VM", t)
            val stopMethod = vmClass.getMethod("stop")
            runCatching { stopMethod.invoke(targetVm) }
            throw t
        }
    }

    /**
     * Connects to vsock port 5555 and returns the descriptor representing the stream.
     */
    fun connectVsock(): ParcelFileDescriptor {
        val targetVm = vm ?: throw IllegalStateException("VM is not initialized or started")
        Log.i(TAG, "Connecting to vsock port $VSOCK_PORT...")
        val connectVsockMethod = vmClass.getMethod("connectVsock", Int::class.java)
        return connectVsockMethod.invoke(targetVm, VSOCK_PORT) as ParcelFileDescriptor
    }

    /**
     * Stops the VM instance.
     */
    fun stopVm() {
        try {
            val targetVm = vm
            if (targetVm != null) {
                val stopMethod = vmClass.getMethod("stop")
                stopMethod.invoke(targetVm)
                Log.i(TAG, "Microdroid VM stopped")
            }
        } catch (t: Throwable) {
            Log.w(TAG, "Error stopping Microdroid VM", t)
        }
    }

    override fun close() {
        stopVm()
        executor.shutdown()
        Log.i(TAG, "MicrodroidController closed")
    }
}
