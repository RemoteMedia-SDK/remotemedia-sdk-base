1. Kotlin API: add `GrpcServiceConfig`, `startGrpcService`, `stopGrpcService`
2. JNI: extend `NativeInterface` with service lifecycle externs
3. Rust JNI host: implement start/stop in `examples/android-inprocess` Rust lib
4. Validation: build, deploy, and start gRPC from Android device
