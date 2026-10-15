## ADDED Requirements

### Requirement: PipelineManager provides idiomatic Kotlin API for pipeline lifecycle
`PipelineManager` SHALL expose a clean, coroutine-friendly API for initializing, loading manifests, executing unary requests, and managing streaming sessions.

#### Scenario: Initialize creates executor and loads native plugins
- **WHEN** `manager.initialize()` is called
- **THEN** native executor is created, loadable plugins extracted from assets to files dir, returns `true` on success

#### Scenario: Load manifest from assets or JSON string
- **WHEN** `manager.loadManifest("voice-assistant.yaml")` or `manager.loadManifestJson(json)`
- **THEN** manifest is stored and validated, ready for execution

#### Scenario: Execute unary returns result synchronously
- **WHEN** `manager.executeUnary("Hello")` is called with loaded manifest
- **THEN** blocks until pipeline completes, returns JSON result string or `null` on error

#### Scenario: Unary execution fails gracefully without initialized executor
- **WHEN** `executeUnary` called before `initialize()`
- **THEN** returns `null`, `onError` invoked with "Executor not initialized"

### Requirement: PipelineManager supports streaming with callback-based output
`PipelineManager` SHALL provide `startStreaming()`, `sendText()`, `sendAudio()`, `stop Streaming()` with `onOutput`, `onError`, `onStateChange` callbacks for real-time pipelines.

#### Scenario: Streaming session created and output received
- **WHEN** `startStreaming()` succeeds, then `sendAudio(pcm)` called
- **THEN** `onOutput` receives JSON output from pipeline nodes asynchronously

#### Scenario: Text input sent to streaming session
- **WHEN** `sendText("Hello")` called during active streaming
- **THEN** native `nativeSendInputText` invoked, returns `true` on success

#### Scenario: Streaming stops and cleans up session
- **WHEN** `stopStreaming()` called
- **THEN** native session closed, `isStreamingActive()` returns `false`, state returns to `READY`

#### Scenario: State machine reflects pipeline lifecycle
- **WHEN** manager transitions through `IDLE` → `INITIALIZING` → `READY` → `RUNNING`/`STREAMING` → `READY`/`ERROR` → `DESTROYED`
- **THEN** `onStateChange` fires at each transition with correct `PipelineState` enum value

### Requirement: PipelineManager handles native plugin extraction automatically
The manager SHALL extract bundled loadable plugins (`.so` files) from assets to the app's private files directory on first initialization.

#### Scenario: Plugins extracted on first init
- **WHEN** `initialize()` called first time
- **THEN** all configured plugin `.so` files copied from `assets/plugins/` to `context.filesDir/`

#### Scenario: Subsequent inits skip extraction
- **WHEN** `initialize()` called again after first success
- **THEN** extraction skipped, existing plugin files reused

### Requirement: Available nodes queryable for dynamic UI
`getAvailableNodes()` SHALL return parsed `List<NodeInfo>` from native registry for building dynamic pipeline editors.

#### Scenario: Node list includes categories and I/O types
- **WHEN** `getAvailableNodes()` called after init
- **THEN** returns list with `name`, `description`, `category` (STT/LLM/TTS/VAD/UTILITY), `inputTypes`, `outputTypes`, `parameters`

### Requirement: Resource cleanup on destroy
`destroy()` SHALL stop streaming, destroy executor, cancel coroutines, and release all native handles.

#### Scenario: Destroyed manager cannot be reused
- **WHEN** `destroy()` called then any API called
- **THEN** operations return error/`false`, `isInitialized()` returns `false`