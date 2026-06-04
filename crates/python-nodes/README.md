# RemoteMedia Python Nodes

Dynamic Python node registration for RemoteMedia pipelines.

## Overview

This crate provides infrastructure for registering Python-based pipeline nodes that run via the multiprocess executor. Instead of hardcoding Rust factory definitions for each Python node, nodes register themselves dynamically at runtime.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
remotemedia-python-nodes = { path = "../python-nodes" }
```

When linked, the `PythonNodesProvider` is automatically registered via the `inventory` system and nodes become available in `create_default_streaming_registry()`.

## Usage

### From Python (Recommended)

Register Python nodes from file paths:

```python
from remotemedia import register_python_node

# Register all MultiprocessNode classes from a file
register_python_node("./my_nodes/custom_ml.py")

# Register with options
register_python_node(
    "./my_nodes/my_tts.py",
    node_type="MyTTS",
    multi_output=True,
    category="tts"
)
```

Or use the `@streaming_node` decorator:

```python
from remotemedia.nodes import streaming_node
from remotemedia.core.multiprocessing import MultiprocessNode

@streaming_node(
    node_type="KokoroTTSNode",
    multi_output=True,
    accepts=["text"],
    produces=["audio"]
)
class KokoroTTSNode(MultiprocessNode):
    async def process(self, data):
        # Yields multiple audio chunks
        for chunk in self.tts_model.synthesize(data.text):
            yield RuntimeData(audio=chunk)
```

### Register a Class Directly

```python
from remotemedia import register_node_class

class MyMLNode(MultiprocessNode):
    async def process(self, data):
        return self.model.predict(data)

# Register without needing a file path
register_node_class(MyMLNode, category="ml", multi_output=False)
```

### From Configuration File

```python
from remotemedia import register_python_nodes_from_config

# YAML config
register_python_nodes_from_config("nodes.yaml")
```

Example `nodes.yaml`:

```yaml
nodes:
  - file_path: ./nodes/transcription.py
    node_type: WhisperNode
    multi_output: false
    
  - file_path: ./nodes/synthesis.py
    node_type: StreamingTTS
    multi_output: true
    category: tts
```

### From Rust (Built-in Nodes)

For built-in Python nodes known at compile time:

```rust
use remotemedia_python_nodes::{register_python_node, PythonNodeConfig};

// Register by node type (Python class must be importable)
register_python_node(PythonNodeConfig::new("MyPythonNode"));

// Register with full configuration
register_python_node(
    PythonNodeConfig::new("MyTTSNode")
        .with_multi_output(true)
        .with_category("tts")
);
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Python Developer                                         │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ register_python_node("./my_node.py")                │ │
│ └─────────────────────┬───────────────────────────────┘ │
└───────────────────────│─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│ Python Loader (loader.py)                                │
│ ├─ Loads .py file via importlib                         │
│ ├─ Discovers MultiprocessNode subclasses                │
│ └─ Registers in _NODE_REGISTRY (for multiprocess exec)  │
└─────────────────────────────────────────────────────────┘
                        │
                        │ When pipeline runs
                        ▼
┌─────────────────────────────────────────────────────────┐
│ Rust Runtime                                             │
│ ├─ PythonNodesProvider (this crate)                     │
│ ├─ Creates DynamicPythonNodeFactory instances           │
│ └─ Factory creates PythonStreamingNode                  │
│     └─ Uses multiprocess executor                        │
│         └─ Looks up class in Python _NODE_REGISTRY      │
└─────────────────────────────────────────────────────────┘
```

## API Reference

### Python API

| Function | Description |
|----------|-------------|
| `register_python_node(file_path, ...)` | Load and register nodes from a Python file |
| `register_node_class(cls, ...)` | Register an already-imported class |
| `register_python_nodes_from_config(path)` | Register multiple nodes from YAML/JSON config |
| `@streaming_node(...)` | Decorator to declare node metadata |
| `get_registered_nodes()` | List all registered Python nodes |

### Rust API

| Type/Function | Description |
|---------------|-------------|
| `PythonNodeConfig` | Configuration for a Python node |
| `PythonNodeRegistry` | Global registry of Python node configs |
| `register_python_node(config)` | Register a Python node config |
| `PythonNodesProvider` | `NodeProvider` impl for Python nodes |

## Built-in Python Nodes

This crate registers these built-in Python nodes (when their dependencies are available):

| Node Type | Description | Multi-Output |
|-----------|-------------|--------------|
| `WhisperXNode` | WhisperX speech recognition | No |
| `KokoroTTSNode` | Kokoro text-to-speech | Yes |
| `OmniASRNode` | OmniASR speech recognition | No |

## Example: Custom ML Pipeline

```python
# my_nodes/sentiment.py
from remotemedia.core.multiprocessing import MultiprocessNode
from remotemedia.nodes import streaming_node

@streaming_node(
    node_type="SentimentAnalyzer",
    accepts=["text"],
    produces=["text"],
    category="ml"
)
class SentimentAnalyzer(MultiprocessNode):
    async def initialize(self):
        from transformers import pipeline
        self.model = pipeline("sentiment-analysis")
    
    async def process(self, data):
        result = self.model(data.text)[0]
        return RuntimeData(text=f"{result['label']}: {result['score']:.2f}")
```

```python
# main.py
from remotemedia import register_python_node

# Register the custom node
register_python_node("./my_nodes/sentiment.py")

# Now use it in a pipeline manifest
manifest = {
    "nodes": [
        {"id": "analyzer", "node_type": "SentimentAnalyzer"}
    ]
}
```

## See Also

- [Custom Node Registration Guide](../../docs/CUSTOM_NODE_REGISTRATION.md)
- [Node Registration Patterns](../../docs/NODE_REGISTRATION_PATTERNS.md)
- [Python Client Documentation](../../clients/python/README.md)
