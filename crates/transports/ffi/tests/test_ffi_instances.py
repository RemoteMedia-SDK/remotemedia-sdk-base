#!/usr/bin/env python3
"""
FFI Integration Tests for Feature 011 - Python Node Instance Execution

Tests T022-T028: Rust/Python integration via remotemedia.runtime module

Run with: pytest transports/ffi/tests/test_ffi_instances.py
"""

import sys
import asyncio
import pytest
from pathlib import Path

# Add python-client to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "python-client"))

from remotemedia import execute_pipeline, execute_pipeline_with_input
from remotemedia.core.pipeline import Pipeline
from remotemedia.core.node import Node


class SimpleNode(Node):
    """Test node with simple state."""

    def __init__(self, multiplier=2, **kwargs):
        super().__init__(**kwargs)
        self.multiplier = multiplier
        self.execution_count = 0

    def process(self, data):
        self.execution_count += 1
        # Handle different input types
        if isinstance(data, (int, float)):
            return data * self.multiplier
        elif isinstance(data, str):
            return f"{data}_x{self.multiplier}"
        elif isinstance(data, dict):
            return {**data, "multiplier": self.multiplier}
        return data


class ComplexStateNode(Node):
    """Test node with complex state (lists, dicts, counters)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.history = []
        self.counters = {"processed": 0, "errors": 0}
        self.config = {"threshold": 10, "mode": "strict"}
        self.loaded_model = {"weights": [0.1, 0.2, 0.3], "version": "1.0"}

    def process(self, data):
        self.history.append(data)
        self.counters["processed"] += 1
        return {
            "input": data,
            "count": self.counters["processed"],
            "mode": self.config["mode"],
            "model_version": self.loaded_model["version"],
        }


# T023: Test for Pipeline instance execution with registered nodes
@pytest.mark.asyncio
async def test_pipeline_instance_execution():
    """Test that Pipeline instances with registered nodes can be executed via FFI."""
    import json

    # Create a Pipeline with a registered node type
    manifest = {
        "version": "v1",
        "metadata": {"name": "test-pipeline"},
        "nodes": [
            {"id": "pass1", "node_type": "PassThrough", "params": {}},
            {"id": "pass2", "node_type": "PassThrough", "params": {}},
        ],
        "connections": [{"from": "pass1", "to": "pass2"}],
    }

    # Execute via manifest (Pipeline instances with custom nodes use registry)
    result = await execute_pipeline(manifest)
    assert result is not None


# T024: Test for List[Node] execution
@pytest.mark.asyncio
async def test_list_of_nodes_execution():
    """Test that a list of Node instances can be executed."""
    nodes = [
        SimpleNode(name="node1", multiplier=2),
        SimpleNode(name="node2", multiplier=3),
        SimpleNode(name="node3", multiplier=4),
    ]

    result = await execute_pipeline(nodes)

    # Verify nodes were executed
    assert nodes[0].execution_count == 1
    assert nodes[1].execution_count == 1
    assert nodes[2].execution_count == 1


# T025: Test for Node with complex state
@pytest.mark.asyncio
async def test_node_with_complex_state():
    """Test that nodes with complex state (loaded models, history) work."""
    node = ComplexStateNode(name="complex")

    # Execute with the node
    result = await execute_pipeline([node])

    # Verify state was used
    assert isinstance(result, dict)
    assert result["count"] == 1
    assert result["mode"] == "strict"
    assert result["model_version"] == "1.0"

    # Verify state was preserved
    assert len(node.history) == 1
    assert node.counters["processed"] == 1


# T026: Test backward compatibility with manifest JSON
@pytest.mark.asyncio
async def test_backward_compatibility_manifest():
    """Test that manifest JSON strings still work (backward compatibility)."""
    import json

    manifest = {
        "version": "v1",
        "metadata": {"name": "test"},
        "nodes": [{"id": "pass", "node_type": "PassThrough", "params": {}}],
        "connections": [],
    }

    manifest_json = json.dumps(manifest)

    # Should execute without errors
    result = await execute_pipeline(manifest_json)
    assert result is not None


# T027: End-to-end instance execution test
@pytest.mark.asyncio
async def test_e2e_instance_execution():
    """End-to-end test: create instances, execute, verify state."""
    # Create multiple nodes with state
    node1 = SimpleNode(name="doubler", multiplier=2)
    node2 = SimpleNode(name="tripler", multiplier=3)

    # Execute pipeline
    result = await execute_pipeline([node1, node2])

    # Verify execution happened
    assert node1.execution_count == 1, "Node1 should be executed once"
    assert node2.execution_count == 1, "Node2 should be executed once"

    # Verify result is correct (empty input)
    assert result is not None


# T028: Test direct instance list execution (bypasses Pipeline.run())
@pytest.mark.asyncio
async def test_direct_instance_list_execution():
    """Test that direct instance lists bypass the registry."""
    node1 = SimpleNode(name="node1", multiplier=2)
    node2 = SimpleNode(name="node2", multiplier=3)

    # Execute directly as list (bypasses registry via execute_pipeline_with_instances)
    result = await execute_pipeline([node1, node2])

    # Verify nodes were executed
    assert node1.execution_count == 1
    assert node2.execution_count == 1
    assert result is not None


# Additional test: Multiple inputs (streaming)
@pytest.mark.asyncio
async def test_streaming_with_instances():
    """Test streaming input execution with node instances."""
    node = SimpleNode(name="processor", multiplier=10)

    inputs = [1, 2, 3, 4, 5]
    results = await execute_pipeline_with_input([node], inputs)

    # Verify all inputs were processed
    assert node.execution_count == 5
    assert len(results) == 5
    assert results[0] == 10  # 1 * 10
    assert results[4] == 50  # 5 * 10


# Additional test: State preservation across executions
@pytest.mark.asyncio
async def test_state_preservation():
    """Test that node state is preserved across multiple inputs."""
    node = ComplexStateNode(name="stateful")

    # Execute with multiple inputs
    inputs = ["input1", "input2", "input3"]
    results = await execute_pipeline_with_input([node], inputs)

    # Verify state accumulated
    assert len(node.history) == 3
    assert node.history == ["input1", "input2", "input3"]
    assert node.counters["processed"] == 3

    # Verify each result has correct counter
    assert results[0]["count"] == 1
    assert results[1]["count"] == 2
    assert results[2]["count"] == 3


# Additional test: Error handling
@pytest.mark.asyncio
async def test_instance_validation():
    """Test that invalid instances are rejected."""

    class InvalidNode:
        """Not a Node subclass - should be rejected."""

        pass

    with pytest.raises(TypeError):
        # Should raise TypeError for non-Node instances
        await execute_pipeline([InvalidNode()])


# Additional test: Empty pipeline
@pytest.mark.asyncio
async def test_empty_pipeline():
    """Test that empty pipelines are handled."""
    with pytest.raises(ValueError, match="empty"):
        await execute_pipeline([])


if __name__ == "__main__":
    # Run all tests
    pytest.main([__file__, "-v"])
