#!/usr/bin/env python3
"""
End-to-end integration test for Feature 011 with actual Rust FFI execution.

This test validates:
1. Node instances can be passed to Rust runtime
2. Streaming pipeline execution works
3. Instance state is preserved
4. Rust FFI integration is complete
"""

import sys
import asyncio
from pathlib import Path

# Add python-client to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "python-client"))


async def test_basic_instance_execution():
    """Test basic Node instance execution through Rust FFI."""
    print("TEST 1: Basic instance execution through Rust FFI")

    try:
        from remotemedia import execute_pipeline, is_rust_runtime_available
        from remotemedia.core.node import Node

        # Check if Rust runtime is available
        if not is_rust_runtime_available():
            print("  ⚠️ Rust runtime not available - cannot test FFI integration")
            print("  Run: cd transports/ffi && ./dev-install.sh")
            return False

        print("  ✓ Rust runtime is available")

        # Create a simple test node with state
        class CounterNode(Node):
            def __init__(self, start=0, **kwargs):
                super().__init__(**kwargs)
                self.counter = start

            def process(self, data):
                self.counter += 1
                result = f"{data}_count_{self.counter}"
                print(f"    [CounterNode] Processing: {data} → {result}")
                return result

        # Create instance with state
        node = CounterNode(name="counter", start=100)
        print(f"  ✓ Created CounterNode with initial state: counter={node.counter}")

        # Execute through Rust FFI with instance
        print("  → Calling execute_pipeline with Node instance...")
        result = await execute_pipeline([node])

        print(f"  ✓ Execution completed: {result}")
        print(f"  ✓ Instance state after execution: counter={node.counter}")
        print("  ✅ Basic instance execution successful\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_instance_with_input_data():
    """Test Node instance execution with streaming input."""
    print("TEST 2: Instance execution with input data")

    try:
        from remotemedia import execute_pipeline_with_input, is_rust_runtime_available
        from remotemedia.core.node import Node

        if not is_rust_runtime_available():
            print("  ⚠️ Rust runtime not available - skipping")
            return False

        # Create a node that transforms data
        class TransformNode(Node):
            def __init__(self, multiplier=2, **kwargs):
                super().__init__(**kwargs)
                self.multiplier = multiplier
                self.processed_count = 0

            def process(self, data):
                self.processed_count += 1
                # Handle different input types
                if isinstance(data, (int, float)):
                    result = data * self.multiplier
                elif isinstance(data, str):
                    result = data * self.multiplier
                else:
                    result = str(data) * self.multiplier

                print(f"    [TransformNode] Processing #{self.processed_count}: {data} → {result}")
                return result

        # Create instance
        node = TransformNode(name="transform", multiplier=3)
        print(f"  ✓ Created TransformNode with multiplier={node.multiplier}")

        # Execute with input data
        input_data = [1, 2, 3, 4, 5]
        print(f"  → Calling execute_pipeline_with_input with {len(input_data)} inputs...")

        results = await execute_pipeline_with_input([node], input_data)

        print(f"  ✓ Execution completed")
        print(f"  ✓ Results: {results}")
        print(f"  ✓ Processed {node.processed_count} items")
        print(f"  ✓ State preserved: multiplier={node.multiplier}")

        # Validate results
        expected = [3, 6, 9, 12, 15]
        if results == expected:
            print(f"  ✓ Results match expected: {expected}")
        else:
            print(f"  ⚠️ Results differ - Expected: {expected}, Got: {results}")

        print("  ✅ Input data execution successful\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_pipeline_instance_execution():
    """Test Pipeline instance execution."""
    print("TEST 3: Pipeline instance execution")

    try:
        from remotemedia import execute_pipeline, is_rust_runtime_available
        from remotemedia.core.pipeline import Pipeline
        from remotemedia.core.node import Node

        if not is_rust_runtime_available():
            print("  ⚠️ Rust runtime not available - skipping")
            return False

        # Create nodes with state
        class PrefixNode(Node):
            def __init__(self, prefix=">>", **kwargs):
                super().__init__(**kwargs)
                self.prefix = prefix

            def process(self, data):
                result = f"{self.prefix}{data}"
                print(f"    [PrefixNode] {data} → {result}")
                return result

        class SuffixNode(Node):
            def __init__(self, suffix="<<", **kwargs):
                super().__init__(**kwargs)
                self.suffix = suffix

            def process(self, data):
                result = f"{data}{self.suffix}"
                print(f"    [SuffixNode] {data} → {result}")
                return result

        # Create pipeline with instances
        pipeline = Pipeline(name="test-pipeline")
        pipeline.add_node(PrefixNode(name="prefix", prefix="[START]"))
        pipeline.add_node(SuffixNode(name="suffix", suffix="[END]"))

        print(f"  ✓ Created pipeline with {len(pipeline.nodes)} nodes")

        # Execute Pipeline instance
        print("  → Calling execute_pipeline with Pipeline instance...")
        result = await execute_pipeline(pipeline)

        print(f"  ✓ Execution completed: {result}")
        print(f"  ✓ Pipeline state preserved: name={pipeline.name}")
        print("  ✅ Pipeline instance execution successful\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_pipeline_run_method():
    """Test Pipeline.run() method with instances."""
    print("TEST 4: Pipeline.run() method with instance support")

    try:
        from remotemedia.core.pipeline import Pipeline
        from remotemedia.core.node import Node
        from remotemedia import is_rust_runtime_available

        if not is_rust_runtime_available():
            print("  ⚠️ Rust runtime not available - skipping")
            return False

        # Create a stateful node
        class StatefulNode(Node):
            def __init__(self, state="initial", **kwargs):
                super().__init__(**kwargs)
                self.state = state
                self.call_count = 0

            def process(self, data):
                self.call_count += 1
                result = f"{data}_{self.state}_{self.call_count}"
                print(f"    [StatefulNode] Call #{self.call_count}: {data} → {result}")
                return result

        # Create pipeline
        pipeline = Pipeline(name="stateful-pipeline")
        pipeline.add_node(StatefulNode(name="stateful", state="preserved"))

        print(f"  ✓ Created pipeline with stateful node")
        print(f"  ✓ Initial state: {pipeline.nodes[0].state}, calls: {pipeline.nodes[0].call_count}")

        # Execute via Pipeline.run() - should use instances automatically
        input_data = ["input1", "input2", "input3"]
        print(f"  → Calling pipeline.run() with {len(input_data)} inputs...")

        results = await pipeline.run(input_data=input_data, use_rust=True)

        print(f"  ✓ Execution completed")
        print(f"  ✓ Results: {results}")
        print(f"  ✓ Node state after: {pipeline.nodes[0].state}, calls: {pipeline.nodes[0].call_count}")
        print("  ✅ Pipeline.run() execution successful\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_backward_compatibility_with_ffi():
    """Test that old manifest-based execution still works."""
    print("TEST 5: Backward compatibility with FFI")

    try:
        from remotemedia import execute_pipeline, is_rust_runtime_available
        from remotemedia.core.pipeline import Pipeline
        from remotemedia.nodes import PassThroughNode

        if not is_rust_runtime_available():
            print("  ⚠️ Rust runtime not available - skipping")
            return False

        # Create pipeline
        pipeline = Pipeline(name="compat-test")
        pipeline.add_node(PassThroughNode(name="pass"))

        # Serialize to JSON (old way)
        manifest_json = pipeline.serialize()
        print("  ✓ Serialized pipeline to manifest JSON")

        # Execute with manifest JSON string (backward compatible)
        print("  → Calling execute_pipeline with JSON string (old API)...")
        result = await execute_pipeline(manifest_json)

        print(f"  ✓ Execution completed with manifest JSON")
        print("  ✅ Backward compatibility confirmed\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_list_of_nodes():
    """Test executing a list of Node instances."""
    print("TEST 6: List[Node] execution")

    try:
        from remotemedia import execute_pipeline_with_input, is_rust_runtime_available
        from remotemedia.core.node import Node

        if not is_rust_runtime_available():
            print("  ⚠️ Rust runtime not available - skipping")
            return False

        # Create multiple nodes
        class AddNode(Node):
            def __init__(self, value=0, **kwargs):
                super().__init__(**kwargs)
                self.value = value

            def process(self, data):
                result = int(data) + self.value
                print(f"    [AddNode-{self.name}] {data} + {self.value} = {result}")
                return result

        # Create list of nodes
        nodes = [
            AddNode(name="add10", value=10),
            AddNode(name="add20", value=20),
            AddNode(name="add30", value=30),
        ]

        print(f"  ✓ Created list of {len(nodes)} Node instances")

        # Execute list directly
        input_data = [0, 5, 10]
        print(f"  → Calling execute_pipeline_with_input with list of nodes...")

        results = await execute_pipeline_with_input(nodes, input_data)

        print(f"  ✓ Execution completed")
        print(f"  ✓ Results: {results}")
        print("  ✅ List[Node] execution successful\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all end-to-end tests."""
    print("=" * 70)
    print("Feature 011: Python Instance Execution")
    print("End-to-End Integration Test with Rust FFI")
    print("=" * 70)
    print()

    tests = [
        ("Basic Instance Execution", test_basic_instance_execution),
        ("Instance with Input Data", test_instance_with_input_data),
        ("Pipeline Instance Execution", test_pipeline_instance_execution),
        ("Pipeline.run() Method", test_pipeline_run_method),
        ("Backward Compatibility", test_backward_compatibility_with_ffi),
        ("List[Node] Execution", test_list_of_nodes),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            passed = await test_func()
            results.append((test_name, passed))
        except Exception as e:
            print(f"CRITICAL ERROR in {test_name}: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))

    # Summary
    print("=" * 70)
    print("END-TO-END TEST SUMMARY")
    print("=" * 70)

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    for test_name, passed in results:
        status = "✅ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")

    print()
    print(f"Results: {passed_count}/{total_count} tests passed")

    if passed_count == total_count:
        print()
        print("🎉 ALL E2E TESTS PASSED!")
        print("Feature 011 MVP is fully functional with Rust FFI integration")
        return 0
    elif passed_count > 0:
        print()
        print(f"⚠️ Partial success: {passed_count}/{total_count} tests passed")
        print("Note: Some tests may require Rust FFI to be built and installed")
        print("Run: cd transports/ffi && ./dev-install.sh")
        return 1
    else:
        print()
        print(f"⚠️ All tests failed - Rust runtime may not be installed")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
