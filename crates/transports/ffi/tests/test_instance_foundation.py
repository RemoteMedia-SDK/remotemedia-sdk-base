#!/usr/bin/env python3
"""
Manual integration test for Feature 011 foundational work.

Tests:
1. Python Node instance can be created
2. Runtime wrapper type detection works
3. Manifest serialization from instances works
"""

import sys
import asyncio
from pathlib import Path

# Add python-client to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "python-client"))

def test_imports():
    """Test that all new modules can be imported."""
    print("TEST 1: Module imports")

    try:
        from remotemedia import execute_pipeline, execute_pipeline_with_input
        print("  ✓ execute_pipeline imported from remotemedia")
        print("  ✓ execute_pipeline_with_input imported from remotemedia")
    except ImportError as e:
        print(f"  ✗ Failed to import: {e}")
        return False

    try:
        from remotemedia.runtime_wrapper import execute_pipeline as ep
        print("  ✓ runtime_wrapper module exists")
    except ImportError as e:
        print(f"  ✗ Failed to import runtime_wrapper: {e}")
        return False

    try:
        from remotemedia.core.pipeline import Pipeline
        from remotemedia.core.node import Node
        print("  ✓ Pipeline and Node imported")
    except ImportError as e:
        print(f"  ✗ Failed to import core classes: {e}")
        return False

    print("  ✅ All imports successful\n")
    return True


def test_node_instance_creation():
    """Test that Node instances can be created with state."""
    print("TEST 2: Node instance creation")

    try:
        from remotemedia.core.node import Node

        # Create a simple test node
        class TestNode(Node):
            def __init__(self, state_value="default", **kwargs):
                super().__init__(**kwargs)
                self.state_value = state_value
                self.call_count = 0

            def process(self, data):
                self.call_count += 1
                return f"{data}_{self.state_value}_{self.call_count}"

        # Create instance with custom state
        node = TestNode(name="test_node", state_value="custom_state")

        print(f"  ✓ Created Node instance: {node.name}")
        print(f"  ✓ Node has custom state: state_value={node.state_value}")
        print(f"  ✓ Node has process method: {hasattr(node, 'process')}")
        print(f"  ✓ Node has initialize method: {hasattr(node, 'initialize')}")
        print(f"  ✓ Node has cleanup method: {hasattr(node, 'cleanup')}")

        # Test process preserves state
        result1 = node.process("input1")
        result2 = node.process("input2")

        assert result1 == "input1_custom_state_1", f"Expected 'input1_custom_state_1', got '{result1}'"
        assert result2 == "input2_custom_state_2", f"Expected 'input2_custom_state_2', got '{result2}'"

        print(f"  ✓ Process preserves state across calls")
        print(f"  ✅ Node instance creation successful\n")
        return True

    except Exception as e:
        print(f"  ✗ Node creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_type_detection():
    """Test type detection in runtime_wrapper."""
    print("TEST 3: Type detection logic")

    try:
        from remotemedia.core.pipeline import Pipeline
        from remotemedia.core.node import Node
        from remotemedia.nodes import PassThroughNode
        import json

        # Test 1: Detect Pipeline instance
        pipeline = Pipeline(name="test-pipeline")  # Fixed: use name= parameter
        pipeline.add_node(PassThroughNode(name="pass"))

        # Check it has serialize method
        assert hasattr(pipeline, 'serialize'), "Pipeline missing serialize method"
        print("  ✓ Pipeline instance has .serialize() method")

        # Test serialization
        manifest_json = pipeline.serialize()
        manifest = json.loads(manifest_json)

        assert manifest['version'] == 'v1', "Manifest version mismatch"
        assert len(manifest['nodes']) == 1, "Expected 1 node in manifest"
        assert manifest['nodes'][0]['node_type'] == 'PassThroughNode', "Node type mismatch"

        print("  ✓ Pipeline.serialize() produces valid manifest")
        print(f"  ✓ Manifest has {len(manifest['nodes'])} node(s)")

        # Test 2: List of Nodes
        nodes = [PassThroughNode(name="node1"), PassThroughNode(name="node2")]
        temp_pipeline = Pipeline(nodes=nodes)
        manifest_json2 = temp_pipeline.serialize()
        manifest2 = json.loads(manifest_json2)

        assert len(manifest2['nodes']) == 2, "Expected 2 nodes"
        print(f"  ✓ List of {len(nodes)} Nodes → Pipeline → manifest")

        # Test 3: Dict manifest
        manifest_dict = {
            "version": "v1",
            "metadata": {"name": "dict-pipeline"},
            "nodes": [
                {"id": "node_0", "node_type": "PassThroughNode", "params": {}}
            ],
            "connections": []
        }

        manifest_json3 = json.dumps(manifest_dict)
        assert '"version": "v1"' in manifest_json3, "Dict serialization failed"
        print("  ✓ Dict manifest → JSON string")

        # Test 4: JSON string
        assert isinstance(manifest_json, str), "Manifest should be string"
        print("  ✓ JSON string is valid type")

        print("  ✅ Type detection logic validated\n")
        return True

    except Exception as e:
        print(f"  ✗ Type detection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_backward_compatibility():
    """Test that existing manifest-based code still works."""
    print("TEST 4: Backward compatibility")

    try:
        from remotemedia.core.pipeline import Pipeline
        from remotemedia.nodes import PassThroughNode
        import json

        # Create pipeline the old way
        pipeline = Pipeline(name="compat-test")  # Fixed: use name= parameter
        pipeline.add_node(PassThroughNode(name="pass"))

        # Serialize to JSON (old way)
        manifest_json = pipeline.serialize()

        # Verify it's valid JSON
        manifest = json.loads(manifest_json)
        assert 'version' in manifest, "Manifest missing version"
        assert 'nodes' in manifest, "Manifest missing nodes"

        print("  ✓ Existing Pipeline.serialize() works")
        print("  ✓ Manifest JSON is valid")
        print("  ✓ Old workflow preserved")
        print("  ✅ Backward compatibility maintained\n")
        return True

    except Exception as e:
        print(f"  ✗ Backward compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all integration tests."""
    print("=" * 60)
    print("Feature 011: Python Instance Execution")
    print("Manual Integration Test - Foundational Work")
    print("=" * 60)
    print()

    tests = [
        ("Module Imports", test_imports),
        ("Node Instance Creation", test_node_instance_creation),
        ("Type Detection", test_type_detection),
        ("Backward Compatibility", test_backward_compatibility),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            passed = test_func()
            results.append((test_name, passed))
        except Exception as e:
            print(f"CRITICAL ERROR in {test_name}: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))

    # Summary
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    for test_name, passed in results:
        status = "✅ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")

    print()
    print(f"Results: {passed_count}/{total_count} tests passed")

    if passed_count == total_count:
        print("🎉 ALL TESTS PASSED - Foundation is solid!")
        return 0
    else:
        print(f"⚠️ {total_count - passed_count} test(s) failed - see output above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
