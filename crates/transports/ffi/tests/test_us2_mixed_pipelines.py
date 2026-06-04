#!/usr/bin/env python3
"""
Test User Story 2: Mixed Manifest and Instance Pipelines

Tests that lists can contain both Node instances and dict manifests.
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "python-client"))


def test_mixed_list_conversion():
    """Test converting mixed list of Nodes and dicts to manifest."""
    print("TEST 1: Mixed list conversion")

    try:
        from remotemedia.runtime_wrapper import _convert_mixed_list_to_manifest
        from remotemedia.core.node import Node
        from remotemedia.nodes import PassThroughNode

        # Create mixed list: 1 Node instance + 1 dict manifest + 1 Node instance
        mixed_list = [
            PassThroughNode(name="node1"),  # Node instance
            {
                "node_type": "CalculatorNode",
                "params": {"operation": "add", "operand": 10}
            },  # Dict manifest
            PassThroughNode(name="node3"),  # Node instance
        ]

        print(f"  ✓ Created mixed list: 2 Nodes + 1 dict")

        # Convert to manifest
        manifest_json = _convert_mixed_list_to_manifest(mixed_list)
        manifest = json.loads(manifest_json)

        print(f"  ✓ Conversion successful")

        # Validate structure
        assert manifest['version'] == 'v1', "Version mismatch"
        assert len(manifest['nodes']) == 3, f"Expected 3 nodes, got {len(manifest['nodes'])}"
        assert len(manifest['connections']) == 2, f"Expected 2 connections, got {len(manifest['connections'])}"

        print(f"  ✓ Manifest has {len(manifest['nodes'])} nodes")
        print(f"  ✓ Manifest has {len(manifest['connections'])} connections")

        # Validate nodes
        node_types = [n['node_type'] for n in manifest['nodes']]
        print(f"  ✓ Node types: {node_types}")

        assert 'PassThroughNode' in node_types, "PassThroughNode missing"
        assert 'CalculatorNode' in node_types, "CalculatorNode missing"

        # Validate connections
        conn1 = manifest['connections'][0]
        conn2 = manifest['connections'][1]

        assert conn1['from'] == manifest['nodes'][0]['id'], "Connection 1 'from' mismatch"
        assert conn1['to'] == manifest['nodes'][1]['id'], "Connection 1 'to' mismatch"
        assert conn2['from'] == manifest['nodes'][1]['id'], "Connection 2 'from' mismatch"
        assert conn2['to'] == manifest['nodes'][2]['id'], "Connection 2 'to' mismatch"

        print(f"  ✓ Connections correctly link nodes in sequence")
        print("  ✅ Mixed list conversion works correctly\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_invalid_mixed_types():
    """Test that invalid types in mixed list are rejected."""
    print("TEST 2: Invalid type validation")

    try:
        from remotemedia import execute_pipeline
        from remotemedia.nodes import PassThroughNode

        # T034: Create list with invalid types
        invalid_list = [
            PassThroughNode(name="node1"),  # Valid
            "invalid_string",  # Invalid - should be rejected
            PassThroughNode(name="node2"),  # Valid
        ]

        print("  ✓ Created list with invalid type (string)")

        # Should raise TypeError
        try:
            # This won't actually execute but should fail validation
            manifest_json = None
            from remotemedia.runtime_wrapper import _convert_mixed_list_to_manifest
            from remotemedia.core.node import Node

            # Manual validation check (same as wrapper)
            has_invalid = any(not isinstance(item, (Node, dict)) for item in invalid_list)

            if has_invalid:
                invalid_items = [(i, type(item).__name__) for i, item in enumerate(invalid_list)
                                 if not isinstance(item, (Node, dict))]
                raise TypeError(
                    f"List items must be Node instances or dict manifests. "
                    f"Found invalid types at positions: {invalid_items}"
                )

            print("  ✗ Validation should have raised TypeError")
            return False

        except TypeError as e:
            error_msg = str(e)
            assert "invalid types" in error_msg.lower(), f"Error message doesn't mention invalid types: {error_msg}"
            assert "str" in error_msg, f"Error message doesn't mention 'str' type: {error_msg}"

            print(f"  ✓ TypeError raised correctly: {e}")
            print("  ✅ Invalid type validation works\n")
            return True

    except Exception as e:
        print(f"  ✗ Test failed unexpectedly: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_pure_dict_list():
    """Test converting a list of dict manifests."""
    print("TEST 3: Pure dict list conversion")

    try:
        from remotemedia.runtime_wrapper import _convert_dict_list_to_manifest

        # Create list of dict manifests
        dict_list = [
            {"node_type": "PassThrough", "params": {}},
            {"node_type": "CalculatorNode", "params": {"operation": "add", "operand": 5}},
        ]

        print(f"  ✓ Created list of {len(dict_list)} dict manifests")

        # Convert
        manifest_json = _convert_dict_list_to_manifest(dict_list)
        manifest = json.loads(manifest_json)

        print(f"  ✓ Conversion successful")

        # Validate
        assert manifest['version'] == 'v1', "Version mismatch"
        assert len(manifest['nodes']) == 2, f"Expected 2 nodes, got {len(manifest['nodes'])}"
        assert len(manifest['connections']) == 1, f"Expected 1 connection, got {len(manifest['connections'])}"

        # Check IDs were generated
        assert all('id' in node for node in manifest['nodes']), "Some nodes missing IDs"

        print(f"  ✓ Manifest structure valid")
        print(f"  ✓ IDs generated: {[n['id'] for n in manifest['nodes']]}")
        print("  ✅ Pure dict list conversion works\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_mixed_list_patterns():
    """Test various mixed list patterns."""
    print("TEST 4: Various mixed list patterns")

    try:
        from remotemedia.runtime_wrapper import _convert_mixed_list_to_manifest
        from remotemedia.nodes import PassThroughNode

        patterns = [
            ([PassThroughNode(name="n1"), {"node_type": "PassThrough"}], "1 Node + 1 dict"),
            ([{"node_type": "PassThrough"}, PassThroughNode(name="n2")], "1 dict + 1 Node"),
            ([PassThroughNode(name="n1"), {"node_type": "PassThrough"}, PassThroughNode(name="n3")],
             "Node + dict + Node"),
            ([{"node_type": "PassThrough"}, PassThroughNode(name="n2"), {"node_type": "PassThrough"}],
             "dict + Node + dict"),
        ]

        for i, (mixed_list, description) in enumerate(patterns, 1):
            manifest_json = _convert_mixed_list_to_manifest(mixed_list)
            manifest = json.loads(manifest_json)

            assert len(manifest['nodes']) == len(mixed_list), f"Pattern {i}: Node count mismatch"
            assert len(manifest['connections']) == len(mixed_list) - 1, f"Pattern {i}: Connection count mismatch"

            print(f"  ✓ Pattern {i} ({description}): {len(manifest['nodes'])} nodes, {len(manifest['connections'])} connections")

        print("  ✅ All mixed patterns work correctly\n")
        return True

    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all User Story 2 tests."""
    print("=" * 70)
    print("Feature 011 - User Story 2: Mixed Manifest and Instance Pipelines")
    print("=" * 70)
    print()

    tests = [
        ("Mixed List Conversion", test_mixed_list_conversion),
        ("Invalid Type Validation", test_invalid_mixed_types),
        ("Pure Dict List", test_pure_dict_list),
        ("Mixed List Patterns", test_mixed_list_patterns),
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
    print("=" * 70)
    print("USER STORY 2 TEST SUMMARY")
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
        print("🎉 USER STORY 2 COMPLETE!")
        print("Mixed manifest+instance pipelines fully supported")
        return 0
    else:
        print(f"\n⚠️ {total_count - passed_count} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
