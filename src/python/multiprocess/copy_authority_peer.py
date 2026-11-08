"""Real Python peer used only by the Rust reporting regression test."""
import ctypes
import json
import os
import sys
from types import SimpleNamespace
sys.path.insert(0, sys.argv[1])
import numpy as np
from remotemedia.core.multiprocessing.legacy_copy_client import LegacyCopyContext, LegacyCopyError
from remotemedia.core.multiprocessing.legacy_copies import copy_operation
from remotemedia.core.multiprocessing.legacy_receive import snapshot_sample
from remotemedia.core.multiprocessing.legacy_wire import audio_segments
from remotemedia.core.multiprocessing.data import RuntimeData

context = LegacyCopyContext(json.load(sys.stdin), 'node', 'session')
mode = sys.argv[2]
if mode == 'abandon':
    context.begin('private_ipc_ingress', 77)
    os._exit(0)
storage = (ctypes.c_uint8 * 4)(1, 2, 3, 4)
sample = SimpleNamespace(payload=lambda: SimpleNamespace(
    contained_type=ctypes.c_uint8, len=lambda: 4, as_ptr=lambda: ctypes.addressof(storage)))
if mode == 'strict':
    ctypes.string_at = lambda *args: (_ for _ in ()).throw(AssertionError('unauthorized read'))
    try:
        snapshot_sample(sample, copies=context)
    except LegacyCopyError:
        os._exit(0)
    raise AssertionError('strict snapshot succeeded')
assert snapshot_sample(sample, copies=context) == b'\x01\x02\x03\x04'
source = np.arange(12, dtype='>f8')[::2]
parts = audio_segments(RuntimeData.audio(source, 24000), copies=context)
assert parts[-1].nbytes == 24
try:
    with copy_operation(context, 'borrowed_input', 5):
        raise ValueError('injected copy failure')
except ValueError:
    pass
context.close()
