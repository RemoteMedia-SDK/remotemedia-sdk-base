from typing import Any, Awaitable
import numpy as np
import numpy.typing as npt

__version__: str

def execute_pipeline(manifest) -> Any: ...
def execute_pipeline_with_input(*args, **kwargs): ...
def execute_pipeline_with_instances(*args, **kwargs): ...
def get_runtime_version(*args, **kwargs): ...
def is_available(*args, **kwargs): ...

# NOTE: Numpy arrays are automatically converted to/from RuntimeData::Numpy
# Just pass numpy arrays directly to execute_pipeline_with_input:
#
# Example:
#     import numpy as np
#     audio_frame = np.zeros(960, dtype=np.float32)
#     result = await execute_pipeline_with_input(manifest, [audio_frame])
#
# The FFI layer automatically:
# 1. Detects numpy arrays and wraps them in RuntimeData::Numpy (zero-copy)
# 2. Passes them through the Rust pipeline without conversion
# 3. Serializes only once at the IPC boundary for iceoryx2 transport
# 4. Deserializes back to RuntimeData::Numpy after IPC
# 5. Converts back to numpy arrays in Python results
#
# This eliminates repeated serialization for streaming audio (20ms frames)
