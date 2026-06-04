"""
Test numpy <-> audio dictionary conversion functions.

This test demonstrates the new FFI functions for converting between
numpy arrays and audio RuntimeData dictionaries.
"""

import numpy as np
import pytest

# Import from the FFI runtime module
from remotemedia.runtime import numpy_to_audio_dict, audio_dict_to_numpy


def test_numpy_to_audio_dict_mono():
    """Test converting mono numpy array to audio dict."""
    # Create mono audio data
    audio_samples = np.array([0.0, 0.5, 1.0, 0.5, 0.0], dtype=np.float32)
    
    # Convert to audio dictionary
    audio_dict = numpy_to_audio_dict(audio_samples, sample_rate=48000, channels=1)
    
    # Verify structure
    assert audio_dict["type"] == "audio"
    assert audio_dict["sample_rate"] == 48000
    assert audio_dict["channels"] == 1
    assert len(audio_dict["samples"]) == 5
    
    # Verify data (approximate comparison for floats)
    samples = audio_dict["samples"]
    assert abs(samples[0] - 0.0) < 1e-6
    assert abs(samples[1] - 0.5) < 1e-6
    assert abs(samples[2] - 1.0) < 1e-6


def test_numpy_to_audio_dict_stereo():
    """Test converting stereo numpy array to audio dict."""
    # Create stereo audio data (interleaved: L, R, L, R, ...)
    audio_samples = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6], dtype=np.float32)
    
    # Convert to audio dictionary
    audio_dict = numpy_to_audio_dict(audio_samples, sample_rate=44100, channels=2)
    
    # Verify structure
    assert audio_dict["type"] == "audio"
    assert audio_dict["sample_rate"] == 44100
    assert audio_dict["channels"] == 2
    assert len(audio_dict["samples"]) == 6


def test_audio_dict_to_numpy_mono():
    """Test converting mono audio dict to numpy array."""
    # Create audio dictionary
    audio_dict = {
        "type": "audio",
        "samples": [0.0, 0.1, 0.2, 0.3, 0.4],
        "sample_rate": 48000,
        "channels": 1,
    }
    
    # Convert to numpy
    numpy_array = audio_dict_to_numpy(audio_dict)
    
    # Verify shape and dtype
    assert numpy_array.dtype == np.float32
    assert numpy_array.shape == (5,)  # 1D for mono
    
    # Verify data
    assert numpy_array[0] == pytest.approx(0.0)
    assert numpy_array[1] == pytest.approx(0.1)
    assert numpy_array[4] == pytest.approx(0.4)


def test_audio_dict_to_numpy_stereo():
    """Test converting stereo audio dict to numpy array."""
    # Create stereo audio dictionary (6 samples = 3 frames × 2 channels)
    audio_dict = {
        "type": "audio",
        "samples": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
        "sample_rate": 44100,
        "channels": 2,
    }
    
    # Convert to numpy
    numpy_array = audio_dict_to_numpy(audio_dict)
    
    # Verify shape and dtype
    assert numpy_array.dtype == np.float32
    assert numpy_array.shape == (3, 2)  # 2D for stereo: (frames, channels)
    
    # Verify data (interleaved format becomes 2D)
    assert numpy_array[0, 0] == pytest.approx(0.1)  # Frame 0, Left
    assert numpy_array[0, 1] == pytest.approx(0.2)  # Frame 0, Right
    assert numpy_array[1, 0] == pytest.approx(0.3)  # Frame 1, Left
    assert numpy_array[1, 1] == pytest.approx(0.4)  # Frame 1, Right


def test_roundtrip_mono():
    """Test roundtrip conversion: numpy -> dict -> numpy."""
    # Original numpy array
    original = np.array([0.0, 0.25, 0.5, 0.75, 1.0], dtype=np.float32)
    
    # Convert to dict
    audio_dict = numpy_to_audio_dict(original, sample_rate=16000, channels=1)
    
    # Convert back to numpy
    reconstructed = audio_dict_to_numpy(audio_dict)
    
    # Verify they match
    assert reconstructed.dtype == np.float32
    assert reconstructed.shape == original.shape
    np.testing.assert_array_almost_equal(reconstructed, original, decimal=6)


def test_roundtrip_stereo():
    """Test roundtrip conversion for stereo: numpy -> dict -> numpy."""
    # Original stereo numpy array (8 samples = 4 frames)
    original = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8], dtype=np.float32)
    
    # Convert to dict
    audio_dict = numpy_to_audio_dict(original, sample_rate=48000, channels=2)
    
    # Convert back to numpy
    reconstructed = audio_dict_to_numpy(audio_dict)
    
    # Verify shape matches expected 2D format
    assert reconstructed.dtype == np.float32
    assert reconstructed.shape == (4, 2)  # 4 frames, 2 channels
    
    # Flatten and compare (since output is 2D)
    np.testing.assert_array_almost_equal(reconstructed.flatten(), original, decimal=6)


def test_usage_with_pipeline_format():
    """Test that the format is compatible with pipeline execution."""
    # This demonstrates how the functions integrate with the pipeline API
    
    # 1. Create audio data from numpy
    audio_data = np.sin(2 * np.pi * 440 * np.linspace(0, 1, 48000, dtype=np.float32))
    
    # 2. Convert to pipeline-compatible format
    audio_dict = numpy_to_audio_dict(audio_data, sample_rate=48000, channels=1)
    
    # 3. Verify it has the expected pipeline format
    assert "type" in audio_dict
    assert "samples" in audio_dict
    assert "sample_rate" in audio_dict
    assert "channels" in audio_dict
    
    # 4. Convert pipeline output back to numpy for processing
    numpy_output = audio_dict_to_numpy(audio_dict)
    
    # 5. Verify we can do numpy operations on it
    assert numpy_output.shape == (48000,)
    assert np.max(numpy_output) <= 1.0
    assert np.min(numpy_output) >= -1.0


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])

