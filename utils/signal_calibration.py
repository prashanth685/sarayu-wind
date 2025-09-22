from typing import Union
import numpy as np

# Constants
DEFAULT_SCALING = 3.3 / 65535.0
DEFAULT_OFFSET = 32768.0

ArrayLike = Union[np.ndarray, list]

def counts_to_volts(counts: ArrayLike, scaling: float = DEFAULT_SCALING, offset: float = DEFAULT_OFFSET) -> np.ndarray:
    """Convert ADC counts to volts with mid-scale offset applied."""
    arr = np.asarray(counts, dtype=np.float64)
    return (arr - float(offset)) * float(scaling)

def calibrate(volts: ArrayLike, correction: float, gain: float, sensitivity: float) -> np.ndarray:
    """Apply calibration: base = volts * (CorrectionValue * Gain) / Sensitivity."""
    sens = float(sensitivity) if sensitivity is not None else 1.0
    if abs(sens) < 1e-12:
        sens = 1e-12
    return np.asarray(volts, dtype=np.float64) * (float(correction) * float(gain)) / sens

def convert_unit(base: ArrayLike, unit: str, channel_type: str = "Displacement") -> np.ndarray:
    """Convert calibrated base value to target display unit for Displacement.

    Rules:
    - Displacement: mil -> base/25.4, um -> base, mm -> base/1000
    - Other types: return base unchanged
    """
    if (channel_type or "Displacement").lower() != "displacement":
        return np.asarray(base, dtype=np.float64)
    u = (unit or "mil").lower()
    base = np.asarray(base, dtype=np.float64)
    if u == "mil":
        return base / 25.4
    if u == "um":
        return base
    if u == "mm":
        return base / 1000.0
    return base

def tacho_scale(volts: ArrayLike, tacho_index: int) -> np.ndarray:
    """Scale tacho channels consistently: first tacho -> volts/100, others -> volts."""
    v = np.asarray(volts, dtype=np.float64)
    return v / 100.0 if tacho_index == 0 else v

def label_for_unit(unit: str) -> str:
    u = (unit or "mil").lower()
    return f"Amplitude ({u})"
