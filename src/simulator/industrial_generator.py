"""Industrial equipment telemetry synthesizer and failure mode injector.
Simulates realistic AC induction motors, CNC spindles, and centrifugal pumps with mechanical/electrical noise.
"""
from __future__ import annotations

import math
import random
import time
from typing import Dict, List, Optional

from src.models.schemas import SensorType, TelemetryRecord


class EquipmentProfile:
    """Nominal operational baselines and physical parameters of an industrial machine."""
    def __init__(
        self,
        device_id: str,
        sensor_type: SensorType,
        base_temp: float,
        base_vib: float,
        base_current: float,
        base_voltage: float = 400.0,
        power_factor: float = 0.88,
    ):
        self.device_id = device_id
        self.sensor_type = sensor_type
        self.base_temp = base_temp
        self.base_vib = base_vib
        self.base_current = base_current
        self.base_voltage = base_voltage
        self.power_factor = power_factor


class IndustrialTelemetrySimulator:
    """Generates continuous realistic telemetry streams with optional fault injections."""

    DEFAULT_PROFILES = [
        EquipmentProfile("motor_unit_01", SensorType.MOTOR, base_temp=54.0, base_vib=0.85, base_current=24.5),
        EquipmentProfile("cnc_spindle_02", SensorType.CNC_SPINDLE, base_temp=42.0, base_vib=1.15, base_current=12.0),
        EquipmentProfile("cooling_pump_03", SensorType.PUMP, base_temp=48.0, base_vib=0.65, base_current=18.2),
        EquipmentProfile("substation_tx_04", SensorType.TRANSFORMER, base_temp=62.0, base_vib=0.25, base_current=85.0),
    ]

    def __init__(self, seed: Optional[int] = 42):
        self.rng = random.Random(seed)
        self.profiles: Dict[str, EquipmentProfile] = {p.device_id: p for p in self.DEFAULT_PROFILES}
        self._step_counter: Dict[str, int] = {p.device_id: 0 for p in self.DEFAULT_PROFILES}
        # Injected faults: {device_id: fault_type}
        self._active_faults: Dict[str, str] = {}

    def inject_fault(self, device_id: str, fault_type: str) -> None:
        """Injects a continuous or persistent failure mode into a device."""
        self._active_faults[device_id] = fault_type

    def clear_fault(self, device_id: str) -> None:
        """Restores nominal operation for a device."""
        if device_id in self._active_faults:
            del self._active_faults[device_id]

    def generate_reading(self, device_id: str, timestamp_ms: Optional[int] = None) -> TelemetryRecord:
        """Synthesizes the next telemetry record for a device."""
        profile = self.profiles.get(device_id)
        if not profile:
            profile = EquipmentProfile(device_id, SensorType.MOTOR, 50.0, 0.8, 20.0)
            self.profiles[device_id] = profile
            self._step_counter[device_id] = 0

        t_ms = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        step = self._step_counter.get(device_id, 0)
        self._step_counter[device_id] = step + 1

        # Periodic sinusoidal machine load cycle (e.g. duty cycle harmonic)
        cycle_wave = math.sin(step * 0.1) * 0.5

        # Gaussian white noise
        t_noise = self.rng.gauss(0, 0.3)
        v_noise = abs(self.rng.gauss(0, 0.08))
        c_noise = self.rng.gauss(0, 0.4)

        temp = profile.base_temp + cycle_wave * 1.5 + t_noise
        vib_rms = max(0.05, profile.base_vib + cycle_wave * 0.15 + v_noise)
        vib_kurt = 3.0 + abs(self.rng.gauss(0, 0.2))
        curr = max(0.5, profile.base_current + cycle_wave * 2.0 + c_noise)
        volt = profile.base_voltage + self.rng.gauss(0, 1.2)
        pf = min(1.0, max(0.5, profile.power_factor + self.rng.gauss(0, 0.01)))

        # Apply active fault modes if present
        fault = self._active_faults.get(device_id)
        if fault == "BEARING_FATIGUE":
            # Acute vibration acceleration + high kurtosis peaks
            vib_rms += self.rng.uniform(3.5, 6.0)
            vib_kurt += self.rng.uniform(4.0, 9.0)
            temp += self.rng.uniform(6.0, 14.0)
        elif fault == "THERMAL_RUNAWAY":
            # Cumulative thermal drift
            temp += min(45.0, step * 0.8 + 25.0)
        elif fault == "PHASE_IMBALANCE":
            # Heavy current surge + voltage sag
            curr += self.rng.uniform(22.0, 38.0)
            volt -= self.rng.uniform(15.0, 30.0)
            pf -= 0.15
        elif fault == "CORRELATED_SEIZURE":
            # Catastrophic simultaneous multi-sensor failure
            vib_rms += 5.5
            curr += 35.0
            temp += 30.0
            vib_kurt += 8.0
        elif fault == "SENSOR_GLITCH":
            # Broken lead drop to 0
            temp = 0.0
            vib_rms = 0.0

        return TelemetryRecord(
            device_id=device_id,
            timestamp_ms=t_ms,
            sensor_type=profile.sensor_type,
            temperature=round(temp, 2),
            vibration_rms=round(vib_rms, 3),
            vibration_kurtosis=round(vib_kurt, 2),
            current=round(curr, 2),
            voltage=round(volt, 1),
            power_factor=round(pf, 3),
            frequency=50.0,
            metadata={"simulated": "true", "fault": fault or "none"},
        )

    def generate_batch(self, count_per_device: int = 10, interval_ms: int = 100) -> List[TelemetryRecord]:
        """Synthesizes a batch of sequential readings across all configured devices."""
        records: List[TelemetryRecord] = []
        now_ms = int(time.time() * 1000)

        for dev_id in self.profiles:
            for i in range(count_per_device):
                t = now_ms - (count_per_device - i) * interval_ms
                records.append(self.generate_reading(dev_id, timestamp_ms=t))

        return records
