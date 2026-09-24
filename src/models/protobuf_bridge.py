"""Protocol Buffer binary wire-format and JSON bridge for TelemetryRecord.
Implements pure-Python wire-format encoding/decoding without requiring external protoc binary.
"""
from __future__ import annotations

import json
import struct

from src.models.schemas import SensorType, TelemetryRecord


def _encode_varint(value: int) -> bytes:
    """Encodes an integer into proto varint format."""
    pieces = []
    while value > 0x7F:
        pieces.append((value & 0x7F) | 0x80)
        value >>= 7
    pieces.append(value & 0x7F)
    return bytes(pieces)


def _decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Decodes a proto varint starting at offset, returning (value, new_offset)."""
    result = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ValueError("Truncated varint in protobuf stream")
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return result, offset


def encode_protobuf_telemetry(record: TelemetryRecord) -> bytes:
    """Serializes TelemetryRecord to binary Protobuf 3 wire-format."""
    buf = bytearray()

    # Field 1: device_id (string, wire_type=2)
    dev_bytes = record.device_id.encode("utf-8")
    buf.extend(_encode_varint((1 << 3) | 2))
    buf.extend(_encode_varint(len(dev_bytes)))
    buf.extend(dev_bytes)

    # Field 2: timestamp_ms (int64, wire_type=0)
    buf.extend(_encode_varint((2 << 3) | 0))
    buf.extend(_encode_varint(record.timestamp_ms))

    # Field 3: sensor_type (enum, wire_type=0)
    enum_map = {
        SensorType.MOTOR: 1,
        SensorType.CNC_SPINDLE: 2,
        SensorType.PUMP: 3,
        SensorType.TRANSFORMER: 4,
    }
    buf.extend(_encode_varint((3 << 3) | 0))
    buf.extend(_encode_varint(enum_map.get(record.sensor_type, 1)))

    # Field 4: temperature (double, wire_type=1 -> 64-bit IEEE 754)
    buf.extend(_encode_varint((4 << 3) | 1))
    buf.extend(struct.pack("<d", float(record.temperature)))

    # Field 5: vibration_rms (double, wire_type=1)
    buf.extend(_encode_varint((5 << 3) | 1))
    buf.extend(struct.pack("<d", float(record.vibration_rms)))

    # Field 6: vibration_kurtosis (double, wire_type=1)
    buf.extend(_encode_varint((6 << 3) | 1))
    buf.extend(struct.pack("<d", float(record.vibration_kurtosis)))

    # Field 7: current (double, wire_type=1)
    buf.extend(_encode_varint((7 << 3) | 1))
    buf.extend(struct.pack("<d", float(record.current)))

    # Field 8: voltage (double, wire_type=1)
    buf.extend(_encode_varint((8 << 3) | 1))
    buf.extend(struct.pack("<d", float(record.voltage)))

    # Field 9: power_factor (double, wire_type=1)
    buf.extend(_encode_varint((9 << 3) | 1))
    buf.extend(struct.pack("<d", float(record.power_factor)))

    # Field 10: frequency (double, wire_type=1)
    buf.extend(_encode_varint((10 << 3) | 1))
    buf.extend(struct.pack("<d", float(record.frequency)))

    # Field 11: auth_token (string, wire_type=2)
    if record.auth_token:
        tok_bytes = record.auth_token.encode("utf-8")
        buf.extend(_encode_varint((11 << 3) | 2))
        buf.extend(_encode_varint(len(tok_bytes)))
        buf.extend(tok_bytes)

    return bytes(buf)


def decode_protobuf_telemetry(data: bytes) -> TelemetryRecord:
    """Deserializes binary Protobuf 3 wire-format into TelemetryRecord."""
    offset = 0
    size = len(data)

    device_id = "unknown_device"
    timestamp_ms = 0
    sensor_type = SensorType.MOTOR
    temperature = 0.0
    vibration_rms = 0.0
    vibration_kurtosis = 3.0
    current = 0.0
    voltage = 400.0
    power_factor = 0.88
    frequency = 50.0
    auth_token: str | None = None

    enum_reverse = {
        1: SensorType.MOTOR,
        2: SensorType.CNC_SPINDLE,
        3: SensorType.PUMP,
        4: SensorType.TRANSFORMER,
    }

    while offset < size:
        tag, offset = _decode_varint(data, offset)
        field_num = tag >> 3
        wire_type = tag & 0x07

        if wire_type == 0:  # Varint
            val, offset = _decode_varint(data, offset)
            if field_num == 2:
                timestamp_ms = val
            elif field_num == 3:
                sensor_type = enum_reverse.get(val, SensorType.MOTOR)
        elif wire_type == 1:  # 64-bit double
            if offset + 8 > size:
                raise ValueError("Truncated 64-bit float in protobuf stream")
            (d_val,) = struct.unpack("<d", data[offset:offset + 8])
            offset += 8
            if field_num == 4:
                temperature = round(d_val, 4)
            elif field_num == 5:
                vibration_rms = round(d_val, 4)
            elif field_num == 6:
                vibration_kurtosis = round(d_val, 4)
            elif field_num == 7:
                current = round(d_val, 4)
            elif field_num == 8:
                voltage = round(d_val, 2)
            elif field_num == 9:
                power_factor = round(d_val, 4)
            elif field_num == 10:
                frequency = round(d_val, 2)
        elif wire_type == 2:  # Length-delimited string
            str_len, offset = _decode_varint(data, offset)
            if offset + str_len > size:
                raise ValueError("Truncated length-delimited string in protobuf stream")
            str_bytes = data[offset:offset + str_len]
            offset += str_len
            if field_num == 1:
                device_id = str_bytes.decode("utf-8")
            elif field_num == 11:
                auth_token = str_bytes.decode("utf-8")
        else:
            raise ValueError(f"Unsupported wire type {wire_type} for field {field_num}")

    return TelemetryRecord(
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        sensor_type=sensor_type,
        temperature=temperature,
        vibration_rms=vibration_rms,
        vibration_kurtosis=vibration_kurtosis,
        current=current,
        voltage=voltage,
        power_factor=power_factor,
        frequency=frequency,
        auth_token=auth_token,
    )


def encode_json_telemetry(record: TelemetryRecord) -> str:
    """Serializes TelemetryRecord to JSON string."""
    return record.model_dump_json()


def decode_json_telemetry(data: str | bytes) -> TelemetryRecord:
    """Deserializes JSON string/bytes to TelemetryRecord."""
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    return TelemetryRecord.model_validate_json(data)
