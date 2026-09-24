"""Empirical Quantitative Benchmark Runner for Industrial Edge Telemetry Platform.
Evaluates Multi-Class Anomaly Classification (with Confusion Matrix),
Stream Ingestion Throughput, Consumer Groups, Real Backpressure Drops, and PEL Crash Recovery.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.models.schemas import TelemetryRecord
from src.pipeline.stream_processor import StreamProcessor
from src.simulator.industrial_generator import IndustrialTelemetrySimulator
from src.streaming.memory_stream import MemoryStreamEngine
from src.streaming.redis_stream import RedisStreamEngine


def benchmark_anomaly_classification():
    print("\n" + "=" * 80)
    print("  SECTION 1: MULTI-CLASS ANOMALY CLASSIFICATION & CONFUSION MATRIX")
    print("=" * 80)

    sim = IndustrialTelemetrySimulator(seed=1337)
    processor = StreamProcessor()

    devices = ["motor_unit_01", "cnc_spindle_02", "cooling_pump_03"]
    nominal_count = 1000
    anomaly_count = 200
    total_events = nominal_count + anomaly_count

    # Ground truth tracking: [(record, ground_truth_class)]
    test_stream: List[tuple[TelemetryRecord, str]] = []
    fault_types = ["BEARING_FATIGUE", "THERMAL_RUNAWAY", "PHASE_IMBALANCE", "CORRELATED_SEIZURE"]
    base_time = int(time.time() * 1000) - (total_events * 50)
    current_step = 0

    # 4 distinct operational shifts (250 nominal baseline + 50 fault burst per shift)
    for fault in fault_types:
        # Clear any past faults for nominal burn-in
        for d in devices:
            sim.clear_fault(d)

        for _ in range(nominal_count // len(fault_types)):
            dev = devices[current_step % len(devices)]
            t_ms = base_time + current_step * 50
            rec = sim.generate_reading(dev, timestamp_ms=t_ms)
            test_stream.append((rec, "NOMINAL"))
            current_step += 1

        # Inject specific machinery fault mode
        for d in devices:
            sim.inject_fault(d, fault)

        for _ in range(anomaly_count // len(fault_types)):
            dev = devices[current_step % len(devices)]
            t_ms = base_time + current_step * 50
            rec = sim.generate_reading(dev, timestamp_ms=t_ms)
            test_stream.append((rec, fault))
            current_step += 1

    # Clear active faults after generation
    for d in devices:
        sim.clear_fault(d)

    # 3. Synchronous inference & latency timing
    classes = ["NOMINAL", "BEARING_FATIGUE", "THERMAL_RUNAWAY", "PHASE_IMBALANCE", "CORRELATED_SEIZURE"]
    class_to_idx = {c: i for i, c in enumerate(classes)}
    cm = [[0 for _ in classes] for _ in classes]

    latencies: List[float] = []
    t_start = time.perf_counter()

    for rec, gt_class in test_stream:
        t0 = time.perf_counter()
        anomalies = processor.run_detectors(rec)
        pred_class = processor.classify_fault(anomalies)
        lat = (time.perf_counter() - t0) * 1000.0
        latencies.append(lat)

        gt_idx = class_to_idx[gt_class]
        pred_idx = class_to_idx.get(pred_class, 0)
        cm[gt_idx][pred_idx] += 1

    t_total = time.perf_counter() - t_start
    throughput = len(test_stream) / t_total if t_total > 0 else 0.0

    # 4. Print Multi-Class Confusion Matrix
    header_title = r"Actual \ Predicted"
    print("\nCONFUSION MATRIX (Ground Truth rows vs Predicted columns):")
    print(f"{header_title:<22} | " + " | ".join(f"{c[:10]:>10}" for c in classes))
    print("-" * 80)
    for i, c_actual in enumerate(classes):
        row_str = " | ".join(f"{cm[i][j]:>10}" for j in range(len(classes)))
        print(f"{c_actual:<22} | {row_str}")
    print("-" * 80)

    # 5. Compute Per-Class Precision, Recall, F1
    total_correct = sum(cm[i][i] for i in range(len(classes)))
    overall_accuracy = total_correct / len(test_stream)

    per_class_metrics: Dict[str, dict] = {}
    print(f"\n{'FAILURE MODE / CLASS':<24} | {'PRECISION':<10} | {'RECALL':<8} | {'F1-SCORE':<9} | {'SUPPORT':<8}")
    print("-" * 72)

    for i, c_name in enumerate(classes):
        tp = cm[i][i]
        fp = sum(cm[j][i] for j in range(len(classes)) if j != i)
        fn = sum(cm[i][j] for j in range(len(classes)) if j != i)
        support = sum(cm[i][j] for j in range(len(classes)))

        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * prec * rec) / (prec + rec) if (prec + rec) > 0 else 0.0

        per_class_metrics[c_name] = {
            "tp": tp, "fp": fp, "fn": fn,
            "precision": prec, "recall": rec, "f1": f1,
            "support": support,
        }

        grade = "🟢" if f1 >= 0.80 else "🟡" if f1 >= 0.50 else "⚪"
        print(f"{grade} {c_name:<22} | {prec:<10.2f} | {rec:<8.2f} | {f1:<9.2f} | {support:<8}")

    print("-" * 72)

    # Macro & Anomaly-only averages
    fault_f1s = [per_class_metrics[f]["f1"] for f in fault_types]
    macro_fault_f1 = sum(fault_f1s) / len(fault_f1s)
    macro_fault_prec = sum(per_class_metrics[f]["precision"] for f in fault_types) / len(fault_types)
    macro_fault_rec = sum(per_class_metrics[f]["recall"] for f in fault_types) / len(fault_types)

    print(f"  {'ANOMALY FAULT MACRO':<22} | {macro_fault_prec:<10.2f} | {macro_fault_rec:<8.2f} | {macro_fault_f1:<9.2f} | {anomaly_count}")

    avg_lat = sum(latencies) / len(latencies) if latencies else 0.0
    lat_sorted = sorted(latencies)
    p95_lat = lat_sorted[int(len(lat_sorted) * 0.95)] if lat_sorted else 0.0
    p99_lat = lat_sorted[int(len(lat_sorted) * 0.99)] if lat_sorted else 0.0

    print(f"\n  • Multi-Class Overall Accuracy : {overall_accuracy * 100:.1f}% ({total_correct}/{len(test_stream)})")
    print(f"  • Detector Ensemble Throughput  : {throughput:,.1f} events/sec")
    print(f"  • Average Latency per Record    : {avg_lat:.3f} ms (p95: {p95_lat:.3f} ms, p99: {p99_lat:.3f} ms)")

    return {
        "overall_accuracy": overall_accuracy,
        "macro_fault_precision": macro_fault_prec,
        "macro_fault_recall": macro_fault_rec,
        "macro_fault_f1": macro_fault_f1,
        "per_class": per_class_metrics,
        "throughput_eps": throughput,
        "avg_lat_ms": avg_lat,
        "p95_lat_ms": p95_lat,
    }


def benchmark_streaming_engine_and_backpressure():
    print("\n" + "=" * 80)
    print("  SECTION 2: DISTRIBUTED STREAMING ENGINE, BACKPRESSURE & RECOVERY")
    print("=" * 80)

    processor = StreamProcessor()
    sim = IndustrialTelemetrySimulator(seed=999)
    n_stream_events = 1000

    is_live_redis = processor.stream.is_connected_to_redis
    engine_name = "Live Redis Streams Server" if is_live_redis else "Deterministic In-Memory Engine (Fallback)"
    print(f"  • Active Stream Engine          : {engine_name} ({processor.stream.redis_url if is_live_redis else 'MemoryStreamEngine'})")

    # Part A: Consumer Group Queue Throughput & ACK Pipeline
    t_ingest_start = time.perf_counter()
    for i in range(n_stream_events):
        rec = sim.generate_reading("motor_unit_01")
        processor.stream.add(rec)
    t_ingest = time.perf_counter() - t_ingest_start
    ingest_throughput = n_stream_events / t_ingest if t_ingest > 0 else 0.0

    t_consume_start = time.perf_counter()
    batches = 0
    while True:
        _ = processor.process_pending_stream(count=100)
        batches += 1
        metrics = processor.stream.get_metrics()
        if metrics.backlog_count == 0:
            break
        if batches > 200:  # safety break
            break
    t_consume = time.perf_counter() - t_consume_start
    consumer_throughput = n_stream_events / t_consume if t_consume > 0 else 0.0

    print(f"  ✓ Stream Ingestion Throughput   : {ingest_throughput:,.1f} events/sec")
    print(f"  ✓ Consumer Group Processing Rate: {consumer_throughput:,.1f} events/sec (XREADGROUP + XACK)")

    # Part B: Empirical Backpressure Capacity Drop Verification (Bounded Memory Buffer)
    bounded_engine = MemoryStreamEngine(max_len=200, drop_policy="drop_oldest")
    flood_count = 600
    for _ in range(flood_count):
        rec = sim.generate_reading("pump_flood_test")
        bounded_engine.add(rec)

    bp_metrics = bounded_engine.get_metrics()
    expected_drops = flood_count - 200
    actual_drops = bp_metrics.dropped_count
    drop_rate_pct = (actual_drops / flood_count) * 100.0

    print("  --- Backpressure Drop Stress Test (Bounded Buffer Capacity: 200) ---")
    print(f"  ✓ Ingested Flood Events         : {flood_count} events without consumer drain")
    print(f"  ✓ Measured Dropped Events       : {actual_drops} (Expected: {expected_drops}, Rate: {drop_rate_pct:.1f}%)")
    print(f"  ✓ Active Buffer Backlog Depth   : {bp_metrics.backlog_count} / {bp_metrics.max_len}")
    assert actual_drops == expected_drops, f"Expected {expected_drops} drops, got {actual_drops}"

    # Part C: Worker Crash & PEL Recovery (XCLAIM)
    print("  --- Worker Crash & Stale Message Reclaim (PEL / XCLAIM) ---")
    if is_live_redis:
        live_engine = RedisStreamEngine(
            redis_url=processor.stream.redis_url,
            stream_name="benchmark:live:crash:test",
            use_fallback_if_unavailable=False,
        )
        crashed_group = "redis-live-recovery-grp"
        live_engine.create_consumer_group(crashed_group)
        for _ in range(50):
            live_engine.add(sim.generate_reading("cnc_crash_test"))

        unacked = live_engine.read_group(crashed_group, "crashed_worker_01", count=50)
        pending_before = live_engine.get_pending_count(crashed_group)
        print(f"  ✓ Live Redis Worker Crash Sim   : 'crashed_worker_01' pulled {len(unacked)} records without ACK")
        print(f"  ✓ Live Redis PEL Tracking       : {pending_before} unacknowledged entries held in Redis PEL")

        reclaimed = live_engine.claim_stale(crashed_group, "standby_worker_02", min_idle_ms=0, count=50)
        print(f"  ✓ Live Redis XCLAIM Reclaim     : 'standby_worker_02' reclaimed {len(reclaimed)}/50 entries via XCLAIM")
        for mid, _ in reclaimed:
            live_engine.ack(crashed_group, mid)

        pending_after = live_engine.get_pending_count(crashed_group)
        print(f"  ✓ Post-Recovery Live Redis PEL  : {pending_after} (100% XCLAIM Recovery on Live Redis Server)")
    else:
        crashed_group = "critical-alerts"
        bounded_engine.create_consumer_group(crashed_group)
        for _ in range(50):
            bounded_engine.add(sim.generate_reading("cnc_crash_test"))

        unacked_entries = bounded_engine.read_group(crashed_group, "crashed_worker_01", count=50)
        pending_before = bounded_engine.get_pending_count(crashed_group)
        print(f"  ✓ In-Memory Crash Simulation    : 'crashed_worker_01' pulled {len(unacked_entries)} records without ACK")
        print(f"  ✓ Pending Entries List (PEL)    : {pending_before} unacknowledged entries held in PEL")

        reclaimed = bounded_engine.claim_stale(crashed_group, "standby_worker_02", min_idle_ms=0, count=50)
        print(f"  ✓ PEL Stale Message Reclaim     : 'standby_worker_02' reclaimed {len(reclaimed)}/50 entries (XCLAIM semantics)")
        for msg_id, _ in reclaimed:
            bounded_engine.ack(crashed_group, msg_id)

        pending_after = bounded_engine.get_pending_count(crashed_group)
        print(f"  ✓ Post-Recovery PEL Depth       : {pending_after} (100% Recovery & Acknowledgment Success)")
    assert pending_after == 0, "PEL was not completely cleared!"

    print("=" * 80 + "\n")
    return {
        "ingest_throughput_eps": ingest_throughput,
        "consumer_throughput_eps": consumer_throughput,
        "measured_drops": actual_drops,
        "drop_rate_pct": drop_rate_pct,
        "pel_recovery_rate_pct": 100.0,
    }


def run_benchmark():
    sec1 = benchmark_anomaly_classification()
    sec2 = benchmark_streaming_engine_and_backpressure()
    return {"classification": sec1, "streaming": sec2}


if __name__ == "__main__":
    run_benchmark()
