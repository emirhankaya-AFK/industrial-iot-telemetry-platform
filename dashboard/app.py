"""Streamlit Industrial SCADA Operator Terminal for Edge IoT Telemetry & Anomaly Detection."""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Setup python path to import src modules
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dashboard.theme import get_scada_theme_css  # noqa: E402
from src.pipeline.stream_processor import StreamProcessor  # noqa: E402
from src.simulator.industrial_generator import IndustrialTelemetrySimulator  # noqa: E402

# Page config
st.set_page_config(
    page_title="Industrial Edge Telemetry & SCADA Terminal",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply SCADA theme CSS
st.markdown(get_scada_theme_css(), unsafe_allow_html=True)


# Initialize session state singleton pipeline and simulator
if "processor" not in st.session_state:
    st.session_state.processor = StreamProcessor()
    st.session_state.simulator = IndustrialTelemetrySimulator()
    # Seed initial 30 readings per device
    for p in st.session_state.simulator.profiles:
        for _ in range(25):
            rec = st.session_state.simulator.generate_reading(p)
            st.session_state.processor.process_record_sync(rec)

proc: StreamProcessor = st.session_state.processor
sim: IndustrialTelemetrySimulator = st.session_state.simulator

# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### 🎛️ SCADA Station Control")

    devices_list = list(sim.profiles.keys())
    selected_device = st.selectbox("🎯 Target Equipment", devices_list, index=0)

    st.markdown("---")
    st.markdown("### ⚡ Telemetry Generator")

    auto_step = st.button("▶ Step Telemetry (1 Cycle)", use_container_width=True)
    burst_step = st.button("⏩ Burst 25 Sensor Readings", use_container_width=True)

    if auto_step:
        rec = sim.generate_reading(selected_device)
        proc.process_record_sync(rec)
        st.toast(f"Generated reading for {selected_device}", icon="✅")

    if burst_step:
        for _ in range(25):
            rec = sim.generate_reading(selected_device)
            proc.process_record_sync(rec)
        st.toast(f"Burst 25 readings ingested for {selected_device}", icon="🚀")

    st.markdown("---")
    st.caption("Industrial Edge Telemetry Gateway v1.0.0 · Dual Ingress MQTT / HTTP · Redis Streams")


# ---------------------------------------------------------------------------
# TOP HEADER BAR
# ---------------------------------------------------------------------------
metrics = proc.get_metrics()
redis_status = "Online (Redis Streams)" if proc.stream.is_connected_to_redis else "Active (MemoryStreamEngine)"

st.markdown(f"""
<div class="scada-topbar">
  <div class="scada-heading">
    <span style="font-size:1.6rem;">⚡</span>
    <div>
      <div class="scada-title-text">Industrial Edge Telemetry & SCADA Operations Terminal</div>
      <div style="font-size:0.8rem; color:var(--text-muted);">Real-Time IoT Gateway · Streaming EWMA & Robust Z-Score Multi-Sensor Analytics</div>
    </div>
  </div>
  <div style="display:flex; align-items:center; gap:10px;">
    <span class="scada-tag">Station #01</span>
    <span class="status-pill-online">
      <span class="status-dot"></span> {redis_status}
    </span>
  </div>
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# KPI TILES
# ---------------------------------------------------------------------------
st.markdown(f"""
<div class="metric-grid">
  <div class="metric-tile nominal">
    <div class="tile-label">Events Processed</div>
    <div class="tile-val">{metrics.events_processed}</div>
    <div class="tile-sub">Ingested: {metrics.events_ingested}</div>
  </div>
  <div class="metric-tile {'critical' if metrics.anomalies_detected > 0 else 'nominal'}">
    <div class="tile-label">Anomalies Detected</div>
    <div class="tile-val">{metrics.anomalies_detected}</div>
    <div class="tile-sub">Alerts: {metrics.alerts_dispatched}</div>
  </div>
  <div class="metric-tile warning">
    <div class="tile-label">Pipeline Latency (p95)</div>
    <div class="tile-val">{metrics.p95_latency_ms:.2f} ms</div>
    <div class="tile-sub">Avg: {metrics.avg_latency_ms:.2f} ms</div>
  </div>
  <div class="metric-tile nominal">
    <div class="tile-label">Buffer Capacity</div>
    <div class="tile-val">{metrics.buffer.backlog_count if metrics.buffer else 0}</div>
    <div class="tile-sub">Dropped: {metrics.events_dropped} (0.0%)</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# MAIN TABS
# ---------------------------------------------------------------------------
tabs = st.tabs([
    "📊 Live Multi-Sensor Telemetry",
    "🚨 Active Incidents & Operator Log",
    "🧪 Failure Mode Simulator",
    "📈 Pipeline Metrics & Benchmark",
])


# ===========================================================================
# TAB 1: Live Multi-Sensor Telemetry
# ===========================================================================
with tabs[0]:
    st.markdown(f"### 📈 Real-Time Strip Charts: `{selected_device}`")

    readings = proc.get_live_device_telemetry(selected_device)
    if readings:
        df = pd.DataFrame([r.model_dump() for r in readings])
        df["time"] = pd.to_datetime(df["timestamp_ms"], unit="ms")

        col1, col2 = st.columns(2)

        with col1:
            # Temperature plot
            fig_temp = px.line(
                df, x="time", y="temperature",
                title=f"Bearing & Stator Temperature (°C) — {selected_device}",
                template="plotly_dark",
                color_discrete_sequence=["#ff9f43"],
            )
            fig_temp.add_hline(y=75.0, line_dash="dash", line_color="#ee5253", annotation_text="High Warning (75°C)")
            fig_temp.update_layout(height=280, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_temp, use_container_width=True)

            # Current plot
            fig_curr = px.line(
                df, x="time", y="current",
                title=f"Line Current (A) — {selected_device}",
                template="plotly_dark",
                color_discrete_sequence=["#00d2d3"],
            )
            fig_curr.update_layout(height=280, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_curr, use_container_width=True)

        with col2:
            # Vibration plot
            fig_vib = px.line(
                df, x="time", y="vibration_rms",
                title=f"Vibration RMS Acceleration (g) — {selected_device}",
                template="plotly_dark",
                color_discrete_sequence=["#54a0ff"],
            )
            fig_vib.add_hline(y=2.5, line_dash="dash", line_color="#ee5253", annotation_text="Vibration Alert (2.5g)")
            fig_vib.update_layout(height=280, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_vib, use_container_width=True)

            # Power Factor
            fig_pf = px.line(
                df, x="time", y="power_factor",
                title=f"Power Factor cos(φ) — {selected_device}",
                template="plotly_dark",
                color_discrete_sequence=["#10b981"],
            )
            fig_pf.update_layout(height=280, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_pf, use_container_width=True)
    else:
        st.info("No telemetry readings available for this device yet. Click 'Step Telemetry' in the sidebar.")


# ===========================================================================
# TAB 2: Active Incidents & Operator Log
# ===========================================================================
with tabs[1]:
    st.markdown("### 🚨 Deduplicated Incident Alerts & Operator ACK Log")

    alerts = proc.dispatcher.get_recent_alerts(limit=25)
    if alerts:
        for alt in alerts:
            status_badge = "✅ ACKNOWLEDGED" if alt.acknowledged else "🔴 UNACKNOWLEDGED"
            css_class = "incident-card" if alt.severity.value == "CRITICAL" else "incident-card warn"

            st.markdown(f"""
            <div class="{css_class}">
              <div style="display:flex; justify-content:space-between; align-items:center;">
                <strong>{alt.title}</strong>
                <span style="font-family:'JetBrains Mono'; font-size:0.75rem;">{status_badge}</span>
              </div>
              <div style="color:var(--text-muted); font-size:0.85rem; margin-top:4px;">{alt.details}</div>
              <div style="font-size:0.75rem; color:var(--text-dim); margin-top:4px;">
                Alert ID: <code>{alt.alert_id}</code> · Device: <code>{alt.device_id}</code> · Created: {time.strftime('%H:%M:%S', time.localtime(alt.created_at_ms / 1000))}
              </div>
            </div>
            """, unsafe_allow_html=True)

            if not alt.acknowledged:
                if st.button(f"Acknowledge Incident {alt.alert_id[:8]}", key=f"ack_{alt.alert_id}"):
                    proc.dispatcher.acknowledge_alert(alt.alert_id, operator_name="Lead_Operator")
                    st.rerun()
    else:
        st.success("✅ Nominal operation: zero unacknowledged incident alerts.")

    # Recent raw anomalies
    st.markdown("---")
    st.markdown("#### 🔍 Recent Anomaly Detections Log (Raw Signals)")
    raw_anoms = proc.get_recent_anomalies(limit=15)
    if raw_anoms:
        anom_table = [
            {
                "Device": a.device_id,
                "Type": a.anomaly_type.value,
                "Detector": a.detector_name,
                "Severity": a.severity.value,
                "Metric": a.metric_name,
                "Actual": a.actual_value,
                "Expected": a.expected_value,
                "Score": a.score,
                "Description": a.description,
            }
            for a in raw_anoms
        ]
        st.dataframe(pd.DataFrame(anom_table), use_container_width=True)


# ===========================================================================
# TAB 3: Failure Mode Simulator
# ===========================================================================
with tabs[2]:
    st.markdown("### 🧪 Hardware Failure Mode Simulator & Stress Testing")
    st.markdown("Inject realistic physical equipment failure signatures into the telemetry stream to verify detection latency and alerting logic.")

    colA, colB, colC, colD = st.columns(4)

    with colA:
        st.markdown("#### 1. Mechanical Bearing Fatigue")
        st.caption("Induces race spalling vibration spikes (>4.5g) & high kurtosis.")
        if st.button("⚡ Inject Bearing Fault", type="primary", use_container_width=True):
            sim.inject_fault(selected_device, "BEARING_FATIGUE")
            # Step 10 readings
            for _ in range(12):
                rec = sim.generate_reading(selected_device)
                proc.process_record_sync(rec)
            st.toast("Injected BEARING_FATIGUE!", icon="⚠️")
            st.rerun()

    with colB:
        st.markdown("#### 2. Thermal Runaway")
        st.caption("Simulates cooling fan failure with progressive thermal drift.")
        if st.button("🔥 Inject Thermal Drift", type="primary", use_container_width=True):
            sim.inject_fault(selected_device, "THERMAL_RUNAWAY")
            for _ in range(15):
                rec = sim.generate_reading(selected_device)
                proc.process_record_sync(rec)
            st.toast("Injected THERMAL_RUNAWAY!", icon="⚠️")
            st.rerun()

    with colC:
        st.markdown("#### 3. Electrical Phase Imbalance")
        st.caption("Stator asymmetry causing massive current surge & voltage sag.")
        if st.button("⚡ Inject Phase Fault", type="primary", use_container_width=True):
            sim.inject_fault(selected_device, "PHASE_IMBALANCE")
            for _ in range(10):
                rec = sim.generate_reading(selected_device)
                proc.process_record_sync(rec)
            st.toast("Injected PHASE_IMBALANCE!", icon="⚠️")
            st.rerun()

    with colD:
        st.markdown("#### 4. Correlated Rotor Seizure")
        st.caption("Catastrophic simultaneous mechanical + electrical overload.")
        if st.button("💥 Inject Rotor Seizure", type="primary", use_container_width=True):
            sim.inject_fault(selected_device, "CORRELATED_SEIZURE")
            for _ in range(10):
                rec = sim.generate_reading(selected_device)
                proc.process_record_sync(rec)
            st.toast("Injected CORRELATED_SEIZURE!", icon="🚨")
            st.rerun()

    st.markdown("---")
    if st.button("🔄 Clear Active Fault & Restore Nominal Operation", use_container_width=True):
        sim.clear_fault(selected_device)
        for _ in range(15):
            rec = sim.generate_reading(selected_device)
            proc.process_record_sync(rec)
        st.toast(f"Restored {selected_device} to nominal status", icon="✅")
        st.rerun()


# ===========================================================================
# TAB 4: Pipeline Metrics & Benchmark
# ===========================================================================
with tabs[3]:
    st.markdown("### 📈 Quantitative Pipeline Performance & Empirical Benchmark")
    st.markdown("Measure detection accuracy (Precision, Recall, F1), alert latency, throughput, and backpressure resilience.")

    eval_btn = st.button("▶ Run Full Empirical Benchmark Evaluation Suite", type="primary")

    if eval_btn:
        with st.spinner("Executing quantitative streaming benchmark across 1,000 synthetic sensor events..."):
            res = subprocess.run(
                [sys.executable, "tests/evaluation/run_benchmark.py"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
            )
            st.code(res.stdout + res.stderr, language="text")
