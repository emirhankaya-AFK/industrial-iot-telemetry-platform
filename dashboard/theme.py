"""Industrial SCADA / Siemens High-Contrast Dark Theme for Streamlit Terminal."""


def get_scada_theme_css() -> str:
    return """
<style>
/* ============================================================
   SCADA INDUSTRIAL OPERATOR THEME
   ============================================================ */
:root {
  --bg-deep:      #080e14;
  --bg-panel:     #0e1724;
  --bg-card:      #142234;
  --bg-card-alt:  #1b2c42;
  --border:       #22374e;
  --border-glow:  #00d2d3;
  --text-main:    #f0f4f8;
  --text-muted:   #94a9c0;
  --text-dim:     #5c7590;
  --cyan-accent:  #00d2d3;
  --pass-green:   #10b981;
  --fail-red:     #ef4444;
  --warn-amber:   #f59e0b;
}

[data-testid="stApp"] {
  background-color: var(--bg-deep) !important;
  color: var(--text-main) !important;
  font-family: 'Inter', -apple-system, system-ui, sans-serif;
}

[data-testid="stHeader"] { display: none !important; }

.block-container {
  padding-top: 1.2rem !important;
  padding-left: 1.5rem !important;
  padding-right: 1.5rem !important;
  max-width: 1480px !important;
}

/* ============================================================
   SCADA TOP BAR
   ============================================================ */
.scada-topbar {
  background: linear-gradient(135deg, #0e1a27 0%, #15273b 100%);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px 22px;
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 12px;
}
.scada-heading {
  display: flex;
  align-items: center;
  gap: 12px;
}
.scada-title-text {
  font-size: 1.25rem;
  font-weight: 800;
  letter-spacing: -0.01em;
  color: #ffffff;
}
.scada-tag {
  background: rgba(0, 210, 211, 0.15);
  border: 1px solid rgba(0, 210, 211, 0.4);
  color: #00d2d3;
  font-size: 0.7rem;
  font-weight: 700;
  padding: 3px 8px;
  border-radius: 4px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.status-pill-online {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  background: rgba(16, 185, 129, 0.15);
  border: 1px solid rgba(16, 185, 129, 0.35);
  color: var(--pass-green);
  font-size: 0.75rem;
  font-weight: 700;
  padding: 4px 10px;
  border-radius: 20px;
}
.status-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--pass-green);
  box-shadow: 0 0 6px var(--pass-green);
}

/* ============================================================
   METRIC TILES
   ============================================================ */
.metric-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 14px;
  margin-bottom: 20px;
}
.metric-tile {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px 18px;
  transition: border-color 0.2s;
}
.metric-tile:hover {
  border-color: var(--cyan-accent);
}
.metric-tile.critical {
  border-left: 4px solid var(--fail-red);
}
.metric-tile.warning {
  border-left: 4px solid var(--warn-amber);
}
.metric-tile.nominal {
  border-left: 4px solid var(--pass-green);
}
.tile-label {
  font-size: 0.75rem;
  font-weight: 700;
  color: var(--text-dim);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: 6px;
}
.tile-val {
  font-size: 1.8rem;
  font-weight: 800;
  font-family: 'JetBrains Mono', monospace;
  color: #ffffff;
  line-height: 1.1;
}
.tile-sub {
  font-size: 0.75rem;
  color: var(--text-muted);
  margin-top: 4px;
}

/* ============================================================
   INCIDENT BANNER
   ============================================================ */
.incident-card {
  background: rgba(239, 68, 68, 0.08);
  border: 1px solid rgba(239, 68, 68, 0.3);
  border-radius: 6px;
  padding: 12px 16px;
  margin-bottom: 10px;
}
.incident-card.warn {
  background: rgba(245, 158, 11, 0.08);
  border-color: rgba(245, 158, 11, 0.3);
}
</style>
"""
