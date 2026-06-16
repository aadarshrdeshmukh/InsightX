"""
InsightX Home Page — Brutalist B&W edition.
"""

import streamlit as st
from datetime import datetime


_LUCIDE = "https://unpkg.com/lucide-static@latest/icons"


def _icon(name: str, size: int = 24) -> str:
    return (
        f'<img src="{_LUCIDE}/{name}.svg" width="{size}" height="{size}" '
        f'style="filter: brightness(0);" />'
    )


def show_home_page():

    today = datetime.now().strftime("%d.%m.%Y")
    st.markdown(
        f"""
        <div style="margin-bottom: 1.5rem;">
            <p style="margin:0; font-family: 'Space Mono', monospace; font-size: 0.72rem;
                      color: #888; text-transform: uppercase; letter-spacing: 2px;">
                {today}
            </p>
            <h1 style="margin: 0.3rem 0 0.5rem; font-size: 2.2rem; font-weight: 900;
                        color: #000; text-transform: uppercase; letter-spacing: -1px;
                        border-bottom: 4px solid #000; display: inline-block;
                        padding-bottom: 0.3rem;">
                InsightX
            </h1>
            <p style="margin: 0.5rem 0 0; font-size: 0.95rem; color: #555; max-width: 550px;
                      line-height: 1.6;">
                Simulate, analyse, and visualise high-throughput streaming data
                pipelines in real time.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Metrics ──────────────────────────────────────────────────────
    results = st.session_state.get("sim_results")
    total_events = results.get("total_produced", 0) if results else 0
    windows_captured = results.get("windows_captured", 0) if results else 0
    throughput = results.get("avg_throughput", 0) if results else 0
    dlq_events = results.get("dlq_size", 0) if results else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events Produced", f"{total_events:,}")
    c2.metric("Windows Captured", f"{windows_captured:,}")
    c3.metric("Throughput", f"{throughput:,.1f} e/s")
    c4.metric("DLQ Events", f"{dlq_events:,}")

    st.write("")

    # ── Quick Start ──────────────────────────────────────────────────
    st.markdown("### How It Works")
    st.markdown(
        """
        InsightX models a production-grade streaming analytics pipeline end-to-end:

        1. **Produce** — synthetic events from 5 heterogeneous data sources
        2. **Ingest** — simulated Kafka message broker with partitioning & DLQ
        3. **Process** — tumbling-window stream analytics (Flink-style)
        4. **Store** — hot data in Redis, cold data in Cassandra
        5. **Visualise** — live metrics, throughput gauges, and breakdowns
        """
    )

    if st.button("Start Simulation", type="primary", use_container_width=False):
        st.session_state.current_page = "simulation"
        st.rerun()

    st.divider()

    # ── System Components ────────────────────────────────────────────
    st.markdown("### System Components")

    components = [
        ("mail",       "Kafka Broker",         "Simulated message queue with partitioning, back-pressure, and DLQ."),
        ("cog",        "Flink Processor",       "Tumbling-window stream processor with configurable window sizes."),
        ("database",   "Redis Hot Store",       "In-memory cache for the latest window metrics and real-time lookups."),
        ("hard-drive", "Cassandra Cold Store",  "Persistent time-series storage for historical window data."),
        ("line-chart", "Dashboard",             "Live visualisation layer powered by Streamlit and Plotly charts."),
    ]

    cols = st.columns(5)
    for col, (icon_name, name, desc) in zip(cols, components):
        col.markdown(
            f"""
            <div class="br-card" style="text-align: center; min-height: 180px;
                        display: flex; flex-direction: column; align-items: center;
                        justify-content: center;">
                <div class="br-card-icon">{_icon(icon_name, 26)}</div>
                <div class="br-card-title">{name}</div>
                <div class="br-card-desc">{desc}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
