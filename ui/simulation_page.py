"""
InsightX Simulation Page — Brutalist B&W edition.
Uses st.rerun() pattern for live updates.
"""

import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from core.engine import SimulationEngine


# ── B&W colour maps ──────────────────────────────────────────────────────
SOURCE_COLORS = {
    "web": "#000000",
    "mobile": "#333333",
    "iot_sensor": "#666666",
    "payment_gateway": "#999999",
    "cdn_log": "#bbbbbb",
}

_CHART_LAYOUT = dict(
    plot_bgcolor="#ffffff",
    paper_bgcolor="#ffffff",
    font=dict(family="Space Mono, Courier New, monospace", color="#000000", size=11),
)

_BATCH_SIZE = 10


# ── Chart builders ───────────────────────────────────────────────────────
def _build_line_chart(windows: list[dict]) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=list(range(len(windows))),
            y=[w["event_count"] for w in windows],
            mode="lines+markers",
            line=dict(color="#000000", width=2),
            marker=dict(size=6, color="#000000", symbol="square"),
            fill="tozeroy",
            fillcolor="rgba(0,0,0,0.04)",
            name="Events",
        )
    )
    fig.update_layout(
        title="Events per Window",
        xaxis_title="Window #",
        yaxis_title="Event Count",
        height=350,
        margin=dict(t=40, b=30, l=10, r=10),
        **_CHART_LAYOUT,
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="#e0e0e0", zeroline=False)
    return fig


def _build_bar_chart(windows: list[dict]) -> go.Figure:
    source_agg: dict[str, int] = {}
    for w in windows:
        for src, cnt in w.get("source_breakdown", {}).items():
            source_agg[src] = source_agg.get(src, 0) + cnt

    fig = go.Figure(
        data=[
            go.Bar(
                x=list(source_agg.keys()),
                y=list(source_agg.values()),
                marker_color=[SOURCE_COLORS.get(s, "#000") for s in source_agg],
            )
        ]
    )
    fig.update_layout(
        title="Events by Source",
        height=350,
        margin=dict(t=40, b=30, l=10, r=10),
        **_CHART_LAYOUT,
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="#e0e0e0", zeroline=False)
    return fig


def _build_pie_chart(windows: list[dict]) -> go.Figure | None:
    type_agg: dict[str, int] = {}
    for w in windows:
        for t, cnt in w.get("type_breakdown", {}).items():
            type_agg[t] = type_agg.get(t, 0) + cnt
    if not type_agg:
        return None

    sorted_types = sorted(type_agg.items(), key=lambda x: -x[1])[:8]
    n = len(sorted_types)
    grays = [f"hsl(0, 0%, {15 + i * (65 // max(n, 1))}%)" for i in range(n)]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=[t[0] for t in sorted_types],
                values=[t[1] for t in sorted_types],
                hole=0.45,
                textposition="inside",
                textinfo="label+percent",
                marker=dict(colors=grays, line=dict(color="#000", width=2)),
            )
        ]
    )
    fig.update_layout(
        title="Event Types",
        height=350,
        margin=dict(t=40, b=10, l=10, r=10),
        showlegend=False,
        font=dict(family="Space Mono, monospace"),
    )
    return fig


def _build_gauge(throughput: float) -> go.Figure:
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=throughput,
            title={"text": "Throughput (e/s)"},
            number={"font": {"family": "Space Mono, monospace"}},
            gauge={
                "axis": {"range": [0, max(500, throughput * 1.5)]},
                "bar": {"color": "#000000"},
                "steps": [
                    {"range": [0, max(500, throughput * 1.5)], "color": "#f0f0f0"},
                ],
                "borderwidth": 2,
                "bordercolor": "#000000",
            },
        )
    )
    fig.update_layout(
        height=350, margin=dict(t=60, b=10, l=30, r=30),
        font=dict(family="Space Mono, monospace"),
    )
    return fig


# ── Config Panel ─────────────────────────────────────────────────────────
def _render_config_panel():
    st.markdown("### Configuration")

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        window_size = st.slider("Window Size (sec)", 3, 15, 5)
    with col_b:
        duration = st.slider("Duration (sec)", 10, 120, 30)
    with col_c:
        interval_ms = st.slider("Producer Interval (ms)", 20, 200, 50)

    start_clicked = st.button(
        "Start Simulation", type="primary", use_container_width=True
    )
    st.divider()
    return window_size, duration, interval_ms, start_clicked


# ── Live Render ──────────────────────────────────────────────────────────
def _render_live_frame(engine: SimulationEngine):
    metrics = engine.get_dashboard_metrics()
    elapsed = metrics["elapsed_time"]
    duration = metrics["duration"]
    progress = min(elapsed / duration, 1.0) if duration else 0

    st.progress(progress, text=f"Simulation running... {elapsed:.1f}s / {duration}s")

    mc1, mc2, mc3, mc4 = st.columns(4)
    mc1.metric("Events Published", f'{metrics["total_produced"]:,}')
    mc2.metric("Events Consumed", f'{metrics["total_consumed"]:,}')
    mc3.metric("Queue Size", f'{metrics["broker_queue_size"]:,}')
    mc4.metric("DLQ Events", metrics["dlq_size"])

    windows = metrics["all_windows"]

    if windows:
        st.markdown("### Live Charts")

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.plotly_chart(_build_line_chart(windows), width="stretch", key="sim_line")
        with chart_col2:
            st.plotly_chart(_build_bar_chart(windows), width="stretch", key="sim_bar")

        chart_col3, chart_col4 = st.columns(2)
        with chart_col3:
            pie_fig = _build_pie_chart(windows)
            if pie_fig:
                st.plotly_chart(pie_fig, width="stretch", key="sim_pie")
        with chart_col4:
            tp = metrics.get("throughput", 0)
            st.plotly_chart(
                _build_gauge(tp), width="stretch", key="sim_gauge"
            )

        st.markdown("### Window History")
        df = pd.DataFrame(windows)
        display_cols = [c for c in df.columns if "breakdown" not in c]
        st.dataframe(df[display_cols], width="stretch", hide_index=True)


# ── Completed summary ────────────────────────────────────────────────────
def _render_completed_summary():
    results = st.session_state.get("sim_results", {})
    if not results:
        return

    st.markdown(
        """
        <div style="
            background: #000; color: #fff;
            border: 2px solid #000;
            padding: 1.2rem 1.5rem;
            margin-bottom: 1rem;
        ">
            <h3 style="margin:0; color: #fff; font-family: 'Space Mono', monospace;
                        font-size: 0.9rem; text-transform: uppercase;
                        letter-spacing: 2px;">
                Simulation Complete
            </h3>
            <p style="margin:0.3rem 0 0; color: #aaa; font-size:0.8rem;
                      font-family: 'Space Mono', monospace;">
                View results or run another simulation.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events Published", f'{results.get("total_produced", 0):,}')
    c2.metric("Events Consumed", f'{results.get("total_consumed", 0):,}')
    c3.metric("Windows Captured", f'{results.get("windows_captured", 0):,}')
    c4.metric("DLQ Events", results.get("dlq_size", 0))

    col1, col2 = st.columns(2)
    with col1:
        if st.button("View Full Results", type="primary", use_container_width=True):
            st.session_state.current_page = "results"
            st.rerun()
    with col2:
        if st.button("Run Another Simulation", use_container_width=True):
            st.session_state.sim_complete = False
            st.session_state.sim_running = False
            st.session_state.pop("sim_engine", None)
            st.rerun()


# ── Public entry point ───────────────────────────────────────────────────
def show_simulation_page():
    st.markdown("## Live Simulation")

    # ── CASE 1: Simulation is in progress ─────────────────────────────
    if st.session_state.get("sim_running"):
        engine: SimulationEngine = st.session_state["sim_engine"]

        if not engine.is_complete():
            engine.advance(_BATCH_SIZE)
            _render_live_frame(engine)

            interval_ms = st.session_state.get("sim_interval_ms", 50)
            batch_time = _BATCH_SIZE * (interval_ms / 1000.0)
            time.sleep(batch_time)
            st.rerun()
        else:
            st.session_state.sim_running = False
            st.session_state.sim_complete = True
            st.session_state.sim_results = engine.get_final_results()

            st.balloons()
            _render_live_frame(engine)

            col1, col2 = st.columns(2)
            with col1:
                if st.button(
                    "View Full Results",
                    type="primary",
                    use_container_width=True,
                ):
                    st.session_state.current_page = "results"
                    st.rerun()
            with col2:
                st.download_button(
                    "Download JSON",
                    data=engine.export_json(),
                    file_name="analytics_output.json",
                    mime="application/json",
                    use_container_width=True,
                )
        return

    # ── CASE 2: Show config / completed summary ──────────────────────
    window_size, duration, interval_ms, start_clicked = _render_config_panel()

    if st.session_state.get("sim_complete"):
        _render_completed_summary()

    if start_clicked:
        engine = SimulationEngine(
            window_size=window_size,
            duration=duration,
            interval_ms=interval_ms,
        )
        st.session_state.sim_engine = engine
        st.session_state.sim_running = True
        st.session_state.sim_complete = False
        st.session_state.sim_results = None
        st.session_state.sim_interval_ms = interval_ms
        st.rerun()
