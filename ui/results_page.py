"""
InsightX Results Page — Brutalist B&W edition.
"""

import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


_CHART_LAYOUT = dict(
    plot_bgcolor="#ffffff",
    paper_bgcolor="#ffffff",
    font=dict(family="Space Mono, Courier New, monospace", color="#000000", size=11),
)

_BW_COLORS = ["#000000", "#444444", "#777777", "#aaaaaa", "#cccccc"]


def show_results_page():
    results = st.session_state.get("sim_results")

    if results is None:
        st.info("No simulation results available yet. Run a simulation first.")
        if st.button("Go to Simulation", type="primary"):
            st.session_state.current_page = "simulation"
            st.rerun()
        return

    st.markdown(
        f"""
        <div style="margin-bottom: 1rem;">
            <h2 style="margin: 0; font-weight: 900; color: #000; text-transform: uppercase;">
                Results
            </h2>
            <p style="margin: 0.3rem 0 0; font-family: 'Space Mono', monospace;
                      font-size: 0.78rem; color: #888; text-transform: uppercase;
                      letter-spacing: 1px;">
                {results.get('total_consumed', 0):,} events /
                {results.get('windows_captured', 0)} windows
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Metrics ──────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Published", f'{results.get("total_produced", 0):,}')
    c2.metric("Consumed", f'{results.get("total_consumed", 0):,}')
    c3.metric("Windows", f'{results.get("windows_captured", 0):,}')
    c4.metric("DLQ", results.get("dlq_size", 0))

    st.divider()

    window_history = results.get("window_history", [])
    if not window_history:
        st.warning("No window history data found.")
        return

    # ── Charts ───────────────────────────────────────────────────────
    st.markdown("### Charts")

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=[w["window_start"] for w in window_history],
                y=[w["event_count"] for w in window_history],
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
            xaxis_title="Window",
            yaxis_title="Count",
            height=380,
            margin=dict(t=40, b=30, l=10, r=10),
            **_CHART_LAYOUT,
        )
        fig.update_xaxes(showgrid=False, zeroline=False)
        fig.update_yaxes(gridcolor="#e0e0e0", zeroline=False)
        st.plotly_chart(fig, width="stretch", key="results_line")

    with chart_col2:
        source_agg: dict[str, int] = {}
        for w in window_history:
            for src, cnt in w.get("source_breakdown", {}).items():
                source_agg[src] = source_agg.get(src, 0) + cnt

        if source_agg:
            fig2 = go.Figure(
                data=[
                    go.Bar(
                        x=list(source_agg.keys()),
                        y=list(source_agg.values()),
                        marker_color="#000000",
                    )
                ]
            )
            fig2.update_layout(
                title="Events by Source",
                height=380,
                margin=dict(t=40, b=30, l=10, r=10),
                **_CHART_LAYOUT,
            )
            fig2.update_xaxes(showgrid=False, zeroline=False)
            fig2.update_yaxes(gridcolor="#e0e0e0", zeroline=False)
            st.plotly_chart(fig2, width="stretch", key="results_bar")

    pie_col1, pie_col2 = st.columns(2)

    with pie_col1:
        type_agg: dict[str, int] = {}
        for w in window_history:
            for t, cnt in w.get("type_breakdown", {}).items():
                type_agg[t] = type_agg.get(t, 0) + cnt

        if type_agg:
            sorted_types = sorted(type_agg.items(), key=lambda x: -x[1])[:10]
            n = len(sorted_types)
            grays = [f"hsl(0, 0%, {15 + i * (65 // max(n, 1))}%)" for i in range(n)]
            fig3 = go.Figure(
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
            fig3.update_layout(
                title="Event Types (Top 10)",
                height=400,
                margin=dict(t=40, b=10, l=10, r=10),
                showlegend=True,
                font=dict(family="Space Mono, monospace"),
            )
            st.plotly_chart(fig3, width="stretch", key="results_pie")

    with pie_col2:
        tp = results.get("avg_throughput", 0)
        fig4 = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=tp,
                title={"text": "Avg Throughput (e/s)"},
                number={"font": {"family": "Space Mono, monospace"}},
                gauge={
                    "axis": {"range": [0, max(500, tp * 1.5)]},
                    "bar": {"color": "#000000"},
                    "steps": [
                        {"range": [0, max(500, tp * 1.5)], "color": "#f0f0f0"},
                    ],
                    "borderwidth": 2,
                    "bordercolor": "#000000",
                },
            )
        )
        fig4.update_layout(
            height=400,
            margin=dict(t=60, b=10, l=30, r=30),
            font=dict(family="Space Mono, monospace"),
        )
        st.plotly_chart(fig4, width="stretch", key="results_gauge")

    st.divider()

    # ── Table ────────────────────────────────────────────────────────
    st.markdown("### Window History")
    df = pd.DataFrame(window_history)
    display_cols = [c for c in df.columns if "breakdown" not in c]
    st.dataframe(df[display_cols], width="stretch", hide_index=True)

    st.divider()

    # ── Export ────────────────────────────────────────────────────────
    st.markdown("### Export")
    exp_col1, exp_col2, exp_col3 = st.columns(3)

    with exp_col1:
        json_str = json.dumps(results, indent=2, default=str)
        st.download_button(
            "Download JSON",
            data=json_str,
            file_name="insightx_results.json",
            mime="application/json",
            use_container_width=True,
        )

    with exp_col2:
        csv_data = df[display_cols].to_csv(index=False)
        st.download_button(
            "Download CSV",
            data=csv_data,
            file_name="insightx_window_history.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with exp_col3:
        if st.button("Run Another", use_container_width=True):
            st.session_state.sim_complete = False
            st.session_state.sim_running = False
            st.session_state.current_page = "simulation"
            st.rerun()
