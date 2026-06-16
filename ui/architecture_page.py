"""
InsightX Architecture Page — Brutalist B&W edition.
Uses diagrams from the Diagram folder for each architecture layer.
"""

import os

import pandas as pd
import streamlit as st


_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DIAGRAM_DIR = os.path.join(_BASE_DIR, "assets", "diagrams")


def show_architecture_page():

    st.markdown("## Architecture")
    st.markdown(
        '<p style="font-family: Space Mono, monospace; font-size: 0.8rem; '
        'color: #888; text-transform: uppercase; letter-spacing: 1px;">'
        "End-to-end streaming analytics pipeline</p>",
        unsafe_allow_html=True,
    )

    # ── Full Pipeline ────────────────────────────────────────────────
    full_pipeline = os.path.join(_DIAGRAM_DIR, "Full Pipeline.png")
    if os.path.isfile(full_pipeline):
        st.image(
            full_pipeline,
            caption="Full Pipeline — End-to-End System Architecture",
            use_container_width=True,
        )
    else:
        # Fallback to old diagram
        old_diagram = os.path.join(_BASE_DIR, "architecture_diagram.png")
        if os.path.isfile(old_diagram):
            st.image(old_diagram, caption="System Architecture", use_container_width=True)

    st.divider()

    st.markdown("### Layers")

    # ── Layer 1 — Ingestion ──────────────────────────────────────────
    with st.expander("Layer 1 — Ingestion Layer", expanded=False):
        ingestion_img = os.path.join(_DIAGRAM_DIR, "Ingestion.png")
        if os.path.isfile(ingestion_img):
            st.image(ingestion_img, caption="Ingestion Layer", use_container_width=True)
        st.markdown(
            """
            Five heterogeneous data sources publish events to a simulated
            **Apache Kafka** broker:

            | Source | Description | Typical Events |
            |--------|-------------|----------------|
            | **Web** | Browser click-streams | `page_view`, `click`, `form_submit` |
            | **Mobile** | App telemetry | `screen_view`, `gesture`, `crash` |
            | **IoT Sensor** | Sensor readings | `reading`, `alert`, `heartbeat` |
            | **Payment** | Transactions | `payment`, `refund`, `chargeback` |
            | **CDN Log** | Edge-node events | `cache_hit`, `cache_miss`, `error` |

            The broker provides topic-based routing, back-pressure, Dead Letter
            Queue (DLQ), and at-least-once delivery semantics.
            """
        )

    # ── Layer 2 — Stream Processing ──────────────────────────────────
    with st.expander("Layer 2 — Stream Processing & Windowing", expanded=False):
        processing_img = os.path.join(_DIAGRAM_DIR, "Stream processing & windowing.png")
        if os.path.isfile(processing_img):
            st.image(processing_img, caption="Stream Processing & Windowing", use_container_width=True)
        st.markdown(
            """
            Simulated **Apache Flink** stream processor:
            - **Tumbling windows** — non-overlapping, fixed-size (3–15 sec configurable)
            - **Per-window aggregation** — event count, source/type breakdown, latency stats
            - **Window flush** — emits `WindowMetrics` downstream and resets accumulator
            """
        )

    # ── Layer 3 — Storage ────────────────────────────────────────────
    with st.expander("Layer 3 — Storage Design (Polyglot Persistence)", expanded=False):
        storage_img = os.path.join(_DIAGRAM_DIR, "Storage design — polyglot persistence.png")
        if os.path.isfile(storage_img):
            st.image(storage_img, caption="Storage Design — Polyglot Persistence", use_container_width=True)
        st.markdown(
            """
            | Store | Role | Simulation |
            |-------|------|------------|
            | **Redis** | Hot store — latest N windows | In-memory dict with eviction |
            | **Cassandra** | Cold store — full history | Append-only session list |
            """
        )

    # ── Layer 4 — Scaling ────────────────────────────────────────────
    with st.expander("Layer 4 — Scaling Strategy", expanded=False):
        scaling_img = os.path.join(_DIAGRAM_DIR, "Scaliing.png")
        if os.path.isfile(scaling_img):
            st.image(scaling_img, caption="Scaling Strategy", use_container_width=True)
        st.markdown(
            """
            Horizontal scaling at each layer:
            - **Producers** — independent scaling per data source
            - **Kafka** — partition-level parallelism across brokers
            - **Flink** — parallel operators per partition with checkpointing
            - **Storage** — Redis cluster sharding, Cassandra ring topology
            """
        )

    # ── Layer 5 — Fault Tolerance ────────────────────────────────────
    with st.expander("Layer 5 — Fault Tolerance & Recovery", expanded=False):
        fault_img = os.path.join(_DIAGRAM_DIR, "Fault tolerance & failure recovery.png")
        if os.path.isfile(fault_img):
            st.image(fault_img, caption="Fault Tolerance & Failure Recovery", use_container_width=True)
        st.markdown(
            """
            - **Kafka** — replication factor ensures broker failure tolerance
            - **Flink** — periodic checkpoints enable exactly-once recovery
            - **DLQ** — poison-pill events diverted without blocking the pipeline
            - **Storage** — Redis AOF persistence, Cassandra multi-DC replication
            """
        )

    st.divider()

    # ── Technology Stack ─────────────────────────────────────────────
    st.markdown("### Technology Stack")

    tech_data = [
        {"Layer": "Producers", "Simulated": "Python generators", "Production": "Application SDKs"},
        {"Layer": "Ingestion", "Simulated": "In-memory queue", "Production": "Apache Kafka"},
        {"Layer": "Processing", "Simulated": "Window engine", "Production": "Apache Flink"},
        {"Layer": "Hot Storage", "Simulated": "Python dict", "Production": "Redis"},
        {"Layer": "Cold Storage", "Simulated": "Python list", "Production": "Cassandra"},
        {"Layer": "Dashboard", "Simulated": "Streamlit + Plotly", "Production": "Grafana"},
        {"Layer": "Orchestration", "Simulated": "Single process", "Production": "Kubernetes"},
    ]

    st.dataframe(pd.DataFrame(tech_data), width="stretch", hide_index=True)
