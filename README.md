# InsightX — Real-Time Streaming Analytics Platform

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://insightx-8vcaart3xxzozeqfhfnf6y.streamlit.app/)

**[Live Demo](https://insightx-8vcaart3xxzozeqfhfnf6y.streamlit.app/)**

A simulation of a production-grade real-time analytics pipeline, built as part of the **System Design Final Examination**. InsightX processes streaming events from multiple heterogeneous data sources, aggregates metrics using tumbling time windows, and visualises everything on an interactive Streamlit dashboard.

---

## Overview

InsightX demonstrates a complete real-time data pipeline with:

- **Multi-source data producers** — web, mobile, IoT sensors, payment gateway, CDN logs
- **Simulated Kafka broker** — in-memory message broker with per-source topic queues and back-pressure
- **Flink-style stream processing** — tumbling window aggregation (count, avg, min, max) with retry logic
- **Polyglot persistence** — hot store (Redis-like) + cold store (Cassandra-like)
- **Dead-letter queue** — failed events captured after max retries
- **Interactive dashboard** — real-time charts, gauges, and window history via Streamlit + Plotly

---

## System Architecture

```
[Web App]  ─┐
[Mobile]   ─┤                                      ┌─ Redis (Hot)  ─┐
[IoT]      ─┼──> Kafka Broker ──> Flink Processor ─┤                ├──> Dashboard
[Payments] ─┤    (Queue)          (Windowing)       └─ Cassandra    ─┘   (Streamlit)
[CDN Logs] ─┘                          │
                                  Dead-Letter Q
```

Architecture diagrams are available in `assets/diagrams/` and on the Architecture page of the dashboard.

---

## Technology Stack

| Component        | Simulated By          | Production Equivalent          |
|------------------|-----------------------|--------------------------------|
| Message Broker   | `queue.Queue`         | Apache Kafka 3.x               |
| Stream Processor | `threading.Thread`    | Apache Flink 1.18              |
| Hot Storage      | Python `dict`         | Redis 7 Cluster                |
| Cold Storage     | `collections.deque`   | Apache Cassandra 4             |
| Dashboard        | Streamlit + Plotly    | Grafana 10 + Prometheus        |
| Orchestration    | Single process        | Kubernetes + Docker            |

---

## Setup

### Prerequisites

- Python 3.10+

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run the Dashboard

```bash
streamlit run app.py
```

### Run the CLI Simulation (no dependencies)

```bash
python main.py
```

---

## Dashboard Pages

| Page | Description |
|------|-------------|
| **Home** | System overview, quick-start metrics, component cards |
| **Simulation** | Live simulation with real-time charts, throughput gauge, window history |
| **Results** | Post-run analysis with JSON/CSV export |
| **Architecture** | Interactive architecture viewer with embedded diagrams |

---

## Configuration

Adjust simulation parameters from the dashboard sidebar, or edit `config.py`:

| Parameter | Range | Default | Description |
|-----------|-------|---------|-------------|
| Window Size | 3–15 sec | 5 sec | Tumbling window duration |
| Duration | 10–120 sec | 30 sec | Total simulation run time |
| Producer Interval | 20–200 ms | 50 ms | Time between event bursts |

---

## Project Structure

```
InsightX_Analytics_Submission/
├── app.py                          # Streamlit entry point
├── config.py                       # Simulation constants
├── main.py                         # CLI simulation (stdlib only)
├── requirements.txt                # streamlit, plotly, pandas
├── README.md
├── documentation.pdf               # Full system design documentation
├── analytics_output.json           # Sample simulation output
├── .streamlit/
│   └── config.toml                 # Theme configuration
├── assets/
│   ├── style.css                   # Dashboard styling
│   ├── screenshot_home.png
│   ├── screenshot_simulation.png
│   └── diagrams/
│       ├── Full Pipeline.png
│       ├── Ingestion.png
│       ├── Stream processing & windowing.png
│       ├── Storage design — polyglot persistence.png
│       ├── Scaliing.png
│       └── Fault tolerance & failure recovery.png
├── core/
│   ├── __init__.py
│   ├── broker.py                   # MessageBroker (Kafka simulation)
│   ├── engine.py                   # SimulationEngine (orchestrator)
│   └── models.py                   # Event, WindowMetrics dataclasses
└── ui/
    ├── __init__.py
    ├── home_page.py
    ├── simulation_page.py
    ├── results_page.py
    └── architecture_page.py
```

---

## Deploy on Streamlit Cloud

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repo
4. Set **Main file path** to `app.py`
5. Deploy

---

## Key Concepts

| Concept               | Implementation                                       |
|-----------------------|------------------------------------------------------|
| Event-driven pipeline | `DataProducer` → `MessageBroker` → `StreamProcessor` |
| Tumbling windows      | `WindowMetrics` — 5s non-overlapping windows          |
| Fault tolerance       | Retry logic + Dead-Letter Queue                      |
| Polyglot persistence  | Hot dict (Redis) + cold deque (Cassandra)            |
| Thread safety         | `threading.RLock` on all shared state                |

---

Submitted as part of the **System Design Final Examination**
