# InsightX — Real-Time Analytics System

A simulation of a production-grade real-time analytics pipeline, designed as part of the **System Design Final Examination**. InsightX processes millions of streaming events from multiple data sources, aggregates metrics using tumbling time windows, and outputs live analytics dashboards — mirroring systems used by companies like Google, Uber, and Netflix.

---

## Project Overview

InsightX demonstrates a complete real-time data pipeline with:

- **Multi-source data producers** — web, mobile, IoT sensors, payment gateway, CDN logs
- **In-memory message broker** — simulates Apache Kafka with per-source topic queues
- **Parallel stream processors** — multi-threaded consumers with retry and fault tolerance
- **Tumbling window aggregation** — count, avg, min, max per 5-second window
- **Dual-layer storage** — hot store (Redis-like dict) + cold store (Cassandra-like deque)
- **Live terminal dashboard** — real-time metrics printed every 5 seconds
- **Dead-letter queue** — failed events captured after max retries
- **JSON export** — full analytics output written to `analytics_output.json`

---

## System Architecture

```
[Web App]  ─┐
[Mobile]   ─┤                                      ┌─ Redis (Hot)  ─┐
[IoT]      ─┼──> Kafka Broker ──> Flink Processor ─┤                ├──> Grafana
[Payments] ─┤    (Queue)          (Windowing)       └─ Cassandra     ─┘   Dashboard
[CDN Logs] ─┘                          │
                                  Dead-Letter Q
```

See `architecture_diagram.png` for the detailed visual diagram.

---

## Technology Stack

| Component        | Simulated By          | Real-World Equivalent         |
|------------------|-----------------------|-------------------------------|
| Message Broker   | `queue.Queue`         | Apache Kafka                  |
| Stream Processor | `threading.Thread`    | Apache Flink / Spark Streaming|
| Hot Storage      | Python `dict`         | Redis                         |
| Cold Storage     | `collections.deque`   | Apache Cassandra              |
| Dashboard        | Terminal print loop   | Grafana                       |
| Orchestration    | Python threading      | Kubernetes + Docker           |

---

## Dependencies

Python 3.8+ is required. The simulation uses only the **Python standard library** — no external packages needed to run `main.py`.

```
python >= 3.8
queue       (stdlib)
threading   (stdlib)
collections (stdlib)
dataclasses (stdlib)
json        (stdlib)
hashlib     (stdlib)
logging     (stdlib)
random      (stdlib)
time        (stdlib)
datetime    (stdlib)
```

To generate the architecture diagram (optional):
```bash
pip install matplotlib
```

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/insightx-realtime-analytics.git
cd insightx-realtime-analytics
```

### 2. (Optional) Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate        # macOS / Linux
venv\Scripts\activate           # Windows
```

### 3. Install Optional Dependencies

```bash
pip install matplotlib           # only needed for architecture diagram
```

---

## Execution Steps

### Run the Main Simulation

```bash
python main.py
```

This will:
1. Start 5 producer threads (one per data source)
2. Start 2 parallel consumer/processor threads
3. Print a live dashboard every 5 seconds for 30 seconds
4. Export final results to `analytics_output.json`

### Expected Output

```
==================================================================
   InsightX Real-Time Analytics System — Starting Simulation
   Duration: 30s | Window: 5s | Consumers: 2
==================================================================

12:56:33 [Producer-web] INFO: Producer started for source: web
...

╔════════════════════════════════════════════════════════════════╗
║  InsightX Real-Time Analytics Dashboard  [12:56:38]          ║
╠════════════════════════════════════════════════════════════════╣
║  LIVE METRICS                                                ║
║    Total Events Processed : 477                              ║
║    Total Value Ingested   : 247547.41                        ║
...
```

### Generate Architecture Diagram

```bash
python gen_diagram.py
# Output: architecture_diagram.png
```

### Configuration

Edit `Config` class in `main.py` to adjust simulation parameters:

```python
class Config:
    WINDOW_SIZE_SEC      = 5     # tumbling window size in seconds
    SIMULATION_DURATION  = 30    # total run time in seconds
    CONSUMER_THREADS     = 2     # number of parallel stream processors
    PRODUCER_INTERVAL    = 0.05  # seconds between producer bursts
    MAX_RETRIES          = 3     # retries before dead-letter queue
```

---

## Project Structure

```
insightx-realtime-analytics/
│
├── main.py                   # Complete simulation (producers, broker, processor, dashboard)
├── gen_diagram.py            # Architecture diagram generator
├── architecture_diagram.png  # System design diagram
├── analytics_output.json     # Auto-generated simulation results
├── README.md                 # This file
└── documentation.pdf         # Full project documentation
```

---

## Key Concepts Demonstrated

| Concept               | Implementation in Code                              |
|-----------------------|-----------------------------------------------------|
| Event-driven pipeline | `DataProducer` → `MessageBroker` → `StreamProcessor`|
| Tumbling windows      | `WindowMetrics` class, 5-second non-overlapping     |
| Fault tolerance       | Retry logic + Dead-Letter Queue in `StreamProcessor`|
| Horizontal scaling    | Multiple `Processor` threads (configurable)         |
| Dual-layer storage    | `AnalyticsStore` with hot dict + cold deque         |
| Thread safety         | `threading.Lock` / `RLock` on all shared state      |

---

## GitHub Repository

> **https://github.com/YOUR_USERNAME/insightx-realtime-analytics**

*(Replace with your actual GitHub repository URL after uploading)*

---

## Author

Submitted as part of the **System Design Final Examination**
Subject: System Design | Real-Time Analytics Systems
