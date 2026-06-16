"""
InsightX Dashboard Configuration
"""

APP_NAME = "InsightX"
VERSION = "2.0"

# ── Data Sources ──────────────────────────────────────────────────────────────
SOURCES = ["web", "mobile", "iot_sensor", "payment_gateway", "cdn_log"]

EVENT_TYPES = {
    "web":             ["page_view", "click", "scroll", "form_submit"],
    "mobile":          ["app_open", "tap", "crash_report", "push_click"],
    "iot_sensor":      ["temperature", "humidity", "motion_detect", "battery_low"],
    "payment_gateway": ["transaction", "refund", "chargeback", "auth_check"],
    "cdn_log":         ["cache_hit", "cache_miss", "404_error", "bandwidth_spike"],
}

# ── UI Icons ──────────────────────────────────────────────────────────────────
SOURCE_ICONS = {
    "web":             "🌐",
    "mobile":          "📱",
    "iot_sensor":      "🌡️",
    "payment_gateway": "💳",
    "cdn_log":         "📡",
}

# ── Chart Colors ──────────────────────────────────────────────────────────────
SOURCE_COLORS = {
    "web":             "#3b82f6",
    "mobile":          "#8b5cf6",
    "iot_sensor":      "#10b981",
    "payment_gateway": "#f59e0b",
    "cdn_log":         "#ef4444",
}

# ── Component Info (for architecture page) ────────────────────────────────────
COMPONENTS = [
    {
        "name": "Apache Kafka",
        "icon": "📨",
        "role": "Message Broker",
        "simulated_by": "queue.Queue per topic",
        "description": "Distributed event streaming platform. Decouples producers from consumers, "
                       "buffers traffic bursts, and provides durable replayable storage.",
        "color": "#3b82f6",
    },
    {
        "name": "Apache Flink",
        "icon": "⚡",
        "role": "Stream Processor",
        "simulated_by": "threading.Thread (StreamProcessor)",
        "description": "Stateful stream processing engine with tumbling window aggregation, "
                       "exactly-once semantics, and fault-tolerant checkpointing.",
        "color": "#8b5cf6",
    },
    {
        "name": "Redis",
        "icon": "🔴",
        "role": "Hot Storage",
        "simulated_by": "Python dict",
        "description": "In-memory key-value store for sub-millisecond dashboard reads. "
                       "Stores latest window metrics with TTL-based eviction.",
        "color": "#ef4444",
    },
    {
        "name": "Apache Cassandra",
        "icon": "🟤",
        "role": "Cold Storage",
        "simulated_by": "collections.deque",
        "description": "Distributed wide-column store optimized for time-series writes. "
                       "Stores all historical window snapshots for trend analysis.",
        "color": "#f59e0b",
    },
    {
        "name": "Grafana",
        "icon": "📊",
        "role": "Visualization",
        "simulated_by": "Streamlit + Plotly",
        "description": "Real-time analytics dashboard with configurable panels, alerting, "
                       "and multi-datasource support.",
        "color": "#10b981",
    },
]

# ── Technology Stack ──────────────────────────────────────────────────────────
TECH_STACK = [
    {"Component": "Message Broker",   "Simulated By": "queue.Queue",        "Production": "Apache Kafka 3.x"},
    {"Component": "Stream Processor", "Simulated By": "threading.Thread",   "Production": "Apache Flink 1.18"},
    {"Component": "Hot Storage",      "Simulated By": "Python dict",        "Production": "Redis 7 (Cluster)"},
    {"Component": "Cold Storage",     "Simulated By": "collections.deque",  "Production": "Apache Cassandra 4"},
    {"Component": "Dashboard",        "Simulated By": "Streamlit + Plotly", "Production": "Grafana 10"},
    {"Component": "Orchestration",    "Simulated By": "Python threading",   "Production": "Kubernetes + Docker"},
]
