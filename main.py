"""
InsightX - Real-Time Analytics System Simulation
=================================================
Simulates a real-time streaming analytics pipeline using:
  - Multi-threaded producers generating live events
  - A shared queue acting as the message broker (Kafka-like)
  - A stream processor consuming and aggregating events
  - A live dashboard printing metrics every 5 seconds
  - Fault tolerance via dead-letter queue and retry logic

Author : InsightX System Design Project
"""

import queue
import threading
import time
import random
import json
import hashlib
import logging
from datetime import datetime
from collections import defaultdict, deque
from dataclasses import dataclass, asdict, field
from typing import Optional

# ─────────────────────────────────────────────
# Logging Configuration
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("InsightX")


# ─────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────
@dataclass
class Event:
    """Represents a single streaming event from any data source."""
    event_id: str
    source: str          # e.g. "web", "mobile", "iot_sensor", "payment"
    event_type: str      # e.g. "click", "transaction", "log_error", "sensor_reading"
    value: float         # numeric payload (amount, count, temperature, etc.)
    timestamp: float     # unix epoch
    retries: int = 0     # fault-tolerance retry counter

    @staticmethod
    def generate(source: str, event_type: str) -> "Event":
        ts = time.time()
        raw = f"{source}{event_type}{ts}{random.random()}"
        eid = hashlib.md5(raw.encode()).hexdigest()[:10]
        value = round(random.uniform(1.0, 1000.0), 2)
        return Event(event_id=eid, source=source, event_type=event_type,
                     value=value, timestamp=ts)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class WindowMetrics:
    """Aggregated metrics for a time window."""
    window_start: float
    window_end: float
    event_count: int = 0
    total_value: float = 0.0
    min_value: float = float("inf")
    max_value: float = float("-inf")
    source_breakdown: dict = field(default_factory=lambda: defaultdict(int))
    type_breakdown: dict = field(default_factory=lambda: defaultdict(int))

    @property
    def avg_value(self) -> float:
        return round(self.total_value / self.event_count, 2) if self.event_count else 0.0

    def update(self, event: Event):
        self.event_count += 1
        self.total_value += event.value
        self.min_value = min(self.min_value, event.value)
        self.max_value = max(self.max_value, event.value)
        self.source_breakdown[event.source] += 1
        self.type_breakdown[event.event_type] += 1


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
class Config:
    BROKER_MAX_SIZE      = 5000     # max events in the in-memory queue
    WINDOW_SIZE_SEC      = 5        # tumbling window duration (seconds)
    DASHBOARD_INTERVAL   = 5        # how often to print dashboard (seconds)
    PRODUCER_INTERVAL    = 0.05     # seconds between each producer event burst
    SIMULATION_DURATION  = 30       # total run time in seconds
    DEAD_LETTER_MAX_SIZE = 500      # dead-letter queue capacity
    MAX_RETRIES          = 3        # max retries before sending to DLQ
    CONSUMER_THREADS     = 2        # number of parallel consumers

    SOURCES = ["web", "mobile", "iot_sensor", "payment_gateway", "cdn_log"]
    EVENT_TYPES = {
        "web":             ["page_view", "click", "scroll", "form_submit"],
        "mobile":          ["app_open", "tap", "crash_report", "push_click"],
        "iot_sensor":      ["temperature", "humidity", "motion_detect", "battery_low"],
        "payment_gateway": ["transaction", "refund", "chargeback", "auth_check"],
        "cdn_log":         ["cache_hit", "cache_miss", "404_error", "bandwidth_spike"],
    }


# ─────────────────────────────────────────────
# Message Broker (Kafka-like Queue)
# ─────────────────────────────────────────────
class MessageBroker:
    """
    In-memory message broker simulating Apache Kafka.
    Uses Python's thread-safe queue.Queue as the underlying transport.
    Supports multiple topics (one queue per source type).
    """

    def __init__(self):
        self._topics: dict[str, queue.Queue] = {}
        self._dead_letter = queue.Queue(maxsize=Config.DEAD_LETTER_MAX_SIZE)
        self._publish_count = 0
        self._lock = threading.Lock()

    def get_topic(self, topic: str) -> queue.Queue:
        if topic not in self._topics:
            self._topics[topic] = queue.Queue(maxsize=Config.BROKER_MAX_SIZE)
        return self._topics[topic]

    def publish(self, event: Event) -> bool:
        """Publish an event to its source topic. Returns False if topic is full."""
        topic = self.get_topic(event.source)
        try:
            topic.put_nowait(event)
            with self._lock:
                self._publish_count += 1
            return True
        except queue.Full:
            logger.warning(f"Topic '{event.source}' is full — dropping event {event.event_id}")
            return False

    def consume(self, topic: str, timeout: float = 0.1) -> Optional[Event]:
        """Consume one event from a topic. Returns None if empty."""
        try:
            return self.get_topic(topic).get(timeout=timeout)
        except queue.Empty:
            return None

    def send_to_dlq(self, event: Event):
        """Send a failed event to the dead-letter queue."""
        try:
            self._dead_letter.put_nowait(event)
            logger.error(f"Event {event.event_id} sent to Dead-Letter Queue after {event.retries} retries.")
        except queue.Full:
            logger.critical("Dead-letter queue is full. Event permanently lost.")

    def dlq_size(self) -> int:
        return self._dead_letter.qsize()

    def total_published(self) -> int:
        with self._lock:
            return self._publish_count

    def all_topics_size(self) -> int:
        return sum(q.qsize() for q in self._topics.values())


# ─────────────────────────────────────────────
# Producer (Data Source Simulator)
# ─────────────────────────────────────────────
class DataProducer(threading.Thread):
    """
    Simulates a real-world data source publishing events to the broker.
    Each producer is assigned a specific source (e.g. "web", "mobile").
    Runs as a daemon thread and publishes events at Config.PRODUCER_INTERVAL.
    """

    def __init__(self, source: str, broker: MessageBroker, stop_event: threading.Event):
        super().__init__(name=f"Producer-{source}", daemon=True)
        self.source = source
        self.broker = broker
        self.stop_event = stop_event
        self.events_produced = 0

    def run(self):
        event_types = Config.EVENT_TYPES.get(self.source, ["generic_event"])
        logger.info(f"Producer started for source: {self.source}")

        while not self.stop_event.is_set():
            # Burst: produce 1–5 events per tick to simulate traffic spikes
            burst = random.randint(1, 5)
            for _ in range(burst):
                etype = random.choice(event_types)
                event = Event.generate(self.source, etype)
                if self.broker.publish(event):
                    self.events_produced += 1
            time.sleep(Config.PRODUCER_INTERVAL)

        logger.info(f"Producer [{self.source}] stopped. Total produced: {self.events_produced}")


# ─────────────────────────────────────────────
# Stream Processor (Consumer + Aggregator)
# ─────────────────────────────────────────────
class StreamProcessor(threading.Thread):
    """
    Consumes events from broker topics and aggregates them into
    tumbling time windows. Supports fault tolerance via retry logic.

    Window Strategy: Tumbling Window
      - Fixed-duration, non-overlapping windows (e.g. every 5 seconds)
      - At window close: finalize metrics and push to analytics store
    """

    def __init__(self, processor_id: int, broker: MessageBroker,
                 analytics_store: "AnalyticsStore", stop_event: threading.Event):
        super().__init__(name=f"Processor-{processor_id}", daemon=True)
        self.processor_id = processor_id
        self.broker = broker
        self.store = analytics_store
        self.stop_event = stop_event
        self.events_processed = 0
        self._current_window: Optional[WindowMetrics] = None
        self._window_lock = threading.Lock()

    def _get_or_create_window(self, ts: float) -> WindowMetrics:
        """Return current window or create a new one if expired."""
        if self._current_window is None:
            window_start = ts - (ts % Config.WINDOW_SIZE_SEC)
            window_end = window_start + Config.WINDOW_SIZE_SEC
            self._current_window = WindowMetrics(window_start, window_end)
        elif ts >= self._current_window.window_end:
            # Window expired → finalize and push to store
            self.store.commit_window(self._current_window)
            window_start = ts - (ts % Config.WINDOW_SIZE_SEC)
            window_end = window_start + Config.WINDOW_SIZE_SEC
            self._current_window = WindowMetrics(window_start, window_end)
        return self._current_window

    def _process_event(self, event: Event):
        """Apply transformation + aggregation logic to a single event."""
        # Simulate occasional processing failure for fault-tolerance demo
        if random.random() < 0.02:   # 2% failure rate
            raise RuntimeError(f"Simulated processing error for event {event.event_id}")

        with self._window_lock:
            window = self._get_or_create_window(event.timestamp)
            window.update(event)
        self.events_processed += 1

    def run(self):
        logger.info(f"StreamProcessor-{self.processor_id} started.")
        # Each processor handles a subset of sources (round-robin partition)
        assigned_sources = [
            s for i, s in enumerate(Config.SOURCES)
            if i % Config.CONSUMER_THREADS == self.processor_id
        ]
        logger.info(f"Processor-{self.processor_id} assigned topics: {assigned_sources}")

        while not self.stop_event.is_set():
            processed_any = False
            for source in assigned_sources:
                event = self.broker.consume(source, timeout=0.05)
                if event:
                    processed_any = True
                    try:
                        self._process_event(event)
                    except RuntimeError as e:
                        # Retry logic
                        event.retries += 1
                        if event.retries <= Config.MAX_RETRIES:
                            self.broker.publish(event)   # requeue
                        else:
                            self.broker.send_to_dlq(event)
            if not processed_any:
                time.sleep(0.01)

        # Flush remaining window on shutdown
        if self._current_window and self._current_window.event_count > 0:
            self.store.commit_window(self._current_window)

        logger.info(f"Processor-{self.processor_id} stopped. Events processed: {self.events_processed}")


# ─────────────────────────────────────────────
# Analytics Store (Dual-Layer Storage)
# ─────────────────────────────────────────────
class AnalyticsStore:
    """
    Simulates a two-tier storage system:
      - Hot store (in-memory dict) → Redis equivalent for live queries
      - Cold store (deque of window snapshots) → Cassandra/PostgreSQL equivalent
    Thread-safe via RLock.
    """

    def __init__(self):
        self._hot: dict[str, float] = {        # live running counters
            "total_events": 0,
            "total_value":  0.0,
            "error_count":  0,
        }
        self._cold: deque[dict] = deque(maxlen=1000)   # historical windows
        self._lock = threading.RLock()

    def commit_window(self, window: WindowMetrics):
        """Finalize a closed window: update hot store + write to cold store."""
        with self._lock:
            self._hot["total_events"] += window.event_count
            self._hot["total_value"]  += window.total_value

            snapshot = {
                "window_start":      datetime.fromtimestamp(window.window_start).strftime("%H:%M:%S"),
                "window_end":        datetime.fromtimestamp(window.window_end).strftime("%H:%M:%S"),
                "event_count":       window.event_count,
                "avg_value":         window.avg_value,
                "min_value":         round(window.min_value, 2) if window.event_count else 0,
                "max_value":         round(window.max_value, 2) if window.event_count else 0,
                "source_breakdown":  dict(window.source_breakdown),
                "type_breakdown":    dict(window.type_breakdown),
            }
            self._cold.append(snapshot)

    def get_live_metrics(self) -> dict:
        with self._lock:
            return dict(self._hot)

    def get_recent_windows(self, n: int = 3) -> list[dict]:
        with self._lock:
            return list(self._cold)[-n:]

    def get_all_windows(self) -> list[dict]:
        with self._lock:
            return list(self._cold)


# ─────────────────────────────────────────────
# Live Dashboard
# ─────────────────────────────────────────────
class LiveDashboard(threading.Thread):
    """
    Prints a real-time analytics dashboard to the terminal every
    Config.DASHBOARD_INTERVAL seconds. Simulates a visualization layer
    like Grafana or a web dashboard.
    """

    def __init__(self, store: AnalyticsStore, broker: MessageBroker,
                 processors: list, stop_event: threading.Event):
        super().__init__(name="Dashboard", daemon=True)
        self.store = store
        self.broker = broker
        self.processors = processors
        self.stop_event = stop_event

    def _render(self):
        live = self.store.get_live_metrics()
        windows = self.store.get_recent_windows(n=3)
        now = datetime.now().strftime("%H:%M:%S")

        border = "═" * 64
        print(f"\n╔{border}╗")
        print(f"║  📊  InsightX Real-Time Analytics Dashboard  [{now}]  ║")
        print(f"╠{border}╣")

        # Live global counters
        print(f"║  LIVE METRICS                                                ║")
        print(f"║    Total Events Processed : {live['total_events']:<10}                    ║")
        print(f"║    Total Value Ingested   : {round(live['total_value'],2):<14}                ║")
        print(f"║    Broker Queue Size      : {self.broker.all_topics_size():<10}                    ║")
        print(f"║    Dead-Letter Queue      : {self.broker.dlq_size():<10}                    ║")

        # Per-processor stats
        total_consumed = sum(p.events_processed for p in self.processors)
        print(f"║    Events Consumed        : {total_consumed:<10}                    ║")

        # Recent windows
        print(f"╠{border}╣")
        print(f"║  RECENT TUMBLING WINDOWS ({Config.WINDOW_SIZE_SEC}s each)                        ║")
        if not windows:
            print(f"║    (no windows closed yet)                                   ║")
        for w in windows:
            print(f"╠{border}╣")
            print(f"║  Window {w['window_start']} → {w['window_end']}                            ║")
            print(f"║    Events : {w['event_count']:<6}  Avg Value : {w['avg_value']:<8}  "
                  f"Min : {w['min_value']:<7}  Max : {w['max_value']:<6}║")
            # Source breakdown
            src_str = "  ".join(f"{k}:{v}" for k, v in w['source_breakdown'].items())
            print(f"║    Sources  → {src_str[:48]:<48}║")
            # Type breakdown (top 3)
            top_types = sorted(w['type_breakdown'].items(), key=lambda x: -x[1])[:3]
            typ_str = "  ".join(f"{k}:{v}" for k, v in top_types)
            print(f"║    Top Types→ {typ_str[:48]:<48}║")

        print(f"╚{border}╝")

    def run(self):
        time.sleep(Config.WINDOW_SIZE_SEC)  # wait for first window to close
        while not self.stop_event.is_set():
            self._render()
            time.sleep(Config.DASHBOARD_INTERVAL)


# ─────────────────────────────────────────────
# Analytics Export (simulate DB write)
# ─────────────────────────────────────────────
def export_results(store: AnalyticsStore, broker: MessageBroker,
                   producers: list, processors: list):
    """Dump final aggregated metrics to a JSON file (simulating DB export)."""
    windows = store.get_all_windows()
    summary = {
        "simulation_end":    datetime.now().isoformat(),
        "total_published":   broker.total_published(),
        "total_consumed":    sum(p.events_processed for p in processors),
        "total_produced":    sum(p.events_produced  for p in producers),
        "dlq_events":        broker.dlq_size(),
        "windows_captured":  len(windows),
        "live_metrics":      store.get_live_metrics(),
        "window_history":    windows,
    }
    out_path = "analytics_output.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Results exported to {out_path}")
    return summary


# ─────────────────────────────────────────────
# Main Orchestrator
# ─────────────────────────────────────────────
def main():
    print("\n" + "=" * 66)
    print("   InsightX Real-Time Analytics System — Starting Simulation")
    print(f"   Duration: {Config.SIMULATION_DURATION}s | Window: {Config.WINDOW_SIZE_SEC}s | "
          f"Consumers: {Config.CONSUMER_THREADS}")
    print("=" * 66 + "\n")

    stop_event = threading.Event()

    # 1. Broker (Kafka equivalent)
    broker = MessageBroker()

    # 2. Analytics Store (Redis + Cassandra equivalent)
    store = AnalyticsStore()

    # 3. Producers — one per source
    producers = [
        DataProducer(source, broker, stop_event)
        for source in Config.SOURCES
    ]

    # 4. Stream Processors — parallel consumers
    processors = [
        StreamProcessor(i, broker, store, stop_event)
        for i in range(Config.CONSUMER_THREADS)
    ]

    # 5. Live Dashboard
    dashboard = LiveDashboard(store, broker, processors, stop_event)

    # ── Start all threads ──
    for p in producers:
        p.start()
    for p in processors:
        p.start()
    dashboard.start()

    logger.info(f"All threads started. Simulation running for {Config.SIMULATION_DURATION} seconds...")

    # ── Run for configured duration ──
    time.sleep(Config.SIMULATION_DURATION)

    # ── Graceful shutdown ──
    logger.info("Stopping simulation...")
    stop_event.set()

    for p in producers:
        p.join(timeout=2)
    for p in processors:
        p.join(timeout=3)
    dashboard.join(timeout=2)

    # ── Final dashboard render ──
    dashboard._render()

    # ── Export results ──
    summary = export_results(store, broker, producers, processors)

    print("\n" + "=" * 66)
    print("   SIMULATION COMPLETE — Final Summary")
    print("=" * 66)
    print(f"   Events Published  : {summary['total_published']}")
    print(f"   Events Consumed   : {summary['total_consumed']}")
    print(f"   Windows Captured  : {summary['windows_captured']}")
    print(f"   DLQ Events        : {summary['dlq_events']}")
    print(f"   Output JSON       : analytics_output.json")
    print("=" * 66 + "\n")


if __name__ == "__main__":
    main()
