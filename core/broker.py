"""
InsightX Message Broker & Analytics Store
"""
import queue
import threading
from datetime import datetime
from collections import deque
from typing import Optional

from core.models import Event, WindowMetrics


class MessageBroker:
    """
    In-memory message broker simulating Apache Kafka.
    Uses Python's thread-safe queue.Queue as the underlying transport.
    """

    def __init__(self, broker_max_size: int = 5000, dlq_max_size: int = 500):
        self._topics: dict[str, queue.Queue] = {}
        self._dead_letter = queue.Queue(maxsize=dlq_max_size)
        self._publish_count = 0
        self._lock = threading.Lock()
        self._broker_max_size = broker_max_size

    def get_topic(self, topic: str) -> queue.Queue:
        if topic not in self._topics:
            self._topics[topic] = queue.Queue(maxsize=self._broker_max_size)
        return self._topics[topic]

    def publish(self, event: Event) -> bool:
        topic = self.get_topic(event.source)
        try:
            topic.put_nowait(event)
            with self._lock:
                self._publish_count += 1
            return True
        except queue.Full:
            return False

    def consume(self, topic: str, timeout: float = 0.01) -> Optional[Event]:
        try:
            return self.get_topic(topic).get(timeout=timeout)
        except queue.Empty:
            return None

    def send_to_dlq(self, event: Event):
        try:
            self._dead_letter.put_nowait(event)
        except queue.Full:
            pass

    def dlq_size(self) -> int:
        return self._dead_letter.qsize()

    def total_published(self) -> int:
        with self._lock:
            return self._publish_count

    def all_topics_size(self) -> int:
        return sum(q.qsize() for q in self._topics.values())

    def reset(self):
        self._topics.clear()
        self._dead_letter = queue.Queue(maxsize=500)
        self._publish_count = 0


class AnalyticsStore:
    """
    Two-tier storage: hot dict (Redis) + cold deque (Cassandra).
    """

    def __init__(self):
        self._hot: dict[str, float] = {
            "total_events": 0,
            "total_value": 0.0,
            "error_count": 0,
        }
        self._cold: deque[dict] = deque(maxlen=1000)
        self._lock = threading.RLock()

    def commit_window(self, window: WindowMetrics):
        with self._lock:
            self._hot["total_events"] += window.event_count
            self._hot["total_value"] += window.total_value
            snapshot = {
                "window_start": datetime.fromtimestamp(window.window_start).strftime("%H:%M:%S"),
                "window_end": datetime.fromtimestamp(window.window_end).strftime("%H:%M:%S"),
                "event_count": window.event_count,
                "avg_value": window.avg_value,
                "min_value": round(window.min_value, 2) if window.event_count else 0,
                "max_value": round(window.max_value, 2) if window.event_count else 0,
                "source_breakdown": dict(window.source_breakdown),
                "type_breakdown": dict(window.type_breakdown),
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

    def reset(self):
        self._hot = {"total_events": 0, "total_value": 0.0, "error_count": 0}
        self._cold.clear()
