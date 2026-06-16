"""
InsightX Data Models
"""
import time
import random
import hashlib
from dataclasses import dataclass, asdict, field
from collections import defaultdict


@dataclass
class Event:
    """Represents a single streaming event from any data source."""
    event_id: str
    source: str          # e.g. "web", "mobile", "iot_sensor", "payment_gateway"
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
