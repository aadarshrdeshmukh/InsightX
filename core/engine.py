"""
InsightX Synchronous Simulation Engine

Replaces the multi-threaded main.py simulation with a synchronous,
step-based engine compatible with Streamlit's execution model.
"""
import time
import random
import json
from datetime import datetime
from typing import Optional

from core.models import Event, WindowMetrics
from core.broker import MessageBroker, AnalyticsStore


# Default configuration
DEFAULT_SOURCES = ["web", "mobile", "iot_sensor", "payment_gateway", "cdn_log"]
DEFAULT_EVENT_TYPES = {
    "web":             ["page_view", "click", "scroll", "form_submit"],
    "mobile":          ["app_open", "tap", "crash_report", "push_click"],
    "iot_sensor":      ["temperature", "humidity", "motion_detect", "battery_low"],
    "payment_gateway": ["transaction", "refund", "chargeback", "auth_check"],
    "cdn_log":         ["cache_hit", "cache_miss", "404_error", "bandwidth_spike"],
}


class SimulationEngine:
    """
    Synchronous simulation engine.
    
    Call step() repeatedly to advance the simulation by one tick.
    Each tick produces events, consumes them, and returns a metrics snapshot.
    """

    def __init__(
        self,
        window_size: int = 5,
        duration: int = 30,
        interval_ms: int = 50,
        max_retries: int = 3,
        sources: list[str] = None,
        event_types: dict = None,
    ):
        self.window_size = window_size
        self.duration = duration
        self.interval_ms = interval_ms
        self.max_retries = max_retries
        self.sources = sources or DEFAULT_SOURCES
        self.event_types = event_types or DEFAULT_EVENT_TYPES

        self.broker = MessageBroker()
        self.store = AnalyticsStore()

        self._current_windows: dict[str, WindowMetrics] = {}  # per-source windows
        self._events_produced = 0
        self._events_consumed = 0
        self._events_failed = 0
        self._step_count = 0
        self._start_time: Optional[float] = None
        self._event_log: list[str] = []

    def _produce_events(self) -> int:
        """Generate a burst of events from all sources. Returns count produced."""
        produced = 0
        for source in self.sources:
            burst = random.randint(2, 5)
            types = self.event_types.get(source, ["generic_event"])
            for _ in range(burst):
                etype = random.choice(types)
                event = Event.generate(source, etype)
                if self.broker.publish(event):
                    produced += 1
        self._events_produced += produced
        return produced

    def _get_or_create_window(self, source: str, ts: float) -> WindowMetrics:
        """Get current window for a source, or create new one if expired."""
        window = self._current_windows.get(source)
        if window is None:
            ws = ts - (ts % self.window_size)
            we = ws + self.window_size
            window = WindowMetrics(ws, we)
            self._current_windows[source] = window
        elif ts >= window.window_end:
            # Window expired -> commit and create new
            self.store.commit_window(window)
            self._event_log.append(
                f"Window closed [{source}]: {window.event_count} events, "
                f"avg={window.avg_value}"
            )
            ws = ts - (ts % self.window_size)
            we = ws + self.window_size
            window = WindowMetrics(ws, we)
            self._current_windows[source] = window
        return window

    def _consume_and_process(self) -> int:
        """Consume events from all topics and aggregate. Returns count consumed."""
        consumed = 0
        for source in self.sources:
            # Cap at 3 events per source per step — slightly less than
            # production rate so queue builds up gradually (~200-500)
            for _ in range(3):
                event = self.broker.consume(source, timeout=0.001)
                if event is None:
                    break

                # 0.4% chance: poison-pill event -> straight to DLQ
                if random.random() < 0.004:
                    self._events_failed += 1
                    self.broker.send_to_dlq(event)
                    self._event_log.append(
                        f"Poison-pill event {event.event_id[:8]} -> DLQ (malformed)"
                    )
                    continue

                # 3% chance: transient failure -> retry
                if random.random() < 0.03:
                    event.retries += 1
                    self._events_failed += 1
                    if event.retries <= self.max_retries:
                        self.broker.publish(event)  # re-queue for retry
                    else:
                        self.broker.send_to_dlq(event)
                        self._event_log.append(
                            f"Event {event.event_id[:8]} -> DLQ after {event.retries} retries"
                        )
                    continue

                window = self._get_or_create_window(source, event.timestamp)
                window.update(event)
                consumed += 1

        self._events_consumed += consumed
        return consumed

    def step(self) -> dict:
        """
        Execute one simulation tick.
        Returns a metrics snapshot dict.
        """
        if self._start_time is None:
            self._start_time = time.time()

        self._step_count += 1
        produced = self._produce_events()
        consumed = self._consume_and_process()

        elapsed = time.time() - self._start_time
        throughput = self._events_consumed / elapsed if elapsed > 0 else 0

        return {
            "step": self._step_count,
            "elapsed": round(elapsed, 1),
            "produced_this_step": produced,
            "consumed_this_step": consumed,
            "total_produced": self._events_produced,
            "total_consumed": self._events_consumed,
            "total_failed": self._events_failed,
            "broker_queue_size": self.broker.all_topics_size(),
            "dlq_size": self.broker.dlq_size(),
            "throughput": round(throughput, 1),
            "windows_captured": len(self.store.get_all_windows()),
            "live_metrics": self.store.get_live_metrics(),
            "recent_windows": self.store.get_recent_windows(n=5),
            "all_windows": self.store.get_all_windows(),
        }

    def flush_windows(self):
        """Commit all open windows on shutdown."""
        for source, window in self._current_windows.items():
            if window.event_count > 0:
                self.store.commit_window(window)
        self._current_windows.clear()

    def get_event_log(self, n: int = 20) -> list[str]:
        """Return the last n log entries."""
        return self._event_log[-n:]

    def export_results(self) -> dict:
        """Generate final results summary (same format as main.py)."""
        self.flush_windows()
        elapsed = time.time() - self._start_time if self._start_time else 1
        avg_tp = round(self._events_consumed / elapsed, 1) if elapsed > 0 else 0
        return {
            "simulation_end": datetime.now().isoformat(),
            "total_published": self.broker.total_published(),
            "total_consumed": self._events_consumed,
            "total_produced": self._events_produced,
            "dlq_events": self.broker.dlq_size(),
            "dlq_size": self.broker.dlq_size(),
            "avg_throughput": avg_tp,
            "windows_captured": len(self.store.get_all_windows()),
            "live_metrics": self.store.get_live_metrics(),
            "window_history": self.store.get_all_windows(),
        }

    def export_json(self) -> str:
        """Export results as JSON string."""
        return json.dumps(self.export_results(), indent=2)

    # ── Methods used by the Streamlit simulation page ────────────────

    def advance(self, n: int = 1):
        """Run n simulation steps."""
        for _ in range(n):
            self.step()

    def is_complete(self) -> bool:
        """Check if the simulation duration has elapsed."""
        if self._start_time is None:
            return False
        return (time.time() - self._start_time) >= self.duration

    def get_dashboard_metrics(self) -> dict:
        """Return a metrics snapshot for the Streamlit live view."""
        elapsed = time.time() - self._start_time if self._start_time else 0
        throughput = self._events_consumed / elapsed if elapsed > 0 else 0
        return {
            "elapsed_time": round(elapsed, 1),
            "duration": self.duration,
            "total_produced": self._events_produced,
            "total_consumed": self._events_consumed,
            "broker_queue_size": self.broker.all_topics_size(),
            "dlq_size": self.broker.dlq_size(),
            "throughput": round(throughput, 1),
            "windows_captured": len(self.store.get_all_windows()),
            "all_windows": self.store.get_all_windows(),
        }

    def get_final_results(self) -> dict:
        """Alias for export_results() used by the simulation page."""
        return self.export_results()

