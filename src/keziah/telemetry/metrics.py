"""Small Prometheus exposition. Job ids are never labels."""

from __future__ import annotations

import threading
from collections import defaultdict


class Histogram:
    def __init__(self, buckets: tuple[float, ...]) -> None:
        self.buckets = buckets
        self.counts = [0 for _ in buckets]
        self.inf = 0
        self.total = 0
        self.sum = 0.0

    def observe(self, value: float) -> None:
        self.total += 1
        self.sum += value
        placed = False
        for index, bound in enumerate(self.buckets):
            if value <= bound:
                self.counts[index] += 1
                placed = True
                break
        if not placed:
            self.inf += 1


class Metrics:
    LATENCY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._hist: dict[tuple[str, tuple[tuple[str, str], ...]], Histogram] = {}

    def inc(self, name: str, amount: float = 1.0, **labels: str) -> None:
        key = (name, tuple(sorted(labels.items())))
        with self._lock:
            self._counters[key] += amount

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = (name, tuple(sorted(labels.items())))
        with self._lock:
            hist = self._hist.get(key)
            if hist is None:
                hist = Histogram(self.LATENCY_BUCKETS)
                self._hist[key] = hist
            hist.observe(value)

    def render(self, *, queue_depth: float, active: dict[str, int], capacity: dict[str, int]) -> str:
        lines: list[str] = []
        with self._lock:
            counters = list(self._counters.items())
            hists = list(self._hist.items())
        lines.append("# HELP keziah_queue_depth Jobs waiting to run, including retry wait and staging.")
        lines.append("# TYPE keziah_queue_depth gauge")
        lines.append(f"keziah_queue_depth {queue_depth}")
        lines.append("# HELP keziah_workers_active Inference calls in progress.")
        lines.append("# TYPE keziah_workers_active gauge")
        lines.append("# HELP keziah_workers_capacity Configured concurrency.")
        lines.append("# TYPE keziah_workers_capacity gauge")
        for model, value in sorted(capacity.items()):
            lines.append(f'keziah_workers_capacity{{model="{_label(model)}"}} {value}')
            lines.append(f'keziah_workers_active{{model="{_label(model)}"}} {active.get(model, 0)}')
        emitted: set[str] = set()
        for (name, labels), value in sorted(counters, key=lambda item: (item[0][0], item[0][1])):
            if name not in emitted:
                lines.append(f"# TYPE {name} counter")
                emitted.add(name)
            lines.append(f"{name}{_fmt_labels(labels)} {value}")
        for (name, labels), hist in sorted(hists, key=lambda item: (item[0][0], item[0][1])):
            lines.append(f"# TYPE {name} histogram")
            cumulative = 0
            for bound, count in zip(hist.buckets, hist.counts, strict=True):
                cumulative += count
                bucket_labels = (*labels, ("le", str(bound)))
                lines.append(f"{name}_bucket{_fmt_labels(bucket_labels)} {cumulative}")
            lines.append(f"{name}_bucket{_fmt_labels((*labels, ('le', '+Inf')))} {hist.total}")
            lines.append(f"{name}_sum{_fmt_labels(labels)} {hist.sum}")
            lines.append(f"{name}_count{_fmt_labels(labels)} {hist.total}")
        lines.append("")
        return "\n".join(lines)


def _fmt_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    body = ",".join(f'{key}="{_label(value)}"' for key, value in labels)
    return "{" + body + "}"


def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "")
