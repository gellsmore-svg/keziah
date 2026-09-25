"""In-memory ready index. A hint for memory and hybrid modes, never the durable truth."""

from __future__ import annotations

import heapq
import itertools

from keziah.config import SchedulerSettings
from keziah.scheduler import PickerState, select
from keziah.types import Candidate


class ReadyIndex:
    """Per client, the head is the highest priority, then the oldest job.

    Heaps use lazy deletion so cancelling a buried job is O(log n).
    """

    def __init__(self) -> None:
        self._seq = itertools.count()
        self._heaps: dict[tuple[str, str, str], list[tuple[int, float, int, str]]] = {}
        self._live: dict[str, tuple[str, str, str]] = {}
        self._meta: dict[str, Candidate] = {}

    def add(self, candidate: Candidate) -> None:
        self.remove(candidate.job_id)
        lane = (candidate.model, candidate.scheduling_class, candidate.client_id)
        entry = (-candidate.priority, candidate.created_at, next(self._seq), candidate.job_id)
        heapq.heappush(self._heaps.setdefault(lane, []), entry)
        self._live[candidate.job_id] = lane
        self._meta[candidate.job_id] = candidate

    def remove(self, job_id: str) -> None:
        self._live.pop(job_id, None)
        self._meta.pop(job_id, None)

    def __len__(self) -> int:
        return len(self._live)

    def __contains__(self, job_id: str) -> bool:
        return job_id in self._live

    def rebuild(self, candidates: list[Candidate]) -> None:
        self._heaps.clear()
        self._live.clear()
        self._meta.clear()
        for candidate in candidates:
            self.add(candidate)

    def heads(self, model: str) -> list[Candidate]:
        found: list[Candidate] = []
        for lane, heap in list(self._heaps.items()):
            if lane[0] != model:
                continue
            head = self._pop_dead(lane, heap)
            if head is not None:
                meta = self._meta.get(head)
                if meta is not None:
                    found.append(meta)
        return found

    def propose(
        self,
        model: str,
        state: PickerState,
        now: float,
        settings: SchedulerSettings,
    ) -> tuple[Candidate | None, PickerState]:
        return select(state, model, self.heads(model), now, settings)

    def _pop_dead(self, lane: tuple[str, str, str], heap: list[tuple[int, float, int, str]]) -> str | None:
        while heap:
            job_id = heap[0][3]
            if self._live.get(job_id) == lane:
                return job_id
            heapq.heappop(heap)
        self._heaps.pop(lane, None)
        return None
