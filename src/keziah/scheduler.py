"""Deterministic weighted scheduling.

Class selection is weighted credit. Within a class, each client contributes only
its current head job, scored by priority, how often that client has been skipped,
and age. Credits and skip counts change only after a claim succeeds, so a lost
race does not consume a turn.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from keziah.config import SchedulerSettings
from keziah.types import SCHEDULING_CLASSES, Candidate


@dataclass
class PickerState:
    credit: dict[str, dict[str, float]] = field(default_factory=dict)
    skips: dict[str, dict[str, int]] = field(default_factory=dict)

    def copy(self) -> PickerState:
        return PickerState(
            credit={model: dict(classes) for model, classes in self.credit.items()},
            skips={model: dict(clients) for model, clients in self.skips.items()},
        )


def select(
    state: PickerState,
    model: str,
    candidates: list[Candidate],
    now: float,
    settings: SchedulerSettings,
) -> tuple[Candidate | None, PickerState]:
    """Return the chosen candidate and the state to commit if the claim succeeds."""
    if not candidates:
        return None, state
    nxt = state.copy()
    by_class: dict[str, list[Candidate]] = {name: [] for name in SCHEDULING_CLASSES}
    for candidate in candidates:
        by_class.setdefault(candidate.scheduling_class, []).append(candidate)
    present = [name for name in SCHEDULING_CLASSES if by_class.get(name)]
    if not present:
        return None, state

    credits = nxt.credit.setdefault(model, {})
    total_base = 0.0
    for name in present:
        oldest_age = max(now - item.created_at for item in by_class[name])
        boost = 0.0
        if settings.starvation_seconds > 0:
            boost = min(oldest_age / settings.starvation_seconds, settings.starvation_boost_cap)
        weight = float(settings.class_weights.get(name, 1.0))
        credits[name] = credits.get(name, 0.0) + weight * (1.0 + boost)
        total_base += weight * (1.0 + boost)

    def class_key(name: str) -> tuple[float, int]:
        # Equal credit falls through to the higher class (lower index).
        return (credits.get(name, 0.0), -SCHEDULING_CLASSES.index(name))

    chosen_class = max(present, key=class_key)
    credits[chosen_class] = credits.get(chosen_class, 0.0) - total_base

    pool = by_class[chosen_class]
    skips = nxt.skips.setdefault(model, {})

    def job_score(candidate: Candidate) -> tuple[float, str]:
        age = max(0.0, now - candidate.created_at)
        score = (
            candidate.priority * settings.priority_weight
            + skips.get(candidate.client_id, 0) * settings.client_skip_weight
            + age * settings.age_weight
        )
        # job_id breaks ties deterministically. Higher score wins, so invert the id.
        return (score, _invert(candidate.job_id))

    best = max(pool, key=job_score)
    clients = {item.client_id for item in pool}
    for client_id in clients:
        if client_id == best.client_id:
            skips[client_id] = 0
        else:
            skips[client_id] = skips.get(client_id, 0) + 1
    return best, nxt


def _invert(value: str) -> str:
    """Max-friendly inverse so the lexicographically smaller id wins ties."""
    return "".join(chr(0x10FFFF - ord(ch)) for ch in value)
