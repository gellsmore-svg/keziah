"""Fair scheduling is deterministic and does not starve a class or a client."""

from keziah.config import SchedulerSettings
from keziah.scheduler import PickerState, select
from keziah.types import Candidate


def _candidate(job_id: str, scheduling_class: str, client: str, priority: int = 0, created: float = 0.0) -> Candidate:
    return Candidate(
        job_id=job_id,
        scheduling_class=scheduling_class,
        priority=priority,
        created_at=created,
        client_id=client,
        model="mock",
        questions_hash="h",
    )


def test_interactive_is_preferred_and_bulk_still_runs() -> None:
    settings = SchedulerSettings()
    state = PickerState()
    interactive = 0
    bulk = 0
    now = 100.0
    for _ in range(90):
        choice, state = select(
            state,
            "mock",
            [
                _candidate("i", "interactive", "a", created=now),
                _candidate("b", "bulk", "a", created=now),
            ],
            now,
            settings,
        )
        assert choice is not None
        if choice.scheduling_class == "interactive":
            interactive += 1
        else:
            bulk += 1
    assert interactive > bulk * 3
    assert bulk > 0


def test_clients_alternate_inside_a_class() -> None:
    settings = SchedulerSettings()
    state = PickerState()
    previous = None
    switches = 0
    for index in range(20):
        choice, state = select(
            state,
            "mock",
            [
                _candidate(f"a{index}", "normal", "alpha", created=1.0),
                _candidate(f"b{index}", "normal", "beta", created=1.0),
            ],
            10.0,
            settings,
        )
        assert choice is not None
        if previous is not None and choice.client_id != previous:
            switches += 1
        previous = choice.client_id
    assert switches >= 15


def test_higher_priority_runs_before_lower_priority_for_one_client() -> None:
    settings = SchedulerSettings()
    # One head per client, so priority is expressed by which job the index would surface.
    # Here both candidates are the same client only if we pass both; select treats them
    # as the pool for the class. Higher priority wins.
    choice, _state = select(
        PickerState(),
        "mock",
        [
            _candidate("low", "normal", "alpha", priority=0, created=1),
            _candidate("high", "normal", "alpha", priority=5, created=2),
        ],
        10.0,
        settings,
    )
    assert choice is not None
    assert choice.job_id == "high"


def test_old_bulk_gains_share_but_interactive_continues() -> None:
    settings = SchedulerSettings(starvation_seconds=10.0, starvation_boost_cap=8.0)
    state = PickerState()
    bulk = 0
    interactive = 0
    # Bulk has been waiting long enough to boost its weight up to the cap.
    for _ in range(40):
        choice, state = select(
            state,
            "mock",
            [
                _candidate("i", "interactive", "a", created=100),
                _candidate("b", "bulk", "b", created=0),
            ],
            100.0,
            settings,
        )
        assert choice is not None
        if choice.scheduling_class == "bulk":
            bulk += 1
        else:
            interactive += 1
    assert interactive > 0
    assert bulk > 0
