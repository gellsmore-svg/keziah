"""Concurrent submission against one queue."""

import threading

from keziah.service import Keziah
from tests.conftest import make_settings


def test_many_clients_do_not_lose_jobs(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "hybrid", concurrency=4)
    with Keziah(settings=settings) as service:
        ids: list[str] = []
        lock = threading.Lock()

        def submit(client: str) -> None:
            local = []
            for index in range(25):
                local.append(
                    service.submit(
                        model="mock",
                        state={"client": client, "n": index},
                        questions=questions,
                        client_id=client,
                        scheduling_class="normal" if index % 2 == 0 else "bulk",
                        priority=index % 3,
                    )
                )
            with lock:
                ids.extend(local)

        threads = [threading.Thread(target=submit, args=(f"client-{n}",)) for n in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert len(ids) == 100
        assert len(set(ids)) == 100
        results = [service.wait(job_id, timeout=10) for job_id in ids]
        assert {item.status for item in results} == {"succeeded"}
        assert sorted(item.attempts for item in results)[0] >= 1
