import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from python_databricks_utils.logging.logging import get_logger

logger = get_logger()


class HasId(Protocol):
    def id(self) -> str: ...


@dataclass
class ExecuteInfo[T]:
    resource: HasId
    tracker: T
    started_at: float


class Scheduler[R: HasId]:
    def __init__(self, resources: list[R], poll_interval_seconds: int):
        self.resources = resources
        self.poll_interval_seconds = poll_interval_seconds

    def execute[T, O](
            self,
            tasks: list[Callable[[R], T]],
            get_task_status: Callable[[T], O | Exception | None],
            timeout_seconds: int | None = None
    ):
        pending: deque[Callable[[R], T]] = deque(tasks)
        active: dict[T, ExecuteInfo[T]] = {}
        successful: list[tuple[ExecuteInfo[T], O]] = []
        failed: list[tuple[ExecuteInfo[T], Exception]] = []
        timed_out: list[ExecuteInfo[T]] = []

        def next_resource() -> R | None:
            active_resource_ids = [e.resource.id() for e in active.values()]
            available_resources = filter(lambda r: r.id() not in active_resource_ids, self.resources)
            return next(available_resources, None)

        def execute_next_tasks() -> None:
            while pending and (resource := next_resource()):
                task = pending.popleft()
                tracker = task(resource)
                logger.info(f"Executing new task: {tracker=} resource={resource.id()}")
                active[tracker] = ExecuteInfo(resource=resource, tracker=tracker, started_at=time.monotonic())

        while pending or active:
            now = time.monotonic()

            for e in list(active.values()):
                if timeout_seconds is not None and (now - e.started_at) >= timeout_seconds:
                    timed_out.append(e)
                    logger.warning(f"TIMEOUT: tracker={e.tracker} resource={e.resource.id()}")
                    active.pop(e.tracker)
                else:
                    status = get_task_status(e.tracker)

                    if isinstance(status, Exception):
                        failed.append((e, status))
                        logger.warning(f"FAIL: tracker={e.tracker} {status=}")
                        active.pop(e.tracker)
                    elif status is not None:
                        successful.append((e, status))
                        logger.info(f"SUCCESS: tracker={e.tracker} {status=}")
                        active.pop(e.tracker)

            execute_next_tasks()

            logger.info(
                f"pending={len(pending)}, "
                f"active={len(active)}, "
                f"successful={len(successful)}, "
                f"failed={len(failed)}, "
                f"timed_out={len(timed_out)}"
            )

            time.sleep(self.poll_interval_seconds)

        return {
            "successful": [((e.resource.id(), e.tracker), o) for e, o in successful],
            "failed": [((e.resource.id(), e.tracker), ex) for e, ex in failed],
            "timed_out": [(e.resource.id(), e.tracker) for e in timed_out]
        }
