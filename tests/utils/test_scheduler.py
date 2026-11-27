import random
from typing import Callable

from python_databricks_utils.utils.scheduler import Scheduler, HasId


def test_scheduler():
    class TestResource(HasId):
        def __init__(self, id: str):
            self._id = id

        def id(self) -> str:
            return self._id

    def task(i: int) -> Callable[[TestResource], int]:
        return lambda r: i

    def get_task_status(tracker: int) -> None | Exception | str:
        match random.choices([1, 2, 3], weights=[0.1, 0.1, 0.8], k=1)[0]:
            case 1:
                return f"success_{tracker}"
            case 2:
                return Exception(f"Task {tracker} failed")
            case 3:
                return None

    resources: list[TestResource] = [TestResource("1"), TestResource("2"), TestResource("3")]

    scheduler: Scheduler[TestResource] = Scheduler(resources, 5)

    print(
        scheduler
        .execute(
            tasks=[task(i) for i in range(0, 10)],
            get_task_status=get_task_status,
            timeout_seconds=30
        )
    )
