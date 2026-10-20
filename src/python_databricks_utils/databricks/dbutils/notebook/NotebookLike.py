from typing import Protocol, Any, NoReturn


class NotebookLike(Protocol):
    @staticmethod
    def exit(value: Any) -> NoReturn:
        ...

    @staticmethod
    def run(path: str, timeout_seconds: int, arguments: dict[str, Any] | None = None, *args, **kwargs):
        ...
