from typing import Protocol


class WidgetsLike(Protocol):
    @staticmethod
    def combobox(name: str, defaultValue: str, choices: list[str], label: str | None = None) -> None:
        ...

    @staticmethod
    def dropdown(name: str, defaultValue: str, choices: list[str], label: str | None = None) -> None:
        ...

    @staticmethod
    def get(self, name: str) -> str:
        ...

    @staticmethod
    def getAll(self) -> dict[str, str]:
        ...

    @staticmethod
    def getArgument(self, name: str, defaultValue: str | None = None) -> str:
        ...

    @staticmethod
    def multiselect(name: str, defaultValue: str, choices: list[str], label: str | None = None) -> None:
        ...

    @staticmethod
    def remove(name: str) -> None:
        ...

    @staticmethod
    def removeAll() -> None:
        ...

    @staticmethod
    def text(name: str, defaultValue: str, label: str | None = None) -> None:
        ...
