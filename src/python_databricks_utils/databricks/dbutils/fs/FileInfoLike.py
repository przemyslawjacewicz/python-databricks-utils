from typing import Protocol


class FileInfoLike(Protocol):
    path: str
    name: str
    size: int
    modificationTime: float

    def isDir(self) -> bool:
        ...

    def isFile(self) -> bool:
        ...
