from typing import Protocol


class MountInfoLike(Protocol):
    mountPoint: str
    source: str
    encryptionType: str
