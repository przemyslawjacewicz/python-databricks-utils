from typing import Protocol

from python_databricks_utils.databricks.dbutils.fs.FileInfoLike import FileInfoLike
from python_databricks_utils.databricks.dbutils.fs.MountInfoLike import MountInfoLike


class FsLike(Protocol):
    @staticmethod
    def cp(source: str, dest: str, recurse: bool = False) -> bool:
        ...

    @staticmethod
    def head(file: str, max_bytes: int = 65536) -> str:
        ...

    @staticmethod
    def ls(path: str) -> list[FileInfoLike]:
        ...

    @staticmethod
    def mkdirs(dir: str) -> bool:
        ...

    @staticmethod
    def mv(source: str, dest: str, recurse: bool = False) -> bool:
        ...

    @staticmethod
    def put(file: str, contents: str, overwrite: bool = False) -> bool:
        ...

    @staticmethod
    def rm(dir: str, recurse: bool = False) -> bool:
        ...

    @staticmethod
    def mount(
            source: str,
            mount_point: str,
            encryption_type: str = '',
            owner: str = None,
            extra_configs: dict[str, str] = None
    ) -> bool:
        ...

    @staticmethod
    def refreshMounts() -> bool:
        ...

    @staticmethod
    def mounts() -> list[MountInfoLike]:
        ...

    @staticmethod
    def unmount(mount_point: str) -> bool:
        ...

    @staticmethod
    def updateMount(
            source: str,
            mount_point: str,
            encryption_type: str = '',
            owner: str = None,
            extra_configs: dict[str, str] = None
    ) -> bool:
        ...
