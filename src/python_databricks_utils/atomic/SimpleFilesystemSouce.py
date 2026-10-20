from collections.abc import Callable

from python_databricks_env import dbutils

from .GenericFilesystemSource import GenericFilesystemSource


class SimpleFilesystemSource(GenericFilesystemSource):
    def __init__(self, source: str, backup_path: Callable[[str], str]):
        super().__init__(source, lambda src, dst: dbutils.fs.cp(src, dst, True), backup_path)
