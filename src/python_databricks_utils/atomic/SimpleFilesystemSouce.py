from collections.abc import Callable

from .GenericFilesystemSource import GenericFilesystemSource
from ..databricks.dbutils.DbUtilsLike import DbUtilsLike


class SimpleFilesystemSource(GenericFilesystemSource):
    def __init__(self, dbutils: DbUtilsLike, source: str, backup_path: Callable[[str], str]):
        super().__init__(
            dbutils,
            source,
            backup_path,
            lambda _source, _backup_ref: dbutils.fs.cp(_source, _backup_ref, True)
        )
