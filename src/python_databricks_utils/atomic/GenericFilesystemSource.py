from collections.abc import Callable

from .BackupableSource import BackupableSource
from ..databricks.dbutils.DbUtilsLike import DbUtilsLike
from ..utils.dbfs_utils import exists


class GenericFilesystemSource(BackupableSource[str]):
    def __init__(
            self,
            dbutils: DbUtilsLike,
            source: str,
            backup_path: Callable[[str], str],
            op: Callable[[str, str], None]):
        super().__init__(source)
        self.dbutils = dbutils
        self.backup_path = backup_path
        self.op = op

    def backup(self) -> None:
        if exists(self.dbutils, self.source):
            self.backup_ref = self.backup_path(self.source)
            self.op(self.source, self.backup_ref)
        else:
            # explicit, not technically needed
            self.backup_ref = None

        return None

    def restore(self) -> None:
        if self.backup_ref is None:
            self.dbutils.fs.rm(self.source, recurse=True)
        else:
            self.op(self.backup_ref, self.source)

    def cleanup(self):
        if self.backup_ref is not None:
            self.dbutils.fs.rm(self.backup_ref, recurse=True)
