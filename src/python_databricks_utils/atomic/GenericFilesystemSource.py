from collections.abc import Callable

from python_databricks_env import dbutils
from python_databricks_env.utils.fs.exists import exists

from .BackupableSource import BackupableSource


class GenericFilesystemSource(BackupableSource[str]):
    def __init__(self, source: str, cp: Callable[[str, str], None], backup_path: Callable[[str], str]):
        super().__init__(source)
        self.cp = cp
        self.backup_path = backup_path

    def backup(self) -> None:
        if exists(self.source):
            self.backup_ref = self.backup_path(self.source)
            self.cp(self.source, self.backup_ref)
        else:
            # explicit, not technically needed
            self.backup_ref = None

        return None

    def restore(self) -> None:
        if self.backup_ref is None:
            dbutils.fs.rm(self.source, recurse=True)
        else:
            self.cp(self.backup_ref, self.source)

    def cleanup(self):
        if self.backup_ref is not None:
            dbutils.fs.rm(self.backup_ref, recurse=True)
