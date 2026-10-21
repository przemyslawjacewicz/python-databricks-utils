from __future__ import annotations

from abc import ABC, abstractmethod


class BackupableSource[S](ABC):
    def __init__(self, source: S):
        self.source = source
        self.backup_ref = None

    @abstractmethod
    def backup(self) -> None:
        ...

    @abstractmethod
    def restore(self) -> None:
        ...

    @abstractmethod
    def cleanup(self) -> None:
        ...

    def merge(self, *others: BackupableSource) -> BackupableSource[list]:
        class BackupableSourceImpl(BackupableSource[list]):
            def __init__(self_impl):
                super().__init__([self.source, *(map(lambda other: other.source, others))])

            def backup(self_impl) -> None:
                self.backup()
                for other in others:
                    other.backup()

            def restore(self_impl) -> None:
                self.restore()
                for other in others:
                    other.restore()

            def cleanup(self_impl):
                self.cleanup()
                for other in others:
                    other.cleanup()

        return BackupableSourceImpl()
