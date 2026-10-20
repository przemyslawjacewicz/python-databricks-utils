from .BackupableSource import BackupableSource


class SimpleSource[S](BackupableSource[S]):
    def __init__(self, source: S):
        super().__init__(source)

    def backup(self) -> None:
        return

    def restore(self) -> None:
        return

    def cleanup(self) -> None:
        return
