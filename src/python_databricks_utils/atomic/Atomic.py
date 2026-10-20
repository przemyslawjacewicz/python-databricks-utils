from __future__ import annotations

import functools
import logging
from collections.abc import Callable

from .AtomicResult import AtomicResult, AtomicFailure, AtomicSuccess
from .BackupableSource import BackupableSource

logger = logging.getLogger(__name__)


class Atomic[O]:

    def __init__(self, source: BackupableSource, transformations=None):
        if transformations is None:
            transformations = []
        self.source = source
        self.transformations = transformations

    def __call__(self) -> AtomicResult[O]:
        def apply(acc: AtomicResult, t: Callable) -> AtomicResult:
            if isinstance(acc, AtomicFailure):
                # already failed
                return acc

            # still success
            current = acc()
            try:
                logger.debug(f"[ATOMIC:EXEC] Performing transformation: current={current}.")
                result = t(current)
                logger.debug(f"[ATOMIC:EXEC] Transformation succeeded: previous={current}, current={result}.")

                return AtomicSuccess(result)
            except Exception as ex:
                logger.debug(f"[ATOMIC:EXEC] Transformation failed: error={ex}.")

                return AtomicFailure(self.source.source)

        logger.debug(f"[ATOMIC:START] Staring transformations: size={len(self.transformations)}.")

        logger.debug(f"[ATOMIC:EXEC] Performing backup for source: source={self.source.source}.")
        self.source.backup()
        logger.debug(f"[ATOMIC:EXEC] Backup completed: backup_ref={self.source.backup_ref}.")

        result = functools.reduce(
            apply,
            self.transformations,
            AtomicSuccess(self.source.source)
        )

        if result.is_success():
            logger.debug(f"[ATOMIC:SUCCESS] Finished transformations. Starting cleanup.")
            self.source.cleanup()
        else:
            logger.debug(f"[ATOMIC:FAILURE] Finished transformations. Starting restore and cleanup.")
            self.source.restore()
            self.source.cleanup()

        return result
