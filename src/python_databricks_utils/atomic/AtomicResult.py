from abc import ABC, abstractmethod


class AtomicResult[R](ABC):
    @abstractmethod
    def __call__(self, *args, **kwargs) -> R:
        ...

    @abstractmethod
    def is_success(self) -> bool:
        ...


class AtomicSuccess[R](AtomicResult[R]):
    def __init__(self, result: R):
        self.result = result

    def __call__(self, *args, **kwargs) -> R:
        return self.result

    def is_success(self) -> bool:
        return True


class AtomicFailure[R](AtomicResult[R]):
    def __init__(self, fallback: R):
        self.fallback = fallback

    def __call__(self, *args, **kwargs) -> R:
        return self.fallback

    def is_success(self) -> bool:
        return False
