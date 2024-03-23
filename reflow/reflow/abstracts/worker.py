from abc import ABC, abstractmethod


class AbstractWorker(ABC):
    @abstractmethod
    def work(self) -> None:
        """Perform work."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stop work."""
        ...

    @abstractmethod
    def is_working(self) -> bool:
        """Return True if the worker is working, False otherwise."""
        ...
