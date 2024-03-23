from abc import ABC, abstractmethod


class AbstractSubscriber(ABC):
    @abstractmethod
    def update(self, message: str) -> None:
        """Receive a message from the publisher."""
        ...
