from abc import ABC, abstractmethod


class AbstractEvent(ABC):
    @abstractmethod
    def subscribe(self, subscriber) -> None:
        """Subscribe a subscriber to the event."""
        ...

    @abstractmethod
    def unsubscribe(self, subscriber) -> None:
        """Unsubscribe a subscriber from the event."""
        ...

    @abstractmethod
    def notify(self, message: str) -> None:
        """Notify all subscribers of the event."""
        ...
