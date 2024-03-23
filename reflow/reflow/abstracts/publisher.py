from abc import ABC, abstractmethod
from src.abstracts.subscribers import AbstractSubscriber
from typing import List


class AbstractPublisher(ABC):
    def __init__(self):
        self.subscribers: List[AbstractSubscriber] = []

    @abstractmethod
    def publish(self, message: str) -> None:
        """Publish a message to all subscribers."""
        ...

    @abstractmethod
    def subscribe(self) -> None:
        """Subscribe a subscriber to the publisher."""
        ...

    @abstractmethod
    def unsubscribe(self) -> None:
        """Unsubscribe a subscriber from the publisher."""
        ...
