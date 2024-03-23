from src.abstracts.publisher import AbstractPublisher
from typing_extensions import override
from typing import List


class Scheduler(AbstractPublisher):
    @override
    def subscribe(self, subscriber) -> None:
        self.subscribers.append(subscriber)

    @override
    def unsubscribe(self, subscriber) -> None:
        self.subscribers.remove(subscriber)

    @override
    def publish(self, message: str) -> None:
        for subscriber in self.subscribers:
            subscriber.update(message)
