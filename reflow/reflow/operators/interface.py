from abc import ABC, abstractmethod


class ReflowOperator(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def bootstrap(self, *args, **kwargs):
        pass
