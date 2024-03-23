from reflow.reflow.operators.interface import ReflowOperator


class DummyOperator(ReflowOperator):
    def __init__(self):
        pass

    def bootstrap(self, *args, **kwargs):
        pass
