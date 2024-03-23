import importlib.util
import sys
import inspect
from reflow.reflow.models.task import Task


class DagFileDecoder:
    def __init__(self, dag_path: str):
        self.absolute_path = dag_path
        self.dag_path = ".".join(dag_path.replace(".py", "").split("/")[1:])

    def decode(self):
        sys.path.append(self.absolute_path)
        spec = importlib.util.spec_from_file_location(self.dag_path, self.absolute_path)
        modulo = importlib.util.module_from_spec(spec)
        sys.modules[self.dag_path] = modulo
        try:
            spec.loader.exec_module(modulo)
        except Exception as e:
            print(e)
            return
        for nome, obj in inspect.getmembers(modulo):
            if isinstance(obj, Task):  # Verifica se o objeto é uma instância de Task
                print(obj)
            if hasattr(obj, "__annotations__") and "DAG" in obj.__annotations__:
                dag_annotation = obj.__annotations__["DAG"]
                dag_params = {}
                for key, value in dag_annotation.__dict__.items():
                    if not key.startswith("__"):
                        dag_params[key] = value
                print(dag_params)
