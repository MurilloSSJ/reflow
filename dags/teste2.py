from reflow.reflow.decorators.dag import DAG
from reflow.reflow.models.task import Task
from reflow.reflow.operators.dummy import DummyOperator


@DAG(name="teste2", start_date="2021-01-01", schedule_interval="0 0 * * *")
def teste(**kwargs):
    bash3 = Task(
        id="task2",
        operator=DummyOperator(),
        command="echo 'teste2'",
        trigger_rule="all_success",
    )
    bash1 = Task(
        id="task1",
        operator=DummyOperator(),
        command="echo 'teste2'",
        trigger_rule="all_success",
        dependencies=[bash3],
    )


teste()
