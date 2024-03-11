from src.decorators.dag import DAG


@DAG(name="teste2", start_date="2021-01-01", schedule_interval="0 0 * * *")
def teste(tasks=[]):
    tasks.append(
        {
            "name": "task1",
            "operator": "bash",
            "command": "echo 'teste2'",
            "trigger_rule": "all_success",
            "dependencies": ["task2"],
        }
    )
    tasks.append(
        {
            "name": "task2",
            "operator": "bash",
            "command": "echo 'teste2'",
            "trigger_rule": "all_success",
        }
    )
