from src.decorators.dag import DAG


@DAG(name="example_dag", schedule_interval="0 0 * * *", default_args={})
def example_dag(tasks):
    # [START howto_operator_bash]
    tasks.append(
        {
            "task_id": "print_date",
            "operator": "bash",
            "bash_command": "ls -l",
        }
    )
    # [END howto_operator_bash]
    # [START howto_operator_bash]
    tasks.append(
        {
            "task_id": "print_date",
            "operator": "bash",
            "bash_command": "date",
        }
    )
    # [END howto_operator_bash]


example_dag()
