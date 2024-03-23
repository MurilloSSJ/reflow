class DAG:
    def __init__(
        self,
        default_args=None,
        owners=["airflow"],
        start_date=None,
        schedule_interval=None,
        name=None,
        is_paused=True,
        is_active=False,
        description=None,
        default_view="grid",
        timetable_description=None,
        tags=[],
        max_active_tasks: int = 16,
        max_active_runs: int = 16,
        has_task_concurrency_limits: bool = True,
    ):
        self.default_args = default_args
        self.owners = owners
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.name = name

    def __call__(self, func):
        def decorated_dag(tasks=[], **kwargs):
            # Chame a função decorada passando os kwargs
            result = func(**kwargs)
            # Acesse tasks após a chamada da função decorada
            print(f"Name: {self.name}")
            print(f"Owner: {self.owners}")
            print(f"Start Date: {self.start_date}")
            print(f"Schedule Interval: {self.schedule_interval}")
            print(f"Default Args: {self.default_args}")
            print(f"Tasks: {tasks}")

            return result

        return decorated_dag
