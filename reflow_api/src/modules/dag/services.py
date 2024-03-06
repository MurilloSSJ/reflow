from src.config import get_settings
from fastapi import HTTPException
from airflow.models.param import DagParam, ParamsDict

settings = get_settings()


@functools.total_ordering
class DAG(LoggingMixin):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional dependencies.

    A dag also has a schedule, a start date and an end date (optional).  For each schedule,
    (say daily or hourly), the DAG needs to run each individual tasks as their dependencies
    are met. Certain tasks have the property of depending on their own past, meaning that
    they can't run until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    Note that if you plan to use time zones all the dates provided should be pendulum
    dates. See :ref:`timezone_aware_dags`.

    .. deprecated:: 2.4
        The arguments *schedule_interval* and *timetable*. Their functionalities
        are merged into the new *schedule* argument.

    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII)
    :param description: The description for the DAG to e.g. be shown on the webserver
    :param schedule: Defines the rules according to which DAG runs are scheduled. Can
        accept cron string, timedelta object, Timetable, or list of Dataset objects.
        If this is not provided, the DAG will be set to the default
        schedule ``timedelta(days=1)``. See also :doc:`/howto/timetable`.
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open-ended scheduling
    :param template_searchpath: This list of folders (non-relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :param template_undefined: Template undefined type.
    :param user_defined_macros: a dictionary of macros that will be exposed
        in your jinja templates. For example, passing ``dict(foo='bar')``
        to this argument allows you to ``{{ foo }}`` in all jinja
        templates related to this DAG. Note that you can pass any
        type of object here.
    :param user_defined_filters: a dictionary of filters that will be exposed
        in your jinja templates. For example, passing
        ``dict(hello=lambda name: 'Hello %s' % name)`` to this argument allows
        you to ``{{ 'world' | hello }}`` in all jinja templates related to
        this DAG.
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :param params: a dictionary of DAG level parameters that are made
        accessible in templates, namespaced under `params`. These
        params can be overridden at the task level.
    :param max_active_tasks: the number of task instances allowed to run
        concurrently
    :param max_active_runs: maximum number of active DAG runs, beyond this
        number of DAG runs in a running state, the scheduler won't create
        new active DAG runs
    :param dagrun_timeout: specify how long a DagRun should be up before
        timing out / failing, so that new DagRuns can be created.
    :param sla_miss_callback: specify a function or list of functions to call when reporting SLA
        timeouts. See :ref:`sla_miss_callback<concepts:sla_miss_callback>` for
        more information about the function signature and parameters that are
        passed to the callback.
    :param default_view: Specify DAG default view (grid, graph, duration,
                                                   gantt, landing_times), default grid
    :param orientation: Specify DAG orientation in graph view (LR, TB, RL, BT), default LR
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
    :param on_failure_callback: A function or list of functions to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :param access_control: Specify optional DAG-level actions, e.g.,
        "{'role1': {'can_read'}, 'role2': {'can_read', 'can_edit', 'can_delete'}}"
    :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
        If the dag exists already, this flag will be ignored. If this optional parameter
        is not specified, the global config setting will be used.
    :param jinja_environment_kwargs: additional configuration options to be passed to Jinja
        ``Environment`` for template rendering

        **Example**: to avoid Jinja from removing a trailing newline from template strings ::

            DAG(
                dag_id="my-dag",
                jinja_environment_kwargs={
                    "keep_trailing_newline": True,
                    # some other jinja2 Environment options here
                },
            )

        **See**: `Jinja Environment documentation
        <https://jinja.palletsprojects.com/en/2.11.x/api/#jinja2.Environment>`_

    :param render_template_as_native_obj: If True, uses a Jinja ``NativeEnvironment``
        to render templates as native Python types. If False, a Jinja
        ``Environment`` is used to render templates as string values.
    :param tags: List of tags to help filtering DAGs in the UI.
    :param owner_links: Dict of owners and their links, that will be clickable on the DAGs view UI.
        Can be used as an HTTP link (for example the link to your Slack channel), or a mailto link.
        e.g: {"dag_owner": "https://airflow.apache.org/"}
    :param auto_register: Automatically register this DAG when it is used in a ``with`` block
    :param fail_stop: Fails currently running tasks when task in DAG fails.
        **Warning**: A fail stop dag can only have tasks with the default trigger rule ("all_success").
        An exception will be thrown if any task in a fail stop dag has a non default trigger rule.
    """

    _comps = {
        "dag_id",
        "task_ids",
        "parent_dag",
        "start_date",
        "end_date",
        "schedule_interval",
        "fileloc",
        "template_searchpath",
        "last_loaded",
    }

    __serialized_fields: frozenset[str] | None = None

    fileloc: str
    """
    File path that needs to be imported to load this DAG or subdag.

    This may not be an actual file on disk in the case when this DAG is loaded
    from a ZIP file or other DAG distribution format.
    """

    parent_dag: "DAG" | None = None  # Gets set when DAGs are loaded

    # NOTE: When updating arguments here, please also keep arguments in @dag()
    # below in sync. (Search for 'def dag(' in this file.)
    def __init__(
        self,
        dag_id: str,
        description: str | None = None,
        schedule: ScheduleArg = NOTSET,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        full_filepath: str | None = None,
        template_searchpath: str | Iterable[str] | None = None,
        template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
        user_defined_macros: dict | None = None,
        user_defined_filters: dict | None = None,
        default_args: dict = {},
        max_active_tasks: int = settings.max_active_tasks_per_dag,
        max_active_runs: int = settings.max_active_runs_per_dag,
        dagrun_timeout: timedelta | None = None,
        sla_miss_callback: None | SLAMissCallback | list[SLAMissCallback] = None,
        default_view: (
            "grid" | "graph" | "duration" | "gantt" | "landing_times"
        ) = settings.dag_default_view.lower(),
        orientation: "LR" | "TB" | "RL" | "BT" = airflow_conf.get_mandatory_value(
            "webserver", "dag_orientation"
        ),
        catchup: bool = False,
        on_success_callback: (
            None | DagStateChangeCallback | list[DagStateChangeCallback]
        ) = None,
        on_failure_callback: (
            None | DagStateChangeCallback | list[DagStateChangeCallback]
        ) = None,
        doc_md: str | None = None,
        params: ParamsDict = ParamsDict({}),
        access_control: dict | None = None,
        is_paused_upon_creation: bool | None = None,
        jinja_environment_kwargs: dict | None = None,
        render_template_as_native_obj: bool = False,
        tags: list[str] | None = None,
        owner_links: dict[str, str] = {},
        auto_register: bool = True,
        fail_stop: bool = False,
    ):
        from airflow.utils.task_group import TaskGroup

        if tags and any(len(tag) > 100 for tag in tags):
            raise HTTPException(403, f"tag cannot be longer than {100} characters")

        self.owner_links = owner_links
        self.user_defined_macros = user_defined_macros
        self.user_defined_filters = user_defined_filters
        self.default_args = copy.deepcopy(default_args or {})
        self.params = params

        validate_key(dag_id)

        self.dag_id = dag_id

        self.max_active_tasks = max_active_tasks
        self.pickle_id: int | None = None

        self._description = description
        # set file location to caller source path
        back = sys._getframe().f_back
        self.fileloc = back.f_code.co_filename if back else ""
        self.task_dict: dict[str, Operator] = {}
        tzinfo = None if start_date and start_date.tzinfo else settings.TIMEZONE
        tz = pendulum.instance(start_date, tz=tzinfo).timezone
        self.timezone: Timezone | FixedTimezone = tz or settings.TIMEZONE
        self.start_date = timezone.convert_to_utc(start_date)
        self.end_date = timezone.convert_to_utc(end_date)

        # sort out DAG's scheduling behavior
        scheduling_args = [schedule]

        if schedule is not None and start_date is None:
            raise ValueError("DAG is missing the start_date parameter")

        self.timetable: Timetable
        self.schedule_interval: ScheduleInterval
        self.dataset_triggers: BaseDatasetEventInput | None = None
        if isinstance(schedule, BaseDatasetEventInput):
            self.dataset_triggers = schedule
        elif isinstance(schedule, Collection) and not isinstance(schedule, str):
            if not all(isinstance(x, Dataset) for x in schedule):
                raise ValueError("All elements in 'schedule' should be datasets")
            self.dataset_triggers = DatasetAll(*schedule)
        elif isinstance(schedule, Timetable):
            timetable = schedule
        elif schedule and not isinstance(schedule, BaseDatasetEventInput):
            schedule_interval = schedule

        if isinstance(schedule, DatasetOrTimeSchedule):
            self.timetable = schedule
            self.dataset_triggers = self.timetable.datasets
            self.schedule_interval = self.timetable.summary
        elif self.dataset_triggers:
            self.timetable = DatasetTriggeredTimetable()
            self.schedule_interval = self.timetable.summary
        elif timetable:
            self.timetable = timetable
            self.schedule_interval = self.timetable.summary
        else:
            self.schedule_interval = (
                DEFAULT_SCHEDULE_INTERVAL
                if isinstance(schedule_interval, ArgNotSet)
                else schedule_interval
            )
            self.timetable = create_timetable(schedule_interval, self.timezone)

        if isinstance(template_searchpath, str):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.template_undefined = template_undefined
        self.last_loaded: datetime = timezone.utcnow()
        self.safe_dag_id = dag_id.replace(".", "__dot__")
        self.max_active_runs = max_active_runs
        if self.timetable.active_runs_limit is not None:
            if self.timetable.active_runs_limit < self.max_active_runs:
                raise AirflowException(
                    f"Invalid max_active_runs: {type(self.timetable)} "
                    f"requires max_active_runs <= {self.timetable.active_runs_limit}"
                )
        self.dagrun_timeout = dagrun_timeout
        self.sla_miss_callback = sla_miss_callback
        self._default_view: str = default_view
        self.orientation = orientation
        self.catchup: bool = catchup
        self.partial: bool = False
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback
        self.edge_info: dict[str, dict[str, EdgeInfoType]] = {}
        self.has_on_success_callback: bool = self.on_success_callback is not None
        self.has_on_failure_callback: bool = self.on_failure_callback is not None
        self.access_control = DAG._upgrade_outdated_dag_access_control(access_control)
        self.is_paused_upon_creation = is_paused_upon_creation
        self.auto_register = auto_register
        self.fail_stop: bool = fail_stop
        self.jinja_environment_kwargs = jinja_environment_kwargs
        self.render_template_as_native_obj = render_template_as_native_obj
        self.doc_md = self.get_doc_md(doc_md)
        self.tags = tags or []
        self._task_group = TaskGroup.create_root(self)
        self.validate_schedule_and_params()
        wrong_links = dict(self.iter_invalid_owner_links())
        if wrong_links:
            raise HTTPException(
                403,
                "Wrong link format was used for the owner. Use a valid link \n"
                f"Bad formatted links are: {wrong_links}",
            )
        self._processor_dags_folder = None

    def get_doc_md(self, doc_md: str | None) -> str | None:
        if doc_md is None:
            return doc_md
        env = self.get_template_env(force_sandboxed=True)
        if not doc_md.endswith(".md"):
            template = jinja2.Template(doc_md)
        else:
            try:
                template = env.get_template(doc_md)
            except jinja2.exceptions.TemplateNotFound:
                return f"""
                # Templating Error!
                Not able to find the template file: `{doc_md}`.
                """
        return template.render()

    def _check_schedule_interval_matches_timetable(self) -> bool:
        """Check ``schedule_interval`` and ``timetable`` match.

        This is done as a part of the DAG validation done before it's bagged, to
        guard against the DAG's ``timetable`` (or ``schedule_interval``) from
        being changed after it's created, e.g.

        .. code-block:: python

            dag1 = DAG("d1", timetable=MyTimetable())
            dag1.schedule_interval = "@once"

            dag2 = DAG("d2", schedule="@once")
            dag2.timetable = MyTimetable()

        Validation is done by creating a timetable and check its summary matches
        ``schedule_interval``. The logic is not bullet-proof, especially if a
        custom timetable does not provide a useful ``summary``. But this is the
        best we can do.
        """
        if self.schedule_interval == self.timetable.summary:
            return True
        try:
            timetable = create_timetable(self.schedule_interval, self.timezone)
        except ValueError:
            return False
        return timetable.summary == self.timetable.summary

    def validate(self):
        """Validate the DAG has a coherent setup.

        This is called by the DAG bag before bagging the DAG.
        """
        if not self._check_schedule_interval_matches_timetable():
            raise HTTPException(
                403,
                f"inconsistent schedule: timetable {self.timetable.summary!r} "
                f"does not match schedule_interval {self.schedule_interval!r}",
            )
        self.validate_schedule_and_params()
        self.timetable.validate()
        self.validate_setup_teardown()

    def validate_setup_teardown(self):
        """
        Validate that setup and teardown tasks are configured properly.

        :meta private:
        """
        for task in self.tasks:
            if task.is_setup:
                for down_task in task.downstream_list:
                    if (
                        not down_task.is_teardown
                        and down_task.trigger_rule != TriggerRule.ALL_SUCCESS
                    ):
                        raise ValueError(
                            "Setup tasks must be followed with trigger rule ALL_SUCCESS."
                        )
            FailStopDagInvalidTriggerRule.check(
                dag=self, trigger_rule=task.trigger_rule
            )

    def __eq__(self, other):
        if type(self) == type(other):
            # Use getattr() instead of __dict__ as __dict__ doesn't return
            # correct values for properties.
            return all(
                getattr(self, c, None) == getattr(other, c, None) for c in self._comps
            )
        return False

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    # Context Manager -----------------------------------------------
    def __enter__(self):
        DagContext.push_context_managed_dag(self)
        return self

    def __exit__(self, _type, _value, _tb):
        DagContext.pop_context_managed_dag()

    # /Context Manager ----------------------------------------------

    def get_next_data_interval(self, dag_model: DagModel) -> DataInterval | None:
        """Get the data interval of the next scheduled run.

        For compatibility, this method infers the data interval from the DAG's
        schedule if the run does not have an explicit one set, which is possible
        for runs created prior to AIP-39.

        This function is private to Airflow core and should not be depended on as a
        part of the Python API.

        :meta private:
        """
        if self.dag_id != dag_model.dag_id:
            raise ValueError(
                f"Arguments refer to different DAGs: {self.dag_id} != {dag_model.dag_id}"
            )
        if dag_model.next_dagrun is None:  # Next run not scheduled.
            return None
        data_interval = dag_model.next_dagrun_data_interval
        if data_interval is not None:
            return data_interval
        return self.infer_automated_data_interval(dag_model.next_dagrun)

    def get_run_data_interval(self, run: DagRun | DagRunPydantic) -> DataInterval:
        """Get the data interval of this run.

        For compatibility, this method infers the data interval from the DAG's
        schedule if the run does not have an explicit one set, which is possible for
        runs created prior to AIP-39.

        This function is private to Airflow core and should not be depended on as a
        part of the Python API.

        :meta private:
        """
        if run.dag_id is not None and run.dag_id != self.dag_id:
            raise ValueError(
                f"Arguments refer to different DAGs: {self.dag_id} != {run.dag_id}"
            )
        data_interval = _get_model_data_interval(
            run, "data_interval_start", "data_interval_end"
        )
        return (
            data_interval
            if data_interval is not None
            else self.infer_automated_data_interval(run.execution_date)
        )

    def infer_automated_data_interval(self, logical_date: datetime) -> DataInterval:
        """Infer a data interval for a run against this DAG.

        This method is used to bridge runs created prior to AIP-39
        implementation, which do not have an explicit data interval. Therefore,
        this method only considers ``schedule_interval`` values valid prior to
        Airflow 2.2.

        DO NOT call this method if there is a known data interval.

        :meta private:
        """
        timetable_type = type(self.timetable)
        if issubclass(
            timetable_type, (NullTimetable, OnceTimetable, DatasetTriggeredTimetable)
        ):
            return DataInterval.exact(timezone.coerce_datetime(logical_date))
        start = timezone.coerce_datetime(logical_date)
        if issubclass(timetable_type, CronDataIntervalTimetable):
            end = cast(CronDataIntervalTimetable, self.timetable)._get_next(start)
        elif issubclass(timetable_type, DeltaDataIntervalTimetable):
            end = cast(DeltaDataIntervalTimetable, self.timetable)._get_next(start)
        else:
            raise ValueError(f"Not a valid timetable: {self.timetable!r}")
        return DataInterval(start, end)

    def next_dagrun_info(
        self,
        last_automated_dagrun: None | datetime | DataInterval,
        *,
        restricted: bool = True,
    ) -> DagRunInfo | None:
        """Get information about the next DagRun of this dag after ``date_last_automated_dagrun``.

        This calculates what time interval the next DagRun should operate on
        (its execution date) and when it can be scheduled, according to the
        dag's timetable, start_date, end_date, etc. This doesn't check max
        active run or any other "max_active_tasks" type limits, but only
        performs calculations based on the various date and interval fields of
        this dag and its tasks.

        :param last_automated_dagrun: The ``max(execution_date)`` of
            existing "automated" DagRuns for this dag (scheduled or backfill,
            but not manual).
        :param restricted: If set to *False* (default is *True*), ignore
            ``start_date``, ``end_date``, and ``catchup`` specified on the DAG
            or tasks.
        :return: DagRunInfo of the next dagrun, or None if a dagrun is not
            going to be scheduled.
        """
        data_interval = last_automated_dagrun
        if restricted:
            restriction = self._time_restriction
        else:
            restriction = TimeRestriction(earliest=None, latest=None, catchup=True)
        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=data_interval,
                restriction=restriction,
            )
        except Exception:
            self.log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                data_interval,
                self.dag_id,
            )
            info = None
        return info

    @functools.cached_property
    def _time_restriction(self) -> TimeRestriction:
        start_dates = [t.start_date for t in self.tasks if t.start_date]
        if self.start_date is not None:
            start_dates.append(self.start_date)
        earliest = None
        if start_dates:
            earliest = timezone.coerce_datetime(min(start_dates))
        latest = self.end_date
        end_dates = [t.end_date for t in self.tasks if t.end_date]
        if len(end_dates) == len(self.tasks):  # not exists null end_date
            if self.end_date is not None:
                end_dates.append(self.end_date)
            if end_dates:
                latest = timezone.coerce_datetime(max(end_dates))
        return TimeRestriction(earliest, latest, self.catchup)

    def iter_dagrun_infos_between(
        self,
        earliest: pendulum.DateTime | None,
        latest: pendulum.DateTime,
        *,
        align: bool = True,
    ) -> Iterable[DagRunInfo]:
        """Yield DagRunInfo using this DAG's timetable between given interval.

        DagRunInfo instances yielded if their ``logical_date`` is not earlier
        than ``earliest``, nor later than ``latest``. The instances are ordered
        by their ``logical_date`` from earliest to latest.

        If ``align`` is ``False``, the first run will happen immediately on
        ``earliest``, even if it does not fall on the logical timetable schedule.
        The default is ``True``, but subdags will ignore this value and always
        behave as if this is set to ``False`` for backward compatibility.

        Example: A DAG is scheduled to run every midnight (``0 0 * * *``). If
        ``earliest`` is ``2021-06-03 23:00:00``, the first DagRunInfo would be
        ``2021-06-03 23:00:00`` if ``align=False``, and ``2021-06-04 00:00:00``
        if ``align=True``.
        """
        if earliest is None:
            earliest = self._time_restriction.earliest
        if earliest is None:
            raise ValueError(
                "earliest was None and we had no value in time_restriction to fallback on"
            )
        earliest = timezone.coerce_datetime(earliest)
        latest = timezone.coerce_datetime(latest)

        restriction = TimeRestriction(earliest, latest, catchup=True)
        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=restriction,
            )
        except Exception:
            self.log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                None,
                self.dag_id,
            )
            info = None

        if info is None:
            if not align:
                yield DagRunInfo.interval(earliest, latest)
            return
        if not align and info.logical_date != earliest:
            yield DagRunInfo.interval(earliest, info.data_interval.start)

        # Generate naturally according to schedule.
        while info is not None:
            yield info
            try:
                info = self.timetable.next_dagrun_info(
                    last_automated_data_interval=info.data_interval,
                    restriction=restriction,
                )
            except Exception:
                self.log.exception(
                    "Failed to fetch run info after data interval %s for DAG %r",
                    info.data_interval if info else "<NONE>",
                    self.dag_id,
                )
                break

    @provide_session
    def get_last_dagrun(self, session=NEW_SESSION, include_externally_triggered=False):
        return get_last_dagrun(
            self.dag_id,
            session=session,
            include_externally_triggered=include_externally_triggered,
        )

    @provide_session
    def has_dag_runs(
        self, session=NEW_SESSION, include_externally_triggered=True
    ) -> bool:
        return (
            get_last_dagrun(
                self.dag_id,
                session=session,
                include_externally_triggered=include_externally_triggered,
            )
            is not None
        )

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def default_view(self) -> str:
        return self._default_view

    def param(self, name: str, default: Any = NOTSET) -> DagParam:
        """
        Return a DagParam object for current dag.

        :param name: dag parameter name.
        :param default: fallback value for dag parameter.
        :return: DagParam instance for specified name and current dag.
        """
        return DagParam(current_dag=self, name=name, default=default)

    @property
    def tasks(self) -> list[Operator]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict)

    @property
    def teardowns(self) -> list[Operator]:
        return [task for task in self.tasks if getattr(task, "is_teardown", None)]

    @property
    def tasks_upstream_of_teardowns(self) -> list[Operator]:
        upstream_tasks = [t.upstream_list for t in self.teardowns]
        return [
            val
            for sublist in upstream_tasks
            for val in sublist
            if not getattr(val, "is_teardown", None)
        ]

    @property
    def task_group(self) -> TaskGroup:
        return self._task_group

    @property
    def relative_fileloc(self) -> pathlib.Path:
        """File location of the importable dag 'file' relative to the configured DAGs folder."""
        path = pathlib.Path(self.fileloc)
        try:
            rel_path = path.relative_to(
                self._processor_dags_folder or settings.DAGS_FOLDER
            )
            if rel_path == pathlib.Path("."):
                return path
            else:
                return rel_path
        except ValueError:
            # Not relative to DAGS_FOLDER.
            return path

    @property
    def folder(self) -> str:
        """Folder location of where the DAG object is instantiated."""
        return os.path.dirname(self.fileloc)

    @property
    def owner(self) -> str:
        """
        Return list of all owners found in DAG tasks.

        :return: Comma separated list of owners in DAG tasks
        """
        return ", ".join({t.owner for t in self.tasks})

    @property
    def allow_future_exec_dates(self) -> bool:
        return settings.ALLOW_FUTURE_EXEC_DATES and not self.timetable.can_be_scheduled

    @provide_session
    def get_concurrency_reached(self, session=NEW_SESSION) -> bool:
        """Return a boolean indicating whether the max_active_tasks limit for this DAG has been reached."""
        TI = TaskInstance
        total_tasks = session.scalar(
            select(func.count(TI.task_id)).where(
                TI.dag_id == self.dag_id,
                TI.state == TaskInstanceState.RUNNING,
            )
        )
        return total_tasks >= self.max_active_tasks

    @provide_session
    def get_is_active(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is active."""
        return session.scalar(
            select(DagModel.is_active).where(DagModel.dag_id == self.dag_id)
        )

    @provide_session
    def get_is_paused(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is paused."""
        return session.scalar(
            select(DagModel.is_paused).where(DagModel.dag_id == self.dag_id)
        )

    @provide_session
    def handle_callback(
        self, dagrun: DagRun, success=True, reason=None, session=NEW_SESSION
    ):
        """
        Triggers on_failure_callback or on_success_callback as appropriate.

        This method gets the context of a single TaskInstance part of this DagRun
        and passes that to the callable along with a 'reason', primarily to
        differentiate DagRun failures.

        .. note: The logs end up in
            ``$AIRFLOW_HOME/logs/scheduler/latest/PROJECT/DAG_FILE.py.log``

        :param dagrun: DagRun object
        :param success: Flag to specify if failure or success callback should be called
        :param reason: Completion reason
        :param session: Database session
        """
        callbacks = self.on_success_callback if success else self.on_failure_callback
        if callbacks:
            dagrun = DAG.fetch_dagrun(
                dag_id=self.dag_id, run_id=dagrun.run_id, session=session
            )
            callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
            tis = dagrun.get_task_instances(session=session)
            if self.partial:
                tis = [ti for ti in tis if not ti.state == State.NONE]
            # filter out removed tasks
            tis = [ti for ti in tis if ti.state != TaskInstanceState.REMOVED]
            ti = tis[-1]  # get first TaskInstance of DagRun
            ti.task = self.get_task(ti.task_id)
            context = ti.get_template_context(session=session)
            context["reason"] = reason
        callbacks, context = None, None
        if callbacks and context:
            for callback in callbacks:
                DAG.logger().info("Executing dag callback function: %s", callback)
                try:
                    callback(context)
                except Exception:
                    DAG.logger().exception("failed to invoke dag state update callback")
                    Stats.incr("dag.callback_exceptions", tags={"dag_id": self.dag_id})

    def get_active_runs(self):
        """
        Return a list of dag run execution dates currently running.s

        :return: List of execution dates
        """
        runs = DagRun.find(dag_id=self.dag_id, state=DagRunState.RUNNING)

        active_dates = []
        for run in runs:
            active_dates.append(run.execution_date)

        return active_dates

    @provide_session
    def get_num_active_runs(
        self, external_trigger=None, only_running=True, session=NEW_SESSION
    ):
        """
        Return the number of active "running" dag runs.

        :param external_trigger: True for externally triggered active dag runs
        :param session:
        :return: number greater than 0 for active dag runs
        """
        query = select(func.count()).where(DagRun.dag_id == self.dag_id)
        if only_running:
            query = query.where(DagRun.state == DagRunState.RUNNING)
        else:
            query = query.where(
                DagRun.state.in_({DagRunState.RUNNING, DagRunState.QUEUED})
            )

        if external_trigger is not None:
            query = query.where(
                DagRun.external_trigger
                == (expression.true() if external_trigger else expression.false())
            )

        return session.scalar(query)

    @staticmethod
    @internal_api_call
    @provide_session
    def fetch_dagrun(
        dag_id: str,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        session: Session = NEW_SESSION,
    ) -> DagRun | DagRunPydantic:
        """
        Return the dag run for a given execution date or run_id if it exists, otherwise none.

        :param dag_id: The dag_id of the DAG to find.
        :param execution_date: The execution date of the DagRun to find.
        :param run_id: The run_id of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        if not (execution_date or run_id):
            raise TypeError("You must provide either the execution_date or the run_id")
        query = select(DagRun)
        if execution_date:
            query = query.where(
                DagRun.dag_id == dag_id, DagRun.execution_date == execution_date
            )
        if run_id:
            query = query.where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
        return session.scalar(query)

    @provide_session
    def get_dagrun(
        self,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        session: Session = NEW_SESSION,
    ) -> DagRun | DagRunPydantic:
        return DAG.fetch_dagrun(
            dag_id=self.dag_id,
            execution_date=execution_date,
            run_id=run_id,
            session=session,
        )

    @provide_session
    def get_dagruns_between(self, start_date, end_date, session=NEW_SESSION):
        """
        Return the list of dag runs between start_date (inclusive) and end_date (inclusive).

        :param start_date: The starting execution date of the DagRun to find.
        :param end_date: The ending execution date of the DagRun to find.
        :param session:
        :return: The list of DagRuns found.
        """
        return session.scalars(
            select(DagRun).where(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date >= start_date,
                DagRun.execution_date <= end_date,
            )
        ).all()

    @provide_session
    def get_latest_execution_date(
        self, session: Session = NEW_SESSION
    ) -> pendulum.DateTime | None:
        """Return the latest date for which at least one dag run exists."""
        return session.scalar(
            select(func.max(DagRun.execution_date)).where(DagRun.dag_id == self.dag_id)
        )

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self, *, force_sandboxed: bool = False) -> jinja2.Environment:
        """Build a Jinja2 environment."""
        # Collect directories to search for template files
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        # Default values (for backward compatibility)
        jinja_env_options = {
            "loader": jinja2.FileSystemLoader(searchpath),
            "undefined": self.template_undefined,
            "extensions": ["jinja2.ext.do"],
            "cache_size": 0,
        }
        if self.jinja_environment_kwargs:
            jinja_env_options.update(self.jinja_environment_kwargs)
        env: jinja2.Environment
        if self.render_template_as_native_obj and not force_sandboxed:
            env = airflow.templates.NativeEnvironment(**jinja_env_options)
        else:
            env = airflow.templates.SandboxedEnvironment(**jinja_env_options)

        # Add any user defined items. Safe to edit globals as long as no templates are rendered yet.
        # http://jinja.pocoo.org/docs/2.10/api/#jinja2.Environment.globals
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)
        if self.user_defined_filters:
            env.filters.update(self.user_defined_filters)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """Set dependency between two tasks that already have been added to the DAG using add_task()."""
        self.get_task(upstream_task_id).set_downstream(
            self.get_task(downstream_task_id)
        )

    @provide_session
    def get_task_instances_before(
        self,
        base_date: datetime,
        num: int,
        *,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        """Get ``num`` task instances before (including) ``base_date``.

        The returned list may contain exactly ``num`` task instances
        corresponding to any DagRunType. It can have less if there are
        less than ``num`` scheduled DAG runs before ``base_date``.
        """
        execution_dates: list[Any] = session.execute(
            select(DagRun.execution_date)
            .where(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date <= base_date,
            )
            .order_by(DagRun.execution_date.desc())
            .limit(num)
        ).all()

        if not execution_dates:
            return self.get_task_instances(
                start_date=base_date, end_date=base_date, session=session
            )

        min_date: datetime | None = execution_dates[-1]._mapping.get(
            "execution_date"
        )  # getting the last value from the list

        return self.get_task_instances(
            start_date=min_date, end_date=base_date, session=session
        )

    @provide_session
    def get_task_instances(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        state: list[TaskInstanceState] | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        if not start_date:
            start_date = (timezone.utcnow() - timedelta(30)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        query = self._get_task_instances(
            task_ids=None,
            start_date=start_date,
            end_date=end_date,
            run_id=None,
            state=state or (),
            include_subdags=False,
            include_parentdag=False,
            include_dependent_dags=False,
            exclude_task_ids=(),
            session=session,
        )
        return session.scalars(
            cast(Select, query).order_by(DagRun.execution_date)
        ).all()

    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True, None] = None,
        start_date: datetime | None,
        end_date: datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        include_subdags: bool,
        include_parentdag: bool,
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        session: Session,
        dag_bag: DagBag | None = None,
        recursion_depth: int = 0,
        max_recursion_depth: int | None = None,
        visited_external_tis: set[TaskInstanceKey] | None = None,
    ) -> Iterable[TaskInstance] | set[TaskInstanceKey]:
        TI = TaskInstance

        # If we are looking at subdags/dependent dags we want to avoid UNION calls
        # in SQL (it doesn't play nice with fields that have no equality operator,
        # like JSON types), we instead build our result set separately.
        #
        # This will be empty if we are only looking at one dag, in which case
        # we can return the filtered TI query object directly.
        result: set[TaskInstanceKey] = set()

        # Do we want full objects, or just the primary columns?
        if as_pk_tuple:
            tis = select(TI.dag_id, TI.task_id, TI.run_id, TI.map_index)
        else:
            tis = select(TaskInstance)
        tis = tis.join(TaskInstance.dag_run)

        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in [*self.subdags, self]:
                conditions.append(
                    (TaskInstance.dag_id == dag.dag_id)
                    & TaskInstance.task_id.in_(dag.task_ids)
                )
            tis = tis.where(or_(*conditions))
        elif self.partial:
            tis = tis.where(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id.in_(self.task_ids),
            )
        else:
            tis = tis.where(TaskInstance.dag_id == self.dag_id)
        if run_id:
            tis = tis.where(TaskInstance.run_id == run_id)
        if start_date:
            tis = tis.where(DagRun.execution_date >= start_date)
        if task_ids is not None:
            tis = tis.where(TaskInstance.ti_selector_condition(task_ids))

        # This allows allow_trigger_in_future config to take affect, rather than mandating exec_date <= UTC
        if end_date or not self.allow_future_exec_dates:
            end_date = end_date or timezone.utcnow()
            tis = tis.where(DagRun.execution_date <= end_date)

        if state:
            if isinstance(state, (str, TaskInstanceState)):
                tis = tis.where(TaskInstance.state == state)
            elif len(state) == 1:
                tis = tis.where(TaskInstance.state == state[0])
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.where(TaskInstance.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.where(
                            or_(
                                TaskInstance.state.in_(not_none_state),
                                TaskInstance.state.is_(None),
                            )
                        )
                else:
                    tis = tis.where(TaskInstance.state.in_(state))

        # Next, get any of them from our parent DAG (if there is one)
        if include_parentdag and self.parent_dag is not None:
            if visited_external_tis is None:
                visited_external_tis = set()

            p_dag = self.parent_dag.partial_subset(
                task_ids_or_regex=r"^{}$".format(self.dag_id.split(".")[1]),
                include_upstream=False,
                include_downstream=True,
            )
            result.update(
                p_dag._get_task_instances(
                    task_ids=task_ids,
                    start_date=start_date,
                    end_date=end_date,
                    run_id=None,
                    state=state,
                    include_subdags=include_subdags,
                    include_parentdag=False,
                    include_dependent_dags=include_dependent_dags,
                    as_pk_tuple=True,
                    exclude_task_ids=exclude_task_ids,
                    session=session,
                    dag_bag=dag_bag,
                    recursion_depth=recursion_depth,
                    max_recursion_depth=max_recursion_depth,
                    visited_external_tis=visited_external_tis,
                )
            )

        if include_dependent_dags:
            # Recursively find external tasks indicated by ExternalTaskMarker
            from airflow.sensors.external_task import ExternalTaskMarker

            query = tis
            if as_pk_tuple:
                all_tis = session.execute(query).all()
                condition = TI.filter_for_tis(
                    TaskInstanceKey(*cols) for cols in all_tis
                )
                if condition is not None:
                    query = select(TI).where(condition)

            if visited_external_tis is None:
                visited_external_tis = set()

            external_tasks = session.scalars(
                query.where(TI.operator == ExternalTaskMarker.__name__)
            )

            for ti in external_tasks:
                ti_key = ti.key.primary
                if ti_key in visited_external_tis:
                    continue

                visited_external_tis.add(ti_key)

                task: ExternalTaskMarker = cast(
                    ExternalTaskMarker, copy.copy(self.get_task(ti.task_id))
                )
                ti.task = task

                if max_recursion_depth is None:
                    # Maximum recursion depth allowed is the recursion_depth of the first
                    # ExternalTaskMarker in the tasks to be visited.
                    max_recursion_depth = task.recursion_depth

                if recursion_depth + 1 > max_recursion_depth:
                    # Prevent cycles or accidents.
                    raise AirflowException(
                        f"Maximum recursion depth {max_recursion_depth} reached for "
                        f"{ExternalTaskMarker.__name__} {ti.task_id}. "
                        f"Attempted to clear too many tasks or there may be a cyclic dependency."
                    )
                ti.render_templates()
                external_tis = session.scalars(
                    select(TI)
                    .join(TI.dag_run)
                    .where(
                        TI.dag_id == task.external_dag_id,
                        TI.task_id == task.external_task_id,
                        DagRun.execution_date == pendulum.parse(task.execution_date),
                    )
                )

                for tii in external_tis:
                    if not dag_bag:
                        from airflow.models.dagbag import DagBag

                        dag_bag = DagBag(read_dags_from_db=True)
                    external_dag = dag_bag.get_dag(tii.dag_id, session=session)
                    if not external_dag:
                        raise AirflowException(f"Could not find dag {tii.dag_id}")
                    downstream = external_dag.partial_subset(
                        task_ids_or_regex=[tii.task_id],
                        include_upstream=False,
                        include_downstream=True,
                    )
                    result.update(
                        downstream._get_task_instances(
                            task_ids=None,
                            run_id=tii.run_id,
                            start_date=None,
                            end_date=None,
                            state=state,
                            include_subdags=include_subdags,
                            include_dependent_dags=include_dependent_dags,
                            include_parentdag=False,
                            as_pk_tuple=True,
                            exclude_task_ids=exclude_task_ids,
                            dag_bag=dag_bag,
                            session=session,
                            recursion_depth=recursion_depth + 1,
                            max_recursion_depth=max_recursion_depth,
                            visited_external_tis=visited_external_tis,
                        )
                    )

        if result or as_pk_tuple:
            # Only execute the `ti` query if we have also collected some other results (i.e. subdags etc.)
            if as_pk_tuple:
                tis_query = session.execute(tis).all()
                result.update(TaskInstanceKey(**cols._mapping) for cols in tis_query)
            else:
                result.update(ti.key for ti in session.scalars(tis))

            if exclude_task_ids is not None:
                result = {
                    task
                    for task in result
                    if task.task_id not in exclude_task_ids
                    and (task.task_id, task.map_index) not in exclude_task_ids
                }

        if as_pk_tuple:
            return result
        if result:
            # We've been asked for objects, lets combine it all back in to a result set
            ti_filters = TI.filter_for_tis(result)
            if ti_filters is not None:
                tis = select(TI).where(ti_filters)
        elif exclude_task_ids is None:
            pass  # Disable filter if not set.
        elif isinstance(next(iter(exclude_task_ids), None), str):
            tis = tis.where(TI.task_id.notin_(exclude_task_ids))
        else:
            tis = tis.where(
                not_(tuple_in_condition((TI.task_id, TI.map_index), exclude_task_ids))
            )

        return tis

    @provide_session
    def set_task_instance_state(
        self,
        *,
        task_id: str,
        map_indexes: Collection[int] | None = None,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        state: TaskInstanceState,
        upstream: bool = False,
        downstream: bool = False,
        future: bool = False,
        past: bool = False,
        commit: bool = True,
        session=NEW_SESSION,
    ) -> list[TaskInstance]:
        """
        Set the state of a TaskInstance and clear downstream tasks in failed or upstream_failed state.

        :param task_id: Task ID of the TaskInstance
        :param map_indexes: Only set TaskInstance if its map_index matches.
            If None (default), all mapped TaskInstances of the task are set.
        :param execution_date: Execution date of the TaskInstance
        :param run_id: The run_id of the TaskInstance
        :param state: State to set the TaskInstance to
        :param upstream: Include all upstream tasks of the given task_id
        :param downstream: Include all downstream tasks of the given task_id
        :param future: Include all future TaskInstances of the given task_id
        :param commit: Commit changes
        :param past: Include all past TaskInstances of the given task_id
        """
        from airflow.api.common.mark_tasks import set_state

        if not exactly_one(execution_date, run_id):
            raise ValueError("Exactly one of execution_date or run_id must be provided")

        task = self.get_task(task_id)
        task.dag = self

        tasks_to_set_state: list[Operator | tuple[Operator, int]]
        if map_indexes is None:
            tasks_to_set_state = [task]
        else:
            tasks_to_set_state = [(task, map_index) for map_index in map_indexes]

        altered = set_state(
            tasks=tasks_to_set_state,
            execution_date=execution_date,
            run_id=run_id,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
            state=state,
            commit=commit,
            session=session,
        )

        if not commit:
            return altered

        # Clear downstream tasks that are in failed/upstream_failed state to resume them.
        # Flush the session so that the tasks marked success are reflected in the db.
        session.flush()
        subdag = self.partial_subset(
            task_ids_or_regex={task_id},
            include_downstream=True,
            include_upstream=False,
        )

        if execution_date is None:
            dag_run = session.scalars(
                select(DagRun).where(
                    DagRun.run_id == run_id, DagRun.dag_id == self.dag_id
                )
            ).one()  # Raises an error if not found
            resolve_execution_date = dag_run.execution_date
        else:
            resolve_execution_date = execution_date

        end_date = resolve_execution_date if not future else None
        start_date = resolve_execution_date if not past else None

        subdag.clear(
            start_date=start_date,
            end_date=end_date,
            include_subdags=True,
            include_parentdag=True,
            only_failed=True,
            session=session,
            # Exclude the task itself from being cleared
            exclude_task_ids=frozenset({task_id}),
        )

        return altered

    @provide_session
    def set_task_group_state(
        self,
        *,
        group_id: str,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        state: TaskInstanceState,
        upstream: bool = False,
        downstream: bool = False,
        future: bool = False,
        past: bool = False,
        commit: bool = True,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        """
        Set TaskGroup to the given state and clear downstream tasks in failed or upstream_failed state.

        :param group_id: The group_id of the TaskGroup
        :param execution_date: Execution date of the TaskInstance
        :param run_id: The run_id of the TaskInstance
        :param state: State to set the TaskInstance to
        :param upstream: Include all upstream tasks of the given task_id
        :param downstream: Include all downstream tasks of the given task_id
        :param future: Include all future TaskInstances of the given task_id
        :param commit: Commit changes
        :param past: Include all past TaskInstances of the given task_id
        :param session: new session
        """
        from airflow.api.common.mark_tasks import set_state

        if not exactly_one(execution_date, run_id):
            raise ValueError("Exactly one of execution_date or run_id must be provided")

        tasks_to_set_state: list[BaseOperator | tuple[BaseOperator, int]] = []
        task_ids: list[str] = []

        if execution_date is None:
            dag_run = session.scalars(
                select(DagRun).where(
                    DagRun.run_id == run_id, DagRun.dag_id == self.dag_id
                )
            ).one()  # Raises an error if not found
            resolve_execution_date = dag_run.execution_date
        else:
            resolve_execution_date = execution_date

        end_date = resolve_execution_date if not future else None
        start_date = resolve_execution_date if not past else None

        task_group_dict = self.task_group.get_task_group_dict()
        task_group = task_group_dict.get(group_id)
        if task_group is None:
            raise ValueError("TaskGroup {group_id} could not be found")
        tasks_to_set_state = [
            task for task in task_group.iter_tasks() if isinstance(task, BaseOperator)
        ]
        task_ids = [task.task_id for task in task_group.iter_tasks()]
        dag_runs_query = select(DagRun.id).where(DagRun.dag_id == self.dag_id)
        if start_date is None and end_date is None:
            dag_runs_query = dag_runs_query.where(DagRun.execution_date == start_date)
        else:
            if start_date is not None:
                dag_runs_query = dag_runs_query.where(
                    DagRun.execution_date >= start_date
                )
            if end_date is not None:
                dag_runs_query = dag_runs_query.where(DagRun.execution_date <= end_date)

        with lock_rows(dag_runs_query, session):
            altered = set_state(
                tasks=tasks_to_set_state,
                execution_date=execution_date,
                run_id=run_id,
                upstream=upstream,
                downstream=downstream,
                future=future,
                past=past,
                state=state,
                commit=commit,
                session=session,
            )
            if not commit:
                return altered

            # Clear downstream tasks that are in failed/upstream_failed state to resume them.
            # Flush the session so that the tasks marked success are reflected in the db.
            session.flush()
            task_subset = self.partial_subset(
                task_ids_or_regex=task_ids,
                include_downstream=True,
                include_upstream=False,
            )

            task_subset.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=True,
                include_parentdag=True,
                only_failed=True,
                session=session,
                # Exclude the task from the current group from being cleared
                exclude_task_ids=frozenset(task_ids),
            )

        return altered

    @property
    def roots(self) -> list[Operator]:
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def leaves(self) -> list[Operator]:
        """Return nodes with no children. These are last to execute and are called leaves or leaf nodes."""
        return [task for task in self.tasks if not task.downstream_list]

    def topological_sort(self, include_subdag_tasks: bool = False):
        """
        Sorts tasks in topographical order, such that a task comes after any of its upstream dependencies.

        Deprecated in place of ``task_group.topological_sort``
        """
        from airflow.utils.task_group import TaskGroup

        def nested_topo(group):
            for node in group.topological_sort(
                _include_subdag_tasks=include_subdag_tasks
            ):
                if isinstance(node, TaskGroup):
                    yield from nested_topo(node)
                else:
                    yield node

        return tuple(nested_topo(self.task_group))

    @provide_session
    def set_dag_runs_state(
        self,
        state: DagRunState = DagRunState.RUNNING,
        session: Session = NEW_SESSION,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        dag_ids: list[str] | None = None,
    ) -> None:
        warnings.warn(
            "This method is deprecated and will be removed in a future version.",
            RemovedInAirflow3Warning,
            stacklevel=3,
        )
        dag_ids = dag_ids or [self.dag_id]
        query = update(DagRun).where(DagRun.dag_id.in_(dag_ids))
        if start_date:
            query = query.where(DagRun.execution_date >= start_date)
        if end_date:
            query = query.where(DagRun.execution_date <= end_date)
        session.execute(
            query.values(state=state).execution_options(synchronize_session="fetch")
        )

    @provide_session
    def clear(
        self,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        confirm_prompt: bool = False,
        include_subdags: bool = True,
        include_parentdag: bool = True,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: bool = False,
        session: Session = NEW_SESSION,
        get_tis: bool = False,
        recursion_depth: int = 0,
        max_recursion_depth: int | None = None,
        dag_bag: DagBag | None = None,
        exclude_task_ids: (
            frozenset[str] | frozenset[tuple[str, int]] | None
        ) = frozenset(),
    ) -> int | Iterable[TaskInstance]:
        """
        Clear a set of task instances associated with the current dag for a specified date range.

        :param task_ids: List of task ids or (``task_id``, ``map_index``) tuples to clear
        :param start_date: The minimum execution_date to clear
        :param end_date: The maximum execution_date to clear
        :param only_failed: Only clear failed tasks
        :param only_running: Only clear running tasks.
        :param confirm_prompt: Ask for confirmation
        :param include_subdags: Clear tasks in subdags and clear external tasks
            indicated by ExternalTaskMarker
        :param include_parentdag: Clear tasks in the parent dag of the subdag.
        :param dag_run_state: state to set DagRun to. If set to False, dagrun state will not
            be changed.
        :param dry_run: Find the tasks to clear but don't clear them.
        :param session: The sqlalchemy session to use
        :param dag_bag: The DagBag used to find the dags subdags (Optional)
        :param exclude_task_ids: A set of ``task_id`` or (``task_id``, ``map_index``)
            tuples that should not be cleared
        """
        if get_tis:
            warnings.warn(
                "Passing `get_tis` to dag.clear() is deprecated. Use `dry_run` parameter instead.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            dry_run = True

        if recursion_depth:
            warnings.warn(
                "Passing `recursion_depth` to dag.clear() is deprecated.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
        if max_recursion_depth:
            warnings.warn(
                "Passing `max_recursion_depth` to dag.clear() is deprecated.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )

        state: list[TaskInstanceState] = []
        if only_failed:
            state += [TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
        if only_running:
            # Yes, having `+=` doesn't make sense, but this was the existing behaviour
            state += [TaskInstanceState.RUNNING]

        tis = self._get_task_instances(
            task_ids=task_ids,
            start_date=start_date,
            end_date=end_date,
            run_id=None,
            state=state,
            include_subdags=include_subdags,
            include_parentdag=include_parentdag,
            include_dependent_dags=include_subdags,  # compat, yes this is not a typo
            session=session,
            dag_bag=dag_bag,
            exclude_task_ids=exclude_task_ids,
        )

        if dry_run:
            return session.scalars(tis).all()

        tis = session.scalars(tis).all()

        count = len(list(tis))
        do_it = True
        if count == 0:
            return 0
        if confirm_prompt:
            ti_list = "\n".join(str(t) for t in tis)
            question = f"You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? [y/n]"
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            clear_task_instances(
                list(tis),
                session,
                dag=self,
                dag_run_state=dag_run_state,
            )
        else:
            count = 0
            print("Cancelled, nothing was cleared.")

        session.flush()
        return count

    @classmethod
    def clear_dags(
        cls,
        dags,
        start_date=None,
        end_date=None,
        only_failed=False,
        only_running=False,
        confirm_prompt=False,
        include_subdags=True,
        include_parentdag=False,
        dag_run_state=DagRunState.QUEUED,
        dry_run=False,
    ):
        all_tis = []
        for dag in dags:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                confirm_prompt=False,
                include_subdags=include_subdags,
                include_parentdag=include_parentdag,
                dag_run_state=dag_run_state,
                dry_run=True,
            )
            all_tis.extend(tis)

        if dry_run:
            return all_tis

        count = len(all_tis)
        do_it = True
        if count == 0:
            print("Nothing to clear.")
            return 0
        if confirm_prompt:
            ti_list = "\n".join(str(t) for t in all_tis)
            question = f"You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? [y/n]"
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            for dag in dags:
                dag.clear(
                    start_date=start_date,
                    end_date=end_date,
                    only_failed=only_failed,
                    only_running=only_running,
                    confirm_prompt=False,
                    include_subdags=include_subdags,
                    dag_run_state=dag_run_state,
                    dry_run=False,
                )
        else:
            count = 0
            print("Cancelled, nothing was cleared.")
        return count

    def __deepcopy__(self, memo):
        # Switcharoo to go around deepcopying objects coming through the
        # backdoor
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k not in ("user_defined_macros", "user_defined_filters", "_log"):
                setattr(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        if hasattr(self, "_log"):
            result._log = self._log
        return result

    def sub_dag(self, *args, **kwargs):
        """Use `airflow.models.DAG.partial_subset`, this method is deprecated."""
        warnings.warn(
            "This method is deprecated and will be removed in a future version. Please use partial_subset",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return self.partial_subset(*args, **kwargs)

    def partial_subset(
        self,
        task_ids_or_regex: str | Pattern | Iterable[str],
        include_downstream=False,
        include_upstream=True,
        include_direct_upstream=False,
    ):
        """
        Return a subset of the current dag based on regex matching one or more tasks.

        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.

        :param task_ids_or_regex: Either a list of task_ids, or a regex to
            match against task ids (as a string, or compiled regex pattern).
        :param include_downstream: Include all downstream tasks of matched
            tasks, in addition to matched tasks.
        :param include_upstream: Include all upstream tasks of matched tasks,
            in addition to matched tasks.
        :param include_direct_upstream: Include all tasks directly upstream of matched
            and downstream (if include_downstream = True) tasks
        """
        from airflow.models.baseoperator import BaseOperator
        from airflow.models.mappedoperator import MappedOperator

        # deep-copying self.task_dict and self._task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        memo = {id(self.task_dict): None, id(self._task_group): None}
        dag = copy.deepcopy(self, memo)  # type: ignore

        if isinstance(task_ids_or_regex, (str, Pattern)):
            matched_tasks = [
                t for t in self.tasks if re2.findall(task_ids_or_regex, t.task_id)
            ]
        else:
            matched_tasks = [t for t in self.tasks if t.task_id in task_ids_or_regex]

        also_include_ids: set[str] = set()
        for t in matched_tasks:
            if include_downstream:
                for rel in t.get_flat_relatives(upstream=False):
                    also_include_ids.add(rel.task_id)
                    if (
                        rel not in matched_tasks
                    ):  # if it's in there, we're already processing it
                        # need to include setups and teardowns for tasks that are in multiple
                        # non-collinear setup/teardown paths
                        if not rel.is_setup and not rel.is_teardown:
                            also_include_ids.update(
                                x.task_id
                                for x in rel.get_upstreams_only_setups_and_teardowns()
                            )
            if include_upstream:
                also_include_ids.update(
                    x.task_id for x in t.get_upstreams_follow_setups()
                )
            else:
                if not t.is_setup and not t.is_teardown:
                    also_include_ids.update(
                        x.task_id for x in t.get_upstreams_only_setups_and_teardowns()
                    )
            if t.is_setup and not include_downstream:
                also_include_ids.update(
                    x.task_id for x in t.downstream_list if x.is_teardown
                )

        also_include: list[Operator] = [self.task_dict[x] for x in also_include_ids]
        direct_upstreams: list[Operator] = []
        if include_direct_upstream:
            for t in itertools.chain(matched_tasks, also_include):
                upstream = (
                    u
                    for u in t.upstream_list
                    if isinstance(u, (BaseOperator, MappedOperator))
                )
                direct_upstreams.extend(upstream)

        # Compiling the unique list of tasks that made the cut
        # Make sure to not recursively deepcopy the dag or task_group while copying the task.
        # task_group is reset later
        def _deepcopy_task(t) -> Operator:
            memo.setdefault(id(t.task_group), None)
            return copy.deepcopy(t, memo)

        dag.task_dict = {
            t.task_id: _deepcopy_task(t)
            for t in itertools.chain(matched_tasks, also_include, direct_upstreams)
        }

        def filter_task_group(group, parent_group):
            """Exclude tasks not included in the subdag from the given TaskGroup."""
            # We want to deepcopy _most but not all_ attributes of the task group, so we create a shallow copy
            # and then manually deep copy the instances. (memo argument to deepcopy only works for instances
            # of classes, not "native" properties of an instance)
            copied = copy.copy(group)

            memo[id(group.children)] = {}
            if parent_group:
                memo[id(group.parent_group)] = parent_group
            for attr, value in copied.__dict__.items():
                if id(value) in memo:
                    value = memo[id(value)]
                else:
                    value = copy.deepcopy(value, memo)
                copied.__dict__[attr] = value

            proxy = weakref.proxy(copied)

            for child in group.children.values():
                if isinstance(child, AbstractOperator):
                    if child.task_id in dag.task_dict:
                        task = copied.children[child.task_id] = dag.task_dict[
                            child.task_id
                        ]
                        task.task_group = proxy
                    else:
                        copied.used_group_ids.discard(child.task_id)
                else:
                    filtered_child = filter_task_group(child, proxy)

                    # Only include this child TaskGroup if it is non-empty.
                    if filtered_child.children:
                        copied.children[child.group_id] = filtered_child

            return copied

        dag._task_group = filter_task_group(self.task_group, None)

        # Removing upstream/downstream references to tasks and TaskGroups that did not make
        # the cut.
        subdag_task_groups = dag.task_group.get_task_group_dict()
        for group in subdag_task_groups.values():
            group.upstream_group_ids.intersection_update(subdag_task_groups)
            group.downstream_group_ids.intersection_update(subdag_task_groups)
            group.upstream_task_ids.intersection_update(dag.task_dict)
            group.downstream_task_ids.intersection_update(dag.task_dict)

        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # make the cut
            t.upstream_task_ids.intersection_update(dag.task_dict)
            t.downstream_task_ids.intersection_update(dag.task_dict)

        if len(dag.tasks) < len(self.tasks):
            dag.partial = True

        return dag

    def has_task(self, task_id: str):
        return task_id in self.task_dict

    def has_task_group(self, task_group_id: str) -> bool:
        return task_group_id in self.task_group_dict

    @functools.cached_property
    def task_group_dict(self):
        return {
            k: v
            for k, v in self._task_group.get_task_group_dict().items()
            if k is not None
        }

    def get_task(self, task_id: str, include_subdags: bool = False) -> Operator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        if include_subdags:
            for dag in self.subdags:
                if task_id in dag.task_dict:
                    return dag.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")

    def pickle_info(self):
        d = {}
        d["is_picklable"] = True
        try:
            dttm = timezone.utcnow()
            pickled = pickle.dumps(self)
            d["pickle_len"] = len(pickled)
            d["pickling_duration"] = str(timezone.utcnow() - dttm)
        except Exception as e:
            self.log.debug(e)
            d["is_picklable"] = False
            d["stacktrace"] = traceback.format_exc()
        return d

    @provide_session
    def pickle(self, session=NEW_SESSION) -> DagPickle:
        dag = session.scalar(
            select(DagModel).where(DagModel.dag_id == self.dag_id).limit(1)
        )
        dp = None
        if dag and dag.pickle_id:
            dp = session.scalar(
                select(DagPickle).where(DagPickle.id == dag.pickle_id).limit(1)
            )
        if not dp or dp.pickle != self:
            dp = DagPickle(dag=self)
            session.add(dp)
            self.last_pickled = timezone.utcnow()
            session.commit()
            self.pickle_id = dp.id

        return dp

    def tree_view(self) -> None:
        """Print an ASCII tree representation of the DAG."""
        for tmp in self._generate_tree_view():
            print(tmp)

    def _generate_tree_view(self) -> Generator[str, None, None]:
        def get_downstream(task, level=0) -> Generator[str, None, None]:
            yield (" " * level * 4) + str(task)
            level += 1
            for tmp_task in sorted(task.downstream_list, key=lambda x: x.task_id):
                yield from get_downstream(tmp_task, level)

        for t in sorted(self.roots, key=lambda x: x.task_id):
            yield from get_downstream(t)

    def get_tree_view(self) -> str:
        """Return an ASCII tree representation of the DAG."""
        rst = ""
        for tmp in self._generate_tree_view():
            rst += tmp + "\n"
        return rst

    @property
    def task(self) -> TaskDecoratorCollection:
        from airflow.decorators import task

        return cast("TaskDecoratorCollection", functools.partial(task, dag=self))

    def add_task(self, task: Operator) -> None:
        """
        Add a task to the DAG.

        :param task: the task you want to add
        """
        FailStopDagInvalidTriggerRule.check(dag=self, trigger_rule=task.trigger_rule)

        from airflow.utils.task_group import TaskGroupContext

        # if the task has no start date, assign it the same as the DAG
        if not task.start_date:
            task.start_date = self.start_date
        # otherwise, the task will start on the later of its own start date and
        # the DAG's start date
        elif self.start_date:
            task.start_date = max(task.start_date, self.start_date)

        # if the task has no end date, assign it the same as the dag
        if not task.end_date:
            task.end_date = self.end_date
        # otherwise, the task will end on the earlier of its own end date and
        # the DAG's end date
        elif task.end_date and self.end_date:
            task.end_date = min(task.end_date, self.end_date)

        task_id = task.task_id
        if not task.task_group:
            task_group = TaskGroupContext.get_current_task_group(self)
            if task_group:
                task_id = task_group.child_id(task_id)
                task_group.add(task)

        if (
            task_id in self.task_dict and self.task_dict[task_id] is not task
        ) or task_id in self._task_group.used_group_ids:
            raise DuplicateTaskIdFound(
                f"Task id '{task_id}' has already been added to the DAG"
            )
        else:
            self.task_dict[task_id] = task
            task.dag = self
            # Add task_id to used_group_ids to prevent group_id and task_id collisions.
            self._task_group.used_group_ids.add(task_id)

        self.task_count = len(self.task_dict)

    def add_tasks(self, tasks: Iterable[Operator]) -> None:
        """
        Add a list of tasks to the DAG.

        :param tasks: a lit of tasks you want to add
        """
        for task in tasks:
            self.add_task(task)

    def _remove_task(self, task_id: str) -> None:
        # This is "private" as removing could leave a hole in dependencies if done incorrectly, and this
        # doesn't guard against that
        task = self.task_dict.pop(task_id)
        tg = getattr(task, "task_group", None)
        if tg:
            tg._remove(task)

        self.task_count = len(self.task_dict)

    def run(
        self,
        start_date=None,
        end_date=None,
        mark_success=False,
        local=False,
        executor=None,
        donot_pickle=airflow_conf.getboolean("core", "donot_pickle"),
        ignore_task_deps=False,
        ignore_first_depends_on_past=True,
        pool=None,
        delay_on_limit_secs=1.0,
        verbose=False,
        conf=None,
        rerun_failed_tasks=False,
        run_backwards=False,
        run_at_least_once=False,
        continue_on_failures=False,
        disable_retry=False,
    ):
        """
        Run the DAG.

        :param start_date: the start date of the range to run
        :param end_date: the end date of the range to run
        :param mark_success: True to mark jobs as succeeded without running them
        :param local: True to run the tasks using the LocalExecutor
        :param executor: The executor instance to run the tasks
        :param donot_pickle: True to avoid pickling DAG object and send to workers
        :param ignore_task_deps: True to skip upstream tasks
        :param ignore_first_depends_on_past: True to ignore depends_on_past
            dependencies for the first set of tasks only
        :param pool: Resource pool to use
        :param delay_on_limit_secs: Time in seconds to wait before next attempt to run
            dag run when max_active_runs limit has been reached
        :param verbose: Make logging output more verbose
        :param conf: user defined dictionary passed from CLI
        :param rerun_failed_tasks:
        :param run_backwards:
        :param run_at_least_once: If true, always run the DAG at least once even
            if no logical run exists within the time range.
        """
        from airflow.jobs.backfill_job_runner import BackfillJobRunner

        if not executor and local:
            from airflow.executors.local_executor import LocalExecutor

            executor = LocalExecutor()
        elif not executor:
            from airflow.executors.executor_loader import ExecutorLoader

            executor = ExecutorLoader.get_default_executor()
        from airflow.jobs.job import Job

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success,
            donot_pickle=donot_pickle,
            ignore_task_deps=ignore_task_deps,
            ignore_first_depends_on_past=ignore_first_depends_on_past,
            pool=pool,
            delay_on_limit_secs=delay_on_limit_secs,
            verbose=verbose,
            conf=conf,
            rerun_failed_tasks=rerun_failed_tasks,
            run_backwards=run_backwards,
            run_at_least_once=run_at_least_once,
            continue_on_failures=continue_on_failures,
            disable_retry=disable_retry,
        )
        run_job(job=job, execute_callable=job_runner._execute)

    def get_default_view(self):
        """Allow backward compatible jinja2 templates."""
        return (
            self.default_view
            if self.default_view
            else settings.dag_default_view.lower()
        )
