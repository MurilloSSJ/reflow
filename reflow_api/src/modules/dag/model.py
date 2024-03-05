from src.connectors.sqlalchemy import Base
from sqlalchemy import Column, Boolean, Integer, String, Text, Interval, Index
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import backref, joinedload, relationship
import pathlib
from sqlalchemy.dialects.postgresql import UUID
from src.generics.models.utc_datetime import UtcDateTime
from datetime import datetime


class DagModel(Base):
    """Table containing DAG properties."""

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    root_dag_id = Column(UUID(as_uuid=True), index=True)
    is_paused = Column(Boolean, default=True)
    # Whether that DAG was seen on the last DagBag load
    is_active = Column(Boolean, default=False)
    # Last time the scheduler started
    last_parsed_time = Column(UtcDateTime)
    # Last time this DAG was pickled
    last_pickled = Column(UtcDateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = Column(UtcDateTime)
    # Whether (one  of) the scheduler is scheduling this DAG at the moment
    scheduler_lock = Column(Boolean)
    # Foreign key to the latest pickle_id
    pickle_id = Column(Integer)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    fileloc = Column(String(2000))
    # The base directory used by Dag Processor that parsed this dag.
    processor_subdir = Column(String(2000), nullable=True)
    # String representing the owners
    owners = Column(String(2000))
    # Description of the dag
    description = Column(Text)
    # Default view of the DAG inside the webserver
    default_view = Column(String(25))
    # Schedule interval
    schedule_interval = Column(Interval)
    # Timetable/Schedule Interval description
    timetable_description = Column(String(1000), nullable=True)
    # Tags for view filter
    tags = relationship(
        "DagTag", cascade="all, delete, delete-orphan", backref=backref("dag")
    )
    # Dag owner links for DAGs view
    dag_owner_links = relationship(
        "DagOwnerAttributes",
        cascade="all, delete, delete-orphan",
        backref=backref("dag"),
    )

    max_active_tasks = Column(
        Integer,
        nullable=False,
        default=airflow_conf.getint("core", "max_active_tasks_per_dag"),
    )
    max_active_runs = Column(
        Integer,
        nullable=True,
        default=airflow_conf.getint("core", "max_active_runs_per_dag"),
    )

    has_task_concurrency_limits = Column(Boolean, nullable=False, default=True)
    has_import_errors = Column(Boolean(), default=False, server_default="0")

    # The logical date of the next dag run.
    next_dagrun = Column(UtcDateTime)

    # Must be either both NULL or both datetime.
    next_dagrun_data_interval_start = Column(UtcDateTime)
    next_dagrun_data_interval_end = Column(UtcDateTime)

    # Earliest time at which this ``next_dagrun`` can be created.
    next_dagrun_create_after = Column(UtcDateTime)

    __table_args__ = (
        Index("idx_root_dag_id", root_dag_id, unique=False),
        Index("idx_next_dagrun_create_after", next_dagrun_create_after, unique=False),
    )

    parent_dag = relationship(
        "DagModel",
        remote_side=[dag_id],
        primaryjoin=root_dag_id == dag_id,
        foreign_keys=[root_dag_id],
    )
    schedule_dataset_references = relationship(
        "DagScheduleDatasetReference",
        cascade="all, delete, delete-orphan",
    )
    schedule_datasets = association_proxy("schedule_dataset_references", "dataset")
    task_outlet_dataset_references = relationship(
        "TaskOutletDatasetReference",
        cascade="all, delete, delete-orphan",
    )
    NUM_DAGS_PER_DAGRUN_QUERY = airflow_conf.getint(
        "scheduler", "max_dagruns_to_create_per_loop", fallback=10
    )

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    @property
    def next_dagrun_data_interval(self) -> DataInterval | None:
        return _get_model_data_interval(
            self,
            "next_dagrun_data_interval_start",
            "next_dagrun_data_interval_end",
        )

    @next_dagrun_data_interval.setter
    def next_dagrun_data_interval(
        self, value: tuple[datetime, datetime] | None
    ) -> None:
        if value is None:
            self.next_dagrun_data_interval_start = (
                self.next_dagrun_data_interval_end
            ) = None
        else:
            self.next_dagrun_data_interval_start, self.next_dagrun_data_interval_end = (
                value
            )

    @property
    def timezone(self):
        return settings.TIMEZONE

    def get_default_view(self) -> str:
        """Get the Default DAG View, returns the default config value if DagModel does not have a value."""
        # This is for backwards-compatibility with old dags that don't have None as default_view
        return (
            self.default_view
            or airflow_conf.get_mandatory_value("webserver", "dag_default_view").lower()
        )

    @property
    def safe_dag_id(self):
        return self.dag_id.replace(".", "__dot__")

    @property
    def relative_fileloc(self) -> pathlib.Path | None:
        """File location of the importable dag 'file' relative to the configured DAGs folder."""
        if self.fileloc is None:
            return None
        path = pathlib.Path(self.fileloc)
        try:
            return path.relative_to(settings.DAGS_FOLDER)
        except ValueError:
            # Not relative to DAGS_FOLDER.
            return path

    def calculate_dagrun_date_fields(
        self,
        dag: DAG,
        last_automated_dag_run: None | datetime | DataInterval,
    ) -> None:
        """
        Calculate ``next_dagrun`` and `next_dagrun_create_after``.

        :param dag: The DAG object
        :param last_automated_dag_run: DataInterval (or datetime) of most recent run of this dag, or none
            if not yet scheduled.
        """
        next_dagrun_info = dag.next_dagrun_info(last_automated_dag_run)
        if next_dagrun_info is None:
            self.next_dagrun_data_interval = self.next_dagrun = (
                self.next_dagrun_create_after
            ) = None
        else:
            self.next_dagrun_data_interval = next_dagrun_info.data_interval
            self.next_dagrun = next_dagrun_info.logical_date
            self.next_dagrun_create_after = next_dagrun_info.run_after
