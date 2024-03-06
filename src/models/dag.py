from src.connectors.sqlalchemy import Base
from sqlalchemy import Column, Boolean, Integer, String, Text, Interval, Index
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import backref, joinedload, relationship
from src.config import get_settings
from sqlalchemy.dialects.postgresql import UUID
from src.generics.models.utc_datetime import UtcDateTime
import uuid

settings = get_settings()


class DagModel(Base):
    """Table containing DAG properties."""

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4())
    is_paused = Column(Boolean, default=True)
    is_active = Column(Boolean, default=False)
    last_parsed_time = Column(UtcDateTime)
    last_pickled = Column(UtcDateTime)
    last_expired = Column(UtcDateTime)
    scheduler_lock = Column(Boolean)
    pickle_id = Column(Integer)
    fileloc = Column(String(2000))
    processor_subdir = Column(String(2000), nullable=True)
    owners = Column(String(2000))
    description = Column(Text)
    default_view = Column(String(25), default=settings.dag_default_view)
    schedule_interval = Column(Interval)
    timetable_description = Column(String(1000), nullable=True)
    tags = relationship(
        "DagTag", cascade="all, delete, delete-orphan", backref=backref("dag")
    )
    dag_owner_links = relationship(
        "DagOwnerAttributes",
        cascade="all, delete, delete-orphan",
        backref=backref("dag"),
    )
    max_active_tasks = Column(
        Integer,
        nullable=False,
        default=settings.max_active_tasks_per_dag,
    )
    max_active_runs = Column(
        Integer,
        nullable=True,
        default=settings.max_active_runs_per_dag,
    )
    has_task_concurrency_limits = Column(Boolean, nullable=False, default=True)
    has_import_errors = Column(Boolean(), default=False, server_default="0")
    next_dagrun = Column(UtcDateTime)
