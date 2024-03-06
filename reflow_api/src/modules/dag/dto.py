from pydantic import BaseModel
from typing import Union, List, Optional
import uuid
from datetime import datetime
from enum import Enum
from src.generics.dto.interval import PydanticInterval


class DefaultViewEnum(str, Enum):
    grid = "grid"
    graph = "graph"
    duration = "duration"
    gantt = "gantt"
    landing_times = "landing_times"


class CreateDagDTO(BaseModel):
    dag_id: uuid.UUID
    is_paused: bool = True
    is_active: bool = False
    last_parsed_time: datetime = datetime.now()
    scheduler_lock: bool = False
    fileloc: str
    owners: List[str]
    description: str
    default_view: DefaultViewEnum
    schedule_interval: Optional[PydanticInterval]
    timetable_description: Union[str, None] = None
    tags: List[str]
    max_active_tasks: Union[int, None] = None
    max_active_runs: Union[int, None] = None
    has_task_concurrency_limits: bool
    has_import_errors: bool = False
    next_dagrun: datetime
