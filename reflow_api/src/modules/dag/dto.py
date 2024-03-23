from pydantic import BaseModel
from typing import Union, List, Optional
import uuid
from datetime import datetime, timedelta
from enum import Enum


class DefaultViewEnum(str, Enum):
    left = "left"
    right = "right"
    up = "up"
    bottom = "bottom"


class CreateDagDTO(BaseModel):
    dag_name: str
    is_paused: bool = True
    is_active: bool = False
    start_date: Optional[datetime]
    fileloc: str
    schedule_interval: Optional[timedelta]
    has_import_errors: bool = False
    next_dagrun: Optional[datetime]


class DagError(BaseModel):
    error_log: str
    absolute_path: str
