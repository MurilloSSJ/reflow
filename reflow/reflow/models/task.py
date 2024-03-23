from pydantic import BaseModel, validator
from typing import List, Optional
from reflow.reflow.operators.interface import ReflowOperator


class Task(BaseModel):
    id: str
    operator: ReflowOperator
    command: str
    trigger_rule: str
    dependencies: Optional[List["Task"]] = None

    @validator("operator")
    def validate_operator_instance(cls, v):
        if not isinstance(v, ReflowOperator):
            raise ValueError("Input should be an instance of ReflowOperator")
        return v

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            ReflowOperator: lambda v: v.__class__.__name__,
        }
