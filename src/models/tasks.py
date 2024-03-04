from pydantic import BaseModel


class Tasks(BaseModel):
    task_id: str
    status: str
