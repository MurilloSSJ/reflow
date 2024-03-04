from pydantic import BaseModel


class WorkerCreate(BaseModel):
    worker_id: str
    ssh: bool
