from src.connectors.sqlalchemy import Base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4
from src.modules.task.model import Task


class DAG(Base):
    __tablename__ = "dags"

    id = Column(UUID, primary_key=True, index=True, default=uuid4)
    name = Column(String, unique=True, index=True)
    start_date = Column(DateTime)
    schedule_interval = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    tasks = relationship("Task", back_populates="dag")

    def __repr__(self):
        return f"<DAG {self.name}>"
