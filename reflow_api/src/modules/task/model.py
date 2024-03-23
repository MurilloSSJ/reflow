from src.connectors.sqlalchemy import Base
from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
from uuid import uuid4


class Task(Base):
    __tablename__ = "tasks"

    id = Column(UUID, primary_key=True, index=True, default=uuid4)
    name = Column(String, index=True)
    description = Column(String)
    dag_id = Column(UUID, ForeignKey("dags.id"))
    dag = relationship("DAG", back_populates="tasks")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Task {self.name}>"
