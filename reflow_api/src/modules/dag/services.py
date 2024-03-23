from src.config import get_settings
from src.modules.dag.model import DAG
from src.modules.dag.dto import CreateDagDTO
from sqlalchemy.orm import Session

settings = get_settings()


class DAGService:
    def __init__(self, session: Session):
        self.session = session

    def get_one(self, id: str):
        return self.session.query(DAG).filter(DAG.id == id).first()

    def create_dag(self, dag: CreateDagDTO):
        db_user = DAG(
            name=dag.dag_name,
            start_date=dag.start_date,
            schedule_interval=dag.schedule_interval,
            active=dag.is_active,
        )
        self.session.add(db_user)
        self.session.commit()
        self.session.refresh(db_user)
        return db_user

    def list_dags(self, offset: int = 0, limit: int = 100):
        return self.session.query(DAG).offset(offset).limit(limit).all()
