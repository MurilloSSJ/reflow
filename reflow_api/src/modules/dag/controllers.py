from src.modules.dag.services import DAGService
from src.modules.dag.dto import CreateDagDTO
from sqlalchemy.orm import Session


class DAGController:
    def __init__(self, session: Session):
        self.service = DAGService(session)

    def get_dag(self):
        return self.service.get_dag()

    def list_dags(self, offset: int = 0, limit: int = 100):
        return self.service.list_dags(offset, limit)

    def set_error(self, dag_error):
        return self.service.set_error(dag_error)

    def create_dag(self, dag: CreateDagDTO):
        return self.service.create_dag(dag)

    def update_dag(self):
        return self.service.update_dag()

    def delete_dag(self):
        return self.service.delete_dag()
