from fastapi import APIRouter, Depends, Header, Request
from src.modules.dag.dto import CreateDagDTO, DagError
from src.modules.dag.controllers import DAGController
from src.connectors.sqlalchemy import get_db
from sqlalchemy.orm import Session
from src.auth.clients.keycloack_client import KeyCloackAuthClient

authorizer = KeyCloackAuthClient()
router = APIRouter(prefix="/dags", tags=["dag"])


@router.get("")
@authorizer.authenticate()
def list_dags(
    request: Request,
    user_info: dict,
    access_token: str = Header("access_token"),
    session: Session = Depends(get_db),
    offset: int = 0,
    limit: int = 100,
):
    return DAGController(session).list_dags(offset, limit)


@router.patch("/set-error")
def set_error(dag_error: DagError, session: Session = Depends(get_db)):
    print(dag_error)
    return "Hello, World!"


@router.post("")
def create_dag(dag: CreateDagDTO, session: Session = Depends(get_db)):
    return DAGController(session).create_dag(dag)


@router.put("")
def update_dag():
    return "Hello, World!"


@router.delete("")
def delete_dag():
    return "Hello, World!"
