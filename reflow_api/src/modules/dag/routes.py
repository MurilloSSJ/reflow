from fastapi import APIRouter, HTTPException, Depends
from src.modules.dag.dto import CreateDagDTO, DagError

router = APIRouter(prefix="/dags", tags=["dag"])


@router.get("")
def get_dag():
    return "Hello, World!"


@router.patch("/set-error")
def set_error(dag_error: DagError):
    print(dag_error)
    return "Hello, World!"


@router.post("")
def create_dag():
    return "Hello, World!"


@router.put("")
def update_dag():
    return "Hello, World!"


@router.delete("")
def delete_dag():
    return "Hello, World!"
