from fastapi import APIRouter, HTTPException, Depends

router = APIRouter(prefix="/dags", tags=["dag"])


@router.get("")
def get_dag():
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
