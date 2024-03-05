from fastapi import APIRouter, HTTPException, Depends
from src.modules.worker.dto import WorkerCreate

router = APIRouter()


@router.post("/worker")
async def read_worker(worker: WorkerCreate):
    print(worker)
    return worker
