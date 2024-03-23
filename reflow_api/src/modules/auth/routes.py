from fastapi import APIRouter, Depends, Header
from src.modules.dag.dto import CreateDagDTO, DagError
from src.modules.dag.controllers import DAGController
from src.modules.auth.dto import LoginDTO
from src.auth.clients.keycloack_client import KeyCloackAuthClient

authorizer = KeyCloackAuthClient()
router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login")
def login(login: LoginDTO):
    return authorizer.get_token(username=login.username, password=login.password)
