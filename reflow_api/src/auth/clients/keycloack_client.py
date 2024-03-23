from src.auth.clients.client import AuthClientInterface
import requests
from fastapi import HTTPException, Request
from src.config import get_settings
from functools import wraps

settings = get_settings()


class KeyCloackAuthClient(AuthClientInterface):
    def __init__(self):
        self.base_url = "http://keycloak:7080"
        self.client_secret = settings.keycloak_client_credentials

    def get_token(self, username: str, password: str) -> str:
        response = requests.post(
            f"{self.base_url}/realms/airflow/protocol/openid-connect/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "client_id": "reflow",
            },
            data={
                "client_id": "reflow",  # "reflow
                "username": username,
                "password": password,
                "grant_type": "password",
                "client_secret": self.client_secret,
                "scope": "openid",
            },
        )
        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return response.json()

    def get_user_info(self, token: str) -> dict:
        print(isinstance(token, str), isinstance(token, dict))
        response = requests.get(
            f"{self.base_url}/realms/airflow/protocol/openid-connect/userinfo",
            headers={"Authorization": f"Bearer {token}"},
        )
        if response.status_code != 200:
            print("get user info exception")
            print(response.text)
            print(response.status_code)
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return response.json()

    def authenticate(self):
        def decorator(func):
            @wraps(func)
            def wrapper(request: Request, user_info, *args, **kwargs):
                return func(
                    request,
                    user_info=self.get_user_info(request.headers.get("access_token")),
                    *args,
                    **kwargs,
                )

            return wrapper

        return decorator

    def create_user(self):
        pass

    def get_user(self, user_id: str) -> dict:
        pass

    def get_user_by_username(self, username: str) -> dict:
        pass

    def update_password(self, user_id: str, password: str) -> dict:
        pass
