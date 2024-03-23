from abc import ABC, abstractmethod


class AuthClientInterface(ABC):
    @abstractmethod
    def authenticate(self, username: str, password: str) -> dict:
        pass

    @abstractmethod
    def get_user(self, user_id: str) -> dict:
        pass

    @abstractmethod
    def get_user_by_username(self, username: str) -> dict:
        pass

    @abstractmethod
    def update_password(self, user_id: str, password: str) -> dict:
        pass
