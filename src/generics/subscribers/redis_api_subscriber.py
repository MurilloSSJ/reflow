from src.abstracts.subscribers import AbstractSubscriber
from redis import Redis
import uuid
import requests


class RedisSubscriber(AbstractSubscriber):
    def __init__(self, host, port, api_url: str, method: str):
        self.redis = Redis(host=host, port=port)
        self.api_url = api_url
        self.method = method

    def update(self, message: dict) -> None:
        task_id = uuid.uuid4()
        self.redis.hset(str(task_id), mapping=message)
        response = requests.request(
            self.method, self.api_url, json={"worker_id": str(task_id), "ssh": False}
        )
        print(f"Redis Subscriber received message: {response.json()}")

    def get_message(self, key):
        return self.redis.hgetall(key)
