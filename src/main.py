from src.generics.publishers.scheduler import Scheduler
from src.generics.subscribers.redis_api_subscriber import RedisSubscriber

scheduler = Scheduler()
redis_subscriber = RedisSubscriber(
    "172.18.0.2", 6379, "http://localhost:3002/api/worker", "POST"
)
scheduler.subscribe(redis_subscriber)
scheduler.publish({"task": "Do something"})
