import redis


class RedisConnector:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def connect(self):
        return redis.StrictRedis(host=self.host, port=self.port, db=0)

    def set(self, key, value):
        self.connect().set(key, value)

    def get(self, key):
        return self.connect().get(key)
