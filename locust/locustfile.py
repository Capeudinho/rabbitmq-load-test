from locust import HttpUser, task, between
import random
import uuid

class OrderUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def place_order(self):
        order = {"user_id": str(uuid.uuid4()), "items": random.sample(["processor", "keyboard", "screen", "mouse", "cabinet"], k = 2), "total": round(random.uniform(5, 30), 2)}
        self.client.post("/order", json = order)