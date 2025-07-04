from fastapi import FastAPI
from pydantic import BaseModel
import aio_pika as pk
import asyncio
import uvicorn
import uuid
import json
from contextlib import asynccontextmanager

EXCHANGE_NAME = "order.exchange"
ROUTING_KEY_PRODUCE = "order.placed"

class BaseOrder(BaseModel):
	user_id: str
	items: list[str]
	total: float

async def connect(app: FastAPI):
	print("[Order service] Connecting to RabbitMQ...")
	for _ in range(100):
		try:
			connection = await pk.connect_robust("amqp://guest:guest@rabbitmq/")
			app.state.connection = connection
			print("[Order service] Connected to RabbitMQ.")
			return
		except Exception:
			print(f"[Order service] Waiting for RabbitMQ...")
			await asyncio.sleep(5)
	raise RuntimeError("[Order service] Failed to connect to RabbitMQ.")

@asynccontextmanager
async def lifespan(app: FastAPI):
	await connect(app)
	produce_channel = await app.state.connection.channel()
	await produce_channel.set_qos(prefetch_count = 10)
	produce_exchange = await produce_channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	app.state.channel = produce_channel
	app.state.exchange = produce_exchange
	yield
	print("[Order service] Disconnectiong from RabbitMQ...")
	await app.state.connection.close()

app = FastAPI(lifespan = lifespan)

@app.post("/order")
async def create_order(base_order: BaseOrder):
	order_id = str(uuid.uuid4())
	order = {"order_id": order_id, "user_id": base_order.user_id, "items": base_order.items, "total": base_order.total}
	await app.state.exchange.publish(pk.Message(body = json.dumps(order).encode(), delivery_mode = pk.DeliveryMode.PERSISTENT), routing_key = ROUTING_KEY_PRODUCE)
	return {"status": "ok", "order_id": order_id}

if __name__ == "__main__":
	uvicorn.run(app, host = "0.0.0.0", port = 8000)