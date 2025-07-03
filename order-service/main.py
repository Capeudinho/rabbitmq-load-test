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

class Order(BaseModel):
	user_id: str
	items: list[str]
	total: float

async def connect():
	for _ in range(100):
		try:
			connection = await pk.connect_robust("amqp://guest:guest@rabbitmq/")
			print("[Order service] Connected to RabbitMQ.")
			return connection
		except Exception:
			print(f"[Order service] Waiting for RabbitMQ...")
			await asyncio.sleep(5)
	raise RuntimeError("[Order service] Connection to RabbitMQ failed.")

@asynccontextmanager
async def lifespan(app: FastAPI):
	print("[Order service] Starting up...")
	print("[Order service] Connecting to RabbitMQ...")
	connection = await connect()
	channel = await connection.channel()
	await channel.set_qos(prefetch_count = 10)
	exchange = await channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	app.state.connection = connection
	app.state.channel = channel
	app.state.exchange = exchange
	yield
	print("[Order service] Disconnectiong from RabbitMQ...")
	await connection.close()
	print("[Order service] Shutting down...")

app = FastAPI(lifespan = lifespan)

@app.post("/order")
async def create_order(order: Order):
	order_id = str(uuid.uuid4())
	message = {"order_id": order_id, "user_id": order.user_id, "items": order.items, "total": order.total}
	exchange = app.state.exchange
	await exchange.publish(pk.Message(body = json.dumps(message).encode(), delivery_mode = pk.DeliveryMode.PERSISTENT), routing_key = ROUTING_KEY_PRODUCE)
	return {"status": "ok", "order_id": order_id}

if __name__ == "__main__":
	uvicorn.run(app, host = "0.0.0.0", port = 8000)