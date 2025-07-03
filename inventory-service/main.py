import asyncio
import aio_pika as pk
import json
import random

EXCHANGE_NAME = "order.exchange"
QUEUE_NAME = "order.placed.queue"
ROUTING_KEY_CONSUME = "order.placed"
ROUTING_KEY_PRODUCE = "order.stocked"

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

async def handle_message(message: pk.IncomingMessage):
	async with message.process():
		order = json.loads(message.body)
		print("[Inventory service] Placed order "+order["order_id"]+" received.")
		if random.random() < 0.8:
			print("[Inventory service] Available stock for "+order["order_id"]+".")
			await publish_stocked_event(order)
		else:
			print("[Inventory service] Unavailable stock for "+order["order_id"]+".")

async def publish_stocked_event(order):
	connection = await connect()
	channel = await connection.channel()
	exchange = await channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	await exchange.publish(pk.Message(body = json.dumps(order).encode(), delivery_mode = pk.DeliveryMode.PERSISTENT),routing_key = ROUTING_KEY_PRODUCE)
	print("[Inventory service] Order "+order["order_id"]+" stocked.")
	await connection.close()

async def main():
	connection = await connect()
	channel = await connection.channel()
	await channel.set_qos(prefetch_count = 10)
	exchange = await channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	queue = await channel.declare_queue(QUEUE_NAME, durable = True)
	await queue.bind(exchange, ROUTING_KEY_CONSUME)
	await queue.consume(handle_message)
	print("[Inventory service] Waiting for messages...")
	await asyncio.Future()

if __name__ == "__main__":
	asyncio.run(main())