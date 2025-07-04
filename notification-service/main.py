import asyncio
import aio_pika as pk
import json

EXCHANGE_NAME = "order.exchange"
QUEUE_NAME = "order.notification.queue"
ROUTING_KEY_CONSUME = "order.*"
connection = None

async def connect():
	global connection
	print("[Notification service] Connecting to RabbitMQ...")
	for _ in range(100):
		try:
			connection = await pk.connect_robust("amqp://guest:guest@rabbitmq/")
			print("[Notification service] Connected to RabbitMQ.")
			return
		except Exception:
			print(f"[Notification service] Waiting for RabbitMQ...")
			await asyncio.sleep(5)
	raise RuntimeError("[Notification service] Failed to connect to RabbitMQ.")

async def handle_message(message: pk.IncomingMessage):
	async with message.process():
		order = json.loads(message.body)
		routing_key = message.routing_key
		if routing_key == "order.placed":
			print("[Notification service] Order "+order["order_id"]+" placed.")
		elif routing_key == "order.stocked":
			print("[Notification service] Order "+order["order_id"]+" stocked.")
		elif routing_key == "order.paid":
			print("[Notification service] Order "+order["order_id"]+" paid.")
		elif routing_key == "order.sent":
			print("[Notification service] Order "+order["order_id"]+" sent.")

async def main():
	global connection
	await connect()
	consume_channel = await connection.channel()
	await consume_channel.set_qos(prefetch_count = 10)
	consume_exchange = await consume_channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	queue = await consume_channel.declare_queue(QUEUE_NAME, durable = True)
	await queue.bind(consume_exchange, ROUTING_KEY_CONSUME)
	await queue.consume(handle_message)
	print("[Notification service] Awaiting messages...")
	await asyncio.Future()

if __name__ == "__main__":
	asyncio.run(main())