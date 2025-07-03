import asyncio
import aio_pika as pk
import json

EXCHANGE_NAME = "order.exchange"
QUEUE_NAME = "order.paid.queue"
ROUTING_KEY_CONSUME = "order.paid"
ROUTING_KEY_PRODUCE = "order.sent"

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
		print("[Shipping service] Paid order "+order["order_id"]+" received.")
		await publish_sent_event(order)

async def publish_sent_event(order):
	connection = await pk.connect_robust("amqp://guest:guest@rabbitmq/")
	channel = await connection.channel()
	exchange = await channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	await exchange.publish(pk.Message(body = json.dumps(order).encode(), delivery_mode = pk.DeliveryMode.PERSISTENT),routing_key = ROUTING_KEY_PRODUCE)
	print("[Shipping service] Order "+order["order_id"]+" sent.")
	await connection.close()

async def main():
	connection = await connect()
	channel = await connection.channel()
	await channel.set_qos(prefetch_count = 10)
	exchange = await channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	queue = await channel.declare_queue(QUEUE_NAME, durable = True)
	await queue.bind(exchange, ROUTING_KEY_CONSUME)
	await queue.consume(handle_message)
	print("[Shipping service] Waiting for messages...")
	await asyncio.Future()

if __name__ == "__main__":
	asyncio.run(main())