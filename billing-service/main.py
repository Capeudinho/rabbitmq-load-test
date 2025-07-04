import asyncio
import aio_pika as pk
import json

EXCHANGE_NAME = "order.exchange"
QUEUE_NAME = "order.stocked.queue"
ROUTING_KEY_CONSUME = "order.stocked"
ROUTING_KEY_PRODUCE = "order.paid"
connection = None
produce_exchange = None

async def connect():
	global connection
	print("[Billing service] Connecting to RabbitMQ...")
	for _ in range(100):
		try:
			connection = await pk.connect_robust("amqp://guest:guest@rabbitmq/")
			print("[Billing service] Connected to RabbitMQ.")
			return
		except Exception:
			print(f"[Billing service] Waiting for RabbitMQ...")
			await asyncio.sleep(5)
	raise RuntimeError("[Billing service] Failed to connect to RabbitMQ.")

async def handle_message(message: pk.IncomingMessage):
	global produce_exchange
	async with message.process():
		order = json.loads(message.body)
		print("[Billing service] Stocked order "+order["order_id"]+" received.")
		await produce_exchange.publish(pk.Message(body = json.dumps(order).encode(), delivery_mode = pk.DeliveryMode.PERSISTENT),routing_key = ROUTING_KEY_PRODUCE)
		print("[Billing service] Order "+order["order_id"]+" paid.")

async def main():
	global connection
	global produce_exchange
	await connect()
	consume_channel = await connection.channel()
	produce_channel = await connection.channel()
	await consume_channel.set_qos(prefetch_count = 10)
	await produce_channel.set_qos(prefetch_count = 10)
	consume_exchange = await consume_channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	produce_exchange = await produce_channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
	queue = await consume_channel.declare_queue(QUEUE_NAME, durable = True)
	await queue.bind(consume_exchange, ROUTING_KEY_CONSUME)
	await queue.consume(handle_message)
	print("[Billing service] Waiting for messages...")
	await asyncio.Future()

if __name__ == "__main__":
	asyncio.run(main())