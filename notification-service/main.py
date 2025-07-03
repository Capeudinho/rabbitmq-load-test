import asyncio
import aio_pika as pk
import json

EXCHANGE_NAME = "order.exchange"
QUEUE_NAME = "order.notification.queue"
ROUTING_KEY_CONSUME = "order.*"

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
    connection = await connect()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count = 10)
    exchange = await channel.declare_exchange(EXCHANGE_NAME, pk.ExchangeType.TOPIC, durable = True)
    queue = await channel.declare_queue(QUEUE_NAME, durable = True)
    await queue.bind(exchange, ROUTING_KEY_CONSUME)
    await queue.consume(handle_message)
    print("[Notification service] Awaiting messages...")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())