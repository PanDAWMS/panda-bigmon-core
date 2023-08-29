import json
import asyncio, time

from channels.generic.websocket import WebsocketConsumer, AsyncWebsocketConsumer
from core.kafka.config import initConsumer
from confluent_kafka import KafkaError
from asgiref.sync import async_to_sync
class KafkaConsumerConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        client = self.scope['client']
        self.consumer = initConsumer(client)
        self.kafka_task = asyncio.create_task(self.kafka_consumer())

    async def disconnect(self, close_code):
        self.consumer.close()

    async def kafka_consumer(self):
        loop = asyncio.get_running_loop()
        try:
            while True:
                message = await loop.run_in_executor(None, self.consumer.poll, 0.1)
                if message is None:
                    continue
                await self.send(text_data=json.dumps({
                            'message': str(message.value())
                }))
                print('Received message: {}'.format(message.value().decode('utf-8')))
        except Exception as ex:
            print(ex)