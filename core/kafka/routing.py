from core.kafka.consumers import KafkaConsumerConsumer
from django.urls import re_path,path

ws_urlpatterns = [
    path('ws/kafka_messages/', KafkaConsumerConsumer.as_asgi()),
]
