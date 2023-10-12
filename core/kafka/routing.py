from core.kafka.consumer import TaskLogsConsumer
from django.urls import re_path, path

ws_urlpatterns = [
    #path('ws/kafka_messages/', KafkaConsumerConsumer.as_asgi()),
    re_path(r'^ws/kafka_messages/(?P<db_source>\w+)/(?P<jeditaskid>\w+)/$', TaskLogsConsumer.as_asgi()),
]