from core.kafka.config import initConsumer
from confluent_kafka import KafkaError
from django.shortcuts import render
from core.oauth.utils import login_customrequired
@login_customrequired
def index(request):
    return render(request, 'test_terminal.html', context={'text':'Hello World'})