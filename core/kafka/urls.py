# """
# URL patterns for kafka application
# """

from django.urls import path
from core.kafka.views import index

urlpatterns = [
    path('test_terminal/', index),
]
