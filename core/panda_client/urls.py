
from django.urls import re_path, include
from core.libs.DateEncoder import DateEncoder

from core.panda_client import views as panda_client

urlpatterns = [
    re_path(r'^panda_client/$', panda_client.client, name='panda_client'),
    ]
