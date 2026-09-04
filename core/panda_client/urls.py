
from django.urls import re_path
from core.panda_client import views as panda_client

urlpatterns = [
    re_path(r'^panda_client/$', panda_client.client, name='panda_client'),
    re_path(r'^api/panda_ask/job_error_analysis/$', panda_client.job_error_analysis, name='api_job_error_analysis'),
]
