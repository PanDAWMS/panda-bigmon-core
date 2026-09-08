
from django.urls import path, re_path
from core.panda_client import views as panda_client

urlpatterns = [
    re_path(r'^panda_client/$', panda_client.client, name='panda_client'),
    path('api/panda_ask/job_error_analysis/<str:analysis_id>/', panda_client.job_error_analysis),
    path('api/panda_ask/job_error_analysis/<str:analysis_id>/rating/', panda_client.submit_job_error_rating),
]
