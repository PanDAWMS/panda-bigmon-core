# """
# URL patterns for kafka application
# """

from django.urls import path, re_path
from core.kafka.views import testTerminal, taskLivePage


urlpatterns = [
    re_path(r'^test_terminal/', testTerminal, name='test_terminal'),
    re_path(r'^live/task/(?P<jeditaskid>.*)/$', taskLivePage, name='task_livepage'),
]
