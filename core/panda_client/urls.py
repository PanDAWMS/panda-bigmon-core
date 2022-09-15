
from django.urls import re_path, include
from core.libs.DateEncoder import DateEncoder

from core.panda_client import views as panda_client

app_name = "bigpandamon"

urlpatterns = [
    re_path(r'^panda_client/get_pandaserver_attr/$', panda_client.get_pandaserver_attr, name='get_pandaserver_attr'),
    re_path(r'^panda_client/setNumSlots/$', panda_client.setNumSlots, name='setNumSlots'),
    # re_path(r'^killTask/(?P<jeditaskid>.*)/$', panda_client.killTask, name='killTask')
    # re_path(r'^finishTask/(?P<jeditaskid>.*)/$', panda_client.finishTask, name='finishTask')
    ]
