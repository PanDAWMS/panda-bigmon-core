
from django.urls import re_path, include
from core.libs.DateEncoder import DateEncoder

from core.panda_client import views as panda_client

app_name = "bigpandamon"

urlpatterns = [
    re_path(r'^get_pandaserver_attr/$', panda_client.get_pandaserver_attr, name='get_pandaservr_attr')
    ]
