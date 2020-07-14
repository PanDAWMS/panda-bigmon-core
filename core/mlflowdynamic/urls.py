
from django.urls import re_path
from core.mlflowdynamic.proxy.MLFlowProxy import MLFlowProxyView

urlpatterns = [
    re_path(r'^(?P<path>.*)$', MLFlowProxyView.as_view(), name='mlflowproxy'),
]

