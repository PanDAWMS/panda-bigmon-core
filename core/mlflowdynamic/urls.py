
from django.urls import re_path
from core.mlflowdynamic.MLFlowSpinner import MLFlowProxyView
urlpatterns = [
#    re_path('(?P<taskid>.*)/', get_mlflow_content, name='mlflow'),
    re_path(r'^mlflow/(?P<path>.*)$', MLFlowProxyView.as_view(), name='mlflow'),

]
