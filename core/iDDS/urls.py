"""
URL patterns for Operational Intelligence related views
"""

from django.urls import re_path
from core.iDDS import views as idds_views
from core.iDDS import workflowprogress as idds_progress
from core.iDDS import logsretrieval, DAGvisualization

urlpatterns = [
    re_path(r'^idds/$', idds_views.main, name='iddsmain'),
    re_path(r'^idds/collections/$', idds_views.collections, name='iddscollections'),
    re_path(r'^idds/transforms/$', idds_views.transforms, name='iddstransforms'),
    re_path(r'^idds/processings/$', idds_views.processings, name='iddprocessings'),
    re_path(r'^idds/contents/$', idds_views.iddsсontents, name='iddsсontents'),
    re_path(r'^idds/getiddsfortask/$', idds_views.getiDDSInfoForTaskRequest, name='getiDDSInfoForTask'),
    re_path(r'^idds/wfprogress/$', idds_progress.wfprogress, name='workflowprogressitems'),
    re_path(r'^idds/daggraph/$', DAGvisualization.daggraph, name='daggraph'),
    re_path(r'^idds/downloadlog/$', logsretrieval.downloadlog, name='downloadlog'),
    re_path(r'^idds/downloadhpometrics/$', logsretrieval.downloadhpometrics, name='downloadlog'),

]
