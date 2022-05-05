"""
URLs patterns for Self-Monitoring app
"""

from django.urls import re_path
from core.monitor import views as monitor_views

urlpatterns = [
    re_path(r'^bigpandamonitor/$', monitor_views.monitorJson, name='bigpandamonitor'),

    re_path(r'^testip/$', monitor_views.testip, name='testip'),
    re_path(r'^serverstatushealth/$', monitor_views.serverStatusHealth, name='serverStatusHealth'),
]