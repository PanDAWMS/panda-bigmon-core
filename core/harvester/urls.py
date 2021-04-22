"""
URLs patterns for Harvester app
"""

from django.urls import re_path
from core.harvester import views as harvester

urlpatterns = [
    re_path(r'^harvesterworkersdash/$', harvester.harvesterWorkersDash, name='harvesterworkersdash'),
    re_path(r'^harvesterworkerslist/$', harvester.harvesterWorkerList, name='harvesterworkerslist'),
    re_path(r'^harvesterworkerinfo/$', harvester.harvesterWorkerInfo, name='harvesterWorkerInfo'),
    re_path(r'^harvestertest/$', harvester.harvesterfm, name='harvesterfm'),
    re_path(r'^harvesters/$', harvester.harvestermon, name='harvesters'),
    re_path(r'^harvesters/slots/$', harvester.harvesterslots, name='harvesterslots'),
    re_path(r'^workers/$', harvester.workersJSON, name='workers'),
    re_path(r'^workersfortask/$', harvester.getHarversterWorkersForTask, name='workersfortask'),
]