"""
URLs patterns for Harvester app
"""

from django.urls import re_path
from core.harvester import views as harvester

urlpatterns = [

    re_path(r'^workersfortask/$', harvester.getHarversterWorkersForTask, name='workersfortask'),

    re_path(r'^harvester/slots/$', harvester.harvesterSlots, name='harvesterSlots'),
    re_path(r'^harvester/instances/$', harvester.harvesterInstances, name='harvesterInstanceList'),
    re_path(r'^harvester/workers/$', harvester.harvesterWorkers, name='harvesterWorkerList'),
    re_path(r'^harvester/worker/(?P<workerid>.*)/$', harvester.harvesterWorkerInfo, name='harvesterWorkerInfo'),
    re_path(r'^harvester/worker/$', harvester.harvesterWorkerInfo, name='harvesterWorkerInfo'),

    # API for datatables
    re_path(r'^harvester/getworkers/$', harvester.get_harvester_workers, name='getworkers'),
    re_path(r'^harvester/getdiagnostics/$', harvester.get_harvester_diagnostics, name='getdiagnostics'),
    re_path(r'^harvester/getworkerstats/$', harvester.get_harvester_worker_stats, name='getworkerstats'),
    re_path(r'^harvester/getjobs/$', harvester.get_harvester_jobs, name='getjobs'),

    # legacy, keep to redirect
    re_path(r'^harvesterworkersdash/$', harvester.harvesterWorkers),
    re_path(r'^harvesterworkerslist/$', harvester.harvesterWorkers),
    re_path(r'^harvesterworkerinfo/$', harvester.harvesterWorkerInfo),
    re_path(r'^harvesters/$', harvester.harvestermon, name='harvesters'),
    re_path(r'^workers/$', harvester.get_harvester_workers, name='workers'),

]
