"""
URL patterns for buildmonitor application
"""

from django.urls import re_path

from core.buildmonitor import viewsglobal as globalview
from core.buildmonitor import viewsartmonit as artmonitview
from core.buildmonitor import viewsci as ciview
from core.buildmonitor import viewsn as nview
from core.buildmonitor import viewstests as testsview
from core.buildmonitor import viewscomps as compsview

urlpatterns = [
    re_path(r'^globalpage/$', globalview.globalviewDemo, name='BuildGlobal'),
    re_path(r'^globalview/$', globalview.globalviewDemo, name='BuildGlobal'),
    re_path(r'^artmonitview/$', artmonitview.artmonitviewDemo, name='BuildARTMonit'),
    re_path(r'^ciview/$', ciview.civiewDemo, name='BuildCI'),
    re_path(r'^nview/$', nview.nviewDemo, name='BuildN'),
    re_path(r'^testsview/$', testsview.testviewDemo, name='TestsRes'),
    re_path(r'^compsview/$', compsview.compviewDemo, name='CompsRes'),
]