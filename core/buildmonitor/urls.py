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
from core.buildmonitor import viewstests_eos as testsview_eos
from core.buildmonitor import viewscomps_eos as compsview_eos

urlpatterns = [
    re_path(r'^globalpage/$', globalview.globalviewDemo, name='BuildGlobal'),
    re_path(r'^globalview/$', globalview.globalviewDemo, name='BuildGlobal'),
    re_path(r'^artmonitview/$', artmonitview.artmonitviewDemo, name='BuildARTMonit'),
    re_path(r'^ciview/$', ciview.civiewDemo, name='BuildCI'),
    re_path(r'^nview/$', nview.nviewDemo, name='BuildN'),
    re_path(r'^testsview/$', testsview.testviewDemo, name='TestsRes'),
    re_path(r'^compsview/$', compsview.compviewDemo, name='CompsRes'),
    re_path(r'^testsview_eos/$', testsview_eos.testview_eosDemo, name='TestsEosRes'),
    re_path(r'^compsview_eos/$', compsview_eos.compview_eosDemo, name='CompsEosRes'),
]