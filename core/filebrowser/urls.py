"""
    filebrowser.urls

"""
from django.urls import re_path

from core.filebrowser import MemoryMonitorPlots as memmon
from core.filebrowser import views as filebrowser_views

urlpatterns = [
    re_path(r'^filebrowser/$', filebrowser_views.index, name='filebrowser'),
    re_path(r'^filebrowser/delete/$', filebrowser_views.delete_files, name='filebrowser-delete'),

    # prmon plots
    re_path(r'^memoryplot/', memmon.getPlots, name='memoryplot'),
    re_path(r'^prmonplots/(?P<pandaid>.*)/', memmon.prMonPlots, name='prMonPlots'),
    re_path(r'^getprmonplotsdata/(?P<pandaid>.*)/', memmon.getPrMonPlotsData, name='getPrMonPlotsData'),
]
