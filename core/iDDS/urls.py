"""
URL patterns for Operational Intelligence related views
"""

from django.urls import re_path
from core.iDDS import views as idds_views

urlpatterns = [
    re_path(r'^$', idds_views.main, name='iddsmain'),
    re_path(r'^collections/$', idds_views.collections, name='iddscollections'),
    re_path(r'^transforms/$', idds_views.transforms, name='iddstransforms'),
    re_path(r'^processings/$', idds_views.processings, name='iddprocessings'),
    re_path(r'^contents/$', idds_views.iddsсontents, name='iddsсontents'),
    re_path(r'^getiddsfortask/$', idds_views.getiDDSInfoForTask, name='getiDDSInfoForTask'),
]
