"""
URL patterns for Operational Intelligence related views
"""

from django.urls import re_path
from core.iDDS import views as idds_views

urlpatterns = [
    re_path(r'^$', idds_views.main, name='iddsmain'),
    re_path(r'^collections/$', idds_views.collections, name='iddscollections'),
]
