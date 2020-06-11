"""
URL patterns for art application
"""

from django.urls import re_path
from core.art import views as art_views

urlpatterns = [
    re_path(r'^$', art_views.art, name='art-mainPage'),
    re_path(r'^overview/$', art_views.artOverview, name='artOverview'),
    re_path(r'^tasks/$', art_views.artTasks, name='artTasks'),
    re_path(r'^jobs/$', art_views.artJobs, name='artJobs'),

    re_path(r'^updatejoblist/$', art_views.updateARTJobList),
    re_path(r'^registerarttest/$', art_views.registerARTTest),
    re_path(r'^sendartreport/$', art_views.sendArtReport),
    re_path(r'^senddevartreport/$', art_views.sendDevArtReport),
    ]
