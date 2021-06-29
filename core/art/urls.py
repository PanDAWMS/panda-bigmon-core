"""
URL patterns for art application
"""

from django.urls import re_path
from core.art import views as art_views

urlpatterns = [
    re_path(r'^art/$', art_views.art, name='art-mainPage'),
    re_path(r'^art/overview/$', art_views.artOverview, name='artOverview'),
    re_path(r'^art/tasks/$', art_views.artTasks, name='artTasks'),
    re_path(r'^art/jobs/$', art_views.artJobs, name='artJobs'),

    re_path(r'^art/updatejoblist/$', art_views.updateARTJobList),
    re_path(r'^art/registerarttest/$', art_views.registerARTTest),
    re_path(r'^art/sendartreport/$', art_views.sendArtReport),
    re_path(r'^art/senddevartreport/$', art_views.sendDevArtReport),
    ]
