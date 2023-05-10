"""
URLs patterns for Data Carousel app
"""

from django.urls import re_path
from core.datacarousel import views as dc_views

urlpatterns = [
    re_path(r'^dc/dash/$', dc_views.dataCarouselleDashBoard, name='dataCarouselleDashBoard'),
    re_path(r'^dc/tails/$', dc_views.dataCarouselTailsDashBoard, name='datacartails'),

    # legacy for user bookmarks
    re_path(r'^datacardash/$', dc_views.dataCarouselleDashBoard),
    re_path(r'^datacartails/$', dc_views.dataCarouselTailsDashBoard),

    re_path(r'^api/dc/tails/$', dc_views.getStagingTailsData, name='datacartaildata'),
    re_path(r'^api/dc/staginginfofortask/$', dc_views.getStagingInfoForTask, name='getStagingInfoForTask'),
    re_path(r'^api/dc/dash/$', dc_views.getDTCSubmissionHist, name='getDTCSubmissionHist'),
    re_path(r'^api/dc/stuckfiles/$', dc_views.get_stuck_files, name='datacarstuckfiles'),

    re_path(r'^dc/sendstalledreport/$', dc_views.send_stalled_requests_report, name='sendStalledReport'),
]