"""
URLs patterns for Data Carousel app
"""

from django.urls import re_path
from core.datacarousel import views as dc_views

urlpatterns = [
    re_path(r'^datacardash/$', dc_views.dataCarouselleDashBoard, name='dataCarouselleDashBoard'),
    re_path(r'^datacartails/$', dc_views.dataCarouselTailsDashBoard, name='datacartails'),

    re_path(r'^datacartaildata/$', dc_views.getStagingTailsData, name='datacartaildata'),
    re_path(r'^getstaginginfofortask/$', dc_views.getStagingInfoForTask, name='getStagingInfoForTask'),
    re_path(r'^getdtcsubmissionhist/$', dc_views.getDTCSubmissionHist, name='getDTCSubmissionHist'),

    re_path(r'^dc/sendstalledreport/$', dc_views.send_stalled_requests_report, name='sendStalledReport'),
]