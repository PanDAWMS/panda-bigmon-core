"""
URLs patterns for Data Carousel app
"""

from django.urls import re_path
from core.datacarousel import views as dc_views
from core.views import decommissioned

urlpatterns = [
    re_path(r'^dc/dash/$', dc_views.data_carousel_dash, name='datacardash'),
    re_path(r'^dc/tails/$', decommissioned),   # decommissioned

    # legacy for user bookmarks
    re_path(r'^datacardash/$', dc_views.data_carousel_dash),
    re_path(r'^datacartails/$', decommissioned),   # decommissioned

    re_path(r'^api/dc/tails/$', decommissioned),  # decommissioned
    re_path(r'^api/dc/staginginfofortask/$', dc_views.get_staging_info_for_task, name='staginginfofortask'),
    re_path(r'^api/dc/dash/$', dc_views.get_data_carousel_data, name='datacardata'),
    re_path(r'^api/dc/stuckfiles/$', dc_views.get_stuck_files, name='datacarstuckfiles'),

    re_path(r'^dc/sendstalledreport/$', dc_views.send_stalled_requests_report, name='sendStalledReport'),
]