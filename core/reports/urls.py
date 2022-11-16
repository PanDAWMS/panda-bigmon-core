"""
URLs patterns for Reports app
"""

from django.urls import re_path
from core.reports import views as report_views

urlpatterns = [
    re_path(r'^reports/$', report_views.reports, name='reportWizard'),
    re_path(r'^report/$', report_views.report, name='report'),
]
