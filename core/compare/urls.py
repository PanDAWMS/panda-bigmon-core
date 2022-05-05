"""
URL patterns for compare application
"""

from django.urls import re_path
from core.compare import views as compare_views

urlpatterns = [
    re_path(r'^compare/jobs/$', compare_views.compareJobs, name='compareJobs'),
    re_path(r'^deletefromcomparison/$', compare_views.deleteFromComparison),
    re_path(r'^addtocomparison/$', compare_views.addToComparison),
    re_path(r'^clearcomparison/$', compare_views.clearComparison),
]