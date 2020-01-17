"""
URL patterns for Operational Intelligence related views
"""

from django.urls import re_path
from core.oi import views as oi_views

urlpatterns = [
    re_path(r'^jobproblems/$', oi_views.job_problems, name='jobProblems'),
    re_path(r'^jobsbuster/$', oi_views.job_problems, name='jobsBuster'),

]
