"""
URL patterns for Operational Intelligence related views
"""

from django.urls import re_path
from core.oi import jbviews

urlpatterns = [
    re_path(r'^jobsbuster/$', jbviews.jbhome, name='jobsBuster'),
]
