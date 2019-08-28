"""
    Interface refactoring urls

"""
from django.urls import re_path

import core.upd.views_jobs as upd_views_jobs

urlpatterns = [
    re_path(r'^jobs/', upd_views_jobs.job_list, name='job_list'),
]
