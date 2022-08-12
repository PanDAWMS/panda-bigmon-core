from django.urls import re_path

import core.status_summary.views as smry_views

urlpatterns = [
    re_path(r'^$', smry_views.index, name='status_summary-index'),
    re_path(r'^api/$', smry_views.api_status_summary, name='status_summary-api'),
]