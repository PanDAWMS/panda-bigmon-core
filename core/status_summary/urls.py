from django.urls import re_path
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import core.status_summary.views as smry_views

urlpatterns = [
    re_path(r'^$', smry_views.index, name='status_summary-index'),
    re_path(r'^api/$', smry_views.api_status_summary, name='status_summary-api'),
]


