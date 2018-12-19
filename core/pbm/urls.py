"""
    pbm.urls

"""
from django.urls import re_path
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import core.pbm.views as pbm_views

urlpatterns = [
    re_path(r'^$', pbm_views.index, name='pbm-index'),
    re_path(r'^plot/$', pbm_views.single_plot, name='pbm-plot'),
    re_path(r'^table/$', pbm_views.single_table, name='pbm-table'),
    re_path(r'^detail/$', pbm_views.detail, name='pbm-detail'),
    re_path(r'^api/$', pbm_views.api_pbm_collector, name='api_pbm_collector'),
]
