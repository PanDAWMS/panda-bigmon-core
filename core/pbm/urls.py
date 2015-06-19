"""
    pbm.urls

"""
from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import views as pbm_views

urlpatterns = patterns('',
    url(r'^$', pbm_views.index, name='pbm-index'),
    url(r'^plot/$', pbm_views.single_plot, name='pbm-plot'),
    url(r'^table/$', pbm_views.single_table, name='pbm-table'),
    url(r'^detail/$', pbm_views.detail, name='pbm-detail'),
    url(r'^api/$', pbm_views.api_pbm_collector, name='api_pbm_collector'),
)
