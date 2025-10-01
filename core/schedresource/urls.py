from django.urls import re_path

from core.schedresource import views as sched_resource_views


urlpatterns = [
    re_path(r'^sites/$', sched_resource_views.siteList, name='siteList'),
    re_path(r'^site/(?P<site>.*)/$', sched_resource_views.siteInfo, name='siteInfo'),
    re_path(r'^site/$', sched_resource_views.siteInfo, name='siteInfo'),
]