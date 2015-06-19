"""
    filebrowser.urls

"""
from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import views as filebrowser_views

urlpatterns = patterns('',
    url(r'^$', filebrowser_views.index, name='filebrowser'),
    url(r'^api/$', filebrowser_views.api_single_pandaid, name='filebrowser-api-single-pandaid'),
)
