"""
    filebrowser.urls

"""
from django.conf.urls import include, url
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import core.filebrowser.views as filebrowser_views

urlpatterns = [
    url(r'^$', filebrowser_views.index, name='filebrowser'),
    url(r'^api/$', filebrowser_views.api_single_pandaid, name='filebrowser-api-single-pandaid'),
    url(r'^delete/$', filebrowser_views.delete_files, name='filebrowser-delete'),
]
