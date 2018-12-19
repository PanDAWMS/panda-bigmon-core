"""
    filebrowser.urls

"""
from django.urls import re_path
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import core.filebrowser.views as filebrowser_views

urlpatterns = [
    re_path(r'^$', filebrowser_views.index, name='filebrowser'),
    re_path(r'^api/$', filebrowser_views.api_single_pandaid, name='filebrowser-api-single-pandaid'),
    re_path(r'^delete/$', filebrowser_views.delete_files, name='filebrowser-delete'),
]
