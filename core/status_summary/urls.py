from django.conf.urls import include, url
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
### #FIXME admin.autodiscover()

import views as smry_views

urlpatterns = [
    url(r'^$', smry_views.index, name='status_summary-index'),
    url(r'^api/$', smry_views.api_status_summary, name='status_summary-api'),
]


