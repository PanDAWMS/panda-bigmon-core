from django.conf.urls import include, url
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView

from django.conf import settings


from core.admin import views as adviews

urlpatterns = [
    url(r'^$', adviews.adMain, name='adMain'),
    url(r'^reqplot/$', adviews.listReqPlot, name='reqPlot'),
]
