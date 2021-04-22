"""
URL patterns for auth
"""

from django.urls import re_path
from core.oauth import views as auth_views
from django.urls import include

urlpatterns = [
    re_path(r'^oauth/', include('social_django.urls', namespace='social')),  # <--
    re_path(r'^testauth/$', auth_views.testauth, name='testauth'),
    re_path(r'^login/$', auth_views.loginauth2, name='loginauth2'),
    re_path(r'^login/$', auth_views.loginauth2, name='login'),
    re_path(r'^logout/$', auth_views.logout, name='logout'),
    re_path(r'^loginerror/$', auth_views.loginerror, name='loginerror'),

    re_path(r'^grantrights/$', auth_views.grantRights, name='grantrights'),
    re_path(r'^denyrights/$', auth_views.denyRights, name='denyrights'),
    re_path(r'^savesettings/$', auth_views.saveSettings, name='saveSettings'),

    re_path(r'^statpixel/$', auth_views.statpixel, name='statpixel'),
]