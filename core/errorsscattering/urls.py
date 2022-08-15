"""
URLs patterns for errors scattering matrix
"""

from django.urls import re_path
from core.errorsscattering import views as errorsscat_views

urlpatterns = [
    re_path(r'^taskserrorsscat/$', errorsscat_views.errorsScattering, name='tasksErrorsScattering'),  # legacy
    re_path(r'^errorsscat/$', errorsscat_views.errorsScattering, name='errorsScattering'),
    re_path(r'^errorsscat/(?P<cloud>.*)/(?P<reqid>.*)/$', errorsscat_views.errorsScatteringDetailed,
            name='errorsScatteringDetailed'),
]