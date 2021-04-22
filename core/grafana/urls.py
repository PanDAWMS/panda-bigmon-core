"""
URLs patterns for Drafana API app
"""

from django.urls import re_path

from core.grafana import views as grafana
from core.grafana import StaginDSProgress as dsProgressView

urlpatterns = [
    re_path(r'^api/grafana$', grafana.grafana_api_es, name='grafana_api'),
    re_path(r'^api/grafana/pledges$', grafana.pledges, name='grafana_pledges'),
    # re_path(r'^grafanaplots', grafana.index, name='grafana_plots'),
    re_path(r'^grafanaplots', grafana.chartjs, name='grafana_chartjsplots'),
    re_path(r'^staginprogress/', dsProgressView.getStageProfileData, name='staginprogress'),
    re_path(r'^staginprogressplot/', dsProgressView.getDATASetsProgressPlot, name='staginprogressplot'),
]