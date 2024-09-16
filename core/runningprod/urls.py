"""
URLs patterns for runningProd
"""

from django.urls import re_path
from core.runningprod import views as runningprod_views

urlpatterns = [
    re_path(r'^runningprodtasks/$', runningprod_views.runningProdTasks, name='runningProdTasks'),
    re_path(r'^prodeventstrend/$', runningprod_views.prodNeventsTrend, name='prodNeventsTrend'),

    # redirects to general view
    re_path(r'^runningmcprodtasks/$', runningprod_views.runningMCProdTasks, name='runningMCProdTasks'),
    re_path(r'^runningdpdprodtasks/$', runningprod_views.runningDPDProdTasks, name='runningDPDProdTasks'),

    # decommissioned
    re_path(r'^runningprodrequests/$', runningprod_views.runningProdRequests, name='runningProdRequests'),
]