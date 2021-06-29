"""
URLs patterns for Global Shares app
"""

from django.urls import re_path
from core.globalshares import views as globalshares

urlpatterns = [
    re_path(r'^globalshares/$', globalshares.globalshares, name='globalshares'),
    re_path(r'^datatable/data/detailedInformationJSON', globalshares.detailedInformationJSON,
            name='detailedInformationJSON'),
    re_path(r'^datatable/data/sharesDistributionJSON', globalshares.sharesDistributionJSON,
            name='sharesDistributionJSON'),
    re_path(r'^datatable/data/siteWorkQueuesJSON', globalshares.siteWorkQueuesJSON, name='siteWorkQueuesJSON'),
    re_path(r'^datatable/data/resourcesType', globalshares.resourcesType, name='resourcesType'),
    re_path(r'^datatable/data/coreTypes', globalshares.coreTypes, name='coreTypes'),
    re_path(r'^datatable/data/fairsharePolicy', globalshares.fairsharePolicy, name='fairsharePolicy'),
]