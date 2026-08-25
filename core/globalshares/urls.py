"""
URLs patterns for Global Shares app
"""

from django.urls import re_path
from core.globalshares import views as globalshares

urlpatterns = [
    re_path(r'^globalshares/$', globalshares.globalshares, name='globalshares'),
    re_path(r'^api/globalshares/detailedInformationJSON', globalshares.detailedInformationJSON,
            name='detailedInformationJSON'),
    re_path(r'^api/globalshares/sharesDistributionJSON', globalshares.sharesDistributionJSON,
            name='sharesDistributionJSON'),
    re_path(r'^api/globalshares/siteWorkQueuesJSON', globalshares.siteWorkQueuesJSON, name='siteWorkQueuesJSON'),
    re_path(r'^api/globalshares/resourcesType', globalshares.resourcesType, name='resourcesType'),
    re_path(r'^api/globalshares/coreTypes', globalshares.coreTypes, name='coreTypes'),
    re_path(r'^api/globalshares/fairsharePolicy', globalshares.fairsharePolicy, name='fairsharePolicy'),
]