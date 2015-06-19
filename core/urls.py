from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView

#import core.settings
from django.conf import settings

from core import views as coremon_views
from core import dpviews as dpviews
#import core.views as coremon_views
import core.pandajob.views_support as core_coremon_support_views
#import core.pandajob.views as core_coremon_views
#import core.api.reprocessing.views as core_coremon_api_reprocessing_views

urlpatterns = patterns('',
    url(r'^$', coremon_views.mainPage, name='mainPage'),
    url(r'^$', coremon_views.mainPage, name='index'),
    url(r'^help/$', coremon_views.helpPage, name='helpPage'),
    url(r'^jobs/$', coremon_views.jobList, name='jobList'),
    url(r'^jobs/(.*)/$', coremon_views.jobList, name='jobList'),
    url(r'^jobs/(.*)/(.*)/$', coremon_views.jobList, name='jobList'),
    url(r'^job$', coremon_views.jobInfo, name='jobInfo'),
    url(r'^job/(.*)/$', coremon_views.jobInfo, name='jobInfo'),
    url(r'^job/(.*)/(.*)/$', coremon_views.jobInfo, name='jobInfo'),
    url(r'^users/$', coremon_views.userList, name='userList'),
    url(r'^user/(?P<user>.*)/$', coremon_views.userInfo, name='userInfo'),
    url(r'^user/$', coremon_views.userInfo, name='userInfo'),
    url(r'^sites/$', coremon_views.siteList, name='siteList'),
    url(r'^site/(?P<site>.*)/$', coremon_views.siteInfo, name='siteInfo'),
    url(r'^site/$', coremon_views.siteInfo, name='siteInfo'),
    url(r'^wns/(?P<site>.*)/$', coremon_views.wnInfo, name='wnInfo'),
    url(r'^wn/(?P<site>.*)/(?P<wnname>.*)/$', coremon_views.wnInfo, name='wnInfo'),
    url(r'^tasks/$', coremon_views.taskList, name='taskList'),
    url(r'^task$', coremon_views.taskInfo, name='taskInfo'),
    url(r'^task/$', coremon_views.taskInfo, name='taskInfo'),
    url(r'^errors/$', coremon_views.errorSummary, name='errorSummary'),
    url(r'^incidents/$', coremon_views.incidentList, name='incidentList'),
    url(r'^logger/$', coremon_views.pandaLogger, name='pandaLogger'),
    url(r'^task/(?P<jeditaskid>.*)/$', coremon_views.taskInfo, name='taskInfo'),
    url(r'^dash/$', coremon_views.dashboard, name='dashboard'),
    url(r'^dash/analysis/$', coremon_views.dashAnalysis, name='dashAnalysis'),
    url(r'^dash/production/$', coremon_views.dashProduction, name='dashProduction'),
    url(r'^workingGroups/$', coremon_views.workingGroups, name='workingGroups'),
    url(r'^fileInfo/$', coremon_views.fileInfo, name='fileInfo'),
    url(r'^fileList/$', coremon_views.fileList, name='fileList'),
    url(r'^datasetInfo/$', coremon_views.datasetInfo, name='datasetInfo'),
    url(r'^datasetList/$', coremon_views.datasetList, name='datasetList'),
    url(r'^workQueues/$', coremon_views.workQueues, name='workQueues'),

#    url(r'^preprocessdata/$', coremon_views.preprocessData, name='preprocessdata'),

    ### data product catalog prototyping                                                                                                                                                         
    url(r'^dp/$', dpviews.doRequest, name='doRequest'),

    ### filebrowser
    url(r'^filebrowser/', include('core.filebrowser.urls'), name='filebrowser'),
    ### PanDA Brokerage Monitor
    url(r'^pbm/', include('core.pbm.urls'), name='pbm'),
    url(r'^status_summary/', include('core.status_summary.urls'), name='status_summary'),

    ### support views for core
    url(r'^support/$', core_coremon_support_views.maxpandaid, name='supportRoot'),
    url(r'^support/maxpandaid/$', core_coremon_support_views.maxpandaid, name='supportMaxpandaid'),
    url(r'^support/jobinfouservohrs/(?P<vo>[-A-Za-z0-9_.+ @]+)/(?P<nhours>\d+)/$', core_coremon_support_views.jobUserOrig, name='supportJobUserVoHrs'),
    url(r'^support/jobinfouservo/(?P<vo>[-A-Za-z0-9_.+ @]+)/(?P<ndays>\d+)/$', core_coremon_support_views.jobUserDaysOrig, name='supportJobUserVo'),


    ###self monitor
    url(r'^admin/', include('core.admin.urls', namespace='admin')),

    ### api
    url(r'^api/$', core_coremon_support_views.maxpandaid, name='supportRoot'),
#    url(r'^api/reprocessing/$', include('core.api.reprocessing.urls')),


    ### robots.txt
    url('^robots\.txt$', TemplateView.as_view(template_name='robots.txt', content_type='text/plain')),

) + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


#urlpatterns += common_patterns
#urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
