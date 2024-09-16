from django.conf.urls.static import static
from django.views.generic import TemplateView

from django.conf import settings
from django.urls import re_path, include

from core import views as coremon_views

from core.dashboards import campaignprediction as campaignprediction
from core.dashboards import campaingprogressDKB
from core.dashboards import taskproblemexplorer
from core.libs import tasksPlots as tasksPlots

import core.pandajob.views_support as core_coremon_support_views

app_name = "bigpandamon"

urlpatterns = [
    re_path(r'^$', coremon_views.mainPage, name='mainPage'),
    re_path(r'^$', coremon_views.mainPage, name='index'),
    re_path(r'^help/$', coremon_views.helpPage, name='helpPage'),
    re_path(r'^rating/$', coremon_views.rating_func, name='rating_func'),
    re_path(r'^jobs/$', coremon_views.jobList, name='jobList'),
    re_path(r'^jobs/(.*)/$', coremon_views.jobList, name='jobList'),
    re_path(r'^jobs/(.*)/(.*)/$', coremon_views.jobList, name='jobList'),
    re_path(r'^job$', coremon_views.jobInfo, name='jobInfo'),
    re_path(r'^job/(.*)/$', coremon_views.jobInfo, name='jobInfo'),
    re_path(r'^job/(.*)/(.*)/$', coremon_views.jobInfo, name='jobInfo'),
    re_path(r'^descendentjoberrsinfo/$', coremon_views.descendentjoberrsinfo, name='descendentjoberrsinfo'),
    re_path(r'^jobrelationships/(?P<pandaid>.*)/$', coremon_views.get_job_relationships, name='jobrelationships'),
    re_path(r'^jobstatuslog/(?P<pandaid>.*)/$', coremon_views.getJobStatusLog, name='getjobstatuslog'),

    re_path(r'^users/$', coremon_views.userList, name='userList'),
    re_path(r'^user/(?P<user>.*)/$', coremon_views.userInfo, name='userInfo'),
    re_path(r'^user/$', coremon_views.userInfo, name='userInfo'),
    re_path(r'^userProfile/(?P<username>.*)/$', coremon_views.userProfile, name='userProfile'),
    re_path(r'^userProfile/$', coremon_views.userProfile, name='userProfile'),
    re_path(r'^userProfileData/$', coremon_views.userProfileData, name='userProfileData'),
    re_path(r'^sites/$', coremon_views.siteList, name='siteList'),
    re_path(r'^site/(?P<site>.*)/$', coremon_views.siteInfo, name='siteInfo'),
    re_path(r'^site/$', coremon_views.siteInfo, name='siteInfo'),
    re_path(r'^wns/(?P<site>.*)/$', coremon_views.wnInfo, name='wnInfo'),
    re_path(r'^wn/(?P<site>.*)/(?P<wnname>.*)/$', coremon_views.wnInfo, name='wnInfo'),

    re_path(r'^tasks/$', coremon_views.taskList, name='taskList'),
    re_path(r'^taskESExtendedInfo/$', coremon_views.taskESExtendedInfo, name='taskESExtendedInfo'),
    re_path(r'^killtasks/$', coremon_views.killtasks, name='killtasks'),
    re_path(r'^task$', coremon_views.taskInfo, name='taskInfo'),
    re_path(r'^task/$', coremon_views.taskInfo, name='taskInfo'),
    re_path(r'^task/(?P<jeditaskid>.*)/$', coremon_views.taskInfo, name='taskInfo'),
    re_path(r'^tasknew/(?P<jeditaskid>.*)/$', coremon_views.taskInfo, name='taskInfoNew'),  # legacy
    re_path(r'^getjobsummaryfortask/(?P<jeditaskid>.*)/$', coremon_views.getJobSummaryForTask, name='getJobSummaryForTask'),
    re_path(r'^getbadeventsfortask/$', coremon_views.getBadEventsForTask, name='getbadeventsfortask'),
    re_path(r'^taskstatuslog/(?P<jeditaskid>.*)/$', coremon_views.getTaskStatusLog, name='gettaskstatuslog'),
    re_path(r'^tasklogs/(?P<jeditaskid>.*)/$', coremon_views.getTaskLogs, name='gettasklogs'),
    re_path(r'^ttc/$', coremon_views.ttc, name='ttc'),
    re_path(r'^taskchain/$', coremon_views.taskchain, name='taskchain'),
    re_path(r'^ganttTaskChain/$', coremon_views.ganttTaskChain, name='ganttTaskChain'),
    re_path(r'^taskprofileplot/$', coremon_views.taskprofileplot, name='taskprofileplot'),  # legacy
    re_path(r'^taskesprofileplot/$', coremon_views.taskESprofileplot, name='taskesprofileplot'),
    re_path(r'^taskprofile/(?P<jeditaskid>.*)/$', coremon_views.taskProfile, name='taskProfileMonitor'),
    re_path(r'^taskprofiledata/(?P<jeditaskid>.*)/$', coremon_views.taskProfileData, name='getTaskProfilePlotData'),
    re_path(r'^eventserrorsummaury/$', coremon_views.getErrorSummaryForEvents, name='eventsErrorSummary'),
    re_path(r'^eventschunks/$', coremon_views.getEventsChunks, name='eventschunks'),
    re_path(r'^taskflow/(?P<jeditaskid>.*)/$', coremon_views.taskFlowDiagram, name='taskFlowDiagram'),
    re_path(r'^api/taskdatamovement/(?P<jeditaskid>.*)/$', coremon_views.getTaskDataMovementData, name='taskdatamovement'),



    re_path(r'^errors/$', coremon_views.errorSummary, name='errorSummary'),
    re_path(r'^incidents/$', coremon_views.incidentList, name='incidentList'),
    re_path(r'^logger/$', coremon_views.pandaLogger, name='pandaLogger'),
    re_path(r'^esatlaslogger/$', coremon_views.esatlasPandaLogger, name='esatlasPandaLogger'),
    re_path(r'^payloadlog/$', coremon_views.getPayloadLog, name='getpayloadlog'),
    re_path(r'^datatable/data/jeditaskid', coremon_views.esatlasPandaLoggerJson, name='dataTableJediTaskId'),

    re_path(r'^fileInfo/$', coremon_views.fileInfo, name='fileInfoLegacy'),
    re_path(r'^fileList/$', coremon_views.fileList, name='fileListLegacy'),
    re_path(r'^file/$', coremon_views.fileInfo, name='fileInfo'),
    re_path(r'^files/$', coremon_views.fileList, name='fileList'),
    re_path(r'^loadFileList/(?P<datasetid>.*)/$', coremon_views.loadFileList, name='loadFileList'),

    re_path(r'^datasetInfo/$', coremon_views.datasetInfo, name='datasetInfoLegacy'),
    re_path(r'^datasetList/$', coremon_views.datasetList, name='datasetListLegacy'),
    re_path(r'^dataset/$', coremon_views.datasetInfo, name='datasetInfo'),
    re_path(r'^datasets/$', coremon_views.datasetList, name='datasetList'),

    re_path(r'^dash/$', coremon_views.dashboard, name='dashboard'),
    re_path(r'^dash/analysis/$', coremon_views.dashAnalysis, name='dashAnalysis'),
    re_path(r'^dash/production/$', coremon_views.dashProduction, name='dashProduction'),
    re_path(r'^dash/objectstore/$', coremon_views.dashObjectStore, name='dashObjectStore'),
    re_path(r'^new/dash/$', coremon_views.dashRegion, name='dashRegionLegacy'),  # legacy
    re_path(r'^dash/region/$', coremon_views.dashRegion, name='dashRegion'),
    re_path(r'^dash/world/$', coremon_views.dashNucleus, name='dashWorld'),
    re_path(r'^dash/es/$', coremon_views.dashES, name='dashES'),
    re_path(r'^status_summary/', include('core.status_summary.urls'), name='status_summary'),
    re_path(r'^workingGroups/$', coremon_views.workingGroups, name='workingGroups'),
    re_path(r'^workQueues/$', coremon_views.workQueues, name='workQueues'),

    re_path(r'^campaignpredictiondash/$', campaignprediction.campaignPredictionDash, name='campaignPredictionDash'),
    re_path(r'^campaignpredictioninfo/$', campaignprediction.campaignPredictionInfo, name='campaignPredictionInfo'),
    re_path(r'^campprog/$', campaingprogressDKB.campaignProgressDash, name='campaignProgressDash'),

    re_path(r'^slowtasks/$', taskproblemexplorer.taskProblemExplorer, name='taskProblemExplorer'),

    # auth
    re_path('', include('core.oauth.urls')),
    re_path(r'^csrftoken/$', coremon_views.getCSRFToken, name='getCSRFToken'),

    # support views for core
    re_path(r'^api/support/maxpandaid/$', core_coremon_support_views.maxpandaid, name='supportMaxpandaid'),
    re_path(r'^api/support/jobinfouservohrs/(?P<vo>[-A-Za-z0-9_.+ @]+)/(?P<nhours>\d+)/$', core_coremon_support_views.jobUserOrig, name='supportJobUserVoHrs'),
    re_path(r'^api/support/jobinfouservo/(?P<vo>[-A-Za-z0-9_.+ @]+)/(?P<ndays>\d+)/$', core_coremon_support_views.jobUserDaysOrig, name='supportJobUserVo'),

    # API
    re_path(r'^api/get_sites/', coremon_views.getSites, name='getsites'),
    re_path(r'^api/tasks_plots$', tasksPlots.getJobsData, name='tasksplots'),
    re_path(r'^api/get_hc_tests/', coremon_views.get_hc_tests, name='gethctests'),
    re_path(r'^api/user_dash/(?P<agg>.*)/$', coremon_views.userDashApi, name='userdashapi'),

    # ????
    re_path(r'^g4exceptions/$', coremon_views.g4exceptions, name='g4exceptions'),

    # robots.txt
    re_path('^robots\.txt$', TemplateView.as_view(template_name='robots.txt', content_type='text/plain')),

] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

# add apps urls if installed
if len(settings.INSTALLED_APPS_EXTRA) > 0:
    for app_name in settings.INSTALLED_APPS_EXTRA:
        urlpatterns.append(re_path('', include('{}.urls'.format(app_name)), name=app_name))

if settings.DEBUG:
    try:
        import debug_toolbar
        urlpatterns += [
            re_path(r'^__debug__/', include(debug_toolbar.urls)),
        ]
    except ImportError:
        pass

