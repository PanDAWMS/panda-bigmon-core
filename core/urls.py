from django.conf.urls.static import static
from django.views.generic import TemplateView, RedirectView
#import core.settings
from django.conf import settings
from django.urls import re_path, include

from core import views as coremon_views
from core import dpviews as dpviews
from core import MemoryMonitorPlots as memmon
from core.art import views as art_views

from core.monitor import views as monitor_views
from core.harvester import views as harvester
from core.grafana import views as grafana

from core.globalshares import views as globalshares
from core.dashboards import dtcdboard as dtcdboard

from core.runningprod import views as runningprod_views
from core.errorsscattering import views as errorsscat_views
from core.compare import views as compare_views

from core.globalpage import views as globalpage

#import core.views as coremon_views
import core.pandajob.views_support as core_coremon_support_views
#import core.pandajob.views as core_coremon_views
#import core.api.reprocessing.views as core_coremon_api_reprocessing_views

app_name = "bigpandamon"

urlpatterns = [
    re_path(r'^$', coremon_views.mainPage, name='mainPage'),
    re_path(r'^$', coremon_views.mainPage, name='index'),
    re_path(r'^help/$', coremon_views.helpPage, name='helpPage'),
    re_path(r'^jobs/$', coremon_views.jobList, name='jobList'),
    re_path(r'^jobs/(.*)/$', coremon_views.jobList, name='jobList'),
    re_path(r'^jobs/(.*)/(.*)/$', coremon_views.jobList, name='jobList'),

    re_path(r'^jobsss/$', coremon_views.jobListP, name='jobListP'),
    re_path(r'^jobsss/(.*)/$', coremon_views.jobListP, name='jobListP'),
    re_path(r'^jobsss/(.*)/(.*)/$', coremon_views.jobListP, name='jobListP'),
    re_path(r'^jobssupt/$', coremon_views.jobListPDiv, name='jobListPDiv'),

    re_path(r'^job$', coremon_views.jobInfo, name='jobInfo'),
    re_path(r'^job/(.*)/$', coremon_views.jobInfo, name='jobInfo'),
    re_path(r'^job/(.*)/(.*)/$', coremon_views.jobInfo, name='jobInfo'),
    re_path(r'^users/$', coremon_views.userList, name='userList'),
    re_path(r'^user/(?P<user>.*)/$', coremon_views.userInfo, name='userInfo'),
    re_path(r'^user/$', coremon_views.userInfo, name='userInfo'),
    re_path(r'^sites/$', coremon_views.siteList, name='siteList'),
    re_path(r'^site/(?P<site>.*)/$', coremon_views.siteInfo, name='siteInfo'),
    re_path(r'^site/$', coremon_views.siteInfo, name='siteInfo'),
    re_path(r'^wns/(?P<site>.*)/$', coremon_views.wnInfo, name='wnInfo'),
    re_path(r'^wn/(?P<site>.*)/(?P<wnname>.*)/$', coremon_views.wnInfo, name='wnInfo'),
    re_path(r'^tasks/$', coremon_views.taskList, name='taskList'),
    re_path(r'^task$', coremon_views.taskInfo, name='taskInfo'),
    re_path(r'^task/$', coremon_views.taskInfo, name='taskInfo'),
    re_path(r'^errors/$', coremon_views.errorSummary, name='errorSummary'),
    re_path(r'^incidents/$', coremon_views.incidentList, name='incidentList'),
    re_path(r'^logger/$', coremon_views.pandaLogger, name='pandaLogger'),
    #re_path(r'^eslogger/$', coremon_views.esPandaLogger, name='esPandaLogger'),
    re_path(r'^esatlaslogger/$', coremon_views.esatlasPandaLogger, name='esatlasPandaLogger'),
    re_path(r'^task/(?P<jeditaskid>.*)/$', coremon_views.taskInfo, name='taskInfo'),
    re_path(r'^tasknew/(?P<jeditaskid>.*)/$', coremon_views.taskInfoNew, name='taskInfoNew'),
    re_path(r'^getjobsummaryfortask/(?P<jeditaskid>.*)/$', coremon_views.getJobSummaryForTask, name='getJobSummaryForTask'),
    re_path(r'^dash/$', coremon_views.dashboard, name='dashboard'),
    re_path(r'^dash/analysis/$', coremon_views.dashAnalysis, name='dashAnalysis'),
    re_path(r'^dash/production/$', coremon_views.dashProduction, name='dashProduction'),
    re_path(r'^dash/objectstore/$', coremon_views.dashObjectStore, name='dashObjectStore'),
    re_path(r'^workingGroups/$', coremon_views.workingGroups, name='workingGroups'),
    re_path(r'^fileInfo/$', coremon_views.fileInfo, name='fileInfo'),
    re_path(r'^fileList/$', coremon_views.fileList, name='fileList'),
    re_path(r'^loadFileList/(?P<datasetid>.*)/$', coremon_views.loadFileList, name='loadFileList'),
    re_path(r'^datasetInfo/$', coremon_views.datasetInfo, name='datasetInfo'),
    re_path(r'^datasetList/$', coremon_views.datasetList, name='datasetList'),
    re_path(r'^workQueues/$', coremon_views.workQueues, name='workQueues'),
    re_path(r'^preprocess/$', coremon_views.preProcess, name='preprocess'),
    re_path(r'^g4exceptions/$', coremon_views.g4exceptions, name='g4exceptions'),
    re_path(r'^errorslist/$', coremon_views.summaryErrorsList, name='summaryErrorsList'),
    re_path(r'^worldjobs/$', coremon_views.worldjobs, name='worldjobs'),
    re_path(r'^getbadeventsfortask/$', coremon_views.getBadEventsForTask, name='getbadeventsfortask'),
    re_path(r'^getstaginginfofortask/$', dtcdboard.getStagingInfoForTask, name='getStagingInfoForTask'),
    re_path(r'^getdtcsubmissionhist/$', dtcdboard.getDTCSubmissionHist, name='getDTCSubmissionHist'),

    re_path(r'^dtcdboard/$', dtcdboard.datatapeCarouselleDashBoard, name='datatapeCarouselleDashBoard'),

    re_path(r'^globalpage/$', globalpage.globaldemo, name='SITGlobalPage'),
    re_path(r'^globalpagedata/$', globalpage.globaldata, name='SITGlobalData'),

                  #    re_path(r'^worldjobs/analysis/$', coremon_views.dashWorldAnalysis, name='dashWorldAnalysis'),
#    re_path(r'^worldjobs/production/$', coremon_views.dashWorldProduction, name='dashWorldProduction'),

    re_path(r'^runningmcprodtasks/$', runningprod_views.runningMCProdTasks, name='runningMCProdTasks'),
    re_path(r'^runningprodtasks/$', runningprod_views.runningProdTasks, name='runningProdTasks'),
    re_path(r'^runningdpdprodtasks/$', runningprod_views.runningDPDProdTasks, name='runningDPDProdTasks'),
    re_path(r'^prodeventstrend/$', runningprod_views.prodNeventsTrend, name='prodNeventsTrend'),
    re_path(r'^runningprodrequests/$', runningprod_views.runningProdRequests, name='runningProdRequests'),
    re_path(r'^worldhs06s/$', coremon_views.worldhs06s, name='worldHS06s'),
    re_path(r'^taskESExtendedInfo/$', coremon_views.taskESExtendedInfo, name='taskESExtendedInfo'),
    re_path(r'^descendentjoberrsinfo/$', coremon_views.descendentjoberrsinfo, name='descendentjoberrsinfo'),
    re_path(r'^taskssummary/$', coremon_views.getSummaryForTaskList, name='taskListSummary'),
    re_path(r'^ttc/$', coremon_views.ttc, name='ttc'),
    re_path(r'^taskchain/$', coremon_views.taskchain, name='taskchain'),
    re_path(r'^ganttTaskChain/$', coremon_views.ganttTaskChain, name='ganttTaskChain'),
    re_path(r'^taskprofileplot/$', coremon_views.taskprofileplot, name='taskprofileplot'),
    re_path(r'^taskesprofileplot/$', coremon_views.taskESprofileplot, name='taskesprofileplot'),
    re_path(r'^eventsinfo/$', coremon_views.eventsInfo, name='eventsInfo'),
    re_path(r'^statpixel/$', coremon_views.statpixel, name='statpixel'),
    re_path(r'^killtasks/$', coremon_views.killtasks, name='killtasks'),
    re_path(r'^eventserrorsummaury/$', coremon_views.getErrorSummaryForEvents, name='eventsErrorSummary'),

    re_path(r'^jobstatuslog/(?P<pandaid>.*)/$', coremon_views.getJobStatusLog, name='getjobstatuslog'),

    re_path(r'^savesettings/$', coremon_views.saveSettings, name='saveSettings'),
    #re_path(r'^globalsharesnew/$', coremon_views.globalsharesnew, name='globalsharesnew'),
                       #    re_path(r'^preprocessdata/$', coremon_views.preprocessData, name='preprocessdata'),
    ### data product catalog prototyping
    re_path(r'^dp/$', dpviews.doRequest, name='doRequest'),
    re_path(r'^report/$', coremon_views.report, name='report'),
    re_path(r'^serverstatushealth/$', coremon_views.serverStatusHealth, name='serverStatusHealth'),

                  ### ART nightly tests
    re_path(r'^art/$', art_views.art, name='art-mainPage'),
    re_path(r'^art/overview/$', art_views.artOverview, name='artOverview'),
    re_path(r'^art/tasks/$', art_views.artTasks, name='artTasks'),
    re_path(r'^art/jobs/$', art_views.artJobs, name='artJobs'),
    re_path(r'^art/getjobsubresults/$', art_views.getJobSubResults, name='artJobSubResults'),
    re_path(r'^art/updatejoblist/$', art_views.updateARTJobList),
    re_path(r'^art/registerarttest/$', art_views.registerARTTest),
    re_path(r'^art/sendartreport/$', art_views.sendArtReport),




    ### filebrowser
    re_path(r'^filebrowser/', include('core.filebrowser.urls'), name='filebrowser'),
    ### PanDA Brokerage Monitor
    re_path(r'^pbm/', include('core.pbm.urls'), name='pbm'),
    re_path(r'^status_summary/', include('core.status_summary.urls'), name='status_summary'),

    ### support views for core
    re_path(r'^support/$', core_coremon_support_views.maxpandaid, name='supportRoot'),
    re_path(r'^support/maxpandaid/$', core_coremon_support_views.maxpandaid, name='supportMaxpandaid'),
    re_path(r'^support/jobinfouservohrs/(?P<vo>[-A-Za-z0-9_.+ @]+)/(?P<nhours>\d+)/$', core_coremon_support_views.jobUserOrig, name='supportJobUserVoHrs'),
    re_path(r'^support/jobinfouservo/(?P<vo>[-A-Za-z0-9_.+ @]+)/(?P<ndays>\d+)/$', core_coremon_support_views.jobUserDaysOrig, name='supportJobUserVo'),
    #### JSON for Datatables
    re_path(r'^datatable/data/jeditaskid',coremon_views.esatlasPandaLoggerJson, name='dataTableJediTaskId'),
    re_path(r'^datatable/data/errorSummaryList', coremon_views.summaryErrorsListJSON, name='summaryErrorsListJSON'),
    ###self monitor
    #re_path(r'^admin/', include('core.admin.urls', namespace='admin')),

    ### api
    re_path(r'^api/$', core_coremon_support_views.maxpandaid, name='supportRoot'),
#    re_path(r'^api/reprocessing/$', include('core.api.reprocessing.urls')),

    ### robots.txt
    re_path('^robots\.txt$', TemplateView.as_view(template_name='robots.txt', content_type='text/plain')),

    re_path(r'^memoryplot/', memmon.getPlots, name='memoryplot'),

    ###Images###
    re_path('^img/',coremon_views.image, name='img'),
    re_path('^grafana/',coremon_views.grafana_image, name='grafana'),

    re_path(r'^oauth/', include('social_django.urls', namespace='social')),  # <--
    re_path(r'^testauth/$', coremon_views.testauth, name='testauth'),
    re_path(r'^login/$', coremon_views.loginauth2, name='loginauth2'),
    re_path(r'^login/$', coremon_views.loginauth2, name='login'),
    re_path(r'^logout/$', coremon_views.logout, name='logout'),
    re_path(r'^loginerror/$', coremon_views.loginerror, name='loginerror'),
    re_path(r'^grantrights/$', coremon_views.grantRights, name='grantrights'),
    re_path(r'^denyrights/$', coremon_views.denyRights, name='denyrights'),


    re_path(r'^testip/$', coremon_views.testip, name='testip'),
    re_path(r'^eventschunks/$', coremon_views.getEventsChunks, name='eventschunks'),

    re_path(r'^taskserrorsscat/$', errorsscat_views.tasksErrorsScattering, name='tasksErrorsScattering'),
    re_path(r'^errorsscat/$', errorsscat_views.errorsScattering, name='errorsScattering'),
    re_path(r'^errorsscat/(?P<cloud>.*)/(?P<reqid>.*)/$', errorsscat_views.errorsScatteringDetailed, name='errorsScatteringDetailed'),
    ###Monitor###
    re_path(r'^bigpandamonitor/$', monitor_views.monitorJson, name='bigpandamonitor'),
    ####HARVESTER####
    re_path(r'^harvesterworkersdash/$', harvester.harvesterWorkersDash, name='harvesterworkersdash'),
    re_path(r'^harvesterworkerslist/$', harvester.harvesterWorkList, name='harvesterworkerslist'),
    re_path(r'^harvesterworkerinfo/$', harvester.harvesterWorkerInfo, name='harvesterWorkerInfo'),
    re_path(r'^harvestertest/$', harvester.harvesterfm, name='harvesterfm'),
    re_path(r'^harvesters/$', harvester.harvesters, name='harvesters'),
    re_path(r'^harvesters/slots/$', harvester.harvesterslots, name='harvesterslots'),
    re_path(r'^workers/$', harvester.workersJSON, name='workers'),
    re_path(r'^workersfortask/$', coremon_views.getHarversterWorkersForTask, name='workersfortask'),

     #re_path(r'^json/harvesterinstances/$', harvester.harvesterinstancesjson, name='harvesterinstancesJSON'),
    ####GLOBALSHARES#####
    re_path(r'^globalshares/$', globalshares.globalshares, name='globalshares'),
    re_path(r'^datatable/data/detailedInformationJSON', globalshares.detailedInformationJSON, name='detailedInformationJSON'),
    re_path(r'^datatable/data/sharesDistributionJSON', globalshares.sharesDistributionJSON, name='sharesDistributionJSON'),
    re_path(r'^datatable/data/siteWorkQueuesJSON', globalshares.siteWorkQueuesJSON, name='siteWorkQueuesJSON'),
    re_path(r'^datatable/data/resourcesType', globalshares.resourcesType, name='resourcesType'),
    re_path(r'^datatable/data/coreTypes', globalshares.coreTypes, name='coreTypes'),
    re_path(r'^datatable/data/fairsharePolicy', globalshares.fairsharePolicy, name='fairsharePolicy'),
    ###Grafana###
    re_path(r'^api/grafana', grafana.grafana_api, name='grafana_api'),
    # re_path(r'^grafanaplots', grafana.index, name='grafana_plots'),
    re_path(r'^grafanaplots', grafana.chartjs, name='grafana_chartjsplots'),
    ###Compare###
    re_path(r'^compare/jobs/$', compare_views.compareJobs, name='compareJobs'),
    re_path(r'^deletefromcomparison/$', compare_views.deleteFromComparison),
    re_path(r'^addtocomparison/$', compare_views.addToComparison),
    re_path(r'^clearcomparison/$', compare_views.clearComparison),

    ###API###
    re_path(r'^api/get_sites/', coremon_views.getSites, name='getsites'),

    ] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


if settings.DEBUG:
    import debug_toolbar
    urlpatterns += [
        # re_path(r'^__debug__/', include(debug_toolbar.urls)),
    ]



#urlpatterns += common_patterns
#urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
