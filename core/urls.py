from django.conf.urls import include, url
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView
from django.views.generic import RedirectView
#import core.settings
from django.conf import settings


from core import views as coremon_views
from core import dpviews as dpviews
from core import MemoryMonitorPlots as memmon
from core.art import views as art_views

from core.monitor import views as monitor_views
from core.harvester import views as harvester
from core.globalshares import views as globalshares
from core.runningprod import views as runningprod_views
#import core.views as coremon_views
import core.pandajob.views_support as core_coremon_support_views
#import core.pandajob.views as core_coremon_views
#import core.api.reprocessing.views as core_coremon_api_reprocessing_views


urlpatterns = [
    url(r'^$', coremon_views.mainPage, name='mainPage'),
    url(r'^$', coremon_views.mainPage, name='index'),
    url(r'^help/$', coremon_views.helpPage, name='helpPage'),
    url(r'^jobs/$', coremon_views.jobList, name='jobList'),
    url(r'^jobs/(.*)/$', coremon_views.jobList, name='jobList'),
    url(r'^jobs/(.*)/(.*)/$', coremon_views.jobList, name='jobList'),

    #url(r'^jobsss/$', coremon_views.jobListProto, name='jobListProto'),
    #url(r'^jobsss/(.*)/$', coremon_views.jobListProto, name='jobListProto'),
    #url(r'^jobsss/(.*)/(.*)/$', coremon_views.jobListProto, name='jobListProto'),

    url(r'^jobsss/$', coremon_views.jobListP, name='jobListP'),
    url(r'^jobsss/(.*)/$', coremon_views.jobListP, name='jobListP'),
    url(r'^jobsss/(.*)/(.*)/$', coremon_views.jobListP, name='jobListP'),
    url(r'^jobssupt/$', coremon_views.jobListPDiv, name='jobListPDiv'),

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
    #url(r'^eslogger/$', coremon_views.esPandaLogger, name='esPandaLogger'),
    url(r'^esatlaslogger/$', coremon_views.esatlasPandaLogger, name='esatlasPandaLogger'),
    url(r'^task/(?P<jeditaskid>.*)/$', coremon_views.taskInfo, name='taskInfo'),
    url(r'^dash/$', coremon_views.dashboard, name='dashboard'),
    url(r'^dash/analysis/$', coremon_views.dashAnalysis, name='dashAnalysis'),
    url(r'^dash/production/$', coremon_views.dashProduction, name='dashProduction'),
    url(r'^dash/objectstore/$', coremon_views.dashObjectStore, name='dashObjectStore'),
    url(r'^workingGroups/$', coremon_views.workingGroups, name='workingGroups'),
    url(r'^fileInfo/$', coremon_views.fileInfo, name='fileInfo'),
    url(r'^fileList/$', coremon_views.fileList, name='fileList'),
    url(r'^datasetInfo/$', coremon_views.datasetInfo, name='datasetInfo'),
    url(r'^datasetList/$', coremon_views.datasetList, name='datasetList'),
    url(r'^workQueues/$', coremon_views.workQueues, name='workQueues'),
    url(r'^preprocess/$', coremon_views.preProcess, name='preprocess'),
    url(r'^g4exceptions/$', coremon_views.g4exceptions, name='g4exceptions'),
    url(r'^errorslist/$', coremon_views.summaryErrorsList, name='summaryErrorsList'),
    url(r'^worldjobs/$', coremon_views.worldjobs, name='worldjobs'),
    url(r'^getbadeventsforfask/$', coremon_views.getBadEventsForTask, name='getbadeventsforfask'),

#    url(r'^worldjobs/analysis/$', coremon_views.dashWorldAnalysis, name='dashWorldAnalysis'),
#    url(r'^worldjobs/production/$', coremon_views.dashWorldProduction, name='dashWorldProduction'),

    url(r'^runningmcprodtasks/$', runningprod_views.runningMCProdTasks, name='runningMCProdTasks'),
    url(r'^runningprodtasks/$', runningprod_views.runningProdTasks, name='runningProdTasks'),
    url(r'^runningdpdprodtasks/$', runningprod_views.runningDPDProdTasks, name='runningDPDProdTasks'),
    url(r'^runningprodrequests/$', runningprod_views.runningProdRequests, name='runningProdRequests'),
    url(r'^worldhs06s/$', coremon_views.worldhs06s, name='worldHS06s'),
    url(r'^taskESExtendedInfo/$', coremon_views.taskESExtendedInfo, name='taskESExtendedInfo'),
    url(r'^descendentjoberrsinfo/$', coremon_views.descendentjoberrsinfo, name='descendentjoberrsinfo'),
    url(r'^taskssummary/$', coremon_views.getSummaryForTaskList, name='taskListSummary'),
    url(r'^ttc/$', coremon_views.ttc, name='ttc'),
    url(r'^taskchain/$', coremon_views.taskchain, name='taskchain'),
    url(r'^ganttTaskChain/$', coremon_views.ganttTaskChain, name='ganttTaskChain'),
    url(r'^taskprofileplot/$', coremon_views.taskprofileplot, name='taskprofileplot'),
    url(r'^taskesprofileplot/$', coremon_views.taskESprofileplot, name='taskesprofileplot'),
    url(r'^eventsinfo/$', coremon_views.eventsInfo, name='eventsInfo'),
    url(r'^statpixel/$', coremon_views.statpixel, name='statpixel'),
    url(r'^killtasks/$', coremon_views.killtasks, name='killtasks'),
    url(r'^eventserrorsummaury/$', coremon_views.getErrorSummaryForEvents, name='eventsErrorSummary'),

    url(r'^savesettings/$', coremon_views.saveSettings, name='saveSettings'),
    #url(r'^globalsharesnew/$', coremon_views.globalsharesnew, name='globalsharesnew'),
                       #    url(r'^preprocessdata/$', coremon_views.preprocessData, name='preprocessdata'),
    ### data product catalog prototyping                                                                                                                                                         
    url(r'^dp/$', dpviews.doRequest, name='doRequest'),

    url(r'^report/$', coremon_views.report, name='report'),

    url(r'^serverstatushealth/$', coremon_views.serverStatusHealth, name='serverStatusHealth'),

    ### ART nightly tests
    url(r'^art/$', art_views.art, name='art-mainPage'),
    url(r'^art/overview/$', art_views.artOverview, name='artOverview'),
    url(r'^art/tasks/$', art_views.artTasks, name='artTasks'),
    url(r'^art/jobs/$', art_views.artJobs, name='artJobs'),
    url(r'^art/getjobsubresults/$', art_views.getJobSubResults, name='artJobSubResults'),
    url(r'^art/updatejoblist/$', art_views.updateARTJobList),
    url(r'^art/registerarttest/$', art_views.registerARTTest),



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
    #### JSON for Datatables
    url (r'^datatable/data/jeditaskid',coremon_views.esatlasPandaLoggerJson, name='dataTableJediTaskId'),
    url(r'^datatable/data/errorSummaryList', coremon_views.summaryErrorsListJSON, name='summaryErrorsListJSON'),
    ###self monitor
    url(r'^admin/', include('core.admin.urls', namespace='admin')),

    ### api
    url(r'^api/$', core_coremon_support_views.maxpandaid, name='supportRoot'),
#    url(r'^api/reprocessing/$', include('core.api.reprocessing.urls')),

    ### robots.txt
    url('^robots\.txt$', TemplateView.as_view(template_name='robots.txt', content_type='text/plain')),

    url(r'^memoryplot/', memmon.getPlots, name='memoryplot'),

    ###Images###
    url('^img/',coremon_views.image, name='img'),

    url(r'^oauth/', include('social_django.urls', namespace='social')),  # <--
    url(r'^testauth/$', coremon_views.testauth, name='testauth'),
    url(r'^login/$', coremon_views.loginauth2, name='loginauth2'),
    url(r'^login/$', coremon_views.loginauth2, name='login'),
    url(r'^logout/$', coremon_views.logout, name='logout'),
    url(r'^loginerror/$', coremon_views.loginerror, name='loginerror'),

    url(r'^testip/$', coremon_views.testip, name='testip'),
    url(r'^eventschunks/$', coremon_views.getEventsChunks, name='eventschunks'),

    url(r'^taskserrorsscat/$', coremon_views.tasksErrorsScattering, name='tasksErrorsScattering'),
    url(r'^errorsscat/$', coremon_views.errorsScattering, name='errorsScattering'),
    url(r'^errorsscat/(?P<cloud>.*)/(?P<reqid>.*)/$', coremon_views.errorsScatteringDetailed, name='errorsScatteringDetailed'),
    ###Monitor###
    url(r'^bigpandamonitor/$', monitor_views.monitorJson, name='bigpandamonitor'),
    ####HARVESTER####
    url(r'^harvesterworkersdash/$', harvester.harvesterWorkersDash, name='harvesterworkersdash'),
    url(r'^harvesterworkerslist/$', harvester.harvesterWorkList, name='harvesterworkerslist'),
    url(r'^harvesterworkerinfo/$', harvester.harvesterWorkerInfo, name='harvesterWorkerInfo'),
    url(r'^harvestertest/$', harvester.harvesterfm, name='harvesterfm'),
    url(r'^harvesters/$', harvester.harvesters, name='harvesters'),
    #url(r'^json/harvesterinstances/$', harvester.harvesterinstancesjson, name='harvesterinstancesJSON'),
    ####GLOBALSHARES#####
    url(r'^globalshares/$', globalshares.globalshares, name='globalshares'),
    url(r'^datatable/data/detailedInformationJSON', globalshares.detailedInformationJSON, name='detailedInformationJSON'),
    url(r'^datatable/data/sharesDistributionJSON', globalshares.sharesDistributionJSON, name='sharesDistributionJSON'),
    url(r'^datatable/data/siteWorkQueuesJSON', globalshares.siteWorkQueuesJSON, name='siteWorkQueuesJSON'),
    url(r'^datatable/data/resourcesType', globalshares.resourcesType, name='resourcesType'),
    url(r'^datatable/data/coresCount', globalshares.coresCount, name='coresCount'),
    ] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


if settings.DEBUG:
    import debug_toolbar
    urlpatterns += [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ]



#urlpatterns += common_patterns
#urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
