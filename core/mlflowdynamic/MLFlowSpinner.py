
from core.mlflowdynamic.proxy.views import ProxyView
from django.shortcuts import redirect
from core.mlflowdynamic.proxy.response import get_django_response
from django.shortcuts import render_to_response
from django.utils.cache import patch_response_headers
from core.mlflowdynamic.models import MLFlowContainers
from core.mlflowdynamic.openshiftcontroller import occlicalls
from datetime import datetime


class MLFlowProxyView(ProxyView):
    upstream = 'https://bigpanda-mlflow.web.cern.ch/'

    def patch_path(self, path, taskid, instance_url):
        return path.replace(str(taskid) + "/", instance_url, 1)

    def extract_taskid(self, path):
        enclose_slash_pos = path.find('/', 1)
        enclose_slash_pos = enclose_slash_pos if enclose_slash_pos > 0 else len(path)
        taskid = path[:enclose_slash_pos]
        try:
            taskid=int(taskid)
        except:
            raise KeyError
        return taskid

    def _spin_up_container(self, taskid, dbentry):
        ocwrap = occlicalls(taskid)
        instance = ocwrap.get_instance()
        dbentry.instanceurl = instance + "/"
        dbentry.spinnedAt = datetime.utcnow()
        dbentry.status = "spinning"
        dbentry.save()

        #Todo: make threaded
        #   job_thread = threading.Thread(target=job_func)
        #   job_thread.daemon = True
        #   job_thread.start()

        ocwrap.register_config_map()
        ocwrap.openshift_actions()
        #dbentry.status = "active"
        #dbentry.save()
        return instance

    def get_mlflow_container_for_task(self,taskid):
        entry, created = MLFlowContainers.objects.get_or_create(
            jeditaskid=taskid,
            errorAt__isnull=True,
            deletedAt__isnull=True,
            status__in=['active', 'spinning']
        )

        if created:
            self._spin_up_container(taskid, entry)
        else:
            new_status = self._check_container_status(entry)
            if new_status != entry.status:
                entry.status = new_status
                entry.save()
        return entry, created

    def _check_container_status(self, entry):
        ocwrap = occlicalls(entry.jeditaskid)
        return ocwrap.get_deployment_status(entry.instanceurl)


    def process_proxy(self, request, path, taskid, instance_url):
        self.request_headers = self.get_request_headers()

        redirect_to = self._format_path_to_redirect(request)
        if redirect_to:
            return redirect(redirect_to)

        proxy_response = self._created_proxy_response(request, self.patch_path(path, taskid, instance_url))
        self._replace_host_on_redirect_location(request, proxy_response)
        self._set_content_type(request, proxy_response)
        response = get_django_response(proxy_response,
                                       strict_cookies=self.strict_cookies)
        self.log.debug("RESPONSE RETURNED: %s", response)
        return response

    def check_task_is_hpo(self, taskid):
        return True

    def dispatch(self, request, path):
        try:
            taskid = self.extract_taskid(path)
        except:
            response = render_to_response('banner.html',
                                      {"message": "HPO Task is not supplied in request", "status": "error"},
                                      content_type='text/html')
        if not self.check_task_is_hpo(taskid):
            response = render_to_response('banner.html',
                                      {"message": "Requested task is not HPO, please correct", "status": "error"},
                                      content_type='text/html')
        (entry, created) = self.get_mlflow_container_for_task(taskid)
        if entry.status == "error":
            response = render_to_response('banner.html', {"message": "Error during spinning up the container. Try later.",
                                                      "status": "error"}, content_type='text/html')
        if entry.status == "spinning":
            response = render_to_response('banner.html',
                                      {"message": "Spinning up an MLFlow containter. Please refresh in 10 seconds.", "status": "spinning"},
                                      content_type='text/html')
        if entry.status == "active":
            response = self.process_proxy(request, path, taskid, entry.instanceurl)
        patch_response_headers(response, cache_timeout=0)
        return response
