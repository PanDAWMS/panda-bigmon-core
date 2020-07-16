
from core.mlflowdynamic.proxy.views import ProxyView
from django.shortcuts import redirect
from core.mlflowdynamic.proxy.response import get_django_response
from django.shortcuts import render_to_response
from core.mlflowdynamic.models import MLFlowContainers
from core.mlflowdynamic.openshiftcontroller import occlicalls
from datetime import datetime


class MLFlowProxyView(ProxyView):
    deployment_label = 'm35fqfq/'
    upstream = 'https://test-mlflow-bigpanda.web.cern.ch/'


    def patch_path(self, path, taskid, instance_url):
        return path.replace(str(taskid) + "/", instance_url, 1)


    def extract_taskid(self, path):
        taskid = path[0:path.find('/',1)]
        try:
            taskid=int(taskid)
        except:
            raise KeyError
        return taskid




    def __spin_up_container(self, taskid, dbentry):
        ocwrap = occlicalls(taskid)
        instance = ocwrap.get_instance()
        dbentry.instanceurl = instance + "/"
        dbentry.status = "spinning"
        dbentry.save()

        #Todo: make threaded
        #   job_thread = threading.Thread(target=job_func)
        #   job_thread.daemon = True
        #   job_thread.start()

        ocwrap.register_config_map()
        ocwrap.openshift_actions()
        dbentry.status = "active"
        dbentry.save()
        return instance


    def get_mlflow_container_for_task(self,taskid):
        entry, created = MLFlowContainers.objects.get_or_create(
            jeditaskid=taskid,
            errorAt__isnull=True,
            deletedAt__isnull=True,
            status__in=['active', 'spinning']
        )

        if created:
            self.__spin_up_container(taskid, entry)
        return (entry, created)


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
            return render_to_response('banner.html',
                                      {"message": "HPO Task is not supplied in request", "status": "error"},
                                      content_type='text/html')

        if not self.check_task_is_hpo(taskid):
            return render_to_response('banner.html',
                                      {"message": "Requested task is not HPO, please correct", "status": "error"},
                                      content_type='text/html')

        (entry, created) = self.get_mlflow_container_for_task(taskid)

        if entry.status == "error":
            return render_to_response('banner.html', {"message": "Error during spinning up the container. Try later.",
                                                      "status": "error"}, content_type='text/html')

        if entry.status == "spinning":
            return render_to_response('banner.html',
                                      {"message": "Spinning up an MLFlow containter", "status": "spinning"},
                                      content_type='text/html')

        if entry.status == "active":
            return self.process_proxy(request, path, taskid, entry.instanceurl)

