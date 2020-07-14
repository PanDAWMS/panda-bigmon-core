from core.mlflowdynamic.models import MLFlowContainers
from django.shortcuts import get_object_or_404

class mlflowspinner:
    def __download_metrics(self, taskid):
        pass

    def __spin_up_container(self):
        pass

    def __entry_exists(self, taskid):
        return MLFlowContainers.objects.get_or_create(
            jeditaskid = taskid,
            errorAt__isnull = True,
            deletedAt__isnull = True
        )

    def get_mlFlow_url_for_task(self, taskid):
        entry, created = self.__entry_exists(taskid)
        if not created:
            return entry
        else:
            entry.status = "data_downloading"
            entry.save()
            self.__download_metrics(taskid)


