import json, socket, random, string
from core.settings.local import OC_TOKEN, OC_ENDPOINT, OC_NAMESPACE
import requests
from typing import Dict

from kubernetes import client, config
from openshift.dynamic import DynamicClient

class occlicalls:
    TIME_OUT_FOR_QUERY = 60 * 5
    URL_CONFIGMAPS = "https://{endpoint}/api/v1/namespaces/{namespace}/configmaps".format(endpoint=OC_ENDPOINT,
                                                                                                  namespace=OC_NAMESPACE)
    configuration = client.Configuration()
    configuration.host = OC_ENDPOINT
    configuration.debug = True
    configuration.api_key = {"authorization": OC_TOKEN}
    configuration.api_key_prefix['authorization'] = 'Bearer'

    ocp_client = DynamicClient(
        client.ApiClient(configuration=configuration)
    )

    def __init__(self):
        self.INSTANCE = ''.join(random.choices(string.ascii_uppercase +
                                               string.digits, k=7))

    def get_instance(self):
        return self.INSTANCE

    def register_config_map(self):
        data = {
            "bigpanda-redirect.conf": "server {\n  listen       8080 default_server;\n\n  server_tokens off; \n\n  location / {\n    rewrite ^/" + self.INSTANCE + "/(.*) /\$1 break;\n    proxy_pass http://127.0.0.1:32768/; \n    proxy_redirect     off;\n    proxy_set_header   Host \$host;\n  }\n  access_log  /dev/stdout;\n  error_log   /dev/stdout;\n\n  error_page   500 502 503 504  /50x.html;\n  location = /50x.html {\n  root   /usr/share/nginx/html;\n  }\n}\n"
        }
        labels = {
            "name": "nginx-redirection-{instance_path}".format(instance_path=self.INSTANCE),
            "app": "bigpanda-mlflow",
            "instance": self.INSTANCE,
        }
        self.create_config_map(configmap_name="nginx-redirection-{instance_path}".format(instance_path= self.INSTANCE),
                               labels=labels, data=data)

    def create_config_map(
        self,
        configmap_name: str,
        labels: Dict[str, str],
        data: Dict[str, str],
    ) -> str:
        """Create a ConfigMap in the given namespace."""
        v1_configmaps = self.ocp_client.resources.get(
            api_version="v1", kind="ConfigMap"
        )
        v1_configmaps.create(
            body={
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "data": data,
                "metadata": {
                    "labels": labels,
                    "name": configmap_name,
                    "namespace": OC_NAMESPACE,
                },
            },
        )
        return configmap_name



if __name__ == "__main__":
    ocwrap = occlicalls()
    ocwrap.register_config_map()




