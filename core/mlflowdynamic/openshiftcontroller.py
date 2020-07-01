import yaml
from kubernetes import client, config
from openshift.dynamic import DynamicClient
import string
import random
from pprint import pprint
from kubernetes.client.rest import ApiException

from core.settings.local import OC_TOKEN, OC_ENDPOINT, OC_NAMESPACE

RANDOM_STRING = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k = 7))


def registerConfigMap():
    configuration = client.Configuration()
    configuration.api_key['authorization'] = OC_TOKEN
    configuration.api_key_prefix['authorization'] = 'Bearer'
    configuration.host = OC_ENDPOINT
    api_instance= client.CoreV1Api(client.ApiClient(configuration))
    body = client.V1ConfigMap()
    body.api_version = 'v1'
    body.data = {
        "bigpanda-redirect.conf": "server {\n  listen       8080 default_server;\n\n  server_tokens off; \n\n  location / {\n    rewrite ^/"+RANDOM_STRING+"/(.*) /\$1 break;\n    proxy_pass http://127.0.0.1:32768/; \n    proxy_redirect     off;\n    proxy_set_header   Host \$host;\n  }\n  access_log  /dev/stdout;\n  error_log   /dev/stdout;\n\n  error_page   500 502 503 504  /50x.html;\n  location = /50x.html {\n  root   /usr/share/nginx/html;\n  }\n}\n"
    }
    body.kind = 'ConfigMap'
    body.metadata = {
        "labels": {
        "name": "nginx-redirection-" + RANDOM_STRING,
        "app": "bigpanda-mlflow",
        "instance": RANDOM_STRING
        },
        "name": "nginx-redirection-" + RANDOM_STRING
    }

    try:
        api_response = api_instance.create_namespaced_config_map(OC_NAMESPACE, body)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling CoreV1Api->create_namespaced_config_map: %s\n" % e)


if __name__ == "__main__":
    registerConfigMap()




