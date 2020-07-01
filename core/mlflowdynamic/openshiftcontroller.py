import yaml
from kubernetes import client, config
from openshift.dynamic import DynamicClient
import string
import random
from pprint import pprint
from kubernetes.client.rest import ApiException


AUTH_TOKEN="eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJ0ZXN0LW1sZmxvdy1iaWdwYW5kYSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJiaWdwYW5kYS10b2tlbi04bjdtOCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJiaWdwYW5kYSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6IjgwN2NhZDIxLWI1NGQtMTFlYS04MzQ4LTAyMTYzZTAxODBmZCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDp0ZXN0LW1sZmxvdy1iaWdwYW5kYTpiaWdwYW5kYSJ9.YgKzH5Lbax_PyC9hHYGnX7JUaEFHpFO_YHcMbTY3M0jrwxpfnURSPH3k8wNrb_8_U5_1qqqOJMfJ2PNkwU6QFxlES11twCda4Oj3uDXcmzBrgsnoex5GfwkZHV3J1IppRLLG2quV8PPIjW8no6GCjuAHYTczjfYhDp2lz9b0JqjqaZByc8rka7BV8qvn5XSbSujIkXEpZ4vFSj5AUeMp2jaks4ZTt55ZsAVrMT09TMWENctuFlXJas89HWzfLoWTB2hu4hry-X_8mZ1F53TmXeYNnt5UqPrQwKgTaJ9U3LikEx0AiBZ8wGXjgLHbISaxubTxauf3-iZX7NloldIOzg"
ENDPOINT="openshift-dev.cern.ch"
NAMESPACE="test-mlflow-bigpanda"

RANDOM_STRING = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k = 7))


def registerConfigMap():
    configuration = client.Configuration()
    configuration.api_key['authorization'] = AUTH_TOKEN
    configuration.api_key_prefix['authorization'] = 'Bearer'
    configuration.host = ENDPOINT
    api_instance= client.CoreV1Api(client.ApiClient(configuration))
    namespace = NAMESPACE
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
        api_response = api_instance.create_namespaced_config_map(namespace, body)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling CoreV1Api->create_namespaced_config_map: %s\n" % e)


if __name__ == "__main__":
    registerConfigMap()




