
def get_nginx_redirection_body(instance_path):
    return """    
    {
        "apiVersion": "v1",
        "data": {
            "bigpanda-redirect.conf": "server {\n  listen       8080 default_server;\n\n  server_tokens off; \n\n  location / {\n    rewrite ^/$RANDOM_STRING/(.*) /\$1 break;\n    proxy_pass http://127.0.0.1:32768/; \n    proxy_redirect     off;\n    proxy_set_header   Host \$host;\n  }\n  access_log  /dev/stdout;\n  error_log   /dev/stdout;\n\n  error_page   500 502 503 504  /50x.html;\n  location = /50x.html {\n  root   /usr/share/nginx/html;\n  }\n}\n"
        },
        "kind": "ConfigMap",
        "metadata": {
           "labels": {
                "name": "nginx-redirection-{instance_path}}",
                "app": "bigpanda-mlflow",
                "instance": {instance_path}
            },
            "name": "nginx-redirection-{instance_path}"
        }
    }
    """.format(instance_path=instance_path)



