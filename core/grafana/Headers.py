from django.conf import settings

class Headers(object):
    def __init__(self, grafana_user = '', grafana_remember = '', grafana_sess = ''):
        self.grafana_user = grafana_user
        self.grafana_remember = grafana_remember
        self.grafana_sess = grafana_sess

    def get_headers_api(self):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if 'Authorization' in settings.GRAFANA:
            headers['Authorization'] = settings.GRAFANA['Authorization']

        return headers