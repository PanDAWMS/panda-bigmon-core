from core.settings.local import GRAFANA

class Headers(object):
    def __init__(self, grafana_user = '', grafana_remember = '', grafana_sess = ''):
        self.grafana_user = grafana_user
        self.grafana_remember = grafana_remember
        self.grafana_sess = grafana_sess

    def get_headers_api(self):
        if 'Authorization' in GRAFANA:
            grafana_token = GRAFANA['Authorization']
        headers = { "Accept": "application/json",
                    "Content-Type": "application/json",
                   "Authorization": grafana_token
        }
        return headers