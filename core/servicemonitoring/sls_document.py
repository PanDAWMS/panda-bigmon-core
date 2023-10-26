import json
import requests
import time

from logger import ServiceLogger

_logger = ServiceLogger("servicemonitoring", __file__).logger

class SlsDocument:
    def __init__(self):
        self.info = {}
        self.data = {}
        self.id = None
        self.producer = 'atlaspandabigmon'

    def set_id(self, id_info):
        self.id = id_info

    def set_status(self, availability):
        if availability in (100, '100'):
            self.info['service_status'] = "available"
        elif availability in (0, '0'):
            self.info['service_status'] = "unavailable"
        else:
            self.info['service_status'] = "degraded"

    def set_avail_desc(self, avail_desc):
        self.info['availabilitydesc'] = avail_desc

    def set_avail_info(self, avail_info):
        self.info['availabilityinfo'] = avail_info

    def add_data(self, name, value):
        self.data[name] = value

    def get_time(self):
        return int(time.time() * 1000)

    def send_document(self, collector_endpoint):
        docs = []
        if not self.id:
            _logger.warning("No id was set. Will not send")

        if self.info:
            self.info['type'] = 'availability'
            self.info['serviceid'] = self.id
            self.info['producer'] = self.producer
            self.info['timestamp'] = self.get_time()
            docs.append(self.info)

        if self.data:
            self.data['type'] = 'metrics'
            self.data['serviceid'] = self.id
            self.data['producer'] = self.producer
            self.data['timestamp'] = self.get_time()
            docs.append(self.data)

        response = requests.post(
            collector_endpoint,
            data=json.dumps(docs),
            headers={"Content-Type": "application/json; charset=UTF-8"})

        _logger.debug('Tried to publish docs {0} with status code: {1}'.format(docs, response.status_code))
