import logging

class RequestLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        return response

    def process_exception(self, request, exception):
        logger = logging.getLogger(__name__)
        logger.error('Unhandled Exception', exc_info=exception, extra={'request': request})
        return None