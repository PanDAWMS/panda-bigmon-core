from django.utils.deprecation import MiddlewareMixin
import logging

logger = logging.getLogger(__name__)

class RequestLoggingMiddleware(MiddlewareMixin):
    def process_exception(self, request, exception):
        logger.error(
            f"Error occurred: {str(exception)}",
            exc_info=True,
            extra={'request_path': request.path, 'request_get': request.GET}
        )