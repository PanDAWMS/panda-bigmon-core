"""
    core.utils
"""
import logging
_logger = logging.getLogger('bigpandamon')


def is_json_request(request):
    """
    Check if request is requires JSON output
    :param request:
    :return: bool: True or False
    """

    if ('HTTP_ACCEPT' in request.META and request.META.get('HTTP_ACCEPT') in ('text/json', 'application/json')) or (
            'json' in request.GET or 'json' in request.POST):
        _logger.info("This is JSON request: {} ".format(request.get_full_path()))
        return True

    return False

