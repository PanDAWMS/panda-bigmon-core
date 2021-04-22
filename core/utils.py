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


def extensibleURL(request, xurl=''):
    """ Return a URL that is ready for p=v query extension(s) to be appended """
    if xurl == '':
        xurl = request.get_full_path()
    if xurl.endswith('/'):
        if 'tag' or '/job/' or '/task/' in xurl:
            xurl = xurl[0:len(xurl)]
        else:
            xurl = xurl[0:len(xurl) - 1]

    if xurl.find('?') > 0:
        xurl += '&'
    else:
        xurl += '?'

    return xurl
