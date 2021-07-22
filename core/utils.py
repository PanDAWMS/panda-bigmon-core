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


def complete_request(request, **kwargs):
    """
    Remove temporary params from session to avoid ORA-22835
    :param request:
    :param kwargs: expects extra_keys as list
    :return:
    """
    _logger.info("Len of session dict at the end: {}".format(len(str(request.session._session))))

    keys_to_remove = ['requestParams', 'viewParams', 'urls_cut', 'urls']
    if 'extra_keys' in kwargs:
        keys_to_remove.extend(kwargs['extra_keys'])

    for k in keys_to_remove:
        if k in request.session:
            del request.session[k]
    request.session.modified = True
    _logger.info("Len of session dict after cleaning: {}".format(len(str(request.session._session))))

    return request
