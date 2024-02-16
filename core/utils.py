"""
    core.utils
"""
import logging
import re
import subprocess
import os
from django.conf import settings

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


def is_wildcards(value):
    if '*' in value or '|' in value or ',' in value or '!' in value:
        return True
    else:
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


def removeParam(urlquery, parname, mode='complete'):
    """Remove a parameter from current query"""
    urlquery = urlquery.replace('&&', '&')
    urlquery = urlquery.replace('?&', '?')
    pstr = '.*({}=[a-zA-Z0-9\.\-\_\,\:]*).*'.format(parname)
    pat = re.compile(pstr)
    mat = pat.match(urlquery)
    if mat:
        pstr = mat.group(1)
        urlquery = urlquery.replace(pstr, '')
        urlquery = urlquery.replace('&&', '&')
        urlquery = urlquery.replace('?&', '?')
        if mode != 'extensible' and (urlquery.endswith('?') or urlquery.endswith('&')):
            urlquery = urlquery[:len(urlquery) - 1]
    return urlquery


def complete_request(request, **kwargs):
    """
    Remove temporary params from session to avoid ORA-22835
    :param request:
    :param kwargs: expects extra_keys as list
    :return:
    """
    _logger.info("Len of session dict at the end: {}".format(len(str(request.session._session))))

    keys_to_remove = ['requestParams', 'viewParams', 'urls_cut', 'urls', 'TFIRST', 'TLAST', 'PLOW', 'PHIGH']
    if 'extra_keys' in kwargs:
        keys_to_remove.extend(kwargs['extra_keys'])

    for k in keys_to_remove:
        if k in request.session:
            del request.session[k]
    request.session.modified = True
    _logger.info("Len of session dict after cleaning: {}".format(len(str(request.session._session))))

    if is_json_request(request):
        request.session.set_expiry(settings.SESSION_API_CALL_AGE)
        _logger.info(f"Set session expiration for API call to {settings.SESSION_API_CALL_AGE}")
        _logger.debug(f"cache_key={request.session.cache_key}")

    return request


def get_most_recent_git_tag():
    """
    Getting recent git tag to show which version is running
    :return: git_tag
    """
    git_path = os.getcwd()

    try:
        git_tag = str(
            subprocess.check_output(
                ['git', '--git-dir={}/.git'.format(git_path), 'describe', '--tags'],
                stderr=subprocess.STDOUT)
        ).strip('\'b\\n')
    except subprocess.CalledProcessError as exc_info:
        _logger.exception('Failed to get latest tag from git repo\n{}'.format(str(exc_info.output)))
        git_tag = 'N/A'

    return git_tag


def is_xss(val):
    """
    Check if str contains XSS suspicious flags
    :param val: str
    :return:  bool
    """
    val = val.replace('%3C', '<').replace('%3E', '>')
    if 'script' in val.lower() and ('</' in val.lower() or '/>' in val.lower()):
        return True
    else:
        return False

