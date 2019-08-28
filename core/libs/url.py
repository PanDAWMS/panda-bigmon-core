"""
A set of functions for URL refactoring
"""

import re


def extensible_url(request, xurl=''):
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


def remove_param(urlquery, parname, mode='complete'):
    """Remove a parameter from current query"""
    urlquery = urlquery.replace('&&', '&')
    urlquery = urlquery.replace('?&', '?')
    pstr = '.*(%s=[a-zA-Z0-9\.\-\_\,\:]*).*' % parname
    pat = re.compile(pstr)
    mat = pat.match(urlquery)
    if mat:
        pstr = mat.group(1)
        urlquery = urlquery.replace(pstr, '')
        urlquery = urlquery.replace('&&', '&')
        urlquery = urlquery.replace('?&', '?')
        if mode != 'extensible':
            if urlquery.endswith('?') or urlquery.endswith('&'): urlquery = urlquery[:len(urlquery) - 1]
    return urlquery
