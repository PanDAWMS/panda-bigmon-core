# https://github.com/TracyWebTech/django-revproxy


class ReverseProxyException(Exception):
    """Base for revproxy exception"""


class InvalidUpstream(ReverseProxyException):
    """Invalid upstream set"""
