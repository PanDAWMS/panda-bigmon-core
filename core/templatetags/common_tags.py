"""
Common template tags that can be used across all apps templates

Created on 21.10.2019
@author Tatiana Korchuganova
"""

from django import template
from core.libs.cache import get_version

register = template.Library()


@register.simple_tag
def cache_bust(filename):
    version = get_version(filename)
    return version
