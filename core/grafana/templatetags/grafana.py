from urllib.parse import urlencode
from django import template

register = template.Library()

@register.simple_tag
def grafana_img(url):
    return "/grafana/img/?" + urlencode({"url": url})