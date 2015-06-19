"""
    pbm.templatetags.pbm_extras
"""
import logging
from django import template
from django.template.loader import render_to_string
from django.conf import settings


##from core.common.utils import getPrefix, getContextVariables
#from ...common.utils import getPrefix, getContextVariables, getAoColumnsList

_logger = logging.getLogger('bigpandamon-pbm')


register = template.Library()


@register.simple_tag
def pbm_plot_pie(data, title='', divid='plot', startdate='', enddate='', \
        colors=[], plotid='', \
        template='pbm/templatetags/pbm_plot_pie.html', *args, **kwargs):
    """
        Template tag to plot data in a higcharts pie plot.
        
    """
    returnData = { \
        'data': data, \
        'title': title, \
        'divid': divid, \
        'startdate': startdate, \
        'enddate': enddate, \
        'colors': colors, \
        'plotid': plotid, \
        'STATIC_URL': settings.STATIC_URL, \
    }
    return render_to_string(template, returnData)

@register.simple_tag
def pbm_table_pie(data, title='', divid='plot', startdate='', enddate='', \
        colors=[], plotid='', \
        template='pbm/templatetags/pbm_table_pie.html', *args, **kwargs):
    """
        Template tag to show tabular data of a plot.
        
    """
    returnData = { \
        'data': data, \
        'title': title, \
        'divid': divid, \
        'startdate': startdate, \
        'enddate': enddate, \
        'colors': colors, \
        'plotid': plotid, \
        'STATIC_URL': settings.STATIC_URL, \
    }
    return render_to_string(template, returnData)

