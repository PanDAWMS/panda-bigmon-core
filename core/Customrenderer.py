## This is a custom renderer for views of bigpanda where standard processing is too slow even leads to 500 error

from django import template
from django.template import Template
from django.core.urlresolvers import reverse


def world_nucleussummary(context, kwargs):
    nucleus = kwargs['nucleus']
    nuclval = kwargs['nuclval']
    statelist = kwargs['statelist']
    estailtojobslinks = kwargs['estailtojobslinks']
    hours = kwargs['hours']
    retStr = """"""

    if nucleus is not None:
        retStr = """
            <tr height=10 colspan=12></tr>
            <tr>
                <td><a href="#nucleus_{0}">{1}</a></td>""".format(nucleus,nucleus)
        for jobstatus in statelist:
            retStr +="""<td  """
            if nuclval.get(jobstatus) > 0:
                retStr += """class ='{0}_fill'""".format(jobstatus)
            retStr +="""> <a href="{0}?jobstatus={1}{2}&nucleus={3}&cloud=WORLD&noarchjobs=1&hours={4}&display_limit=100">{5}</a></td>""".format(reverse('jobList'),jobstatus, estailtojobslinks, nucleus, hours, nuclval.get(jobstatus))
        if ('eventsfailed' in nuclval and len(estailtojobslinks) > 0):
            retStr += """<td>"""+str(nuclval.get('eventsfailed'))+"""</td>"""
            retStr += """<td>"""+str(nuclval.get('eventsfinished'))+"""</td>"""
            retStr += """<td>"""+str(nuclval.get('eventsmerging'))+"""</td>"""

        retStr += """</tr>"""

    return Template(retStr).render(context)

def world_computingsitesummary(context, kwargs):
    nucleus = kwargs['nucleus']
    nuclval = kwargs['nuclval']
    statelist = kwargs['statelist']
    estailtojobslinks = kwargs['estailtojobslinks']
    hours = kwargs['hours']
    retStr = """"""

    for computingsite, computingsiteval in nuclval.iteritems():
        retStr += """<tr><td>{0}</td>""".format(computingsite)
        for jobstatus in statelist:
            retStr += """<td """
            if computingsiteval.get(jobstatus) > 0:
                retStr += """class ='{0}_fill'""".format(jobstatus)
            retStr += """> <a href="{0}?jobstatus={1}{2}&nucleus={3}&computingsite={6}&cloud=WORLD&noarchjobs=1&hours={4}&display_limit=100">{5}</a></td>""".format(reverse('jobList'),jobstatus, estailtojobslinks, nucleus, hours, computingsiteval.get(jobstatus), computingsite)

        if ('eventsfailed' in computingsiteval and len(estailtojobslinks) > 0):
            retStr += """<td>"""+str(computingsiteval.get('eventsfailed'))+"""</td>"""
            retStr += """<td>"""+str(computingsiteval.get('eventsfinished'))+"""</td>"""
            retStr += """<td>"""+str(computingsiteval.get('eventsmerging'))+"""</td>"""

        retStr += """</tr>"""

    return Template(retStr).render(context)

