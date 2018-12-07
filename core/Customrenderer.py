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


def region_sitesummary(context, kwargs):
    errorSummary = kwargs['errorSummary']
    estailtojobslinks = kwargs['estailtojobslinks']
    cloudname = kwargs['cloudname']
    cloudview = kwargs['cloudview']
    requestParams = kwargs['requestParams']
    hours = kwargs['hours']
    view = kwargs['view']
    site = kwargs['site']
    joblisturl = kwargs['joblisturl']
    errthreshold = kwargs['errthreshold']

    if site.get("name", "") == 'Australia-ATLAS':
        print 'Australia-ATLAS'


    retStr = """"""
    retStr += "<td><span class=\""+site.get("status", "")+"\">"+site.get("status", "")+"</span> </td>"
    retStr += "<td align='right'>"
    if site.get("parent", False):
         retStr +="""<a href="{0}?computingsite={1}{2}""".format(joblisturl, site['parent'], estailtojobslinks)
         if cloudview != 'region' and view !='analysis':
             retStr += """&cloud={0}""".format(cloudname)
         if requestParams.get('workinggroup', False):
             retStr += """&workinggroup={0}""".format(requestParams['workinggroup'])
         if requestParams.get('processingtype', False):
             retStr += """&processingtype={0}""".format(requestParams['processingtype'])
         if requestParams.get('tasktype', False):
             retStr += """&tasktype={0}""".format(requestParams['tasktype'])
         if requestParams.get('project', False):
             retStr += """&project={0}""".format(requestParams['project'])
         retStr += """&hours={0}""".format(hours)

         if cloudname == '':
             retStr += """&&mismatchedcloudsite=true"""

         retStr += """&resourcetype={0}&display_limit=100">{1}""".format(site.get('resource', 'resource'), site['count'])
    else:
        retStr +="""<a href="{0}?""".format(joblisturl)
        if requestParams.get('workinggroup', False):
            retStr += """&workinggroup={0}""".format(requestParams['workinggroup'])
        if requestParams.get('processingtype', False):
            retStr += """&processingtype={0}""".format(requestParams['processingtype'])
        if requestParams.get('tasktype', False):
            retStr += """&tasktype={0}""".format(requestParams['tasktype'])
        retStr += """&jobtype={0}""".format(view)
        if requestParams.get('project', False):
            retStr += """&project={0}""".format(requestParams['project'])
        retStr += """&computingsite={0}""".format(site['name'])
        if cloudname == '':
            retStr += """&mismatchedcloudsite=true"""
        retStr += """&hours={0}&display_limit=100{1}">{2}</a>""".format(hours, estailtojobslinks, site['count'])

    retStr += "</td>"

    if cloudview == 'region':
        retStr += "<td>"
        if site.get("parent", False):
            retStr += "-"
        else:
            retStr += "{0}({1})".format(site.get("pilots", ''),site.get("nojobabs", '') )
        retStr += "</td>"

    for state in site['summary']:
        if site.get("parent", False):
            retStr += """<td class='{0}""".format(state['name'])
            if state.get('count', 0) > 0:
                retStr += """_fill"""
            retStr += """' align='right'><a href="{0}?computingsite={1}{2}""".format(joblisturl, site['parent'], estailtojobslinks)
            if cloudview != 'region' and view != 'analysis':
                retStr += """&cloud={0}""".format(cloudname)
            if requestParams.get('workinggroup', False):
                retStr += """&workinggroup={0}""".format(requestParams['workinggroup'])
            if requestParams.get('processingtype', False):
                retStr += """&processingtype={0}""".format(requestParams['processingtype'])
            if requestParams.get('tasktype', False):
                retStr += """&tasktype={0}""".format(requestParams['tasktype'])
            retStr += """&jobtype={0}""".format(view)
            if requestParams.get('project', False):
                retStr += """&project={0}""".format(requestParams['project'])
            retStr += """&jobstatus={0}""".format(state['name'])
            retStr += """&hours={0}&display_limit=100""".format(hours)
            if cloudname == '':
                retStr += """&mismatchedcloudsite=true"""
            if state.get('corecount', 0) > 0:
                retStr += """&corecount={0}""".format(state['corecount'])
            if site.get('resource', False):
                retStr += """&resourcetype={0}""".format(site['resource'])
            retStr += """\"> <span class=\"{0}""".format(state['name'])
            if state.get('count', 0) > 0:
                retStr += """_fill"""
            retStr += """\">{0}</span></a></td>""".format(state.get('count', ''))

        else:
            retStr += """<td class='{0}""".format(state['name'])
            if state.get('count', 0) > 0:
                retStr += """_fill"""
            retStr += """' align='right'><a href="{0}?computingsite={1}{2}""".format(joblisturl, site.get('name', ''), estailtojobslinks)
            if cloudview != 'region' and view != 'analysis':
                retStr += """&cloud={0}""".format(cloudname)
            if requestParams.get('workinggroup', False):
                retStr += """&workinggroup={0}""".format(requestParams['workinggroup'])
            if requestParams.get('processingtype', False):
                retStr += """&processingtype={0}""".format(requestParams['processingtype'])
            if requestParams.get('tasktype', False):
                retStr += """&tasktype={0}""".format(requestParams['tasktype'])
            retStr += """&jobtype={0}""".format(view)
            if requestParams.get('project', False):
                retStr += """&project={0}""".format(requestParams['project'])
            retStr += """&jobstatus={0}""".format(state['name'])
            retStr += """&hours={0}&display_limit=100""".format(hours)
            if cloudname == '':
                retStr += """&mismatchedcloudsite=true"""
            retStr += """\"> <span class=\"{0}""".format(state['name'])
            if state.get('count', 0) > 0:
                retStr += """_fill"""
            retStr += """\">{0}</span></a></td>""".format(state.get('count', ''))


    retStr += """<td> <a href="{0}?jobtype={1}{2}&sortby=count&computingsite={3}&hours={4}">""".format(errorSummary,view, estailtojobslinks, site['name'], hours )
    if site.get('pctfail', 0) > errthreshold:
        retStr += """ <font color=red>{0}</font>""".format(site.get('pctfail', 0))
    else:
        retStr += """{0}""".format(site.get('pctfail', 0))
    retStr += """ </a> </td></tr>"""

    return Template(retStr).render(context)






