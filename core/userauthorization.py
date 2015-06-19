
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.template import RequestContext
from core.pandajob.models import MonitorUsers
from django.core.cache import cache
from django.db.models import Q
import logging
import subprocess
import datetime

userToRunVoms = 'atlpan'

class processAuth(object):
    
    def process_request(self, request):
        
        data = {'debug':'no'}

        if 'SSL_CLIENT_S_DN' in request.META or 'HTTP_X_SSL_CLIENT_S_DN' in request.META:
            if 'SSL_CLIENT_S_DN' in request.META:
               userdn = request.META['SSL_CLIENT_S_DN']
            else:
               userdn = request.META['HTTP_X_SSL_CLIENT_S_DN']
            proc = subprocess.Popen(['/usr/bin/openssl', 'x509', '-email', '-noout'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            certificate_email, stderr = proc.communicate(input=request.META['SSL_CLIENT_CERT'])
            if ( (len(userdn) > 5) and (len(certificate_email) > 5)):
                userrec = MonitorUsers.objects.filter( Q(dname__startswith=userdn) | Q(email=certificate_email.lower()), isactive=1).values()
            else:
                render_to_response('errorAuth.html', data, RequestContext(request))
            if len(userrec) > 0:
                return None
            else:
                theListOfVMUsers = cache.get('voms-users-list')
                if (theListOfVMUsers is None) or (len(theListOfVMUsers) == 0):
                    proc = subprocess.Popen('sudo -u atlpan /usr/bin/voms-admin --host lcg-voms2.cern.ch --vo atlas list-users', shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    theListOfVMUsers, stderr = proc.communicate()
                    if len(theListOfVMUsers) < 20 :
                        logging.error('Error of getting list of users (voms-admin). stderr:' + theListOfVMUsers +" "+ stderr)
                        return render_to_response('errorAuth.html', data, RequestContext(request))
                    cache.set('voms-users-list', theListOfVMUsers, 1800)
                if ( (len(userdn) > 5) and (len(certificate_email) > 5)):
                    logging.error('authorization info: Started Compare, theListOfVMUsers.find(userdn):' + str(theListOfVMUsers.find(userdn)) + ' (theListOfVMUsers.find(certificate_email)' + str(theListOfVMUsers.find(certificate_email)))
                    if ((theListOfVMUsers.find(userdn) > 0) or (theListOfVMUsers.lower().find(certificate_email.lower()) > 0)):
                        newUser = MonitorUsers(dname=userdn, isactive=1, firstdate=datetime.datetime.utcnow().strftime("%Y-%m-%d"), email=certificate_email.lower())
                        newUser.save()
                        return None
                    else:
                        return render_to_response('errorAuth.html', data, RequestContext(request))
                return render_to_response('errorAuth.html', data, RequestContext(request))
#        else:
#            return render_to_response('errorAuth.html', RequestContext(request))
