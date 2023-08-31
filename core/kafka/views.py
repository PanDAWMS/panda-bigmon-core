
from django.conf import settings
from django.shortcuts import render
from core.oauth.utils import login_customrequired
from core.views import initRequest
from django.http import HttpResponse, JsonResponse

@login_customrequired
def testTerminal(request,):
    valid, response = initRequest(request)
    return render(request, 'testTerminal.html', context={'text':'Test terminal'})
@login_customrequired
def taskLivePage(request,):
    valid, response = initRequest(request)
    return render(request, 'taskLivePage.html', context={'text':'Test terminal'})