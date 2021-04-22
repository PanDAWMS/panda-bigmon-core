# -*- coding: utf-8 -*-
from social_core.exceptions import AuthFailed, SocialAuthBaseException
from django.contrib import messages
from social_core.utils import social_logger
from social_django.middleware import SocialAuthExceptionMiddleware
from social_core import exceptions as social_exceptions
from django.contrib.messages.api import MessageFailure
from django.shortcuts import redirect
from django.utils.http import urlquote

class CustomSocialAuthExceptionMiddleware(SocialAuthExceptionMiddleware):

    def get_message(self, request, exception):

        # typeEvent = 'socialexception'
        # infEvent = 'socialexception:'
        # # infEvent = 'socialexception:' + request._messages.level
        # logLevel = 'ERROR'
        path = 'Internal Server Error: ' + request.path+'\n'
        msg = path
        if 'USER' in request.META:
            user = 'USER:' + request.META['USER'] + '\n'
            msg += user
        if 'REMOTE_ADDR' in request.META:
            remoteaddr = 'REMOTE_ADDR:' + request.META['REMOTE_ADDR'] + '\n'
            msg += remoteaddr
        if 'HTTP_REFERER' in request.META:
            refferer = 'HTTP_REFERER:' + request.META['HTTP_REFERER'] + '\n'
            msg += refferer
        else:
            refferer = 'HTTP_REFERER:' + '-' + '\n'
            msg += refferer
        if 'HTTP_USER_AGENT' in request.META:
            useragent = 'HTTP_USER_AGENT:' + request.META['HTTP_USER_AGENT'] + '\n'
            msg +=useragent
        msg += 'EXCEPTION:' + str(exception) + '\n\n'
        msg += 'Backend information' + '\n'

        ACCESS_TOKEN_METHOD = 'ACCESS_TOKEN_METHOD:' + request.backend.ACCESS_TOKEN_METHOD + '\n'
        ACCESS_TOKEN_URL = 'ACCESS_TOKEN_URL:' + request.backend.ACCESS_TOKEN_URL + '\n'
        msg += ACCESS_TOKEN_METHOD +  ACCESS_TOKEN_URL

        return msg

    def get_redirect_uri(self,request, exception):
        strategy = getattr(request, 'social_strategy', None)
        return strategy.setting('LOGIN_ERROR_URL')