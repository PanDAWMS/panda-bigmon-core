from importlib import import_module

import time
from django.conf import settings
from django.contrib.sessions.backends.base import UpdateError
from django.contrib.sessions.middleware import SessionMiddleware
from django.core.exceptions import SuspiciousOperation
from django.shortcuts import redirect
from django.utils.cache import patch_vary_headers
from django.utils.http import cookie_date


class CustomSessionMiddleware(SessionMiddleware):
    # def process_request(self, request):
    #     engine = import_module(settings.SESSION_ENGINE)
    #     session_key = request.COOKIES.get(settings.SESSION_COOKIE_NAME, None)
    #     request.session = engine.SessionStore(session_key)
    #     if not request.session.exists(request.session.session_key):
    #         request.session.create()

    def process_response(self, request, response):
        """
        If request.session was modified, or if the configuration is to save the
        session every time, save the changes and set a session cookie or delete
        the session cookie if the session has been emptied.
        """
        if ('statpixel' in request.META['PATH_INFO']):
            return response

        try:
            accessed = request.session.accessed
            modified = request.session.modified
            empty = request.session.is_empty()
        except AttributeError:
            pass
        else:
            # First check if we need to delete this cookie.
            # The session should be deleted only if the session is entirely empty
            if settings.SESSION_COOKIE_NAME in request.COOKIES and empty:
                response.delete_cookie(
                    settings.SESSION_COOKIE_NAME,
                    path=settings.SESSION_COOKIE_PATH,
                    domain=settings.SESSION_COOKIE_DOMAIN,
                )
            else:
                if accessed:
                    patch_vary_headers(response, ('Cookie',))
                if (modified or settings.SESSION_SAVE_EVERY_REQUEST) and not empty:
                    if request.session.get_expire_at_browser_close():
                        max_age = None
                        expires = None
                    else:
                        max_age = request.session.get_expiry_age()
                        expires_time = time.time() + max_age
                        expires = cookie_date(expires_time)
                    # Save the session data and refresh the client cookie.
                    # Skip session save for 500 responses, refs #3881.
                    if response.status_code != 500:
                        try:
                            # if self.session_key is None:
                                # return self.create()
                            # if must_create:
                            #     func = self._cache.add
                            if request.session._cache.get(request.session.cache_key) is None and request.session.session_key is not None:
                                result = request.session._cache.set(request.session.cache_key,
                                        request.session._get_session(no_load=True),
                                        request.session.get_expiry_age())
                            request.session.save()
                        except UpdateError:
                            # The user is now logged out; redirecting to same
                            # page will result in a redirect to the login page
                            # if required.
                            #return redirect(request.path)
                            raise SuspiciousOperation(
                                "The request's session was deleted before the "
                                "request completed. The user may have logged "
                                "out in a concurrent request, for example."
                            )
                        response.set_cookie(
                            settings.SESSION_COOKIE_NAME,
                            request.session.session_key, max_age=max_age,
                            expires=expires, domain=settings.SESSION_COOKIE_DOMAIN,
                            path=settings.SESSION_COOKIE_PATH,
                            secure=settings.SESSION_COOKIE_SECURE or None,
                            httponly=settings.SESSION_COOKIE_HTTPONLY or None,
                        )
        return response
