import os
import logging

from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack


# Initialize Django ASGI application early to ensure the AppRegistry
# is populated before importing code that may import ORM models.

_logger = logging.getLogger('bigpandamon')

DEPLOYMENT = os.environ.get('BIGMON_DEPLOYMENT', None)

import django
django.setup()

from django.core.asgi import get_asgi_application

application = ProtocolTypeRouter({
  'http': get_asgi_application(),
  'websocket': AuthMiddlewareStack(URLRouter())
})