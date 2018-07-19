from __future__ import unicode_literals

from ..pandajob.columns_config import COLUMNS, ORDER_COLUMNS, COL_TITLES, FILTERS

import json
from django.core.exceptions import ObjectDoesNotExist
from django.db import connection
from django.db import connections
from django.utils import timezone
from django.contrib.auth.models import AbstractUser
#from rest_framework.authtoken.models import Token
#import uuid
#from hashlib import sha1
#import hmac


from django.db import models
models.options.DEFAULT_NAMES += ('allColumns', 'orderColumns',
                                 'primaryColumns', 'secondaryColumns',
                                 'columnTitles', 'filterFields',)

class ObjectsComparison(models.Model):
    id = models.IntegerField(db_column='ID', null=False, primary_key=True )
    userid = models.IntegerField(db_column='USERID', null=False )
    object = models.CharField(db_column='OBJECT', max_length=20)
    comparisonlist = models.CharField(db_column='COMPARISON_LIST', max_length=2000)
    class Meta:
        db_table = u'"ATLAS_PANDABIGMON"."OBJECTS_COMPARISON"'