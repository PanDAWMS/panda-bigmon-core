"""

"""

from django.contrib.auth.models import AbstractUser
from django.db import models
from core.settings.config import DB_SCHEMA

class BPUser(AbstractUser):
    is_tester = models.NullBooleanField(db_column='IS_TESTER', null=True, blank=False)
    last_login = models.DateTimeField(db_column='LAST_LOGIN', auto_now_add=True, blank=False)

    class Meta:
        db_table = f'"{DB_SCHEMA}"."AUTH_USER"'


class BPUserSettings(models.Model):
    userid = models.IntegerField(db_column='USERID', null=False)
    page = models.CharField(db_column='PAGE', max_length=100, null=False)
    preferences = models.CharField(db_column='PREFERENCES', max_length=4000)

    class Meta:
        db_table = f'"{DB_SCHEMA}"."USER_SETTINGS"'


class Visits(models.Model):
    visitId = models.BigIntegerField(primary_key=True, db_column='VISITID')
    url = models.CharField(null=True, db_column='URL', max_length=1000)
    time = models.DateTimeField(db_column='TIME', null=False)
    remote = models.CharField(null=True, db_column='REMOTE', max_length=20)
    userid = models.IntegerField(null=True, db_column='USERID', blank=True)
    service = models.IntegerField(null=True, db_column='SERVICE', blank=True)

    class Meta:
        db_table= f'"{DB_SCHEMA}"."VISITS"'