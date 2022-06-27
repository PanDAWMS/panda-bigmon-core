"""

"""

from django.contrib.auth.models import AbstractUser
from django.db import models
from core.settings.config import DB_SCHEMA


class BPUser(AbstractUser):
    is_tester = models.NullBooleanField(db_column='is_tester', null=True, blank=False)
    last_login = models.DateTimeField(db_column='last_login', auto_now_add=True, blank=False)
    class Meta:
        db_table = f'"{DB_SCHEMA}"."auth_user"'


class BPUserSettings(models.Model):
    userid = models.IntegerField(db_column='userid', null=False)
    page = models.CharField(db_column='page', max_length=100, null=False)
    preferences = models.CharField(db_column='preferences', max_length=4000)

    class Meta:
        db_table = f'"{DB_SCHEMA}"."user_settings"'


class Visits(models.Model):
    visitId = models.BigIntegerField(primary_key=True, db_column='visitid')
    url = models.CharField(null=True, db_column='url', max_length=1000)
    time = models.DateTimeField(db_column='time', null=False)
    remote = models.CharField(null=True, db_column='remote', max_length=20)
    userid = models.IntegerField(null=True, db_column='userid', blank=True)
    service = models.IntegerField(null=True, db_column='service', blank=True)

    class Meta:
        db_table = f'"{DB_SCHEMA}"."visits"'
