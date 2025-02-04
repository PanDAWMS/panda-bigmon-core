"""

"""

from django.contrib.auth.models import AbstractUser, Group
from django.db import models
from django.conf import settings



class BPUser(AbstractUser):
    is_tester = models.BooleanField(db_column='is_tester', null=True, blank=False)
    last_login = models.DateTimeField(db_column='last_login', auto_now_add=True, blank=False)
    is_expert = models.BooleanField(db_column='is_expert', null=True, blank=False)

    groups = models.ManyToManyField(
        Group,
        through="UserGroups",  # Custom through model
        related_name="bpuser_groups",
    )

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."auth_user"'


class UserGroups(models.Model):
    user = models.ForeignKey(BPUser, on_delete=models.DO_NOTHING, db_column="user_id")
    group = models.ForeignKey(Group, on_delete=models.DO_NOTHING, db_column="group_id")

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."auth_user_groups"'
        unique_together = ("user", "group")


class BPUserSettings(models.Model):
    userid = models.IntegerField(db_column='userid', null=False, primary_key=True)
    page = models.CharField(db_column='page', max_length=100, null=False)
    preferences = models.CharField(db_column='preferences', max_length=4000)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."user_settings"'


class Visits(models.Model):
    visitId = models.BigIntegerField(primary_key=True, db_column='visitid')
    url = models.CharField(null=True, db_column='url', max_length=1000)
    time = models.DateTimeField(db_column='time', null=False)
    remote = models.CharField(null=True, db_column='remote', max_length=20)
    userid = models.IntegerField(null=True, db_column='userid', blank=True)
    service = models.IntegerField(null=True, db_column='service', blank=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."visits"'
