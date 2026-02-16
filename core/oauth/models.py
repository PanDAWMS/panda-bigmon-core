"""

"""
import binascii
import os
from django.contrib.auth.models import AbstractUser, Group, Permission
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

    user_permissions = models.ManyToManyField(
        Permission,
        through="UserPermissions",  # Custom through model
        related_name="bpuser_permissions",
    )

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."auth_user"'


class UserGroups(models.Model):
    user = models.ForeignKey(BPUser, on_delete=models.DO_NOTHING, db_column="user_id")
    group = models.ForeignKey(Group, on_delete=models.DO_NOTHING, db_column="group_id")

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."auth_user_groups"'
        unique_together = ("user", "group")


class UserPermissions(models.Model):
    user = models.ForeignKey(BPUser, on_delete=models.DO_NOTHING, db_column="user_id")
    permission = models.ForeignKey(Permission, on_delete=models.DO_NOTHING, db_column="permission_id")

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."auth_user_user_permissions"'
        unique_together = ("user", "permission")


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


class BPToken(models.Model):
    """
    The default authorization token model.
    """
    key = models.CharField(max_length=40, primary_key=True)
    user = models.OneToOneField(BPUser, related_name='bp_auth_token', on_delete=models.DO_NOTHING)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."authtoken_token"'

    def save(self, *args, **kwargs):
        if not self.key:
            self.key = self.generate_key()
        return super().save(*args, **kwargs)

    @classmethod
    def generate_key(cls):
        return binascii.hexlify(os.urandom(20)).decode()

    def __str__(self):
        return self.key

