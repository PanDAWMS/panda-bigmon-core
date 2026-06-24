"""
ART related models
"""

from __future__ import unicode_literals
from django.db import models
from django.conf import settings


class ARTTests(models.Model):
    # required fields
    pandaid = models.DecimalField(decimal_places=0, max_digits=12, db_column='pandaid', primary_key=True)
    test_type = models.CharField(max_length=10, db_column='test_type')
    testname = models.CharField(max_length=300, db_column='test_name')
    nightly_release_short = models.CharField(max_length=24, db_column='nightly_release_short')
    project = models.CharField(max_length=256, db_column='project')
    platform = models.CharField(max_length=150, db_column='platform')
    branch = models.CharField(max_length=256, db_column='branch')
    package = models.CharField(max_length=32, db_column='package')
    nightly_tag = models.CharField(max_length=32, db_column='nightly_tag')
    nightly_tag_display = models.CharField(max_length=32, db_column='nightly_tag_display')
    nightly_tag_date = models.DateTimeField(null=True, db_column='nightly_tag_date')
    extrainfo = models.CharField(max_length=1000, db_column='extra_info', null=True)
    created = models.DateTimeField(null=True, db_column='created')
    status = models.DecimalField(decimal_places=0, max_digits=3, db_column='status')
    # grid test extra info
    jeditaskid = models.DecimalField(decimal_places=0, max_digits=12, db_column='jeditaskid', null=True)
    computingsite = models.CharField(max_length=128, db_column='computingsite', null=True)
    inputfileid = models.DecimalField(decimal_places=0, max_digits=12, db_column='inputfileid', null=True)
    gitlabid = models.DecimalField(decimal_places=0, max_digits=12, db_column='gitlabid', null=True)
    tarindex = models.DecimalField(decimal_places=0, max_digits=3, db_column='tarindex', null=True)
    attemptnr = models.DecimalField(decimal_places=0, max_digits=3, db_column='attemptnr', null=True)
    maxattempt = models.DecimalField(decimal_places=0, max_digits=3, db_column='maxattempt', null=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."art_tests"'


class ARTSubResult(models.Model):
    pandaid = models.OneToOneField('ARTTests',
        related_name='artsubresult', on_delete=models.CASCADE, db_column='pandaid', primary_key=True)
    subresult = models.CharField(max_length=4000, db_column='subresult_json', null=True)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."art_subresult"'


class ARTResultsQueue(models.Model):
    row_id = models.BigIntegerField(db_column='row_id', primary_key=True)
    pandaid = models.BigIntegerField(db_column='pandaid')
    is_locked = models.IntegerField(db_column='is_locked')
    lock_time = models.DateTimeField(null=True, db_column='lock_time', blank=True)
    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."art_results_queue"'

