"""
Data Carousel models for tables in DEFT DB
Created by Tatiana Korchuganova
"""

from django.db import models

DB_SCHEMA_DEFT = 'ATLAS_DEFT'

class DeftDatasetStaging(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='dataset_staging_id', primary_key=True)
    dataset = models.CharField(max_length=255, db_column='dataset', null=True)
    start_time = models.DateTimeField(db_column='start_time')
    end_time = models.DateTimeField(db_column='end_time')
    ruleid = models.CharField(max_length=100, db_column='rse')
    total_files = models.DecimalField(decimal_places=0, max_digits=12, db_column='total_files')
    staged_files = models.DecimalField(decimal_places=0, max_digits=12, db_column='staged_files')
    status = models.CharField(max_length=20, db_column='status', null=True)
    source = models.CharField(max_length=200, db_column='source_rse', null=True)
    update_time = models.DateTimeField(db_column='update_time')
    dataset_size = models.DecimalField(decimal_places=0, max_digits=20, db_column='dataset_bytes', null=True)
    staged_size = models.DecimalField(decimal_places=0, max_digits=20, db_column='staged_bytes', null=True)
    source_expression = models.CharField(max_length=400, db_column='source_expression', null=True)

    class Meta:
        app_label = 'deft'
        db_table = f'"{DB_SCHEMA_DEFT}"."t_dataset_staging"'


class DeftActionStaging(models.Model):

    id = models.BigIntegerField(db_column='ACT_ST_ID', primary_key=True)
    step_action = models.BigIntegerField(db_column='step_action_id')
    dataset_stage = models.BigIntegerField(db_column='dataset_staging_id')
    taskid = models.BigIntegerField(db_column='TASKID')
    share_name = models.CharField(max_length=100, db_column='SHARE_NAME')

    class Meta:
        app_label = 'deft'
        db_table = f'"{DB_SCHEMA_DEFT}"."t_action_staging"'
