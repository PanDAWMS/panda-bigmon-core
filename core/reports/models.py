

from django.db import models
from core.settings.config import DB_SCHEMA


class ReportEmails(models.Model):
    id = models.DecimalField(decimal_places=0, max_digits=12, db_column='ID', primary_key=True)
    report = models.CharField(max_length=150, db_column='REPORT', null=False)
    type = models.CharField(max_length=150, db_column='TYPE', null=False)
    email = models.CharField(max_length=256, db_column='EMAIL', null=False)

    class Meta:
        db_table = f'"{DB_SCHEMA}"."REPORT_EMAIL"'
        