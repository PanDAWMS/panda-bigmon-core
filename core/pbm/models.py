"""
    pbm.models
    
"""
from django.db import models

class DailyLog(models.Model):
    dailylogid = models.BigIntegerField(null=False, db_column='DAILYLOGID', blank=True)
    logdate = models.DateField(null=False, db_column='LOGDATE', blank=True)
    category = models.CharField(max_length=3, db_column='CATEGORY', blank=True, null=False)
    site = models.CharField(max_length=300, db_column='SITE', blank=True, null=True)
    cloud = models.CharField(max_length=300, db_column='CLOUD', blank=True, null=True)
    dnuser = models.CharField(max_length=300, db_column='DNUSER', blank=True, null=True)
    jobdefcount = models.BigIntegerField(db_column='JOBDEFCOUNT')
    jobcount = models.BigIntegerField(db_column='JOBCOUNT')
    country = models.CharField(max_length=300, db_column='COUNTRY', blank=True, null=True)
    jobset = models.CharField(max_length=300, db_column='JOBSET', blank=True, null=True)

    class Meta:
        app_label = 'pbm'
        db_table = u'dailylogv3'


