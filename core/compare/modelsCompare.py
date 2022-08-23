
from django.db import models
from django.conf import settings


class ObjectsComparison(models.Model):
    id = models.IntegerField(db_column='id', null=False, primary_key=True )
    userid = models.IntegerField(db_column='userid', null=False )
    object = models.CharField(db_column='object', max_length=20)
    comparisonlist = models.CharField(db_column='comparison_list', max_length=2000)

    class Meta:
        db_table = f'"{settings.DB_SCHEMA}"."OBJECTS_COMPARISON"'
        app_label = 'pandamon'
