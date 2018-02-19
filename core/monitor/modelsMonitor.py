from django.db import models

models.options.DEFAULT_NAMES += ('allColumns', 'orderColumns', \
                                 'primaryColumns', 'secondaryColumns', \
                                 'columnTitles', 'filterFields',)




class AtlasDBA (models.Model):
    t_stamp = models.TimeField(db_column='T_STAMP', primary_key=True)
    db_name = models.CharField(max_length=256, db_column='DB_NAME')
    node_id = models.IntegerField(db_column='NODE_ID', null=False)
    username = models.CharField(max_length=256, db_column='USERNAME')
    num_active_sess = models.IntegerField(db_column='NUM_ACTIVE_SESS', null=False)
    num_sess = models.IntegerField(db_column='NUM_SESS', null=False)
    osuser = models.CharField(max_length=256, db_column='OSUSER')
    machine = models.CharField(max_length=256, db_column='MACHINE')
    program = models.CharField(max_length=256, db_column='PROGRAM')
    class Meta:
        db_table = u'"ATLAS_DBA"."COUNT_PANDAMON_SESSIONS"'