from django.db.models import Transform
from django.conf import settings


class CastDate(Transform):
    lookup_name = 'castdate'
    bilateral = True

    def as_sql(self, compiler, connection):
        sql, params = compiler.compile(self.lhs)
        if len(params) > 0:
            if settings.DEPLOYMENT != "POSTGRES":
                sql = 'CAST(%s AS DATE)' % sql
        return sql, params
