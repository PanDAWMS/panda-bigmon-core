from django.db.models import Transform
from django.db.models import Lookup


class CastDate(Transform):
    lookup_name = 'castdate'
    bilateral = True

    def as_sql(self, compiler, connection):
        sql, params = compiler.compile(self.lhs)
        if len(params) > 0:
            sql = 'CAST(%s AS DATE)' % sql
        return sql, params
