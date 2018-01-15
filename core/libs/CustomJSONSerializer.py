import json


from django.core.serializers.json import DjangoJSONEncoder
try:
    from django.utils.six.moves import cPickle as pickle
except ImportError:
    import pickle

class CustomJSONSerializer(object):
    def dumps(self, obj):
        try:
            return json.dumps(obj,cls=DjangoJSONEncoder).encode('latin-1')
        except:
            return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
    def loads(self, data):
        try:
            return json.loads(data.decode('latin-1'))
        except:
            return pickle.loads(data)