import decimal
import json
import numpy as np

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

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)