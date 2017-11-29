import json
def deleteTestData(request,data):
### Filtering data
    if request.user.is_authenticated() and request.user.is_tester:
        return data
    else:
        if data is not None:
            data = json.loads(data)
            for key in data.keys():
                if '_test' in key:
                    del data[key]
            data = json.dumps(data)
    return data