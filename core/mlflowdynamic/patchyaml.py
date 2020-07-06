

def patchConfig(inputObj, instance):
    if isinstance(inputObj, dict):
        inputObj.update((k, patchConfig(v, instance)) for k, v in inputObj.items())
        return inputObj
    elif isinstance(inputObj, list):
        for i in range(len(inputObj)):
            inputObj[i] = patchConfig(inputObj[i], instance)
            return inputObj
    if isinstance(inputObj, str):
        return inputObj.replace('randomstring', instance)



