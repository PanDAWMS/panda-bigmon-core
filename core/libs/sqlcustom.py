"""
Everything for raw queries to DB which is not supported by Django ORM
"""

from django.utils import encoding


def fix_lob(cur):
    fixRowsList = []
    for row in cur:
        newRow = []
        for col in row:
            if type(col).__name__ == 'LOB':
                newRow.append(str(col))
            else:
                newRow.append(col)
        fixRowsList.append(tuple(newRow))
    return fixRowsList


def escape_input(str_to_escape):
    """Replace reserved symbols in str for LIKE queries"""
    chars_to_escape = '$%^&()[]{};<>?\\`~+%\'\\"'
    chars_replacement = '_' * len(chars_to_escape)
    tbl = str.maketrans(chars_to_escape, chars_replacement)
    str_to_escape = encoding.smart_str(str_to_escape, encoding='ascii', errors='ignore')
    str_to_escape = str_to_escape.translate(tbl)

    return str_to_escape


def preprocess_wild_card_string(strToProcess, fieldToLookAt, **kwargs):
    if len(strToProcess) == 0:
        return '(1=1)'

    # we wrap field values by UPPER() for case insensitiveness but it leads to NOT using the DB indexes
    # this flag intends removing UPPER() in cases where the query uses just one field and we want to avoid FULL scan of tables
    prefix = 'UPPER('
    postfix = ')'
    if 'case_sensitivity' in kwargs and kwargs['case_sensitivity'] is True:
        prefix = ''
        postfix = ''

    isNot = False
    if strToProcess.startswith('!'):
        isNot = True
        strToProcess = strToProcess[1:]

    cardParametersRaw = strToProcess.split('*')
    cardRealParameters = [s for s in cardParametersRaw if len(s) >= 1]
    countRealParameters = len(cardRealParameters)
    countParameters = len(cardParametersRaw)

    if countParameters == 0:
        return '(1=1)'
    currentRealParCount = 0
    currentParCount = 0
    extraQueryString = '('

    for parameter in cardParametersRaw:
        leadStar = False
        trailStar = False
        if len(parameter) > 0:

            if currentParCount - 1 >= 0:
                leadStar = True

            if currentParCount + 1 < countParameters:
                trailStar = True

            if fieldToLookAt.lower() == 'produserid':
                leadStar = True
                trailStar = True

            if fieldToLookAt.lower() == 'resourcetype':
                fieldToLookAt = 'resource_type'

            isEscape = False
            if '_' in parameter and fieldToLookAt.lower() != 'nucleus':
                parameter = parameter.replace('_', '!_')
                isEscape = True

            extraQueryString += "({}{}{}".format(prefix, fieldToLookAt, postfix)
            if isNot:
                extraQueryString += "NOT "
            if leadStar and trailStar:
                extraQueryString += " LIKE {}'%%{}%%'{}".format(prefix, parameter, postfix)
            elif not leadStar and not trailStar:
                extraQueryString += " LIKE {}'{}'{}".format(prefix, parameter, postfix)
            elif leadStar and not trailStar:
                extraQueryString += " LIKE {}'%%{}'{}".format(prefix, parameter, postfix)
            elif not leadStar and trailStar:
                extraQueryString += " LIKE {}'{}%%'{}".format(prefix, parameter, postfix)
            if isEscape:
                extraQueryString += " ESCAPE '!'"
            extraQueryString += ")"
            currentRealParCount += 1
            if currentRealParCount < countRealParameters:
                extraQueryString += ' AND '
        currentParCount += 1

    extraQueryString += ')'
    extraQueryString = extraQueryString.replace("%20", " ") if not '%%20' in extraQueryString else extraQueryString

    return extraQueryString


def filter_dict_by_wildcards(data_dict, param, value):
    """
    Filtering dict by supported in the system wildcards: '*' '|', '!', ','
    :param data_dict: dict with val as a another dict
    :param param: str
    :param value: str value with wildcards
    :return: dict
    """

    # firstly divide str by OR condition
    if ',' in value or '|' in value:
        value = value.replace('|', ',')
        values_or = value.split(',')
    else:
        values_or = [value]

    # go through each of OR and filter the full copy of input data dict separately
    data_dicts_or = []
    for val_or in values_or:
        data_dict_tmp = {k: v[param] for k,v in data_dict.items()}
        is_negative = False
        if val_or.startswith('!'):
            is_negative = True
            val_or = val_or[1:]
        if '*' in val_or:
            sub_vals = val_or.split('*')
            if not val_or.startswith('*'):
                if is_negative:
                    data_dict_tmp = {k: v for k, v in data_dict_tmp.items() if not v.startswith(sub_vals[0])}
                else:
                    data_dict_tmp = {k: v for k,v in data_dict_tmp.items() if v.startswith(sub_vals[0])}
            if not val_or.endswith('*'):
                if is_negative:
                    data_dict_tmp = {k: v for k,v in data_dict_tmp.items() if not v.endswith(sub_vals[-1])}
                else:
                    data_dict_tmp = {k: v for k,v in data_dict_tmp.items() if v.endswith(sub_vals[-1])}
            if len(sub_vals) > 2:
                for val_and in sub_vals[1:-1]:
                    if is_negative:
                        data_dict_tmp = {k: v for k, v in data_dict_tmp.items() if val_and not in v}
                    else:
                        data_dict_tmp = {k: v for k, v in data_dict_tmp.items() if val_and in v}
        else:
            if is_negative:
                data_dict_tmp = {k: v for k, v in data_dict_tmp.items() if val_or == v}
            else:
                data_dict_tmp = {k: v for k, v in data_dict_tmp.items() if val_or == v}

        data_dicts_or.append(data_dict_tmp)

    # merge OR results of filtering
    keys_filtered = []
    for ddo in data_dicts_or:
        keys_filtered.extend(ddo.keys())
    keys_filtered = list(set(keys_filtered))

    # collecting final result of filtering
    data_dict_filtered = {k:v for k,v in data_dict.items() if k in keys_filtered}

    return data_dict_filtered










