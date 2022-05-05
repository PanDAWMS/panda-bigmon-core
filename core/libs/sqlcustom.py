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
    chars_to_escape = '$%^&()[]{};<>?\`~+%\'\"'
    chars_replacement = '_' * len(chars_to_escape)
    tbl = str.maketrans(chars_to_escape, chars_replacement)
    str_to_escape = encoding.smart_str(str_to_escape, encoding='ascii', errors='ignore')
    str_to_escape = str_to_escape.translate(tbl)

    return str_to_escape


def preprocess_wild_card_string(strToProcess, fieldToLookAt):
    if len(strToProcess) == 0:
        return '(1=1)'
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

            extraQueryString += "(UPPER(" + fieldToLookAt + ") "
            if isNot:
                extraQueryString += "NOT "
            if leadStar and trailStar:
                extraQueryString += " LIKE UPPER('%%" + parameter + "%%')"
            elif not leadStar and not trailStar:
                extraQueryString += " LIKE UPPER('" + parameter + "')"
            elif leadStar and not trailStar:
                extraQueryString += " LIKE UPPER('%%" + parameter + "')"
            elif not leadStar and trailStar:
                extraQueryString += " LIKE UPPER('" + parameter + "%%')"
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