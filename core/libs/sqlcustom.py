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

