"""
Everything for raw queries to DB which is not supported by Django ORM
"""


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