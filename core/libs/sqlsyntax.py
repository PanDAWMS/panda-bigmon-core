"""
A set of functions to handle syntax differences between DBs
"""


def bind_var(var, db='oracle'):
    """Format of named bind variable"""
    if db == 'postgresql':
        return '%({})s'.format(var)
    elif db == 'oracle':
        return ':{}'.format(var)
    else:
        return ':{}'.format(var)