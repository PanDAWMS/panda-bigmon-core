"""
Utils to check variables, fields in a dict etc
"""

def is_positive_int_field(d, k):
    if k in d and d[k] is not None and isinstance(d[k], int) and d[k] > -1:
        return True
    return False