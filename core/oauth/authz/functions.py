import json


def match_object(obj, constraints):
    if not constraints or constraints == "{}":
        return True

    constraint_dict = json.loads(constraints)
    return constraint_dict.items() <= obj.items()


def match_role(user_roles, policy_role):
    return policy_role in user_roles


def match_constrains(new, constraints):
    if not constraints or constraints == "{}":
        return True

    constraint_dict = json.loads(constraints)

    for k, v in constraint_dict.items():
        if isinstance(v, list) and len(v) == 2 and all(isinstance(i, (int, float)) for i in v):
            val_min, val_max = v
            val_new = new.get(k)
            if val_new is None:
                return False
            if val_min < val_new < val_max:
                return True
        elif isinstance(v, list) and all(isinstance(i, str) for i in v):
            val_new = new.get(k)
            if val_new is None:
                return False
            if val_new in v:
                return True
    return False