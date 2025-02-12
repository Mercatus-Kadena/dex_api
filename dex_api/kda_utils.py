
COIN = "coin"

def modref(fqn):
    _sep = fqn.split(".")
    if len(_sep) == 1:
        return {"namespace":None, "name":_sep[0]}
    return {"namespace":_sep[0], "name":_sep[1]}

def module_fqn(x):
    return "{0[namespace]:s}.{0[name]:s}".format(x)  if x["namespace"] else x["name"]

def pair_to_asset(p):
    asset_a, asset_b = p.split(":")
    if asset_a == COIN:
        return asset_b
    if asset_b == COIN:
        return asset_a
    return None

def to_pair(x, y):
    return "{}:{}".format(x, y) if x < y else "{}:{}".format(y, x)
