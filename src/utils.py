def flatten(x):
    res = x[1]
    res['category'] = x[0]
    return res