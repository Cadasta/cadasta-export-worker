def fetch_attr(obj, attr):
    value = obj
    for sub_attr in attr.split('.'):
        value = value[sub_attr]
    return value
