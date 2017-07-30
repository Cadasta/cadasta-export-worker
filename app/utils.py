import requests


def get_attr(obj, attr):
    value = obj
    for sub_attr in attr.split('.'):
        value = value[sub_attr]
    if isinstance(value, list):
        value = ', '.join(value)
    return value


def get_session(token):
    sesh = requests.session()
    sesh.headers['Authorization'] = 'TmpToken %s' % token
    return sesh


# TODO: Mv this to workertoolbox as helper functionality
def extract_followups(task):
    """
    Retrieve callbacks and errbacks from provided task instance, disables
    tasks callbacks.
    """
    callbacks = task.request.callbacks
    errbacks = task.request.errbacks
    task.request.callbacks = None
    return {'link': callbacks, 'link_error': errbacks}
