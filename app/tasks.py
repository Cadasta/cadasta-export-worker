from celery.canvas import chord

from .celery import app
from .settings import QUEUE
from .utils import extract_followups
from .subtasks.xls import locations_xls, parties_xls, relationships_xls


@app.task(name='{}.export'.format(QUEUE), bind=True)
def export(self, org_slug, project_slug, api_key, type):
    _chain = chord([
        locations_xls.s(org_slug, project_slug, api_key),
        parties_xls.s(org_slug, project_slug, api_key),
        relationships_xls.s(org_slug, project_slug, api_key),
    ])
    callback = merge.s().set(**extract_followups(self))
    _chain(callback)


@app.task(name='{}.merge'.format(QUEUE))
def merge(*args, **kwargs):
    print('merge', *args, **kwargs)
    return 'TODO: Create a zip of:', args, kwargs
