import requests

from .celery import app
from . import QUEUE


def get_session(token):
    sesh = requests.session()
    sesh.headers['Authorization'] = 'TmpToken %s' % token
    return sesh


@app.task(name='{}.export'.format(QUEUE), bind=True)
def export(self, org_slug, project_slug, api_key, type):
    sesh = get_session(api_key)

    url = 'http://localhost:8000/api/v1/organizations/{org}/projects/{proj}/parties/'
    url = url.format(org=org_slug, proj=project_slug)
    print(sesh.get(url).json())
