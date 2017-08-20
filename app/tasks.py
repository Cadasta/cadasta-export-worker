import datetime
from urllib.parse import urljoin

import requests
from celery.canvas import chord

from .celery import app
from .settings import QUEUE, ZIPSTREAM_URL
from .utils.tasks import extract_followups
from .subtasks.xls import locations_xls, parties_xls, relationships_xls
from .subtasks.resources import export_resources


@app.task(name='{}.export'.format(QUEUE), bind=True)
def export(self, org_slug, project_slug, api_key, output_type):
    payload = (org_slug, project_slug, api_key)
    _chain = chord([
        locations_xls.s(*payload, out_dir='xls'),
        parties_xls.s(*payload, out_dir='xls'),
        relationships_xls.s(*payload, out_dir='xls'),
        export_resources.s(*payload, out_dir='resources')
    ])
    callback = create_zip.s(
        org_slug=org_slug, project_slug=project_slug
    ).set(**extract_followups(self))
    _chain(callback)


@app.task(name='{}.create_zip'.format(QUEUE))
def create_zip(bundles, org_slug, project_slug, **kwargs):
    # Flatten nested lists of lists
    bundles = [payload for sublist in bundles for payload in sublist]

    filename = '{}_{}_{}.zip'.format(
        datetime.date.today().isoformat(), org_slug, project_slug
    )
    resp = requests.post(ZIPSTREAM_URL, json={
        'filename': filename,
        'files': bundles
    })
    resp.raise_for_status()
    return urljoin(ZIPSTREAM_URL, resp.json()['id'])
