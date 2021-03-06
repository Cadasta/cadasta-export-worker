import os
import datetime

import requests
from cadasta.workertoolbox.utils import extract_followups
from celery.canvas import chord

from .celery import app
from .settings import QUEUE, ZIPSTREAM_URL, REQ_TIMEOUT
from .subtasks.shp import export_shp
from .subtasks.xls import export_xls
from .subtasks.resources import export_resources
from .subtasks.utils import create_result


@app.task(name='{}.project'.format(QUEUE), bind=True)
def export(self, org_slug, project_slug, api_key, output_type):
    assert output_type in ['all', 'shp', 'xls', 'res'], (
        "Unsupported output type {!r}".format(output_type))

    # Create ZipStream resource, pass to tasks
    filename = '{}_{}_{}.zip'.format(
        datetime.date.today().isoformat(),
        project_slug,
        output_type)
    resp = requests.post(
        ZIPSTREAM_URL, json={'filename': filename}, timeout=REQ_TIMEOUT)
    resp.raise_for_status()
    json = resp.json()
    bundle_read_url = os.path.join(ZIPSTREAM_URL, json['id'])
    bundle_edit_url = os.path.join(bundle_read_url, json['secret'])

    # Prepare tasks
    payload = (org_slug, project_slug, api_key, bundle_edit_url)
    tasks = []
    if output_type in ('xls', 'shp', 'all'):
        out_dir = '' if output_type == 'xls' else 'xls'
        tasks.append(export_xls.s(*payload, out_dir=out_dir))

    if output_type in ('res', 'all'):
        out_dir = '' if output_type == 'res' else 'resources'
        tasks.append(export_resources.s(*payload, out_dir=out_dir))

    if output_type in ('shp', 'all'):
        out_dir = '' if output_type == 'shp' else 'shp'
        tasks.append(export_shp.s(*payload, out_dir=out_dir))

    # Prepare callback
    callback = create_result.si(
        filename, bundle_read_url
    ).set(**extract_followups(self)).set(is_result=True)

    # Execute
    chord(tasks)(callback)
    return True
