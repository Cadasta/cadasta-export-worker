import datetime
from urllib.parse import urljoin

import requests
from cadasta.workertoolbox.utils import extract_followups
from celery.canvas import chord

from .celery import app
from .settings import QUEUE, ZIPSTREAM_URL
from .subtasks.shp import export_shp
from .subtasks.xls import locations_xls, parties_xls, relationships_xls
from .subtasks.resources import export_resources


@app.task(name='{}.export'.format(QUEUE), bind=True)
def export(self, org_slug, project_slug, api_key, output_type):
    payload = (org_slug, project_slug, api_key)
    tasks = []

    if output_type in ('xls', 'shp', 'all'):
        out_dir = '' if output_type == 'xls' else 'xls'
        tasks += [
            locations_xls.s(*payload, out_dir=out_dir),
            parties_xls.s(*payload, out_dir=out_dir),
            relationships_xls.s(*payload, out_dir=out_dir),
        ]

    if output_type in ('res', 'all'):
        out_dir = '' if output_type == 'res' else 'resources'
        tasks.append(export_resources.s(*payload, out_dir=out_dir))

    if output_type in ('shp', 'all'):
        out_dir = '' if output_type == 'shp' else 'shp'
        tasks.append(export_shp.s(*payload, out_dir=out_dir))

    # TODO: assemble & upload README.md
    callback = create_zip.s(
        filename='{}_{}.zip'.format(project_slug, output_type),
        many_results=len(tasks) > 1
    ).set(**extract_followups(self))
    chord(tasks)(callback)


@app.task(name='{}.create_zip'.format(QUEUE))
def create_zip(bundles, filename, many_results=False):
    """
    filename - Filename of zip to be generated. Note that the day's date
        will be prepended to the filename.
    many_results - Boolean, representing if task is to take results from
        just one task or many tasks. If many_results == True, results
        will be flatted into single array of results.
    """
    # Flatten nested lists of lists
    if many_results:
        bundles = [payload for sublist in bundles for payload in sublist]

    filename = '{}_{}'.format(datetime.date.today().isoformat(), filename)
    resp = requests.post(ZIPSTREAM_URL, json={
        'filename': filename,
        'files': bundles
    })
    resp.raise_for_status()
    return urljoin(ZIPSTREAM_URL, resp.json()['id'])
