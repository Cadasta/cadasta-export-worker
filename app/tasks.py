import os
import tempfile

import boto3
import requests
from celery.canvas import chord
from openpyxl import Workbook

from .celery import app
from .settings import QUEUE, BASE_URL, S3_BUCKET
from .utils import fetch_attr


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


@app.task(name='{}.export'.format(QUEUE), bind=True)
def export(self, org_slug, project_slug, api_key, type):
    operations = chord([
        parties_xls.s(org_slug, project_slug, api_key),
        parties_xls.s(org_slug, project_slug, api_key),
    ])
    callback = merge.s().set(**extract_followups(self))
    operations(callback)


@app.task(name='{}.parties_xls'.format(QUEUE), bind=True)
def parties_xls(self, org_slug, project_slug, api_key):
    """
    Generates an XLS file of a project's parties.
    Returns a list with a single tuple containing the name of the bucket
    and key for file generated.
    """
    # Get data, build headers
    sesh = get_session(api_key)
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/parties/'
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    headers = ['id', 'name', 'type']
    data = {'next': url}
    output = []

    while data['next']:
        resp = sesh.get(data['next'])
        resp.raise_for_status()
        data = resp.json()

        for obj in data['results']:
            for attr in obj['attributes']:
                key = 'attributes.{}'.format(attr)
                if key not in headers:
                    headers.append(key)
            output.append(obj)

    # Create file
    prefix = '_'.join([org_slug, project_slug, '-'])
    with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:

        wb = Workbook(write_only=True)
        sheet = wb.create_sheet(title='parties')
        sheet.append([header.split('.')[-1] for header in headers])

        for obj in output:
            sheet.append([fetch_attr(obj, attr) for attr in headers])

        path = os.path.join(tmpdir, 'parties.xlsx')
        wb.save(path)

        # Upload to S3
        key = os.path.join(org_slug, project_slug, self.request.id, 'parties.xlsx')
        client = boto3.client('s3')
        client.upload_file(path, S3_BUCKET, key)
    return [(S3_BUCKET, key)]


@app.task(name='{}.merge'.format(QUEUE))
def merge(*args, **kwargs):
    print('merge', *args, **kwargs)
    return 'TODO: Create a zip of:', args, kwargs
