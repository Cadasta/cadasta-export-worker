import os
import tempfile

import boto3
from celery.canvas import chord
from openpyxl import Workbook

from .celery import app
from .settings import QUEUE, BASE_URL, S3_BUCKET
from .utils import fetch_attr, get_session, extract_followups


@app.task(name='{}.export'.format(QUEUE), bind=True)
def export(self, org_slug, project_slug, api_key, type):
    operations = chord([
        parties_xls.s(org_slug, project_slug, api_key),
        relationships_xls.s(org_slug, project_slug, api_key),
    ])
    callback = merge.s().set(**extract_followups(self))
    operations(callback)


class XlsExport(app.Task):

    def run(self, org_slug, project_slug, api_key):
        pass

    def fetch_data(self, sesh, url):
        """
        Generator returning elements from across all paginated responses
        """
        data = {'next': url}
        while data['next']:
            try:
                resp = sesh.get(data['next'])
                resp.raise_for_status()
            except:
                if resp.status_code >= 500:
                    self.retry()
            data = resp.json()
            for obj in data['results']:
                yield obj

    @staticmethod
    def append_missing_headers(obj, headers):
        """
        Add any key values from the object's 'attributes' property to
        the array of headers.
        """
        for attr in obj['attributes']:
            key = 'attributes.{}'.format(attr)
            if key not in headers:
                headers.append(key)
        return headers

    def create_xls(self, out_dir, name, headers, data):
        """ Generate and upload XLS file """
        wb = Workbook(write_only=True)
        sheet = wb.create_sheet(title=name)
        sheet.append([header.split('.')[-1] for header in headers])

        for obj in data:
            sheet.append([fetch_attr(obj, attr) for attr in headers])

        path = os.path.join(out_dir, '{}.xlsx'.format(name))
        wb.save(path)
        return path

    def upload_file(self, org_slug, project_slug, path, bucket=S3_BUCKET):
        """ Upload provided filepath to S3 """
        key = os.path.join(
            org_slug, project_slug, self.request.id, path.split('/')[-1])
        client = boto3.client('s3')
        client.upload_file(path, bucket, key)
        return bucket, key


@app.task(name='{}.parties_xls'.format(QUEUE), bind=True, base=XlsExport)
def parties_xls(self, org_slug, project_slug, api_key):
    """
    Generates an XLS file of a project's parties.
    Returns a list with a single tuple containing the name of the bucket
    and key for file generated.
    """
    sesh = get_session(api_key)
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/parties/'
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    headers = ['id', 'name', 'type']
    data = []
    for obj in self.fetch_data(sesh, url):
        headers = self.append_missing_headers(obj, headers)
        data.append(obj)

    prefix = '_'.join([org_slug, project_slug, '-'])
    with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
        path = self.create_xls(tmpdir, 'parties', headers, data)
        bucket, key = self.upload_file(org_slug, project_slug, path)
    return [(bucket, key)]


@app.task(name='{}.relationships_xls'.format(QUEUE), bind=True, base=XlsExport)
def relationships_xls(self, org_slug, project_slug, api_key):
    """
    Generates an XLS file of a project's relationships.
    Returns a list with a single tuple containing the name of the bucket
    and key for file generated.
    """
    sesh = get_session(api_key)
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/parties/' # TODO: Change to relationships URL
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    headers = ['id', 'name', 'type']
    # headers = ['party_id', 'spatial_unit_id', 'tenure_type']
    data = []
    for obj in self.fetch_data(sesh, url):
        headers = self.append_missing_headers(obj, headers)
        data.append(obj)

    prefix = '_'.join([org_slug, project_slug, '-'])
    with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
        path = self.create_xls(tmpdir, 'relationships', headers, data)
        bucket, key = self.upload_file(org_slug, project_slug, path)
    return [(bucket, key)]


@app.task(name='{}.merge'.format(QUEUE))
def merge(*args, **kwargs):
    print('merge', *args, **kwargs)
    return 'TODO: Create a zip of:', args, kwargs
