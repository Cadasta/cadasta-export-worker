import os

from shapely.geometry import shape

from ..celery import app
from ..settings import BASE_URL, QUEUE
from ..utils.api import fetch_data
from ..utils import data as data_utils


@app.task(name='{}.xls.locations'.format(QUEUE), bind=True)
def locations_xls(self, org_slug, project_slug, api_key, out_dir):
    """
    Generates an XLS file of a project's locations.
    Returns a list with a single tuple containing the name of the bucket
    and key for file generated.
    """
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/spatial/'
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    headers = [
        ('id', 'properties.id'),
        ('geometry.ewkt', 'geometry'),
        ('tenure_type', 'properties.type')
    ]
    data = []
    for resp in fetch_data(api_key, url, array_response=False):
        for obj in resp['features']:
            headers = data_utils.append_missing_headers(
                obj, headers, attrs='properties.attributes')
            obj['geometry'] = shape(obj['geometry']).wkt
            data.append(obj)

    key_prefix = os.path.join(org_slug, project_slug, self.request.id)
    src = data_utils.create_and_upload_xls(
        key_prefix, 'locations', headers, data)
    return [data_utils.get_zipstream_payload(src, out_dir)]


@app.task(name='{}.xls.parties'.format(QUEUE), bind=True)
def parties_xls(self, org_slug, project_slug, api_key, out_dir):
    """
    Generates an XLS file of a project's parties.
    Returns a list with a single tuple containing the name of the bucket
    and key for file generated.
    """
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/parties/'
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    headers = ['id', 'name', 'type']
    data = []
    for obj in fetch_data(api_key, url):
        headers = data_utils.append_missing_headers(obj, headers)
        data.append(obj)

    key_prefix = os.path.join(org_slug, project_slug, self.request.id)
    src = data_utils.create_and_upload_xls(
        key_prefix, 'parties', headers, data)
    return [data_utils.get_zipstream_payload(src, out_dir)]


@app.task(name='{}.xls.relationships'.format(QUEUE), bind=True)
def relationships_xls(self, org_slug, project_slug, api_key, out_dir):
    """
    Generates an XLS file of a project's relationships.
    Returns a list with a single tuple containing the name of the bucket
    and key for file generated.
    """
    url = ('{base}/api/v1/organizations/{org}/projects/{proj}/'
           'relationships/tenure/')
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    headers = [
        ('party_id', 'party'),
        ('spatial_unit_id', 'spatial_unit'),
        'tenure_type'
    ]
    data = []
    for obj in fetch_data(api_key, url):
        headers = data_utils.append_missing_headers(obj, headers)
        data.append(obj)

    key_prefix = os.path.join(org_slug, project_slug, self.request.id)
    src = data_utils.create_and_upload_xls(
        key_prefix, 'relationships', headers, data)
    return [data_utils.get_zipstream_payload(src, out_dir)]
