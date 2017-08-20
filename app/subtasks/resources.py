import os

from ..celery import app
from ..settings import QUEUE, BASE_URL
from ..utils.api import fetch_data
from ..utils.data import get_zipstream_payload, create_and_upload_xls


@app.task(name='{}.resources.export'.format(QUEUE), bind=True)
def export_resources(self, org_slug, project_slug, api_key, out_dir):
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/resources/'
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    bundle_paths = []
    headers = ['id', 'name', 'description', 'original_file',
               'locations', 'parties',
               ('tenure relationship', 'tenurerelationship')]
    rows = []
    filename_cache = {'resources.xlsx': 1}

    for obj in fetch_data(api_key, url):
        # Get bundle
        fname = obj['original_file']
        fname_count = filename_cache.setdefault(fname, 0)
        filename_cache[fname] += 1
        if fname_count:
            split = fname.split('.', 1)
            name, ext = (split if len(split) == 2 else split[0], '')
            fname = '{}_{}{}'.format(name, fname_count, ext)
        bundle_paths.append((obj['file'], fname))

        # Generate XLS row
        links = {}
        for l in obj['links']:
            links.setdefault(l['type'], []).append(l['id'])

        obj['locations'] = ', '.join(links.get('locations', []))
        obj['parties'] = ', '.join(links.get('parties', []))
        obj['tenurerelationship'] = ', '.join(
            links.get('tenurerelationship', []))
        rows.append(obj)

    key_prefix = os.path.join(org_slug, project_slug, self.request.id)
    xls_path = create_and_upload_xls(key_prefix, 'resources', headers, rows)
    bundle_paths.append((xls_path, None))
    return [get_zipstream_payload(src, out_dir, out_name)
            for (src, out_name) in bundle_paths]
