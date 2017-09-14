import os

from ..celery import app
from ..settings import QUEUE, PLATFORM_URL
from ..utils.api import fetch_data, ZipStreamQueue
from ..utils.data import get_zipstream_payload, create_and_upload_xls


@app.task(name='{}.project.resources'.format(QUEUE), bind=True)
def export_resources(self, org_slug, project_slug, api_key, bundle_url,
                     out_dir):
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/resources/'
    url = url.format(base=PLATFORM_URL, org=org_slug, proj=project_slug)

    headers = ['id', 'name', 'description', 'original_file', 'filename',
               'locations', 'parties',
               ('tenure relationship', 'tenurerelationship')]
    rows = []
    filename_cache = {'resources.xlsx': 1}

    with ZipStreamQueue(bundle_url) as q:
        for obj in fetch_data(api_key, url):
            # Deconflict filenames
            fname = obj['original_file']
            fname_count = filename_cache.setdefault(fname, 1)
            filename_cache[fname] += 1
            if fname_count > 1:
                split = fname.split('.', 1)
                name, ext = split if len(split) == 2 else (split[0], '')
                if ext:
                    ext = '.{}'.format(ext)
                fname = '{}_{}{}'.format(name, fname_count, ext)
            obj['filename'] = fname
            # Add to bundle array
            q.insert(get_zipstream_payload(obj['file'], out_dir, fname))

            # Generate XLS row of obect's relationships
            links = {}
            for l in obj['links']:
                links.setdefault(l['type'], []).append(l['id'])

            obj['locations'] = ', '.join(links.get('locations', []))
            obj['parties'] = ', '.join(links.get('parties', []))
            obj['tenurerelationship'] = ', '.join(
                links.get('tenurerelationship', []))
            rows.append(obj)

        # Generate/upload XLS
        key_prefix = os.path.join(org_slug, project_slug, self.request.id)
        xls_path = create_and_upload_xls(
            key_prefix, 'resources', headers, rows)
        q.insert(get_zipstream_payload(xls_path, out_dir))
