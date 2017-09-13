import os
import tempfile

import fiona

from ..celery import app
from ..settings import QUEUE, BASE_URL
from ..utils.api import fetch_data, upload_dir, upload_file, ZipStreamQueue
from ..utils.data import get_zipstream_payload


@app.task(name='{}.project.shp'.format(QUEUE), bind=True)
def export_shp(self, org_slug, project_slug, api_key, bundle_url, out_dir):
    url = '{base}/api/v1/organizations/{org}/projects/{proj}/spatial/'
    url = url.format(base=BASE_URL, org=org_slug, proj=project_slug)

    # Fetch features
    features = {}
    for resp in fetch_data(api_key, url, array_response=False):
        for feature in resp['features']:
            _type = feature['geometry']['type']
            features.setdefault(_type, []).append(feature)

    with ZipStreamQueue(bundle_url) as q:
        # Write features to shapefile by type, upload
        for feature_type, layers in features.items():
            dir_prefix = '{}_{}_shp'.format(org_slug, project_slug)
            dir_prefix = dir_prefix.replace('/', '_')
            with tempfile.TemporaryDirectory(prefix=dir_prefix) as tmpdir:
                filename = '{}s.shp'.format(feature_type.lower())
                path = os.path.join(tmpdir, filename)
                props = {
                    'path': path,
                    'mode': 'w',
                    'driver': 'ESRI Shapefile',
                    'crs': {
                        'proj': 'longlat',
                        'ellps': 'WGS84',
                        'datum': 'WGS84',
                        'no_defs': True
                    },
                    'schema': {
                        'geometry': feature_type,
                        'properties': {
                            'id': 'str',
                            'type': 'str',
                        }
                    }
                }
                with fiona.open(**props) as sink:
                    for layer in layers:
                        sink.write({
                            'type': layer['type'],
                            'geometry': layer['geometry'],
                            'properties': {
                                k: v for k, v in layer['properties'].items()
                                if k in props['schema']['properties']
                            }
                        })

                key_prefix = os.path.join(
                    org_slug, project_slug, self.request.id)
                key_prefix = os.path.join(key_prefix, 'shp', feature_type)
                # Upload and store links
                q.insert(*[
                    get_zipstream_payload(
                        's3://{}/{}'.format(key, bucket), out_dir)
                    for key, bucket in upload_dir(key_prefix, tmpdir)
                ])

        # Add README.txt
        readme_path = os.path.join(
            os.path.dirname(__file__), '..', 'templates', 'shp_readme.txt')
        bucket, key = upload_file('README.txt', readme_path)
        payload = get_zipstream_payload(
            's3://{}/{}'.format(bucket, key), out_dir)
        q.insert(payload)
