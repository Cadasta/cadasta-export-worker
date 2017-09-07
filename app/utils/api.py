import os

import boto3
import requests

from ..settings import S3_BUCKET


def get_session(token):
    sesh = requests.session()
    sesh.headers['Authorization'] = 'TmpToken %s' % token
    return sesh


def fetch_data(api_token, url, array_response=True):
    """
    Generator returning elements from across all paginated responses
    """
    sesh = get_session(api_token)
    data = {'next': url}
    while data['next']:
        try:
            resp = sesh.get(data['next'])
            resp.raise_for_status()
        except:
            if resp.status_code >= 500:
                raise
        data = resp.json()
        if array_response:
            for obj in data['results']:
                yield obj
        else:
            yield data['results']


def upload_file(key, path, bucket=S3_BUCKET):
    """
    Upload provided filepath to S3.
        key - str, s3 keyname
        path - str, location to file on filesystem
    """
    client = boto3.client('s3')
    client.upload_file(path, bucket, key)
    return bucket, key


def upload_dir(key_prefix, dirpath, walk=True):
    for (dirpath, _, filenames) in os.walk(dirpath):
        for filename in filenames:
            yield upload_file(
                os.path.join(key_prefix, filename),
                os.path.join(dirpath, filename)
            )
        if not walk:
            break


class ZipStreamQueue:
    """
    Queue to manage payloads to be uploaded to zipstream service. In
    efforts to avoid hitting 'request entity too large' error on
    ZipStream server. Use as a context manager to ensure that remaining
    elements in queue is uploaded at end of use.
    """
    def __init__(self, url, chunk_size=100):
        self.url = url
        self.queue = []
        self.chunk_size = chunk_size

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self.queue:
            self.flush()

    def insert(self, *new_items):
        self.queue.extend(new_items)
        self.flush()

    def flush(self, force=False):
        while (len(self.queue) >= self.chunk_size) or (force and self.queue):
            resp = requests.put(self.url, json={
                'files': self.queue[:self.chunk_size]
            })
            resp.raise_for_status()
            self.queue = self.queue[self.chunk_size:]
