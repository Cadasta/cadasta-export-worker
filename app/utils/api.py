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
    """ Upload provided filepath to S3 """
    client = boto3.client('s3')
    client.upload_file(path, bucket, key)
    return bucket, key
