from ..celery import app
from ..settings import QUEUE


@app.task(name='{}.create_result'.format(QUEUE))
def create_result(filename, bundle_url):
    return {
        'links': [
            {
                'text': filename,
                'url': bundle_url
            }
        ]
    }