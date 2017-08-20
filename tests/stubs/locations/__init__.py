import os
import json


RESP = [
    json.load(
        open('{}/resp{}.json'.format(os.path.dirname(__file__), i), 'r')
    )['results'] for i in range(1, 3)
]
