import os
import json


def gen_response():
    paths = [
        '{}/resp{}.json'.format(os.path.dirname(__file__), i)
        for i in range(1, 3)]
    for path in paths:
        yield json.load(open(path, 'r'))['results']
