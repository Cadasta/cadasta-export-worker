from os import environ as env

QUEUE = 'export'
PLATFORM_URL = env.get("PLATFORM_URL", "http://localhost:8000")
S3_BUCKET = env["S3_BUCKET"]

ZIPSTREAM_URL = env.get("ZIPSTREAM_URL", "http://localhost:4040")
REQ_TIMEOUT = 2
