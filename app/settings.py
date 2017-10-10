import os

QUEUE = 'export'
PLATFORM_URL = os.environ.get("PLATFORM_URL", "http://localhost:8000")
S3_BUCKET = os.environ["S3_BUCKET"]

ZIPSTREAM_URL = os.environ.get("ZIPSTREAM_URL", "http://localhost:4040")
REQ_TIMEOUT = 2
