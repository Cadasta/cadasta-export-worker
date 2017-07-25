import os

QUEUE = 'export'
BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
S3_BUCKET = os.environ["S3_BUCKET"]
