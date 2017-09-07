import os
import tempfile
from urllib import parse

from openpyxl import Workbook

from .api import upload_file


def get_attr(obj, attr):
    value = obj
    for sub_attr in attr.split('.'):
        value = value.get(sub_attr, '')
    if isinstance(value, list):
        value = ', '.join(value)
    return value


def create_xls(out_dir, name, headers, data):
    """
    Generate and upload XLS file.
        out_dir - Dir where generated XLS file is to be stored.
        name - Name for XLS file and XLS file's sheet.
        headers - List of either tuples of header name (as will appear in
            output) and attribute name (as it appears from the api) or a
            string (if output name matches attribute name).
        data - array of dictionaries representing rows.
    """
    headers = normalize_headers(headers)
    wb = Workbook(write_only=True)
    sheet = wb.create_sheet(title=name)
    sheet.append([header[0].split('.')[-1] for header in headers])

    for obj in data:
        sheet.append([get_attr(obj, attr[1]) for attr in headers])

    path = os.path.join(out_dir, '{}.xlsx'.format(name))
    wb.save(path)
    return path


def create_and_upload_xls(key_prefix, *xls_args):
    """
    key_prefix - S3 key directory where data should be uploaded
    """
    dir_prefix = '{}_'.format(key_prefix.replace('/', '_'))
    with tempfile.TemporaryDirectory(prefix=dir_prefix) as tmpdir:
        path = create_xls(tmpdir, *xls_args)
        filename = path.split('/')[-1]
        key = os.path.join(key_prefix, filename)
        bucket, key = upload_file(key, path)
    return 's3://{bucket}/{key}'.format(bucket=bucket, key=key)


def normalize_headers(headers):
    """
    Turn each header into a tuple of header and path if path not
    specified.
    """
    return [h if isinstance(h, tuple) else (h, h) for h in headers]


def append_missing_headers(obj, headers, attrs='attributes'):
    """
    Add any key values from the object's attributes property to
    the array of headers.
    """
    attributes = get_attr(obj, attrs)
    for attr in attributes:
        key = '{}.{}'.format(attrs, attr)
        if key not in [h[1] if isinstance(h, tuple) else h for h in headers]:
            headers.append((key, key))
    return headers


def order_headers(headers, order_after):
    return headers[:order_after] + sorted(headers[order_after:])


def get_zipstream_payload(src, out_dir, out_name=None):
    out_name = out_name or parse.urlparse(src).path.split('/')[-1]
    return {
        'src': src,
        'dst': os.path.join(out_dir, out_name)
    }
