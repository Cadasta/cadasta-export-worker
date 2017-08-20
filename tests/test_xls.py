import os
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.subtasks.xls import locations_xls
from tests.stubs.locations import RESP as stub_location_resp


class FakeReq:
    id = 'fakeId'
    _protected = False
    called_directly = False


class MockTemporaryDirectory(TemporaryDirectory):
    def __init__(self, suffix=None, prefix=None, dir=None):
        self.name = os.path.join(prefix or '', dir or 'fake-tmp-dir', suffix or '')
        self._finalizer = lambda *args, **kwargs: 1

    def __exit__(self, *args, **kwargs):
        pass


class TestXls(unittest.TestCase):

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDirectory)
    def test_location(self, mock_upload, mock_workbook, mock_fetch_data):
        mock_fetch_data.return_value = stub_location_resp
        mock_upload.return_value = ('a_bucket', 'some/key.xlsx')
        locations_xls.__self__ = FakeReq
        locations_xls.request_stack.push(FakeReq)

        output = locations_xls('an_org', 'a_proj', 'my-api-key', '')

        # import pudb; pu.db
        mock_upload.assert_called_once_with(
            'an_org/a_proj/fakeId/locations.xlsx',
            'an_org_a_proj_fakeId_/fake-tmp-dir/locations.xlsx')
        self.assertEqual(
            output,
            [{'dst': 'key.xlsx', 'src': 's3://a_bucket/some/key.xlsx'}])
    pass
