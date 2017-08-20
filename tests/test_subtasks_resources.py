import unittest
from unittest.mock import MagicMock, patch, call

from app.subtasks.resources import export_resources
from tests.stubs.resources import gen_response as gen_resources_resp
from tests.stubs.utils import FakeReq, MockTemporaryDir


class TestResources(unittest.TestCase):
    def tearDown(self):
        export_resources.__self__ = None
        export_resources.request_stack.stack.clear()

    @patch('app.subtasks.resources.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_resources(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_resources_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/resources.xlsx')
        # Mock Celery Request
        export_resources.__self__ = FakeReq()
        export_resources.request_stack.push(FakeReq())

        # Call Func
        output = export_resources('cadasta', 'export-tests', 'my-api-key', '')

        # Test API
        mock_fetch_data.assert_called_once_with(
            'my-api-key',
            ('http://localhost:8000/api/v1/'
             'organizations/cadasta/projects/export-tests/resources/'))
        # Test Excel
        mock_workbook.assert_called_once_with(
            write_only=True)
        Workbook.create_sheet.assert_called_once_with(
            title='resources')
        self.assertEqual(
            Sheet.append.call_args_list,
            [call(['id', 'name', 'description', 'original_file', 'locations', 'parties', 'tenure relationship']),
             call(['0', 'foo_0', None, 'baz0', '', '', '']),
             call(['1', 'foo_1', None, 'baz1', '', '', '']),
             call(['3', 'foo_3', None, 'baz0', '', '', '']),
             call(['4', 'foo_4', None, 'baz1', '', '', ''])])
        Workbook.save.assert_called_once_with(
            'cadasta_export-tests_fakeId_/fake-tmp-dir/resources.xlsx')
        # Test S3
        mock_upload.assert_called_once_with(
            'cadasta/export-tests/fakeId/resources.xlsx',
            'cadasta_export-tests_fakeId_/fake-tmp-dir/resources.xlsx')
        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'baz0',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img1.jpg'},
             {'dst': 'baz1',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img2.jpg'},
             {'dst': 'baz0_1',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img3.jpg'},
             {'dst': 'baz1_1',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img4.jpg'},
             {'dst': 'resources.xlsx',
              'src': 's3://a_bucket/cadasta/export-tests/fakeId/resources.xlsx'}])

    @patch('app.subtasks.resources.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_resources_w_outdir(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_resources_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/resources.xlsx')
        # Mock Celery Request
        export_resources.__self__ = FakeReq()
        export_resources.request_stack.push(FakeReq())

        # Call Func
        output = export_resources('cadasta', 'export-tests', 'my-api-key', 'foo')

        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'foo/baz0',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img1.jpg'},
             {'dst': 'foo/baz1',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img2.jpg'},
             {'dst': 'foo/baz0_1',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img3.jpg'},
             {'dst': 'foo/baz1_1',
              'src': 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources/img4.jpg'},
             {'dst': 'foo/resources.xlsx',
              'src': 's3://a_bucket/cadasta/export-tests/fakeId/resources.xlsx'}])
