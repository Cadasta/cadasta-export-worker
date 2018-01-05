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
    @patch('app.subtasks.resources.ZipStreamQueue')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_resources(self, q, mock_upload, mock_workbook, mock_fetch_data):
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
        export_resources(
          'cadasta', 'export-tests', 'my-api-key',
          'https://zipstream.com/asdf/shhh', 'someDir')

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
            [call(['id', 'name', 'description', 'original_file', 'filename',
                   'locations', 'parties', 'tenure relationship']),
             call(['0', 'foo_0', None, 'foo', 'foo', '1', '2', '3']),
             call(['1', 'foo_1', None, 'foo', 'foo_2', '', '', '']),
             call(['3', 'foo_3', None, 'bar.tar.gz', 'bar.tar.gz', '', '', '']),
             call(['4', 'foo_4', None, 'bar.tar.gz', 'bar_2.tar.gz', '5', '6', ''])])
        Workbook.save.assert_called_once_with(
            'cadasta_export-tests_fakeId_/fake-tmp-dir/resources.xlsx')
        # Test S3
        mock_upload.assert_called_once_with(
            'cadasta/export-tests/fakeId/resources.xlsx',
            'cadasta_export-tests_fakeId_/fake-tmp-dir/resources.xlsx')
        # Test Output
        s3_base = 'https://s3-us-west-2.amazonaws.com/cadasta-test-bucket/resources'
        # ZipStreamQueue instanciated
        q.assert_called_once_with('https://zipstream.com/asdf/shhh')
        # ZipStreamQueue had file inserted
        self.assertEqual(
            q.return_value.__enter__.return_value.insert.call_args_list,
            [call({'dst': '{}/foo'.format('someDir'),
                   'src': '{}/img1.jpg'.format(s3_base)}),
             call({'dst': '{}/foo_2'.format('someDir'),
                   'src': '{}/img2.jpg'.format(s3_base)}),
             call({'dst': '{}/bar.tar.gz'.format('someDir'),
                   'src': '{}/img3.jpg'.format(s3_base)}),
             call({'dst': '{}/bar_2.tar.gz'.format('someDir'),
                   'src': '{}/img4.jpg'.format(s3_base)}),
             call({'dst': '{}/resources.xlsx'.format('someDir'),
                   'src': 's3://a_bucket/cadasta/export-tests/fakeId/resources.xlsx'})
             ]
         )
