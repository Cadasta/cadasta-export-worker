import unittest
from unittest.mock import MagicMock, patch, call

from app.subtasks.xls import locations_xls, parties_xls, relationships_xls
from tests.stubs.locations import gen_response as gen_location_resp
from tests.stubs.parties import gen_response as gen_parties_resp
from tests.stubs.relationships import gen_response as gen_relationships_resp
from tests.stubs.utils import FakeReq, MockTemporaryDir


class TestXlsLocation(unittest.TestCase):

    def tearDown(self):
        locations_xls.__self__ = None
        locations_xls.request_stack.stack.clear()

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_location(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_location_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/locations.xlsx')
        # Mock Celery Request
        locations_xls.__self__ = FakeReq()
        locations_xls.request_stack.push(FakeReq())

        # Call Func
        output = locations_xls('cadasta', 'export-tests', 'my-api-key', '')

        # Test API
        mock_fetch_data.assert_called_once_with(
            'my-api-key',
            ('http://localhost:8000/api/v1/'
             'organizations/cadasta/projects/export-tests/spatial/'),
            array_response=False)
        # Test Excel
        mock_workbook.assert_called_once_with(
            write_only=True)
        Workbook.create_sheet.assert_called_once_with(
            title='locations')
        self.assertEqual(
            Sheet.append.call_args_list,
            [call(['id', 'ewkt', 'tenure_type', 'profil_kebun10', 'profil_kebun11', 'profil_kebun12', 'profil_kebun2', 'profil_kebun3', 'profil_kebun5', 'profil_kebun6', 'profil_kebun7', 'profil_kebun8', 'profil_kebun9']),
             call(['3t56e3me9e9krur5ftifsjdt', 'POLYGON ((-57.65624999999999 -30.14512718337611, -58.71093750000001 -34.37971258046219, -54.66796875 -36.38591277287651, -53.173828125 -33.87041555094182, -53.173828125 -31.87755764334002, -57.12890625 -30.06909396443886, -57.65624999999999 -30.14512718337611))', 'PA', 4, 5, 18, 1, '', 'gelombang', 2, 3, '', '']),
             call(['6ikwpzz5g3ynfvwrk56r9cw5', 'POLYGON ((24.92122528665211 60.14935271120949, 24.92222637615322 60.14908991700298, 24.92152698485793 60.14840732483581, 24.92051903857942 60.14867353746651, 24.92122528665211 60.14935271120949))', '', '', '', '', '', '', '', '', '', '', '']),
             call(['8fcyxavzh5b3sj2jdfev3dnd', 'POLYGON ((24.92122528665211 60.14935271120949, 24.92222637615322 60.14908991700298, 24.92152698485793 60.14840732483581, 24.92051903857942 60.14867353746651, 24.92122528665211 60.14935271120949))', '', '', '', '', '', '', '', '', '', '', '']),
             call(['asdf', 'POLYGON ((24.92122528665211 60.14935271120949, 24.92222637615322 60.14908991700298, 24.92152698485793 60.14840732483581, 24.92051903857942 60.14867353746651, 24.92122528665211 60.14935271120949))', '', '', '', '', '', '', '', '', '', '', ''])
            ])
        Workbook.save.assert_called_once_with(
            'cadasta_export-tests_fakeId_/fake-tmp-dir/locations.xlsx')
        # Test S3
        mock_upload.assert_called_once_with(
            'cadasta/export-tests/fakeId/locations.xlsx',
            'cadasta_export-tests_fakeId_/fake-tmp-dir/locations.xlsx')
        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'locations.xlsx', 'src': 's3://a_bucket/cadasta/export-tests/fakeId/locations.xlsx'}])

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_location(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_location_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/locations.xlsx')
        # Mock Celery Request
        locations_xls.__self__ = FakeReq()
        locations_xls.request_stack.push(FakeReq())

        # Call Func
        output = locations_xls('cadasta', 'export-tests', 'my-api-key', 'foo')

        # Test API
        mock_fetch_data.assert_called_once_with(
            'my-api-key',
            ('http://localhost:8000/api/v1/'
             'organizations/cadasta/projects/export-tests/spatial/'),
            array_response=False)
        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'foo/locations.xlsx', 'src': 's3://a_bucket/cadasta/export-tests/fakeId/locations.xlsx'}])


class TestXlsParties(unittest.TestCase):

    def tearDown(self):
        parties_xls.__self__ = None
        parties_xls.request_stack.stack.clear()

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_parties(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_parties_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/parties.xlsx')
        # Mock Celery Request
        parties_xls.__self__ = FakeReq()
        parties_xls.request_stack.push(FakeReq())

        # Call Func
        output = parties_xls('cadasta', 'export-tests', 'my-api-key', '')

        # Test API
        mock_fetch_data.assert_called_once_with(
            'my-api-key',
            ('http://localhost:8000/api/v1/'
             'organizations/cadasta/projects/export-tests/parties/'))
        # Test Excel
        mock_workbook.assert_called_once_with(
            write_only=True)
        Workbook.create_sheet.assert_called_once_with(
            title='parties')
        self.assertEqual(
            Sheet.append.call_args_list,
            [call(['id', 'name', 'type', 'desa', 'dusun', 'nama_pemilik', 'suku', 'telphp']),
             call(['ueac4p3dw9jtm8vybmeg545p', 'A', 'GR', '', '', 'asd', 'foo', 12]),
             call(['duqwrmus556cxxetutpsxewz', 'B', 'GR', '', '', '', '', 25]),
             call(['mutrsbhsvyi89fy54r3spewq', 'Bob', 'IN', 'My Village', 'My Hamlet', "Bob's Uncle", 'My Tribe', 1234]),
             call(['xkrtqij77c6c8qniqwukyex3', 'Corp', 'CO', '', '', '', '', 234523])
            ])
        Workbook.save.assert_called_once_with(
            'cadasta_export-tests_fakeId_/fake-tmp-dir/parties.xlsx')
        # Test S3
        mock_upload.assert_called_once_with(
            'cadasta/export-tests/fakeId/parties.xlsx',
            'cadasta_export-tests_fakeId_/fake-tmp-dir/parties.xlsx')
        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'parties.xlsx', 'src': 's3://a_bucket/cadasta/export-tests/fakeId/parties.xlsx'}])

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_parties_w_outdir(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_parties_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/parties.xlsx')
        # Mock Celery Request
        parties_xls.__self__ = FakeReq()
        parties_xls.request_stack.push(FakeReq())

        # Call Func
        output = parties_xls('cadasta', 'export-tests', 'my-api-key', 'asdf')

        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'asdf/parties.xlsx', 'src': 's3://a_bucket/cadasta/export-tests/fakeId/parties.xlsx'}])


class TestXlsRelationships(unittest.TestCase):

    def tearDown(self):
        relationships_xls.__self__ = None
        relationships_xls.request_stack.stack.clear()

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_relationships(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_relationships_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/relationships.xlsx')
        # Mock Celery Request
        relationships_xls.__self__ = FakeReq()
        relationships_xls.request_stack.push(FakeReq())

        # Call Func
        output = relationships_xls('cadasta', 'export-tests', 'my-api-key', '')

        # Test API
        mock_fetch_data.assert_called_once_with(
            'my-api-key',
            ('http://localhost:8000/api/v1/'
             'organizations/cadasta/projects/export-tests/relationships/tenure/'))
        # Test Excel
        mock_workbook.assert_called_once_with(
            write_only=True)
        Workbook.create_sheet.assert_called_once_with(
            title='relationships')
        self.assertEqual(
            Sheet.append.call_args_list,
            [call(['party_id', 'spatial_unit_id', 'tenure_type', 'asal_usul_lahan2', 'asal_usul_lahan3', 'asal_usul_lahan4']),
             call(['mutrsbhsvyi89fy54r3spewq', '3t56e3me9e9krur5ftifsjdt', 'CU', 'sertifikat, bpn, stdb, sppl', 'sendiri, warisan, beli, beli_kebunjadi', 'more_ten']),
             call(['kj3d2k9wkuz9sykn7f93ewzg', '3t56e3me9e9krur5ftifsjdt', 'GR', 'skt', 'sendiri', 'less_ten']),
             call(['xkrtqij77c6c8qniqwukyex3', '3t56e3me9e9krur5ftifsjdt', 'WR', 'tanah_adat', 'beli_kebunjadi', 'less_ten']),
             call(['duqwrmus556cxxetutpsxewz', '3t56e3me9e9krur5ftifsjdt', 'MR', 'sppl', 'lainnya2', 'less_ten'])])
        Workbook.save.assert_called_once_with(
            'cadasta_export-tests_fakeId_/fake-tmp-dir/relationships.xlsx')
        # Test S3
        mock_upload.assert_called_once_with(
            'cadasta/export-tests/fakeId/relationships.xlsx',
            'cadasta_export-tests_fakeId_/fake-tmp-dir/relationships.xlsx')
        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'relationships.xlsx', 'src': 's3://a_bucket/cadasta/export-tests/fakeId/relationships.xlsx'}])

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_relationships_w_outdir(self, mock_upload, mock_workbook, mock_fetch_data):
        # Mock API
        mock_fetch_data.return_value = gen_relationships_resp()
        # Mock XLS Create
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        # Mock S3
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/relationships.xlsx')
        # Mock Celery Request
        relationships_xls.__self__ = FakeReq()
        relationships_xls.request_stack.push(FakeReq())

        # Call Func
        output = relationships_xls('cadasta', 'export-tests', 'my-api-key', 'baz')

        # Test Output
        self.assertEqual(
            output,
            [{'dst': 'baz/relationships.xlsx', 'src': 's3://a_bucket/cadasta/export-tests/fakeId/relationships.xlsx'}])
