import os
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch, call

from app.subtasks.xls import locations_xls, parties_xls, relationships_xls
from tests.stubs.locations import RESP as stub_location_resp
from tests.stubs.parties import RESP as stub_parties_resp


class FakeReq:
    id = 'fakeId'
    _protected = False
    called_directly = False


class MockTemporaryDir(TemporaryDirectory):
    def __init__(self, suffix=None, prefix=None, dir=None):
        self.name = os.path.join(prefix or '', dir or 'fake-tmp-dir', suffix or '')
        self._finalizer = lambda *args, **kwargs: 1

    def __exit__(self, *args, **kwargs):
        pass


class TestXls(unittest.TestCase):

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_location(self, mock_upload, mock_workbook, mock_fetch_data):
        mock_fetch_data.return_value = stub_location_resp
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        mock_upload.return_value = ('a_bucket', 'cadasta/export-tests/fakeId/locations.xlsx')
        locations_xls.__self__ = FakeReq()
        locations_xls.request_stack.push(FakeReq())

        output = locations_xls('cadasta', 'export-tests', 'my-api-key', '')

        mock_fetch_data.assert_called_once_with(
            'my-api-key',
            ('http://localhost:8000/api/v1/'
             'organizations/cadasta/projects/export-tests/spatial/'),
            array_response=False)
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
        mock_upload.assert_called_once_with(
            'cadasta/export-tests/fakeId/locations.xlsx',
            'cadasta_export-tests_fakeId_/fake-tmp-dir/locations.xlsx')
        self.assertEqual(
            output,
            [{'dst': 'locations.xlsx', 'src': 's3://a_bucket/cadasta/export-tests/fakeId/locations.xlsx'}])

    @patch('app.subtasks.xls.fetch_data')
    @patch('app.utils.data.Workbook')
    @patch('app.utils.data.upload_file')
    @patch('app.utils.data.tempfile.TemporaryDirectory', MockTemporaryDir)
    def test_parties(self, mock_upload, mock_workbook, mock_fetch_data):
        mock_fetch_data.return_value = stub_parties_resp
        Sheet = MagicMock()
        Workbook = MagicMock()
        Workbook.create_sheet.return_value = Sheet
        mock_workbook.return_value = Workbook
        mock_upload.return_value = ('a_bucket', 'some/key.xlsx')
        parties_xls.__self__ = FakeReq()
        parties_xls.request_stack.push(FakeReq())

        output = parties_xls('cadasta', 'export-tests', 'my-api-key', '')

        mock_fetch_data.assert_called_once_with(
            'my-api-key',
            ('http://localhost:8000/api/v1/'
             'organizations/cadasta/projects/export-tests/parties/'))
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
        mock_upload.assert_called_once_with(
            'cadasta/export-tests/fakeId/parties.xlsx',
            'cadasta_export-tests_fakeId_/fake-tmp-dir/parties.xlsx')
        self.assertEqual(
            output,
            [{'dst': 'key.xlsx', 'src': 's3://a_bucket/some/key.xlsx'}])
