import unittest
from unittest.mock import MagicMock, patch, call

from app.subtasks import xls
from tests.stubs.locations import gen_response as gen_location_resp
from tests.stubs.parties import gen_response as gen_parties_resp
from tests.stubs.relationships import gen_response as gen_relationships_resp


class TestXlsExport(unittest.TestCase):

    @patch('app.subtasks.xls.write_locations_sheet')
    @patch('app.subtasks.xls.write_parties_sheet')
    @patch('app.subtasks.xls.write_relationships_sheet')
    @patch('app.subtasks.xls.ZipStreamQueue')
    @patch('app.subtasks.xls.upload_file')
    @patch('app.subtasks.xls.Workbook')
    @patch('app.subtasks.xls.tempfile.TemporaryDirectory')
    def test_export_task(self, tmpdir, workbook, upload, q,
                         write_rel, write_parties, write_locations):
        """ Ensure export task properly orchestrates actions """
        # Mock Request ID
        xls.export_xls.request_stack = MagicMock()
        xls.export_xls.request_stack.top.id = 'fake-id-1234'
        # Mock tempfile.TemporaryDirectory
        mockContextMngr = MagicMock()
        mockContextMngr.__enter__.return_value = '/my/tmp/dir'
        tmpdir.return_value = mockContextMngr
        # Mock upload_file
        upload.return_value = ('my_bucket', 'a_new_key')

        org_slug = 'my_org'
        project_slug = 'my_proj'
        api_key = 'abcd1234'
        bundle_url = 'https://zipstream.com/asdf/shhh'
        out_dir = 'my_dir'

        xls.export_xls(org_slug, project_slug, api_key, bundle_url, out_dir)

        # Tmpdir created
        tmpdir.assert_called_once_with(prefix='my_org_my_proj_fake-id-1234_')
        # Workbook created and save
        workbook.assert_called_once_with(write_only=True)
        url = 'http://localhost:8000/api/v1/organizations/my_org/projects/my_proj'
        workbook.return_value.save.assert_called_once_with(
            '/my/tmp/dir/my_proj.xlsx')
        # Sheets generated
        write_rel.assert_called_once_with(
            workbook.return_value, url, 'abcd1234')
        write_parties.assert_called_once_with(
            workbook.return_value, url, 'abcd1234')
        write_locations.assert_called_once_with(
            workbook.return_value, url, 'abcd1234')
        # Workbook uploaded
        upload.assert_called_once_with(
            'my_org/my_proj/fake-id-1234/my_proj.xlsx',
            '/my/tmp/dir/my_proj.xlsx'
        )
        # ZipStreamQueue instanciated
        q.assert_called_once_with('https://zipstream.com/asdf/shhh')
        # ZipStreamQueue had file inserted
        q.return_value.__enter__.return_value.insert.assert_called_once_with(
            {'src': 's3://my_bucket/a_new_key', 'dst': 'my_dir/a_new_key'})

    @patch('app.subtasks.xls.fetch_data')
    def test_location_sheet(self, mock_fetch_data):
        """ Ensure location sheet is created with proper data """
        # Mock API
        mock_fetch_data.return_value = gen_location_resp()
        # Func Args
        workbook = MagicMock()
        workbook.create_sheet.return_value
        base_url = 'http://cadasta.org/api/v1/organization/my_org/projects/my_proj'
        api_key = 'abcd1234'

        xls.write_locations_sheet(workbook, base_url, api_key)

        mock_fetch_data.assert_called_once_with(api_key, base_url + '/spatial/', array_response=False)
        workbook.create_sheet.assert_called_once_with('locations')
        self.assertEqual(
            workbook.create_sheet.return_value.append.call_args_list,
            [
                call(['id', 'ewkt', 'type', 'area', 'profil_kebun10', 'profil_kebun11', 'profil_kebun12', 'profil_kebun2', 'profil_kebun3', 'profil_kebun5', 'profil_kebun6', 'profil_kebun7', 'profil_kebun8', 'profil_kebun9']),
                call(['3t56e3me9e9krur5ftifsjdt', 'POLYGON ((-57.65624999999999 -30.14512718337611, -58.71093750000001 -34.37971258046219, -54.66796875 -36.38591277287651, -53.173828125 -33.87041555094182, -53.173828125 -31.87755764334002, -57.12890625 -30.06909396443886, -57.65624999999999 -30.14512718337611))', 'PA', '', 4, 5, 18, 1, '', 'gelombang', 2, 3, '', '']),
                call(['6ikwpzz5g3ynfvwrk56r9cw5', 'LINESTRING (24.92122528665211 60.14935271120949, 24.92222637615322 60.14908991700298, 24.92152698485793 60.14840732483581, 24.92051903857942 60.14867353746651, 24.92122528665211 60.14935271120949)', '', '', '', '', '', '', '', '', '', '', '', '']),
                call(['8fcyxavzh5b3sj2jdfev3dnd', 'POLYGON ((24.92122528665211 60.14935271120949, 24.92222637615322 60.14908991700298, 24.92152698485793 60.14840732483581, 24.92051903857942 60.14867353746651, 24.92122528665211 60.14935271120949))', '', '', '', '', '', '', '', '', '', '', '', '']),
                call(['asdf', 'POLYGON ((24.92122528665211 60.14935271120949, 24.92222637615322 60.14908991700298, 24.92152698485793 60.14840732483581, 24.92051903857942 60.14867353746651, 24.92122528665211 60.14935271120949))', '', '', '', '', '', '', '', '', '', '', '', ''])
            ]
        )

    @patch('app.subtasks.xls.fetch_data')
    def test_parties_sheet(self, mock_fetch_data):
        """ Ensure parties sheet is created with proper data """
        # Mock API
        mock_fetch_data.return_value = gen_parties_resp()
        # Func Args
        workbook = MagicMock()
        workbook.create_sheet.return_value
        base_url = 'http://cadasta.org/api/v1/organization/my_org/projects/my_proj'
        api_key = 'abcd1234'

        xls.write_parties_sheet(workbook, base_url, api_key)

        mock_fetch_data.assert_called_once_with(api_key, base_url + '/parties/')
        workbook.create_sheet.assert_called_once_with('parties')
        self.assertEqual(
            workbook.create_sheet.return_value.append.call_args_list,
            [
                call(['id', 'name', 'type', 'desa', 'dusun', 'nama_pemilik', 'suku', 'telphp']),
                call(['ueac4p3dw9jtm8vybmeg545p', 'A', 'GR', '', '', 'asd', 'foo', 12]),
                call(['duqwrmus556cxxetutpsxewz', 'B', 'GR', '', '', '', '', 25]),
                call(['mutrsbhsvyi89fy54r3spewq', 'Bob', 'IN', 'My Village', 'My Hamlet', "Bob's Uncle", 'My Tribe', 1234]),
                call(['xkrtqij77c6c8qniqwukyex3', 'Corp', 'CO', '', '', '', '', 234523])
             ]
        )

    @patch('app.subtasks.xls.fetch_data')
    def test_relationships_sheet(self, mock_fetch_data):
        """ Ensure relationships sheet is created with proper data """
        # Mock API
        mock_fetch_data.return_value = gen_relationships_resp()
        # Func Args
        workbook = MagicMock()
        workbook.create_sheet.return_value
        base_url = 'http://cadasta.org/api/v1/organization/my_org/projects/my_proj'
        api_key = 'abcd1234'

        xls.write_relationships_sheet(workbook, base_url, api_key)

        mock_fetch_data.assert_called_once_with(api_key, base_url + '/relationships/tenure/')
        workbook.create_sheet.assert_called_once_with('relationships')
        self.assertEqual(
            workbook.create_sheet.return_value.append.call_args_list,
            [
                call(['party_id', 'spatial_unit_id', 'tenure_type', 'asal_usul_lahan2', 'asal_usul_lahan3', 'asal_usul_lahan4']),
                call(['mutrsbhsvyi89fy54r3spewq', '3t56e3me9e9krur5ftifsjdt', 'CU', 'sertifikat, bpn, stdb, sppl', 'sendiri, warisan, beli, beli_kebunjadi', 'more_ten']),
                call(['kj3d2k9wkuz9sykn7f93ewzg', '3t56e3me9e9krur5ftifsjdt', 'GR', 'skt', 'sendiri', 'less_ten']),
                call(['xkrtqij77c6c8qniqwukyex3', '3t56e3me9e9krur5ftifsjdt', 'WR', 'tanah_adat', 'beli_kebunjadi', 'less_ten']),
                call(['duqwrmus556cxxetutpsxewz', '3t56e3me9e9krur5ftifsjdt', 'MR', 'sppl', 'lainnya2', 'less_ten'])
            ]
        )
