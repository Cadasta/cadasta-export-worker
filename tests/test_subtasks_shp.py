import unittest
from unittest.mock import MagicMock, patch, call

from app.subtasks import shp
from tests.stubs.locations import gen_response as gen_location_resp


class TestXlsExport(unittest.TestCase):

    @patch('app.subtasks.shp.upload_file')
    @patch('app.subtasks.shp.upload_dir')
    @patch('app.subtasks.shp.fiona')
    @patch('app.subtasks.shp.tempfile.TemporaryDirectory')
    @patch('app.subtasks.shp.ZipStreamQueue')
    @patch('app.subtasks.shp.fetch_data')
    def test_export_task(self, fetch, q, tmpdir, fiona, upload_d, upload_f):
        """ Ensure export task properly orchestrates actions """
        # Mock Request ID
        shp.export_shp.request_stack = MagicMock()
        shp.export_shp.request_stack.top.id = 'fake-id-1234'
        # Mock fetch
        fetch.return_value = gen_location_resp()
        # Mock tempfile.TemporaryDirectory
        mockContextMngr = MagicMock()
        mockContextMngr.__enter__.return_value = '/my/tmp/dir'
        tmpdir.return_value = mockContextMngr
        # Mock upload_file
        def side_effect(key_prefix, tmpdir):
            filename = key_prefix.split('/')[-1]
            return [
                ('my_bucket', '{}.{}'.format(filename, ext))
                for ext in ['shp', 'prj', 'dbf']
            ]

        upload_d.side_effect = side_effect
        upload_f.return_value = ('my_bucket', 'a_new_key')

        org_slug = 'my_org'
        project_slug = 'my_proj'
        api_key = 'abcd1234'
        bundle_url = 'https://zipstream.com/asdf/shhh'
        out_dir = 'my_dir'

        shp.export_shp(org_slug, project_slug, api_key, bundle_url, out_dir)

        # ZipStreamQueue instanciated
        q.assert_called_once_with('https://zipstream.com/asdf/shhh')
        # ZipStreamQueue had file inserted
        self.assertEqual(
            q.return_value.__enter__.return_value.insert.call_args_list,
            [
                call(
                    {'src': "s3://my_bucket/Polygon.shp", 'dst': 'my_dir/Polygon.shp'},
                    {'src': "s3://my_bucket/Polygon.prj", 'dst': 'my_dir/Polygon.prj'},
                    {'src': "s3://my_bucket/Polygon.dbf", 'dst': 'my_dir/Polygon.dbf'}
                ),
                call(
                    {'src': "s3://my_bucket/LineString.shp", 'dst': 'my_dir/LineString.shp'},
                    {'src': "s3://my_bucket/LineString.prj", 'dst': 'my_dir/LineString.prj'},
                    {'src': "s3://my_bucket/LineString.dbf", 'dst': 'my_dir/LineString.dbf'}
                ),
                call(
                    {'src': 's3://my_bucket/a_new_key', 'dst': 'my_dir/a_new_key'}
                )
            ]

        )

        # Tmpdir created
        self.assertEqual(
            tmpdir.call_args_list,
            [
                call(prefix='my_org_my_proj_shp'),
                call(prefix='my_org_my_proj_shp'),
            ]
        )

        # Fiona files created properly
        props = dict(
            crs={'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True},
            driver='ESRI Shapefile',
            mode='w',
        )
        self.assertEqual(
            fiona.open.call_args_list,
            [
                call(
                    path='/my/tmp/dir/polygons.shp',
                    schema={'geometry': 'Polygon', 'properties': {'id': 'str', 'type': 'str'}},
                    **props),
                call(
                    path='/my/tmp/dir/linestrings.shp',
                    schema={'geometry': 'LineString', 'properties': {'id': 'str', 'type': 'str'}},
                    **props)
            ]
        )
        self.assertEqual(
            fiona.open.return_value.__enter__.return_value.write.call_args_list,
            [
                call({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[-57.65624999999999, -30.145127183376115], [-58.71093750000001, -34.37971258046219], [-54.66796875, -36.38591277287651], [-53.173828125, -33.87041555094182], [-53.173828125, -31.87755764334002], [-57.12890625, -30.06909396443886], [-57.65624999999999, -30.145127183376115]]]
                    },
                    'properties': {
                        'id': '3t56e3me9e9krur5ftifsjdt', 'type': 'PA'
                    }
                }),
                call({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[24.921225286652113, 60.14935271120949], [24.92222637615322, 60.14908991700298], [24.921526984857927, 60.14840732483581], [24.920519038579418, 60.14867353746651], [24.921225286652113, 60.14935271120949]]]
                    },
                    'properties': {
                        'id': '8fcyxavzh5b3sj2jdfev3dnd', 'type': ''}
                }),
                call({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Polygon',
                        'coordinates': [[[24.921225286652113, 60.14935271120949], [24.92222637615322, 60.14908991700298], [24.921526984857927, 60.14840732483581], [24.920519038579418, 60.14867353746651], [24.921225286652113, 60.14935271120949]]]
                    },
                    'properties': {
                        'id': 'asdf', 'type': ''}
                }),
                call({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'LineString',
                        'coordinates': [[24.921225286652113, 60.14935271120949], [24.92222637615322, 60.14908991700298], [24.921526984857927, 60.14840732483581], [24.920519038579418, 60.14867353746651], [24.921225286652113, 60.14935271120949]]
                    },
                    'properties': {
                        'id': '6ikwpzz5g3ynfvwrk56r9cw5', 'type': ''}
                })]
        )
