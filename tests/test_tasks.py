import unittest
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from app.tasks import export, create_zip
from app.subtasks import xls, shp, resources


class TestExport(unittest.TestCase):

    @patch('app.tasks.chord')
    def test_export_all(self, chord):
        export('my-org', 'my-proj', api_key='asdf', output_type='all')

        payload = ('my-org', 'my-proj', 'asdf')
        chord.assert_called_once_with([
            xls.locations_xls.s(*payload, out_dir='xls'),
            xls.parties_xls.s(*payload, out_dir='xls'),
            xls.relationships_xls.s(*payload, out_dir='xls'),
            resources.export_resources.s(*payload, out_dir='resources'),
            shp.export_shp.s(*payload, out_dir='shp')
        ])
        chord.return_value.assert_called_once_with(
            create_zip.s(
                filename='my-proj_all.zip', many_results=True
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )

    @patch('app.tasks.chord')
    def test_export_res(self, chord):
        export('my-org', 'my-proj', api_key='asdf', output_type='res')

        payload = ('my-org', 'my-proj', 'asdf')
        chord.assert_called_once_with([
            resources.export_resources.s(*payload, out_dir=''),
        ])
        chord.return_value.assert_called_once_with(
            create_zip.s(
                filename='my-proj_res.zip', many_results=False
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )

    @patch('app.tasks.chord')
    def test_export_shp(self, chord):
        export('my-org', 'my-proj', api_key='asdf', output_type='shp')

        payload = ('my-org', 'my-proj', 'asdf')
        chord.assert_called_once_with([
            xls.locations_xls.s(*payload, out_dir='xls'),
            xls.parties_xls.s(*payload, out_dir='xls'),
            xls.relationships_xls.s(*payload, out_dir='xls'),
            shp.export_shp.s(*payload, out_dir='')
        ])
        chord.return_value.assert_called_once_with(
            create_zip.s(
                filename='my-proj_shp.zip', many_results=True
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )

    @patch('app.tasks.chord')
    def test_export_shp(self, chord):
        export('my-org', 'my-proj', api_key='asdf', output_type='xls')

        payload = ('my-org', 'my-proj', 'asdf')
        chord.assert_called_once_with([
            xls.locations_xls.s(*payload, out_dir=''),
            xls.parties_xls.s(*payload, out_dir=''),
            xls.relationships_xls.s(*payload, out_dir=''),
        ])
        chord.return_value.assert_called_once_with(
            create_zip.s(
                filename='my-proj_xls.zip', many_results=True
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )


class TestCreateZip(unittest.TestCase):

    @patch('app.tasks.ZIPSTREAM_URL', 'http://zipstream.com')
    @patch('app.tasks.requests.post')
    @freeze_time("2017-08-29")
    def test_create_zip_many_results(self, post):
        post.return_value = MagicMock()
        post.return_value.json.return_value = {'id': 'abcd1234'}
        payload = [[{
            'src': 'keys{}/{}.jpg'.format(run, i),
            'dst': '{}.jpg'.format(i),
        } for i in range(2)] for run in range(3)]

        result = create_zip(payload, 'a_zipfile.zip', many_results=True)

        post.assert_called_once_with(
            'http://zipstream.com',
            json={
                'filename': '2017-08-29_a_zipfile.zip',
                'files': [
                    {
                        'src': 'keys0/0.jpg', 'dst': '0.jpg'
                    },
                    {
                        'src': 'keys0/1.jpg', 'dst': '1.jpg'
                    },
                    {
                        'src': 'keys1/0.jpg', 'dst': '0.jpg'
                    },
                    {
                        'src': 'keys1/1.jpg', 'dst': '1.jpg'
                    },
                    {
                        'src': 'keys2/0.jpg', 'dst': '0.jpg'
                    },
                    {
                        'src': 'keys2/1.jpg', 'dst': '1.jpg'
                    }
                ]
            }
        )
        post.return_value.raise_for_status.assert_called_once_with()
        post.return_value.json.assert_called_once_with()
        self.assertEqual(result, {
            'links': [{
                'text': '2017-08-29_a_zipfile.zip',
                'url': 'http://zipstream.com/abcd1234',
            }]
        })

    @patch('app.tasks.ZIPSTREAM_URL', 'http://zipstream.com')
    @patch('app.tasks.requests.post')
    @freeze_time("2017-08-29")
    def test_create_zip_one_result(self, post):
        post.return_value = MagicMock()
        post.return_value.json.return_value = {'id': 'abcd1234'}
        payload = [{
            'src': 'keys0/{}.jpg'.format(i),
            'dst': '{}.jpg'.format(i),
        } for i in range(2)]

        result = create_zip(payload, 'a_zipfile.zip')

        post.assert_called_once_with(
            'http://zipstream.com',
            json={
                'filename': '2017-08-29_a_zipfile.zip',
                'files': [
                    {
                        'src': 'keys0/0.jpg', 'dst': '0.jpg'
                    },
                    {
                        'src': 'keys0/1.jpg', 'dst': '1.jpg'
                    },
                ]
            }
        )
        post.return_value.raise_for_status.assert_called_once_with()
        post.return_value.json.assert_called_once_with()
        self.assertEqual(result, {
            'links': [{
                'text': '2017-08-29_a_zipfile.zip',
                'url': 'http://zipstream.com/abcd1234',
            }]
        })
