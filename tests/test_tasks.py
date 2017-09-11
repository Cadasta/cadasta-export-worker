import unittest
from unittest.mock import MagicMock, patch

from freezegun import freeze_time

from app.tasks import export, create_result
from app.subtasks import xls, shp, resources


def prepare_mock_post(mock_post):
    mock_post.return_value = MagicMock()
    mock_post.return_value.json.return_value = {
        'id': 'abcd1234',
        'secret': 'shhh'
    }
    return mock_post


@freeze_time("2017-08-29")
@patch('app.tasks.ZIPSTREAM_URL', 'http://zipstream.com')
@patch('app.tasks.requests.post')
@patch('app.tasks.chord')
class TestExport(unittest.TestCase):

    def test_export_all(self, chord, post):
        prepare_mock_post(post)

        export('my-org', 'my-proj', api_key='asdf', output_type='all')

        post.assert_called_once_with(
            'http://zipstream.com',
            json={'filename': '2017-08-29_my-proj_all.zip'})

        payload = ('my-org', 'my-proj', 'asdf',
                   'http://zipstream.com/abcd1234/shhh')
        chord.assert_called_once_with([
            xls.export_xls.s(*payload, out_dir='xls'),
            resources.export_resources.s(*payload, out_dir='resources'),
            shp.export_shp.s(*payload, out_dir='shp')
        ])
        chord.return_value.assert_called_once_with(
            create_result.si(
                '2017-08-29_my-proj_all.zip', 'http://zipstream.com/abcd1234'
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )

    def test_export_res(self, chord, post):
        prepare_mock_post(post)

        export('my-org', 'my-proj', api_key='asdf', output_type='res')

        post.assert_called_once_with(
            'http://zipstream.com',
            json={'filename': '2017-08-29_my-proj_res.zip'})
        payload = ('my-org', 'my-proj', 'asdf',
                   'http://zipstream.com/abcd1234/shhh')
        chord.assert_called_once_with([
            resources.export_resources.s(*payload, out_dir=''),
        ])
        chord.return_value.assert_called_once_with(
            create_result.si(
                '2017-08-29_my-proj_res.zip', 'http://zipstream.com/abcd1234'
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )

    def test_export_shp(self, chord, post):
        prepare_mock_post(post)

        export('my-org', 'my-proj', api_key='asdf', output_type='shp')

        post.assert_called_once_with(
            'http://zipstream.com',
            json={'filename': '2017-08-29_my-proj_shp.zip'})

        payload = ('my-org', 'my-proj', 'asdf',
                   'http://zipstream.com/abcd1234/shhh')
        chord.assert_called_once_with([
            xls.export_xls.s(*payload, out_dir='xls'),
            shp.export_shp.s(*payload, out_dir='')
        ])
        chord.return_value.assert_called_once_with(
            create_result.si(
                '2017-08-29_my-proj_shp.zip', 'http://zipstream.com/abcd1234'
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )

    def test_export_xls(self, chord, post):
        prepare_mock_post(post)

        export('my-org', 'my-proj', api_key='asdf', output_type='xls')

        post.assert_called_once_with(
            'http://zipstream.com',
            json={'filename': '2017-08-29_my-proj_xls.zip'})
        payload = ('my-org', 'my-proj', 'asdf',
                   'http://zipstream.com/abcd1234/shhh')
        chord.assert_called_once_with([
            xls.export_xls.s(*payload, out_dir=''),
        ])
        chord.return_value.assert_called_once_with(
            create_result.si(
                '2017-08-29_my-proj_xls.zip', 'http://zipstream.com/abcd1234'
            ).set(
                is_result=True
            ).set(
                # TODO: Test followup extraction
                **{'link': None, 'link_error': None}
            )
        )


class TestCreateResult(unittest.TestCase):

    def test_create_result(self):
        filename = 'foo.zip'
        bundle_url = 'https://demo.cadasta.org/asdf'
        self.assertEqual(
            create_result(filename, bundle_url),
            {
                'links': [
                    {
                        'text': filename,
                        'url': bundle_url
                    }
                ]
            }
        )
