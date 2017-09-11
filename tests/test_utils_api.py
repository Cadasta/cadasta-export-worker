import unittest
from unittest.mock import MagicMock, patch, call

from app.utils import api


def mock_api_response(results, next_url=None, status_code=200, raise_for_status=None):
    resp = MagicMock()
    resp.json.return_value = {
        'next': next_url,
        'results': results,
    }
    resp.status_code = status_code
    if raise_for_status:
        resp.raise_for_status.side_effect = raise_for_status
    return resp


class TestGetSession(unittest.TestCase):
    def test_get_session(self):
        self.assertEqual(
            api.get_session('a-fake-token').headers['Authorization'],
            'TmpToken a-fake-token')


class TestUploadFile(unittest.TestCase):
    @patch('app.utils.api.boto3')
    def test_upload_file(self, boto3):
        client = MagicMock()
        boto3.client.return_value = client
        output = api.upload_file(
            'my/key.jpg', 'path/to/file/on/disk.jpg', 'a_bucket')
        self.assertEqual(output, ('a_bucket', 'my/key.jpg'))
        boto3.client.assert_called_once_with('s3')
        client.upload_file.assert_called_once_with(
            'path/to/file/on/disk.jpg', 'a_bucket', 'my/key.jpg')


class TestFetchData(unittest.TestCase):
    @patch('app.utils.api.get_session')
    def test_fetch_data(self, get_session):
        sesh = MagicMock()

        sesh.get.side_effect = [
            mock_api_response(**{
                'next_url': 'http://url.com/?page=2',
                'results': [{'a': 1}, {'a': 2}]
            }),
            mock_api_response(**{
                'next_url': None,
                'results': [{'a': 3}, {'a': 4}]
            }),
        ]
        get_session.return_value = sesh
        for i, x in enumerate(api.fetch_data('myToken', 'http://url.com')):
            self.assertEqual(x, {'a': i + 1})

        self.assertEqual(
            sesh.get.call_args_list,
            [call('http://url.com'), call('http://url.com/?page=2')]
        )
        for resp in sesh.get.side_effect:
            resp.raise_for_status.assert_called_once_with()

    @patch('app.utils.api.get_session')
    def test_fetch_non_array_data(self, get_session):
        sesh = MagicMock()

        sesh.get.side_effect = [
            mock_api_response(**{
                'next_url': 'http://url.com/?page=2',
                'results': {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                ]
                            },
                            "properties": {
                                "id": "1",
                                "type": "PA",
                                "attributes": {},
                                "area": 1
                            }
                        }
                    ]
                }
            }),
            mock_api_response(**{
                'next_url': None,
                'results': {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                ]
                            },
                            "properties": {
                                "id": "2",
                                "type": "PA",
                                "attributes": {},
                                "area": 4
                            }
                        }
                    ]
                }
            }),
        ]
        get_session.return_value = sesh
        data = api.fetch_data(
            'myToken', 'http://url.com', array_response=False)
        for i, x in enumerate(data):
            i = i + 1
            self.assertEqual(
                x, {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                ]
                            },
                            "properties": {
                                "id": str(i),
                                "type": "PA",
                                "attributes": {},
                                "area": i**2
                            }
                        }
                    ]
                }
            )

        self.assertEqual(
            sesh.get.call_args_list,
            [call('http://url.com'), call('http://url.com/?page=2')]
        )
        for resp in sesh.get.side_effect:
            resp.raise_for_status.assert_called_once_with()


class TestUploadDir(unittest.TestCase):
    @patch('app.utils.api.upload_file')
    @patch('app.utils.api.os.walk')
    def test_upload_dir(self, walk, upload):
        walk.return_value = [
            ('a_dir', ['subdir'], ['1.txt', '2.txt']),
            ('subdir', [], ['a.txt', 'b.txt']),
        ]
        upload.side_effect = range(100)
        output = [x for x in api.upload_dir('foobar', '/tmp/my/dir')]
        self.assertEqual(output, [0, 1, 2, 3])
        self.assertEqual(upload.call_args_list, [
            call('foobar/1.txt', 'a_dir/1.txt'),
            call('foobar/2.txt', 'a_dir/2.txt'),
            call('foobar/a.txt', 'subdir/a.txt'),
            call('foobar/b.txt', 'subdir/b.txt')
        ])

    @patch('app.utils.api.upload_file')
    @patch('app.utils.api.os.walk')
    def test_upload_dir_no_walk(self, walk, upload):
        walk.return_value = [
            ('a_dir', ['subdir'], ['1.txt', '2.txt']),
            ('subdir', [], ['a.txt', 'b.txt']),
        ]
        upload.side_effect = range(100)
        output = [x for x in api.upload_dir('foobar', '/tmp/my/dir', False)]
        self.assertEqual(output, [0, 1])
        self.assertEqual(upload.call_args_list, [
            call('foobar/1.txt', 'a_dir/1.txt'),
            call('foobar/2.txt', 'a_dir/2.txt'),
        ])


@patch('app.utils.api.requests')
class TestZipStreamQueue(unittest.TestCase):

    def test_zipstream(self, requests):
        with api.ZipStreamQueue('http://zipstream.com/foo', 2) as q:
            for i in range(21):
                q.insert(i)

        self.assertEqual(
            requests.put.call_args_list, [
                call('http://zipstream.com/foo', json={'files': [0, 1]}),
                call('http://zipstream.com/foo', json={'files': [2, 3]}),
                call('http://zipstream.com/foo', json={'files': [4, 5]}),
                call('http://zipstream.com/foo', json={'files': [6, 7]}),
                call('http://zipstream.com/foo', json={'files': [8, 9]}),
                call('http://zipstream.com/foo', json={'files': [10, 11]}),
                call('http://zipstream.com/foo', json={'files': [12, 13]}),
                call('http://zipstream.com/foo', json={'files': [14, 15]}),
                call('http://zipstream.com/foo', json={'files': [16, 17]}),
                call('http://zipstream.com/foo', json={'files': [18, 19]}),
                call('http://zipstream.com/foo', json={'files': [20]})
            ]
        )

    def test_zipstream_single_large_insert(self, requests):
        with api.ZipStreamQueue('http://zipstream.com/foo', 2) as q:
            q.insert(*[i for i in range(21)])

        self.assertEqual(
            requests.put.call_args_list, [
                call('http://zipstream.com/foo', json={'files': [0, 1]}),
                call('http://zipstream.com/foo', json={'files': [2, 3]}),
                call('http://zipstream.com/foo', json={'files': [4, 5]}),
                call('http://zipstream.com/foo', json={'files': [6, 7]}),
                call('http://zipstream.com/foo', json={'files': [8, 9]}),
                call('http://zipstream.com/foo', json={'files': [10, 11]}),
                call('http://zipstream.com/foo', json={'files': [12, 13]}),
                call('http://zipstream.com/foo', json={'files': [14, 15]}),
                call('http://zipstream.com/foo', json={'files': [16, 17]}),
                call('http://zipstream.com/foo', json={'files': [18, 19]}),
                call('http://zipstream.com/foo', json={'files': [20]})
            ]
        )
