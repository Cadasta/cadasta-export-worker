import unittest
from unittest.mock import MagicMock, patch, call

from app.utils import api


def mock_api_response(results, next_url=None, status_code=200):
    resp = MagicMock()
    resp.json.return_value = {
        'next': next_url,
        'results': results,
    }
    resp.status_code = status_code
    return resp


class TestApiUtils(unittest.TestCase):
    def test_get_session(self):
        self.assertEqual(
            api.get_session('a-fake-token').headers['Authorization'],
            'TmpToken a-fake-token')

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
    def test_fetch_bad_data(self, get_session):
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
