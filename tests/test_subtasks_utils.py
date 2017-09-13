import unittest
from app.subtasks import utils


class TestCreateResult(unittest.TestCase):

    def test_create_result(self):
        filename = 'foo.zip'
        bundle_url = 'https://demo.cadasta.org/asdf'
        self.assertEqual(
            utils.create_result(filename, bundle_url),
            {
                'links': [
                    {
                        'text': filename,
                        'url': bundle_url
                    }
                ]
            }
        )
