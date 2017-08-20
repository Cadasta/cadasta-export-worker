import unittest

from app.utils import api


class TestApiUtils(unittest.TestCase):
    def test_get_session(self):
        self.assertEqual(
            api.get_session('a-fake-token').headers['Authorization'],
            'TmpToken a-fake-token')
