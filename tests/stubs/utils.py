import os
from tempfile import TemporaryDirectory


class FakeReq:
    id = 'fakeId'
    _protected = False
    called_directly = False


class MockTemporaryDir(TemporaryDirectory):
    def __init__(self, suffix=None, prefix=None, dir=None):
        self.name = os.path.join(
            prefix or '',
            dir or 'fake-tmp-dir',
            suffix or '')
        self._finalizer = lambda *args, **kwargs: 1

    def __exit__(self, *args, **kwargs):
        pass
