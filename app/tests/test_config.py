from cadasta.workertoolbox.tests import build_functional_tests

from export.celery import app

FunctionalTests = build_functional_tests(app)
