from __future__ import absolute_import

from celery import Celery
from cadasta.workertoolbox.conf import Config


conf = Config(
    CHORD_UNLOCK_MAX_RETRIES=30
)
app = Celery()
app.config_from_object(conf)
