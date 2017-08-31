from __future__ import absolute_import

from celery import Celery
from cadasta.workertoolbox.conf import Config


conf = Config()
app = Celery()
app.config_from_object(conf)
