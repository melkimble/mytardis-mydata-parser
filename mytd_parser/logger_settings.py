"""
logger_settings.py
https://stackoverflow.com/questions/49580313/create-a-log-file
Created By: Tasneem Haider
LAST MODIFIED: 03/24/2021
"""

import sys
import logging
from logging.config import dictConfig
from datetime import datetime

from . import settings

logging_config = dict(
    version=1,
    formatters={
        'verbose': {
            'format': ("[%(asctime)s] %(levelname)s "
                       "[%(name)s:%(lineno)s] %(message)s"),
            'datefmt': "%d/%b/%Y %H:%M:%S",
        },
        'simple': {
            'format': '%(levelname)s %(message)s',
        },
    },
    handlers={
        'api-logger': {'class': 'logging.handlers.RotatingFileHandler',
                           'formatter': 'verbose',
                           'level': logging.DEBUG,
                           'filename': datetime.now().strftime(settings.LOG_FILE_DIR+'logfile_%Y%m%d.log'),
                           'maxBytes': 52428800,
                           'backupCount': 7},
        'batch-process-logger': {'class': 'logging.handlers.RotatingFileHandler',
                             'formatter': 'verbose',
                             'level': logging.DEBUG,
                             'filename': settings.LOG_FILE_DIR+'batch.log',
                             'maxBytes': 52428800,
                             'backupCount': 7},
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple',
            'stream': sys.stdout,
        },
    },
    loggers={
        'api_logger': {
            'handlers': ['api-logger', 'console'],
            'level': logging.DEBUG
        },
        'batch_process_logger': {
            'handlers': ['batch-process-logger', 'console'],
            'level': logging.DEBUG
        }
    }
)

dictConfig(logging_config)

api_logger = logging.getLogger('api_logger')
batch_process_logger = logging.getLogger('batch_process_logger')