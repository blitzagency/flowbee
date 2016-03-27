from __future__ import absolute_import
import logging
import logging.config


def init_logging():
    config = {
        "version": 1,
        "formatters": {
            "simple": {
                "format": '[%(levelname)s] %(asctime)s %(message)s'
            }
        },
        "handlers": {
            "stream": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "simple"
            }
        },
        "loggers": {
            "flowbee": {
                "handlers": ["stream"],
                "level": "INFO",
                "propagate": False,
            }
        }
    }

    logging.config.dictConfig(config)
