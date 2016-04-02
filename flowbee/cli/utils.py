from __future__ import absolute_import
import logging
import logging.config
import os
import os.path
import json
import dotenv


def normalize_path(path):
    result = path
    if os.path.isabs(path):
        result = path
    else:
        if path.startswith("~"):
            result = os.path.expanduser(path)
        else:
            result = os.path.realpath(path)

    return result


def init_environment(filename):
    log = logging.getLogger(__name__)

    if filename is None:
        return

    path = normalize_path(filename)

    log.debug("Loading environment from '%s'", path)

    try:
        dotenv.read_dotenv(path)
    except Exception as e:
        log.error(e.message)
        raise


def init_logging(filename, workflow, log_level="INFO"):
    if filename is None:
        logging.config.dictConfig(
            default_logging_config(workflow=workflow, log_level=log_level)
        )
        return

    path = normalize_path(filename)
    result = {}

    with open(path) as f:
        data = os.path.expandvars(f.read())
        result = json.loads(data)

    logging.config.dictConfig(result)


def default_logging_config(workflow=None, log_level="INFO"):
    config = {
        "version": 1,
        "formatters": {
            "simple": {
                "format": '[%(levelname)s] %(asctime)s %(message)s'
            }
        },
        "handlers": {
            "stream": {
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "simple"
            }
        },
        "loggers": {
            "flowbee": {
                "handlers": ["stream"],
                "level": log_level,
                "propagate": False,
            }
        }
    }

    if workflow:
        root = workflow.split(".")[0]
        if root not in config["loggers"]:
            config["loggers"][root] = {
                "handlers": ["stream"],
                "level": log_level,
                "propagate": False,
            }

    return config
