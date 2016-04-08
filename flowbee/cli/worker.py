from __future__ import absolute_import
import os
import sys
import logging
import click
from .utils import (init_environment, init_logging)
from .runner import Runner
from ..workers import Worker
from .. import utils


class WorkerRunner(Runner):
    def process(self, workflow_name, environ=None, log_config=None, log_level="INFO"):

        log = logging.getLogger("flowbee.cli.worker")

        pid = os.getpid()
        log.info("[%s] Starting Worker", pid)

        try:
            workflow_class = utils.import_class(workflow_name)
        except:
            log.error("Failed to import %s", workflow_name)
            sys.exit(1)

        log.debug("Loaded worker workflow '%s'", workflow_name)

        workflow = workflow_class()
        worker = Worker(workflow)
        worker.poll()


@click.command()
@click.version_option(version='0.0.1')
@click.option('--workflow', "-f", required=True, help="Python path for workflow ex: foo.bar.Baz")
@click.option('--sync/--no-sync', default=True, help="Should AWS SWF Resources be created?")
@click.option('--environ', "-e", default=None, help="Enviroment variables to load")
@click.option('--log-config', default=None, help="Standard python logging configuration formatted file")
@click.option('--log-level', default="INFO", help="Logging level. Specifying a log config negates this option")
def main(workflow, sync, environ, log_config, log_level):
    log_level = log_level.upper()
    # load environment first as logging can use environment
    # var expansion via os.path.expandvars
    init_environment(environ)
    init_logging(log_config, workflow=workflow, log_level=log_level)

    runner = WorkerRunner(
        workflow_name=workflow,
        environ=environ,
        log_config=log_config,
        log_level=log_level
    )

    runner.start(sync=sync)

if __name__ == "__main__":
    main()
