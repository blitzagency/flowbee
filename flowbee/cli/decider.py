from __future__ import absolute_import
import os
import sys
import logging
import signal
import click
from .utils import (init_environment, init_logging)
from .runner import Runner
from ..deciders import Decider
from .. import utils


class DeciderRunner(Runner):
    def process(self, process_id, workflow_name, environ=None, log_config=None, log_level="INFO"):
        # load environment first as logging can use environment
        # var expansion via os.path.expandvars
        init_environment(environ)
        init_logging(log_config, workflow=workflow_name, log_level=log_level)

        log = logging.getLogger("flowbee.cli.decider")
        pid = os.getpid()
        log.info("[%s] Starting Decider Worker", pid)

        try:
            workflow_class = utils.import_class(workflow_name)
        except:
            log.error("Failed to import %s", workflow_name)
            sys.exit(1)

        log.debug("Loaded workflow '%s'", workflow_name)

        workflow = workflow_class()
        decider = Decider(workflow)
        decider.poll()


@click.command()
@click.version_option(version='0.0.1')
@click.option('--workers', "-w", default=1, help="Number of workers.")
@click.option('--workflow', "-f", required=True, help="Python path for workflow ex: foo.bar.Baz")
@click.option('--pidfile', "-p", default="/tmp/flowbee_decider.pid", help="PID file")
@click.option('--sync/--no-sync', default=True, help="Should AWS SWF Resources be created?")
@click.option('--environ', "-e", default=None, help="Enviroment variables to load")
@click.option('--log-config', default=None, help="Standard python logging configuration formatted file")
@click.option('--log-level', default="INFO", help="Logging level. Specifying a log config negates this option")
@click.option('--dev', is_flag=True, help="Ignore workers, start in foregound")
def main(workers, workflow, pidfile, sync, environ, log_config, log_level, dev):
    log_level = log_level.upper()

    # load environment first as logging can use environment
    # var expansion via os.path.expandvars
    init_environment(environ)
    init_logging(log_config, workflow=workflow, log_level=log_level)

    runner = DeciderRunner(
        workers=workers,
        workflow=workflow,
        pidfile=pidfile,
        environ=environ,
        log_config=log_config,
        log_level=log_level
    )

    def sighup(signal, frame):
        runner.hup()

    def sigint(signal, frame):
        runner.stop()
        os.remove(pidfile)
        sys.exit(0)

    signal.signal(signal.SIGINT, sigint)
    signal.signal(signal.SIGHUP, sighup)

    with open(pidfile, 'w') as f:
        pid = str(os.getpid())
        f.write(pid)

    runner.start(sync=sync, dev=dev)

if __name__ == "__main__":
    main()
