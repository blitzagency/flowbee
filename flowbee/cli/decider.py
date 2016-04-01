from __future__ import absolute_import
import os
import sys
import logging
import signal
import click
from .logging import init_logging
from .runner import Runner
from ..deciders import Decider
from .. import utils


log = logging.getLogger(__name__)


class DeciderRunner(Runner):
    def process(self, process_id, workflow_name, environ=None):
        pid = os.getpid()
        log.info("Starting Decider Worker '%s' @ %s", pid)

        try:
            workflow_class = utils.import_class(workflow_name)
        except:
            log.error("Failed to import %s", workflow_name)
            sys.exit(1)

        log.info("Loaded workflow '%s'", workflow_name)

        workflow = workflow_class()
        workflow.load_environment(environ)

        decider = Decider(workflow)
        decider.poll()


@click.command()
@click.version_option(version='0.0.1')
@click.option('--workers', "-w", default=1, help="Number of workers.")
@click.option('--workflow', "-f", required=True, help="Python path for workflow ex: foo.bar.Baz")
@click.option('--pidfile', "-p", default="/tmp/swfdecider.pid", help="PID file")
@click.option('--sync/--no-sync', default=True, help="Should AWS SWF Resources be created?")
@click.option('--environ', "-e", default=None, help="Enviroment variables to load")
@click.option('--dev', is_flag=True, help="Ignore workers, start in foregound")
def main(workers, workflow, pidfile, sync, environ, dev):
    init_logging()
    runner = DeciderRunner(
        workers=workers,
        workflow=workflow,
        pidfile=pidfile,
        environ=environ
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
