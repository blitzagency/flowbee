from __future__ import absolute_import
import os
import sys
import logging
import signal
import click
from .logging import init_logging
from .runner import Runner
from ..workers import Worker
from .. import utils


log = logging.getLogger(__name__)


class WorkerRunner(Runner):
    def process(self, process_id, workflow_name):
        pid = os.getpid()
        log.info("Starting Decider Worker '%s'", pid)

        try:
            workflow_class = utils.import_class(workflow_name)
        except:
            log.error("Failed to import %s", workflow_name)
            sys.exit(1)

        log.info("Loaded workflow '%s'", workflow_name)
        worker = Worker(workflow_class())
        worker.poll()


@click.command()
@click.version_option(version='0.0.1')
@click.option('--workers', "-w", default=1, help="Number of workers.")
@click.option('--workflow', "-f", required=True, help="Python path for workflow ex: foo.bar.Baz")
@click.option('--pidfile', "-p", default="/tmp/swfworker.pid", help="PID file")
@click.option('--sync/--no-sync', default=True, help="Should AWS SWF Resources be created?")
def main(workers, workflow, pidfile, sync):
    init_logging()
    runner = WorkerRunner(workers=workers, workflow=workflow, pidfile=pidfile)

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

    runner.start(sync=sync)

if __name__ == "__main__":
    main()
