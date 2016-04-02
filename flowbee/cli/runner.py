from __future__ import absolute_import
import os
import sys
import logging
from multiprocessing import Process
from .. import utils

log = logging.getLogger(__name__)


class Runner(object):
    def __init__(self, **kwargs):
        self.processes = []
        self.workers = kwargs["workers"]
        self.workflow = kwargs["workflow"]
        self.pid = kwargs["pidfile"]
        self.environ = kwargs.get("environ")
        self.log_config = kwargs.get("log_config")
        self.log_level = kwargs.get("log_level", "INFO")

    def get_workflow_class(self):
        try:
            workflow_class = utils.import_class(self.workflow)
        except:
            log.error("Failed to import %s", self.workflow)
            raise

        return workflow_class

    def create_resources(self):
        try:
            workflow_class = self.get_workflow_class()
        except:
            sys.exit(1)

        utils.create_resources(workflow_class)

    def start(self, sync=True, dev=False, **kwargs):

        if sync:
            log.info("Verifying SWF Resources")
            self.create_resources()

        log.info("Starting workers")

        if dev:
            log.info("Developer Mode Enabled")
            self.process(0, self.workflow, self.environ, self.log_config, self.log_level, **kwargs)
            return

        for id in xrange(self.workers):
            args = (id, self.workflow, self.environ, self.log_config, self.log_level)
            p = Process(target=self.process, args=args, kwargs=kwargs)
            p.daemon = True
            p.start()
            self.processes.append(p)

        for process in self.processes:
            process.join()

    def stop(self):
        log.info("Terminating workers")
        for each in self.processes:
            if each.is_alive():
                each.terminate()

        self.processes = []

    def hup(self):
        log.info("Received HUP")
        self.stop()
        # see importlib for py3 for the same functionality
        module = utils.import_action(self.workflow.rsplit(".", 1)[0])
        reload(module)
        self.start()

    def process(self, process_id, workflow_name, environ=None, log_config=None):
        raise NotImplementedError()
