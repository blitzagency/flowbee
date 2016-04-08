from __future__ import absolute_import
import sys
import logging
from .. import utils

log = logging.getLogger(__name__)


class Runner(object):
    def __init__(self, **kwargs):
        self.workflow = kwargs["workflow"]
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

    def start(self, sync=True, **kwargs):

        if sync:
            log.info("Verifying SWF Resources")
            self.create_resources()

        log.info("Starting worker")

        self.process(
            workflow_name=self.workflow,
            environ=self.environ,
            log_config=self.log_config,
            log_level=self.log_level,
            **kwargs
        )

    def process(self, process_id, workflow_name, environ=None, log_config=None, log_level="INFO"):
        raise NotImplementedError()
