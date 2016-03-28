from __future__ import absolute_import
import logging
import uuid
from .. import utils
from .. import compression
from .utils import timer


log = logging.getLogger(__name__)


class Activities(object):

    def __init__(self):
        self.client = utils.get_client()
        self.meta = None
        self.is_decider = False

    @timer
    def sleep(self, seconds):
        identifier = self.workflow.identifier

        log.info("Scheduling '%s' to continue %ss from now", identifier, seconds)

        utils.schedule_later(
            client=self.client,
            task_token=self.workflow.meta.task_token,
            seconds=seconds,
            timer_id=identifier
        )

        return identifier


class Workflow(object):
    @classmethod
    def start_execution(
        cls, input=None, version="0.0.1",
        child_policy="TERMINATE", priority=0, workflow_id=None,
        execution_start_to_close_timeout=60,  # "31536000 is 1 year
        task_start_to_close_timeout=10,
    ):

        domain = cls.domain
        tasklist = cls.tasklist
        workflow_type_version = version

        # cls.name is set with @workflow() from activities.utils
        workflow_type_name = "{0}.{1}".format(
            cls.name, cls.activities.name
        )

        if input is not None:
            input = compression.compress_b64_json(input)

        if workflow_id is None:
            workflow_id = "{0}-{1}".format(workflow_type_name, uuid.uuid4().hex)

        entrypoint = None

        for name, method in cls.__dict__.iteritems():
            if hasattr(method, "is_entrypoint") \
               and method.version == version:
                entrypoint = True

        if entrypoint is None:
            raise Exception("No @entrypoint found in {0}.{1} for version {2}".format(
                cls.__module__, cls.__name__, version)
            )

        kwargs = {
            "domain": domain,
            "workflowId": workflow_id,
            "workflowType": {
                "name": workflow_type_name,
                "version": workflow_type_version
            },
            "taskList": {
                "name": tasklist
            },
            "taskPriority": str(priority),
            "input": input,
            "executionStartToCloseTimeout": str(execution_start_to_close_timeout),
            "taskStartToCloseTimeout": str(task_start_to_close_timeout),
            "childPolicy": child_policy
        }

        client = utils.get_client()

        try:
            log.debug("Starting workflow execution with: %s", kwargs)
            result = client.start_workflow_execution(**kwargs)
        except Exception as e:
            log.error(
                "Failed to start workflow '%s@%s' "
                "reason: %s",
                workflow_type_name, workflow_type_version, e.message
            )

        run_id = result["runId"]
        workflow_id = workflow_id

        return workflow_id, run_id

    def __init__(self):
        self.meta = None
        self.client = None
        self.identifier = None



