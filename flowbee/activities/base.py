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
    def cancel_execution(cls, workflow_id, run_id, reason, details="", child_policy="TERMINATE"):
        """Cancel Workflow Execution

        :param workflow_id: the workflow_id returned from Workflow.start_execution
        :param run_id: the run_id returned from Workflow.start_execution
        :param reason: the reason for this termination
        :param details: the details behind the reason (default "")
        :param child_policy: one of TERMINATE|REQUEST_CANCEL|ABANDON. (default "TERMINATE")
                             see docs for why you should consider REQUEST_CANCEL
                             http://boto3.readthedocs.org/en/latest/reference/services/swf.html#SWF.Client.terminate_workflow_execution
        :returns: None
        """
        domain = cls.domain
        kwargs = {
            "domain": domain,
            "workflowId": workflow_id,
            "runId": run_id,
            "reason": reason,
            "details": details,
            "childPolicy": child_policy
        }

        client = utils.get_client()

        try:
            log.debug("Canceling workflow execution with: %s", kwargs)
            response = client.terminate_workflow_execution(**kwargs)
        except Exception as e:
            log.error(
                "Failed to cancel workflow '%s' with run_id '%s' "
                "reason: %s",
                workflow_id, run_id, e.message
            )
            return False

        try:
            status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        except KeyError as e:
            log.error(
                "Failed to get status code from cancel "
                "response. Missing key: '%s' %s", e.message, response
            )
            return False

        if status_code == 200:
            return True
        else:
            log.error(
                "Cancel response status code was not "
                "'200', got '%s' in %s", status_code, response
            )
            return False

    @classmethod
    def start_execution(
        cls, input=None, version="0.0.1",
        child_policy="TERMINATE", priority=0, workflow_id=None,
        execution_start_to_close_timeout=60,  # "31536000 is 1 year
        task_start_to_close_timeout=10,
    ):
        """Start workflow execution

        :param input: the JSON serializable value to pass to the designated @entrypoint
                      function (default None)
        :param child_policy: one of TERMINATE|REQUEST_CANCEL|ABANDON. (default "TERMINATE")
                             see docs for you should consider REQUEST_CANCEL
                             http://boto3.readthedocs.org/en/latest/reference/services/swf.html#SWF.Client.start_workflow_execution
        :param priority: valid values are integers that range from Java's Integer.MIN_VALUE (-2147483648) to
                         Integer.MAX_VALUE (2147483647). Higher numbers indicate higher priority. (default 0)
        :param workflow_id: workflow id for this workflow. If None a workflow id will be automatically
                            generated for you. This value is retruned to you upon success weather passed in
                            or generated automatically. (default None)
        :param execution_start_to_close_timeout: How long can this execution take in total before it's timed out
        :param task_start_to_close_timeout: How long before this task is resent to the decider
        :returns: (workflow_id, run_id)
        """
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
            raise

        run_id = result["runId"]
        workflow_id = workflow_id

        return workflow_id, run_id

    def __init__(self):
        self.meta = None
        self.client = None
        self.identifier = None
