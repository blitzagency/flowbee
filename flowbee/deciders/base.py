from __future__ import absolute_import
import logging
import pprint
from botocore.exceptions import ClientError
from .utils import prepare_event
from .. import utils
from .. import exceptions

log = logging.getLogger(__name__)


class Decider(object):
    def __init__(self, workflow):
        self.workflow = workflow
        self.workflow.activities.is_decider = True
        self.meta = None
        self.client = utils.get_client()

    def poll(self):
        while True:
            task, event_history = self.poll_for_task(
                domain=self.workflow.domain,
                identity=self.workflow.name,
                tasklist=self.workflow.tasklist
            )

            if task is None:
                continue

            last_event = event_history[-1]
            log.debug("Received new task '%s'", last_event["eventType"])

            meta = utils.get_task_meta(task, self.workflow.domain, self.workflow.tasklist)
            client = self.client

            self.meta = meta
            self.workflow.meta = meta
            self.workflow.client = client
            self.workflow.identifier = "com.{0}.{1}.{2}.{3}".format(
                meta.domain, self.workflow.name, meta.workflow_id, meta.run_id
            )

            try:
                self.entrypoint(meta, event_history)
            except (exceptions.EventException, exceptions.DeciderException) as e:
                log.error("Workflow failed")
                self.fail_workflow(
                    client, meta.task_token,
                    reason=e.__class__.__name__,
                    details=e.message
                )
            except exceptions.RetryLimitExceededException as e:
                message = "Retry limit exceeded, failing workflow"
                log.error(message)
                self.fail_workflow(
                    client, meta.task_token,
                    reason=e.__class__.__name__,
                    details=e.message
                )
            except exceptions.TimerStarted:
                continue
            except exceptions.ActivityTimeoutException:
                continue
            except exceptions.ActivityFailedException:
                continue
            except exceptions.ActivityTaskScheduled:
                continue
            except exceptions.WorkflowComplete:
                self.complete_workflow(client, meta.task_token)
            except ClientError as e:
                log.error(e.message)
                self.fail_workflow(
                    client, meta.task_token,
                    reason=e.__class__.__name__,
                    details=e.message
                )
            except Exception as e:
                log.error("Unhandled Workflow Failure %s", e.message)
                log.exception(e)
                self.fail_workflow(
                    client, meta.task_token,
                    reason=e.__class__.__name__,
                    details=e.message
                )

    def complete_workflow(self, client, task_token, result="success"):
        try:
            utils.complete_workflow(client, task_token, result=result)
        except ClientError as e:
            log.error("Unable to complete workflow: %s", e.message)

    def fail_workflow(self, client, task_token, reason, details=""):
        try:
            utils.fail_workflow(client, task_token, reason=reason, details=details)
        except ClientError as e:
            log.error("Unable to fail workflow: %s", e.message)

    def entrypoint(self, meta, event_history):
        workflow = self.workflow
        events = [prepare_event(meta, evt, event_history) for evt in event_history]

        # event[0] should be WorkflowExecutionStarted, that will have workflow_name
        # and workflow_version, which we will use to match the entry point to
        # execute below
        try:
            assert(events[0].type == "WorkflowExecutionStarted")
        except:
            message = "Expected first event to be 'WorkflowExecutionStarted', found '{0}'".format(events[0].type)
            log.error(message)
            raise exceptions.DeciderException(message)

        version = events[0].workflow_version

        try:
            entrypoint = [
                name for name, obj in workflow.__class__.__dict__.iteritems()
                if hasattr(obj, "is_entrypoint") and obj.version == version
            ][0]
        except IndexError:
            log.error("No @entrypoint defined on workflow '%s' @ %s", workflow.__class__, version)

        method = getattr(workflow, entrypoint)
        method(meta, events)

    def filter_out_decision_events(self, events):
        events = (evt for evt in events if not evt["eventType"].startswith("Decision"))
        return events

    def poll_for_task(self, domain, identity, tasklist, next_page_token=None):
        client = self.client

        if next_page_token is None:
            log.debug("Listening for Decision Task on '%s@%s'", tasklist, domain)
        else:
            log.debug("Fetching next page for '%s@%s'", tasklist, domain)

        task = utils.poll_for_decision_task(
            client=client,
            domain=domain,
            identity=identity,
            tasklist=tasklist,
            next_page_token=next_page_token
        )

        if task is None:
            return task, []

        events = task["events"]
        events = list(self.filter_out_decision_events(events))

        log.debug("Filtered events:\n%s", pprint.pformat(events))

        next_page = task.get('nextPageToken')

        if next_page:
            _, more_events = self.poll_for_task(domain, identity, tasklist, next_page_token=next_page)
            events.extend(more_events)

        return task, events
