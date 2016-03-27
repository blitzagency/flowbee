"""SWF Event Types

Possible Decider Events:
http://boto3.readthedocs.org/en/latest/reference/services/swf.html#SWF.Client.poll_for_decision_task

WorkflowExecutionStarted
WorkflowExecutionCancelRequested
WorkflowExecutionCompleted
CompleteWorkflowExecutionFailed
WorkflowExecutionFailed
FailWorkflowExecutionFailed
WorkflowExecutionTimedOut
WorkflowExecutionCanceled
CancelWorkflowExecutionFailed
WorkflowExecutionContinuedAsNew
ContinueAsNewWorkflowExecutionFailed
WorkflowExecutionTerminated
DecisionTaskScheduled
DecisionTaskStarted
DecisionTaskCompleted
DecisionTaskTimedOut
ActivityTaskScheduled
ScheduleActivityTaskFailed
ActivityTaskStarted
ActivityTaskCompleted
ActivityTaskFailed
ActivityTaskTimedOut
ActivityTaskCanceled
ActivityTaskCancelRequested
RequestCancelActivityTaskFailed
WorkflowExecutionSignaled
MarkerRecorded
RecordMarkerFailed
TimerStarted
StartTimerFailed
TimerFired
TimerCanceled
CancelTimerFailed
StartChildWorkflowExecutionInitiated
StartChildWorkflowExecutionFailed
ChildWorkflowExecutionStarted
ChildWorkflowExecutionCompleted
ChildWorkflowExecutionFailed
ChildWorkflowExecutionTimedOut
ChildWorkflowExecutionCanceled
ChildWorkflowExecutionTerminated
SignalExternalWorkflowExecutionInitiated
SignalExternalWorkflowExecutionFailed
ExternalWorkflowExecutionSignaled
RequestCancelExternalWorkflowExecutionInitiated
RequestCancelExternalWorkflowExecutionFailed
ExternalWorkflowExecutionCancelRequested
LambdaFunctionScheduled
LambdaFunctionStarted
LambdaFunctionCompleted
LambdaFunctionFailed
LambdaFunctionTimedOut
ScheduleLambdaFunctionFailed
StartLambdaFunctionFailed
"""

import logging
from .. import exceptions
from .. import compression
from .. import utils

log = logging.getLogger(__name__)


class DeciderEvent(object):

    def __init__(
            self, meta, event, event_history):
        self.client = utils.get_client()
        self.meta = meta
        self.type = event["eventType"]
        self.event = event
        self.event_history = event_history
        self.payload = None

        self.prepare_event()

    def prepare_event(self, event):
        raise NotImplementedError()

    def deserialize(self, data):
        return compression.decompress_b64_json(data)

    def serialize(self, data):
        return compression.compress_b64_json(data)


class WorkflowExecutionStarted(DeciderEvent):
    def prepare_event(self):
        try:
            attributes = self.event["workflowExecutionStartedEventAttributes"]
        except KeyError as e:
            message = "Unable to lookup '{0}' in {1}".format(e.message, self.event)
            log.error(message)
            raise exceptions.EventException(message=message)

        data = attributes.get("input", None)

        try:
            self.workflow_name = attributes["workflowType"]["name"]
            self.workflow_version = attributes["workflowType"]["version"]
        except KeyError as e:
            message = "Unable to lookup '{0}' in {1}".format(e.message, attributes)
            log.error(message)
            raise exceptions.EventException(message=message)

        if data is not None:
            self.payload = self.deserialize(data)
        else:
            self.payload = None


class ActivityAbstractFailure(DeciderEvent):
    def retry(self):
        log.info(
            "Retrying task '%s@%s'. Retry attempt: %s",
            self.task_name, self.task_version, self.num_retries
        )
        utils.schedule_activity(
            client=self.client,
            tasklist=self.tasklist,
            activity_id=self.activity_id,
            task_token=self.meta.task_token,
            name=self.task_name,
            version=self.task_version,
            payload=self.payload,
            attempt=self.num_retries
        )

    def process_history(self, attributes):
        try:
            scheduled_event_id = attributes["scheduledEventId"]
        except KeyError as e:
            message = "Unable to lookup '{0}' in {1}".format(e.message, self.event)
            log.error(message)
            raise exceptions.EventException(message=message)

        try:
            scheduled_activity_event = [evt for evt in self.event_history if evt["eventId"] == scheduled_event_id][0]
        except IndexError:
            message = "Unable to find event id '{0}' in event_history".format(scheduled_event_id)
            log.error(message)
            raise exceptions.EventException(message=message)

        try:
            activity = scheduled_activity_event["activityTaskScheduledEventAttributes"]
        except KeyError as e:
            message = "Unable to lookup '{0}' in {1}".format(e.message, scheduled_activity_event)
            log.error(message)
            raise exceptions.EventException(message=message)

        try:
            self.activity_id = activity["activityId"].rsplit("-", 1)[0]
            self.tasklist = activity["taskList"]["name"]
            self.task_name = activity["activityType"]["name"]
            self.task_version = activity["activityType"]["version"]
            self.payload = activity["input"]
        except KeyError as e:
            message = "Unable to find key '{0}' in 'activityTaskScheduledEventAttributes'".format(e.message)
            log.error(message)
            raise exceptions.EventException(message=message)

        self.num_retries = sum([
            1 for evt in self.event_history
            if evt["eventType"] == "ActivityTaskScheduled" and
            evt["activityTaskScheduledEventAttributes"]["activityId"].startswith(self.activity_id)
        ])


class ActivityTaskTimedOut(ActivityAbstractFailure):

    def prepare_event(self):
        self.num_retries = 0
        attributes = self.event.get("activityTaskTimedOutEventAttributes")
        self.process_history(attributes)


class ActivityTaskFailed(ActivityAbstractFailure):
    def prepare_event(self):
        self.num_retries = 0
        attributes = self.event.get("activityTaskFailedEventAttributes")
        self.process_history(attributes)


class ActivityTaskStarted(DeciderEvent):
    def prepare_event(self):
        # {
        #     u'activityTaskStartedEventAttributes': {u'identity': u'fx-cms-publisher-worker-1', u'scheduledEventId': 8},
        #     u'eventId': 9,
        #     u'eventTimestamp': datetime.datetime(2016, 3, 25, 15, 2, 43, 874000, tzinfo=tzlocal()),
        #     u'eventType': u'ActivityTaskStarted'
        # }
        return


class ActivityTaskScheduled(DeciderEvent):
    def prepare_event(self):
        try:
            attributes = self.event["activityTaskScheduledEventAttributes"]
        except KeyError:
            message = "Unable to lookup '{0}' in {1}".format(e.message, self.event)
            log.error(message)
            raise exceptions.EventException(message=message)

        self.tasklist = attributes["taskList"]["name"]
        self.priority = attributes["taskPriority"]
        self.name = attributes["activityType"]["name"]
        self.version = attributes["activityType"]["version"]
        self.activity_id = attributes["activityId"]

        data = attributes.get("input", None)

        if data is not None:
            self.payload = self.deserialize(data)
        else:
            self.payload = None


class ActivityTaskCompleted(DeciderEvent):
    def prepare_event(self):
        data = self.event \
            .get("activityTaskCompletedEventAttributes", {}) \
            .get("result", None)

        if data is not None:
            self.payload = self.deserialize(data)
        else:
            self.payload = None


class ScheduleActivityTaskFailed(DeciderEvent):
    def prepare_event(self):
        attributes = self.event["scheduleActivityTaskFailed"]
        activity_id = attributes.get("activityId", "unknown activity id")
        activity_name = attributes.get("activityType", {}).get("name", "unknown name")
        activity_version = attributes.get("activityType", {}).get("version", "unknown version")
        cause = attributes.get("cause", "unknown")

        message = "Failed to schedule activity[%s@%s]: %s - %s" \
            .format(cause, activity_name, activity_version, activity_id)

        log.error(message)
        raise exceptions.EventException(message=message)


class TimerStarted(DeciderEvent):
    def prepare_event(self):

        try:
            attributes = self.event["timerStartedEventAttributes"]
        except KeyError:
            message = "Unable to locate 'timerStartedEventAttributes' on {0}".format(self.event)
            log.error(message)
            raise exceptions.EventException(message=message)

        self.timer_id = attributes["timerId"]
        self.seconds = int(attributes["startToFireTimeout"])
        try:
            data = attributes["control"]
        except KeyError:
            data = None

        if data is None:
            self.payload = None
        else:
            self.payload = self.deserialize(data)


class TimerFired(DeciderEvent):
    def prepare_event(self):
        timer_id = self.event.get("timerFiredEventAttributes", {}).get("timerId")
        self.timer_id = timer_id

        if timer_id is None:
            message = "Unable to locate 'timerId' on 'timerFiredEventAttributes'"
            log.error(message)
            raise exceptions.EventException(message=message)

        try:
            timer_started_event = [
                x for x in self.event_history
                if x["eventType"] == "TimerStarted" and
                x["timerStartedEventAttributes"]["timerId"] == timer_id][0]
        except KeyError as e:
            message = "Failed to find key in event_history '{0}'".format(e.message)
            log.error(message)
            raise exceptions.EventException(message=message)

        except IndexError as e:
            message = "Failed to locate corresponding 'TimerStarted' event with id '{0}'".format(timer_id)
            log.error(message)
            raise exceptions.EventException(message=message)

        data = timer_started_event \
            .get("timerStartedEventAttributes", {}) \
            .get("control", None)

        if data is not None:
            self.payload = self.deserialize(data)
        else:
            self.payload = None
