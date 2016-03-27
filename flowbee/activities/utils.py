from __future__ import absolute_import
import logging
from collections import deque
from functools import wraps
from .. import exceptions
from .. import utils
from .. import compression

log = logging.getLogger(__name__)


def timer(func):
    @wraps(func)
    def action(self, *args, **kwargs):
        event = None

        try:
            event = self.event_queue.popleft()
        except IndexError:
            pass

        if event is None:
            func(self, *args, **kwargs)
            raise exceptions.TimerStarted()

        if event.type == "TimerStarted":
            next_event = self.event_queue.popleft()
            try:
                assert(next_event.type == "TimerFired")
            except:
                log.error(
                    "Expected next event to be TimerFired, got '%s'",
                    next_event.type
                )
                raise

    return action


def activity(name=None, version="0.0.1", retries=0):
    def wrap(func):
        name_value = name

        if name_value is None:
            name_value = func.func_name

        func.swf_version = version
        func.swf_name = name_value
        func.swf_retries = retries
        func.is_activity = True

        @wraps(func)
        def action(self, *args, **kwargs):
            if self.is_decider is False:
                result = func(self, *args, **kwargs)
                return result
            else:
                event = None
                try:
                    event = self.event_queue.popleft()
                except IndexError:
                    pass

                if event is None:
                    payload = {"args": args, "kwargs": kwargs}
                    z_payload = compression.compress_b64_json(payload)

                    activity_id = "{0}.{1}@{2}".format(
                        self.workflow.identifier, func.swf_name, func.swf_version
                    )

                    log.info("Scheduling Activity '%s@%s' with payload %s", func.swf_name, func.swf_version, z_payload)
                    print("Scheduling Activity")
                    utils.schedule_activity(
                        client=self.client,
                        tasklist=self.workflow.meta.tasklist,
                        activity_id=activity_id,
                        task_token=self.workflow.meta.task_token,
                        name=func.swf_name,
                        version=func.swf_version,
                        payload=z_payload,
                    )
                    raise exceptions.ActivityTaskScheduled()

                if event.type == "ActivityTaskScheduled":
                    # The order should be this
                    # Scheduled
                    # Started | Timeout - It has to start in order to fail
                    # Timeout | Failure | Completed
                    next_event = self.event_queue.popleft()

                    if next_event.type == "ActivityTaskTimedOut":
                        if next_event.num_retries <= func.swf_retries:
                            log.info("ActivityTask timed out, Retrying...")
                            next_event.retry()
                            raise exceptions.ActivityTimeoutException()
                        else:
                            raise exceptions.RetryLimitExceededException()

                    # this would be ActivityTaskStarted
                    # Which would be followed by Failed or Completed
                    if len(self.event_queue) > 0:
                        next_event = self.event_queue.popleft()

                    if next_event.type == "ActivityTaskFailed" or \
                       next_event.type == "ActivityTaskTimedOut":

                        if next_event.num_retries <= func.swf_retries:
                            log.info("ActivityTask timed out, Retrying...")
                            next_event.retry()
                            if next_event.type == "ActivityTaskFailed":
                                raise exceptions.ActivityFailedException()
                            if next_event.type == "ActivityTaskTimedOut":
                                raise exceptions.ActivityTimeoutException()
                        else:
                            raise exceptions.RetryLimitExceededException()

                    if next_event.type == "ActivityTaskCompleted":
                        return next_event.payload
        return action
    return wrap


def entrypoint(version="0.0.1"):
    def wrap(func):
        func.is_entrypoint = True
        func.version = version

        @wraps(func)
        def action(self, meta, events, *args, **kwargs):
            complete = False
            event_queue = deque(events)
            self.activities.event_history = events
            self.activities.event_queue = event_queue
            self.activities.meta = meta
            self.activities.workflow = self

            first_event = event_queue.popleft()

            if first_event.type != "WorkflowExecutionStarted":
                raise exceptions.DeciderException(
                    "Unexpected first event '%s' expecting "
                    "'WorkflowExecutionStarted'", first_event.type
                )

            kwargs["input"] = first_event.payload

            try:
                result = func(self, *args, **kwargs)
                complete = True
            except:
                raise
            if complete:
                raise exceptions.WorkflowComplete()
        return action

    # the user just used @entrypoint with no arguments
    # which means the first argument will be a func not a string
    if callable(version):
        func = version
        version = "0.0.1"
        return wrap(func)

    return wrap


def workflow(domain, tasklist):

    def wrap(cls):
        cls.domain = domain
        cls.tasklist = tasklist

        if not hasattr(cls, "activities"):
            raise ValueError("@workflows must specify an 'activities' class level attribute")

        if not hasattr(cls, "name"):
            cls.name = cls.__name__

        if not hasattr(cls.activities.__class__, "name"):
            cls.activities.__class__.name = cls.activities.__class__.__name__

        return cls

    return wrap
