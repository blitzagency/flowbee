from __future__ import absolute_import
import logging
from .. import exceptions
from . import events

log = logging.getLogger(__name__)


def maybe_get_event_type(event):
    try:
        event_type = event["eventType"]
    except KeyError:
        pass

    return event_type


def prepare_event(meta, event, event_history):
    event_type = maybe_get_event_type(event)

    if event_type is None:
        message = "Unable to find 'eventType' in {0}".format(event)
        log.error(message)
        raise exceptions.EventException(message)

    event_class = {
        "WorkflowExecutionStarted": events.WorkflowExecutionStarted,
        "TimerFired": events.TimerFired,
        "TimerStarted": events.TimerStarted,
        "ScheduleActivityTaskFailed": events.ScheduleActivityTaskFailed,
        "ActivityTaskTimedOut": events.ActivityTaskTimedOut,
        "ActivityTaskScheduled": events.ActivityTaskScheduled,
        "ActivityTaskStarted": events.ActivityTaskStarted,
        "ActivityTaskCompleted": events.ActivityTaskCompleted,
        "ActivityTaskFailed": events.ActivityTaskFailed,
    }.get(event_type)

    if event_class is None:
        message = "Unknown workflow event '{0}'".format(event_type)
        log.error(message)
        raise exceptions.EventException(message)

    log.info("Initializing event class '%s'", event_type)
    event = event_class(meta, event, event_history)
    return event
