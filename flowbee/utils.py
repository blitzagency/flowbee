from __future__ import absolute_import
import pprint
import logging
import importlib
from itertools import repeat
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from .models import TaskMeta


log = logging.getLogger(__name__)


def import_class(qualname):
    parts = qualname.split(".")
    module_name = '.'.join(parts[0:-1])
    class_name = parts[-1]

    return import_action(from_name=module_name, import_name=class_name)


def import_action(from_name, import_name=None):
    try:
        module = importlib.import_module(from_name)
    except ImportError as e:
        log.exception(e)
        raise

    if import_name is None:
        return module

    try:
        return getattr(module, import_name)
    except AttributeError as e:
        log.exception(e)
        raise


def get_client():
    boto_config = Config(connect_timeout=50, read_timeout=70)
    swf = boto3.client('swf', config=boto_config)
    return swf


def get_workflow_data(workflow_class):
    domain = workflow_class.domain
    tasklist = workflow_class.tasklist
    workflow_type_versions = []
    activities = []
    # workflow_class.name is set with @workflow() from activities.utils
    workflow_type_name = "{0}.{1}".format(
        workflow_class.name, workflow_class.activities.name
    )

    # get the entrypoint versions for our workflow types
    for name, method in workflow_class.__dict__.iteritems():
        if not hasattr(method, "is_entrypoint"):
            continue

        workflow_type_versions.append(method.version)

    for name, method in workflow_class.activities.__class__.__dict__.iteritems():
        if not hasattr(method, "is_activity"):
            continue

        activities.append((method.swf_name, method.swf_version))

    # namedtuple might be better here
    return {
        "domain": domain,
        "tasklist": tasklist,
        "workflows": zip(repeat(workflow_type_name), workflow_type_versions),
        "activities": activities
    }


def create_resources(workflow_class):
    client = get_client()
    data = get_workflow_data(workflow_class)

    domain = data["domain"]
    tasklist = data["tasklist"]
    workflows = data["workflows"]
    activities = data["activities"]

    create_domain(client, domain)

    for name, version in workflows:
        create_workflow(client, domain, name, version, tasklist)

    for name, version in activities:
        create_activity(client, domain, name, version, tasklist)


def create_domain(client, domain, description="", retention_period=1):
    log.debug("Creating SWF Domain: '%s'", domain)
    try:
        client.register_domain(
            name=domain,
            description=description,
            workflowExecutionRetentionPeriodInDays=str(retention_period)
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        log.debug("Domain already exists '%s'", code)


def create_workflow(client, domain, workflow, version, tasklist, description="", max_execution_length=(86400 * 365)):
    log.debug(
        "Creating SWF Workflow: '%s:%s@%s' on task list: '%s'",
        workflow, version, domain, tasklist
    )

    try:
        client.register_workflow_type(
            domain=domain,
            name=workflow,
            version=version,
            description=description,
            defaultExecutionStartToCloseTimeout=str(max_execution_length),
            defaultTaskStartToCloseTimeout="NONE",
            defaultChildPolicy="TERMINATE",
            defaultTaskList={"name": tasklist}
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        log.debug("Workflow already exists '%s'", code)


def create_activity(client, domain, activity, version, tasklist, description=""):
    log.debug(
        "Creating SWF Activity: '%s:%s@%s' on task list: '%s'",
        activity, version, domain, tasklist
    )
    try:
        client.register_activity_type(
            domain=domain,
            name=activity,
            version=version,
            description=description,
            defaultTaskStartToCloseTimeout="NONE",
            defaultTaskList={"name": tasklist}
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        log.debug("Activity '%s:%s' already exists '%s'", activity, version, code)


def schedule_later(client, task_token, seconds, timer_id, payload=None):

    decision = {
        "timerId": timer_id,
        "startToFireTimeout": str(seconds)
    }

    if payload is not None:
        decision["control"] = payload

    client.respond_decision_task_completed(
        taskToken=task_token,
        decisions=[{
            "decisionType": "StartTimer",
            "startTimerDecisionAttributes": decision
        }]
    )


def schedule_activity(
        client, task_token, name, version, activity_id,
        tasklist, payload="", close_timeout="NONE", start_timeout="10",
        timeout="10", heartbeat_timeout="NONE", priority=0, attempt=0):

    client.respond_decision_task_completed(
        taskToken=task_token,
        decisions=[{
            "decisionType": "ScheduleActivityTask",
            "scheduleActivityTaskDecisionAttributes": {
                "activityId": "{0}-{1}".format(activity_id, attempt),
                "input": payload,
                "taskPriority": str(priority),
                "scheduleToCloseTimeout": timeout,  # maximum duration for this task
                "scheduleToStartTimeout": start_timeout,  # maximum duration the task can wait to be assigned to a worker
                "startToCloseTimeout": close_timeout,  # maximum duration a worker may take to process this task
                "heartbeatTimeout": heartbeat_timeout,  # maximum time before which a worker processing a task of this type must report progress
                "activityType": {
                    "name": name,
                    "version": version
                },
                "taskList": {
                    "name": tasklist
                },
            }
        }]
    )


def schedule_activity_later(client, task_token, payload, timer_id):
    later = 5
    schedule_later(
        client=client,
        task_token=task_token,
        seconds=later,
        payload=payload,
        timer_id=timer_id
    )
    log.info("Scheduled task for later: '%ss' with payload '%s' %s'", later, payload, timer_id)


def cancel_workflow(client, task_token, reason=""):
    client.respond_decision_task_completed(
        taskToken=task_token,
        decisions=[{
            "decisionType": "CancelWorkflowExecution",
            "cancelWorkflowExecutionDecisionAttributes": {
                "details": reason
            }
        }]
    )


def complete_activity(client, task_token, result=None):
    client.respond_activity_task_completed(
        taskToken=task_token,
        result=result
    )


def fail_activity(client, task_token, reason, details=""):
    client.respond_activity_task_failed(
        taskToken=task_token,
        reason=reason,
        details=details
    )


def fail_workflow(client, task_token, reason, details=""):
    client.respond_decision_task_completed(
        taskToken=task_token,
        decisions=[{
            "decisionType": "FailWorkflowExecution",
            "failWorkflowExecutionDecisionAttributes": {
                "reason": reason,
                "details": details
            }
        }]
    )


def complete_workflow(client, task_token, result="success"):
    client.respond_decision_task_completed(
        taskToken=task_token,
        decisions=[{
            "decisionType": "CompleteWorkflowExecution",
            "completeWorkflowExecutionDecisionAttributes": {
                "result": result
            }
        }]
    )


def poll_for_decision_task(client, domain, identity, tasklist, next_page_token=None):
    params = {
        "domain": domain,
        "taskList": {"name": tasklist},
        "identity": identity,
        "reverseOrder": False
    }

    if next_page_token:
        params["nextPageToken"] = next_page_token

    try:
        task = client.poll_for_decision_task(**params)
    except ClientError as e:
        log.error(e.message)
        return None

    log.debug("Received new decision task: \n%s", pprint.pformat(task))
    if "taskToken" not in task:
        log.info("Poll timed out, no new task.")
        return None

    if "events" not in task:
        log.info("No events found in new task")
        return None

    return task


def poll_for_activity_task(client, domain, identity, tasklist):
    params = {
        "domain": domain,
        "taskList": {"name": tasklist},
        "identity": identity,
    }

    try:
        task = client.poll_for_activity_task(**params)
    except ClientError as e:
        print("WORKER FAILURE")
        log.error(e.message)
        return None

    log.debug("Received new activity task: \n%s", pprint.pformat(task))

    if "taskToken" not in task:
        log.info("Poll timed out, no new task.")
        return None

    return task


def get_task_meta(task, domain, tasklist):
    task_token = task["taskToken"]
    run_id = task["workflowExecution"]["runId"]
    workflow_id = task["workflowExecution"]["workflowId"]

    meta = TaskMeta(
        task_token=task_token,
        run_id=run_id,
        workflow_id=workflow_id,
        domain=domain,
        tasklist=tasklist
    )

    return meta
