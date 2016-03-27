from __future__ import absolute_import
import logging
from . import events
from .. import utils
from .. import exceptions

log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, workflow):
        self.workflow = workflow
        self.meta = None
        self.client = utils.get_client()

    def poll(self):
        while True:
            task = self.poll_for_task(
                domain=self.workflow.domain,
                identity=self.workflow.name,
                tasklist=self.workflow.tasklist
            )

            if task is None:
                continue

            meta = utils.get_task_meta(task, self.workflow.domain, self.workflow.tasklist)
            client = self.client

            try:
                activity = events.Activity(meta, task)
            except exceptions.EventException as e:
                log.error(e.message)
                utils.fail_activity(client=client, task_token=meta.task_token, reason=e.message)
                continue

            try:
                action = self.find_activity_in_workflow(activity)
            except NameError as e:
                log.error(e)
                utils.fail_activity(client=client, task_token=meta.task_token, reason=e.message)

            try:
                result = action(*activity.payload["args"], **activity.payload["kwargs"])
            except Exception as e:
                log.exception(e)
                utils.fail_activity(client=client, task_token=meta.task_token, reason=e.message)
                continue

            z_result = activity.serialize(result)
            print("serialized result " + result)

            try:
                utils.complete_activity(
                    client=client,
                    task_token=meta.task_token,
                    result=z_result
                )
            except Exception as e:
                log.error("Unable to notify SWF of activity completion: %s", e.message)

    def find_activity_in_workflow(self, activity):
        activities = self.workflow.activities

        for name, method in activities.__class__.__dict__.iteritems():
            if not hasattr(method, "is_activity"):
                continue

            if method.swf_name == activity.name and \
               method.swf_version == activity.version:
                return getattr(activities, name)

        qualname = "{0}.{1}".format(activities.__module__, activities.__class__.__name__)
        raise NameError(
            "Unable to find method for '{0}@{1}' on {3}".format(
                activity.name, activity.version, qualname)
        )

    def poll_for_task(self, domain, identity, tasklist):
        client = self.client

        log.info("Listening for Activity Task on '%s@%s'", tasklist, domain)

        task = utils.poll_for_activity_task(
            client=client,
            domain=domain,
            identity=identity,
            tasklist=tasklist
        )

        if task is None:
            return task

        return task
