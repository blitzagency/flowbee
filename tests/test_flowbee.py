import unittest
import uuid
from flowbee import utils


class TestFlowbee(unittest.TestCase):

    def test_action(self):
        return
        client = utils.get_client()

        domain = "fx-cms-publishing"
        tasklist = "fx-cms-publish-tasks"
        name = "fx-cms-sns-publisher-decider-{0}".format(1)

        client.start_workflow_execution(
            domain=domain,
            workflowId="publish-{0}".format(uuid.uuid4().hex),
            workflowType={
                "name": "fx-cms-publish",
                "version": "0.0.1"
            },
            taskList={
                "name": tasklist
            },
            taskPriority="0",
            input="H4sIACfo8lYC/6tWyk0tSVSyUqhWKqksSAUylApKk3IyizOUanUUlFISoZIgUilJSQdEWCkY1tYCAPSlBhY5AAAA",
            executionStartToCloseTimeout="60",  # "31536000",
            taskStartToCloseTimeout="10",
            childPolicy="TERMINATE"
        )

