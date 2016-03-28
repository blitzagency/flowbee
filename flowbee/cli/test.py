from __future__ import absolute_import
import logging
from flowbee.activities import (Activities, Workflow)
from flowbee.activities.utils import (activity, entrypoint, workflow)


log = logging.getLogger(__name__)


class MyActivities(Activities):
    @activity(version="0.0.1", retries=3)
    def stage1(self, flavor, foo):
        log.info("'flavor' arg: %s", flavor)
        log.info("'foo' kwarg: %s", foo)
        return "strawberries"


@workflow(domain="flowbee-test", tasklist="flowbee-test-tasks")
class MyWorkflow(Workflow):
    activities = MyActivities()

    @entrypoint(version="0.0.1")
    def start_v1(self, input=None):
        self.activities.sleep(5)
        result = self.activities.stage1("lucy", foo=input["data"])
        self.activities.sleep(5)
        log.info("Got Result: %s", result)

    @entrypoint(version="0.0.2")
    def start_v2(self, input=None):
        result = self.activities.stage1("ollie", foo=input["data"])
        log.info("Got Result: %s", result)
