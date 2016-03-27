from flowbee.activities import (Activities, Workflow)
from flowbee.activities.utils import (activity, entrypoint, workflow)


class MyActivities(Activities):
    @activity(version="0.0.1", retries=3)
    def stage1(self, flavor, foo):
        print("Flavor of the day is " + flavor)
        print("And some delicious " + str(foo))
        return "strawberries"


@workflow(domain="flowbee-test", tasklist="flowbee-test-tasks")
class MyWorkflow(Workflow):
    activities = MyActivities()

    @entrypoint(version="0.0.1")
    def start_v1(self, input=None):
        self.activities.sleep(5)
        result = self.activities.stage1("lucy", foo=input["data"])
        self.activities.sleep(5)
        print("GOT RESULT:", result)

    @entrypoint(version="0.0.2")
    def start_v2(self, input=None):
        result = self.activities.stage1("ollie", foo=input["data"])
        print("GOT RESULT:", result)
