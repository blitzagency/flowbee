## Demo


#### The Code
```python
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

```


#### Start the Decider and the Workers:

```
# do each of these in separate terminals
# starting either the worker or decider ensures the
# SWF resources are created on your AWS account
# If you don't want this behavior pass --no-sync

python -m flowbee.cli.main --help

or from the source:

https://github.com/blitzagency/flowbee/blob/master/flowbee/cli/main.py


python -m flowbee.cli.main \
    --type worker \                                                   # worker or decider
    -w 2 \                                                            # num listeners
    -e configs/prod.env \                                             # any anvironment vars to load
    -f path.to.my.Workflow \                                          # what workflow are we executing
    --pidfile /tmp/scheduled_publishing_worker.pid \                  # pid file
    --log-config configs/logging.json                                 # logging config

python -m flowbee.cli.main \
    --type decider \                                                  # worker or decider
    -w 2 \                                                            # num listeners
    -e configs/prod.env \                                             # any anvironment vars to load
    -f path.to.my.Workflow \                                          # what workflow are we executing
    --pidfile /tmp/scheduled_publishing_decider.pid \                 # pid file
    --log-config configs/logging.json                                 # logging config
```

Note the above invocations are the same with the exception of `--type`

Now lets invoke it:

```python
for each in xrange(2):
    MyWorkflow.start_execution(
        input={"meta": {}, "data": {"a": "b", "b": 1}},
        version="0.0.1",
        execution_start_to_close_timeout="60",
        task_start_to_close_timeout="10"
    )

for each in xrange(2):
    MyWorkflow.start_execution(
        input={"meta": {}, "data": {"a": "b", "b": 1}},
        version="0.0.2",
        execution_start_to_close_timeout="60",
        task_start_to_close_timeout="10"
    )
```
