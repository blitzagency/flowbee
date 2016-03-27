import unittest
import logging
from flowbee.workers import Worker
from flowbee.deciders import Decider
from flowbee.cli.test import MyWorkflow

log = logging.getLogger("flowbee.test")


class TestFlow(unittest.TestCase):

    def test_create_resources(self):
        return
        from flowbee.cli.runner import Runner

        runner = Runner(workers=1, workflow="flowbee.cli.test.MyWorkflow", pidfile="")
        runner.create_resources()

    def test_flow(self):
        for each in xrange(2):
            MyWorkflow.start_execution(
                input={"meta": {}, "data": {"a": "b", "b": 1}},
                version="0.0.1",
                execution_start_to_close_timeout="60",
                task_start_to_close_timeout="10"
            )

        for each in xrange(0):
            MyWorkflow.start_execution(
                input={"meta": {}, "data": {"a": "b", "b": 1}},
                version="0.0.2",
                execution_start_to_close_timeout="60",
                task_start_to_close_timeout="10"
            )

        return

        import signal
        import sys
        from multiprocessing import Process

        def decider():
            def signal_handler(signal, frame):
                print 'CHILD SIGINT [decider]'
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)

            decider = Decider(MyWorkflow())
            decider.poll()

        def worker():
            def signal_handler(signal, frame):
                print 'CHILD SIGINT [worker]'
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)

            worker = Worker(MyWorkflow())
            worker.poll()

        processes = []

        def signal_handler(signal, frame):
            print 'MASTER SIGINT'
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        #for each in [decider]:
        for each in [worker, decider]:
            p = Process(target=each)
            p.daemon = True
            p.start()
            processes.append(p)

        for process in processes:
            process.join()





