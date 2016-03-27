from __future__ import absolute_import
import logging
from .. import exceptions
from .. import compression
from .. import utils

log = logging.getLogger(__name__)


class WorkerEvent(object):

    def __init__(self, meta, event):
        self.client = utils.get_client()
        self.meta = meta
        self.event = event
        self.payload = None

        self.prepare_event()

    def prepare_event(self, event):
        raise NotImplementedError()

    def deserialize(self, data):
        return compression.decompress_b64_json(data)

    def serialize(self, data):
        return compression.compress_b64_json(data)


class Activity(WorkerEvent):
    def prepare_event(self):
        # 'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': '3f663919-f2e3-11e5-b16e-770391d15078'},
        #  u'activityId': u'com.fx-cms-publishing.MyWorkflow.publish-044e31df1fe34aa68a0af588c95205ed.23aphJS6isIl+r7ko+cCEDfY3hAWqgNL6HEKBPaBwd60A=.fx-cms-sns-publish@0.0.1-0',
        #  u'activityType': {u'name': u'fx-cms-sns-publish', u'version': u'0.0.1'},
        #  u'input': u'H4sIAJXN9VYC/6tWSixKL1ayUohWSkrMA0JtQ6VYHQWl7HKoeLVSWn4+mE4EkkpJSjogwkrBsLa2FgCh4CSEPQAAAA==',
        #  u'startedEventId': 6,
        #  u'taskToken': u'AAAAKgAAAAIAAAAAAAAAA0AxqlYEAAaUiImvV1oKuPDizfaoElORPr83RQcnPKCzD8YFO3a5jcmBg1c8qSgIbDizaFQF7YUzWMp9j0TIoEyIfPWrWnQW1EQowKF1q69kic6YhniZv8ZsSq0oQN9mdrVg6tYgCu8A6HlBZQBia9OFMcmTYHB/KKbCjsbxJBrQs35u7lWU7z+OW+GkCwOmkd3KN/o5sD3iALBwQSKq5MDRNugNtMuRr07Ry8MgectIa4J9HhHHtYSdaU5qudAEQj1ACEWb249A6rpuhEPoMmGfc9KrcalhBrVu9fbX4MIof30U8BtI3vtJXDpEl0FBBaIJ4NiDi3c4CWIKXYexrrE=',
        #  u'workflowExecution': {u'runId': u'23aphJS6isIl+r7ko+cCEDfY3hAWqgNL6HEKBPaBwd60A=', u'workflowId': u'publish-044e31df1fe34aa68a0af588c95205ed'}}

        data = self.event.get("input")

        try:
            self.name = self.event["activityType"]["name"]
            self.version = self.event["activityType"]["version"]
        except KeyError as e:
            message = "Unable to find '{0}' in task {1}".format(e.message, task)
            log.error(message)
            raise exceptions.EventException(message)

        if data is not None:
            self.payload = self.deserialize(data)
        else:
            self.payload = None
