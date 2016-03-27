class MessageException(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


class EventException(MessageException):
    pass


class DeciderException(MessageException):
    pass


class TimerStarted(Exception):
    pass


class ActivityTaskScheduled(Exception):
    pass


class ActivityTimeoutException(Exception):
    pass


class ActivityFailedException(Exception):
    pass


class RetryLimitExceededException(Exception):
    pass


class WorkflowComplete(Exception):
    pass
