class StreamError(Exception):
    """
    Base class for errors with wiki edit stream
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self) -> str:
        return f"{self.msg}"

class PrimaryStreamError(StreamError):
    """Error with primary stream"""

class BackupStreamError(StreamError):
    """Error with backup stream"""