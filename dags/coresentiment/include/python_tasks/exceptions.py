from datetime import date

class CoreSentimentException(Exception):
    """ Base class for all CTPExceptions exceptions"""


class FileNotReadyError(CoreSentimentException):
    def __init__(self, dump_date: date, dump_hour: int):
        super().__init__()
        self.log = f"File not available for {dump_date} hour {dump_hour}"

