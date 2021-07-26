import logging
import logging.handlers
from threading import Lock

class Logger:
    def __init__(self, protected=False):
        self.protected = protected
        if self.protected:
            self.lock = Lock()
        """
        Python custom logging initialization
        Current timestamp is added to be able to identify in docker
        compose logs the date when the log has arrived
        """
        logging.getLogger("pika").setLevel(logging.WARNING)
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.DEBUG,
            datefmt='%Y-%m-%d %H:%M:%S',
        )

    def info(self, msg):
        self._call_logging(msg, logging.info)

    def debug(self, msg):
        self._call_logging(msg, logging.debug)

    def error(self, msg):
        self._call_logging(msg, logging.error)

    def critical(self, msg):
        self._call_logging(msg, logging.critical)

    def _call_logging(self, msg, call_function):
        if self.protected:
            with self.lock:
                call_function(msg)
        else:
            call_function(msg)
