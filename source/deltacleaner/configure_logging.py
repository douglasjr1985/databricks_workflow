import logging
import sys

class LoggingConfigurator:
    """
    Class to configure logging settings.
    """

    def __init__(self, level=logging.INFO, log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
        """
        Initialize LoggingConfigurator with specified logging level and format.

        :param level: Logging level (e.g., logging.INFO, logging.DEBUG)
        :param log_format: Format for logging output
        """
        self.level = level
        self.log_format = log_format

    def configure_logging(self):
        """
        Configures logging with the specified level and format.
        """
        logging.basicConfig(stream=sys.stdout, level=self.level, format=self.log_format)

