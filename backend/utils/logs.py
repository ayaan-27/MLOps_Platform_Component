import logging
from logging.handlers import TimedRotatingFileHandler


def get_logger(filename="Pace_log.log"):
    FORMATTER = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(process)d - %(filename)s - %(funcName)s -  %(message)s"
    )
    LOGGER = logging.getLogger("logger")
    LOGGER.setLevel("INFO")

    if not LOGGER.hasHandlers():
        LOG_HANDLER = TimedRotatingFileHandler(filename, when="midnight", interval=1)
        LOG_HANDLER.suffix = "%Y%m%d"
        LOG_HANDLER.setLevel("INFO")
        LOG_HANDLER.setFormatter(FORMATTER)
        LOGGER.addHandler(LOG_HANDLER)
    return LOGGER
