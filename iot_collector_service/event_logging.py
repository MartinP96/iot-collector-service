import logging
from datetime import datetime


def setup_logger(name, log_file, level=logging.INFO):

    handler = logging.FileHandler(log_file + "_" + '{:%Y-%m-%d}.log'.format(datetime.now()))
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
