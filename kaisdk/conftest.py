import sys

import pytest
from loguru import logger


@pytest.fixture(scope="session", autouse=True)
def configure_logger():
    LOGGER_FORMAT = "{time} {level} {message}"

    logger.remove()
    logger.add(
        sys.stdout,
        colorize=True,
        format=LOGGER_FORMAT,
        backtrace=True,
        diagnose=True,
        level="ERROR",
    )

    logger.configure(extra={"context": "[TEST]", "metadata": {}, "origin": "[TEST]"})

    yield

    logger.remove()
