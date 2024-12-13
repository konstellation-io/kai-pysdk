import pytest
from loguru import logger


@pytest.fixture(scope="session", autouse=True)
def configure_logger():
    LOGGER_FORMAT = "{time} {level} {message}"

    logger.remove()
    logger.add(
        "stdout",
        colorize=True,
        format=LOGGER_FORMAT,
        backtrace=True,
        diagnose=True,
        level="ERROR",
    )

    logger.configure(extra={"context": "", "metadata": {}, "origin": "[TEST]"})

    logger.bind(context="[TEST]")

    yield

    logger.remove()
