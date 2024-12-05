from unittest.mock import Mock, patch

import pytest
from opentelemetry.metrics import Meter
from vyper import v

from sdk.measurements.exceptions import FailedToInitializeMeasurementsError
from sdk.measurements.measurements import Measurements


@pytest.fixture
def mock_meter():
    return Mock(spec=Meter)


@patch.object(Measurements, "_setup_metrics")
def test_ok(_):
    v.set("measurements.endpoint", "localhost:4317")
    v.set("measurements.insecure", True)
    v.set("measurements.timeout", 5)
    v.set("measurements.metrics_interval", 1)
    expected_metrics_client = Mock(spec=Meter)
    m_measurements = Measurements()
    m_measurements.metricsClient = expected_metrics_client

    assert m_measurements.get_metrics_client() == expected_metrics_client


@patch.object(Measurements, "_setup_metrics", side_effect=Exception("error"))
def test_ko(_):
    v.set("measurements.endpoint", "localhost:4317")
    v.set("measurements.insecure", True)
    v.set("measurements.timeout", 5)
    v.set("measurements.metrics_interval", 1)
    with pytest.raises(FailedToInitializeMeasurementsError):
        measurements = Measurements()

        assert measurements.metrics_client is None
