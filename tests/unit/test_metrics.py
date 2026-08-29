from iot_platform.utils.metrics import PipelineMetrics


def test_validation_rate():
    metrics = PipelineMetrics(
        input_records=100,
        valid_records=95,
        invalid_records=5,
    )

    assert metrics.validation_rate == 95.0


def test_validation_rate_with_no_records():
    metrics = PipelineMetrics()

    assert metrics.validation_rate == 0.0