import pytest

from iot_platform.simulator.fleet import DeviceFleet
from iot_platform.simulator.generator import EventGenerator


def test_generator_produces_expected_event_count():
    fleet = DeviceFleet.create(
        number_of_devices=10,
        seed=42,
    )

    generator = EventGenerator(fleet)

    events = list(
        generator.generate(batches=5)
    )

    assert len(events) == 50


def test_generator_rejects_invalid_batch_count():
    fleet = DeviceFleet.create(
        number_of_devices=10,
    )

    generator = EventGenerator(fleet)

    with pytest.raises(ValueError):
        list(generator.generate(batches=0))

def test_generator_writes_requested_output(tmp_path, monkeypatch):
    from jobs.generate_iot_data import main

    output_dir = tmp_path / "raw"

    config = {
        "simulation": {
            "devices": 2,
        },
        "storage": {
            "raw_path": str(output_dir),
        },
    }

    monkeypatch.setattr(
        "jobs.generate_iot_data.load_config",
        lambda _: config,
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "generate_iot_data.py",
            "--output",
            "telemetry_002.jsonl",
            "--batches",
            "2",
        ],
    )

    main()

    output_file = (
        output_dir / "telemetry_002.jsonl"
    )

    assert output_file.exists()

    lines = output_file.read_text(
        encoding="utf-8"
    ).splitlines()

    assert len(lines) == 4