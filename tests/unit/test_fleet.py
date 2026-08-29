import pytest

from iot_platform.simulator.fleet import DeviceFleet


def test_fleet_creates_requested_number_of_devices():
    fleet = DeviceFleet.create(
        number_of_devices=10,
        seed=42,
    )

    assert len(fleet.devices) == 10


def test_fleet_generates_one_event_per_device():
    fleet = DeviceFleet.create(
        number_of_devices=10,
        seed=42,
    )

    events = fleet.generate_events()

    assert len(events) == 10


def test_device_ids_are_unique():
    fleet = DeviceFleet.create(
        number_of_devices=100,
        seed=42,
    )

    device_ids = [
        device.device_id
        for device in fleet.devices
    ]

    assert len(device_ids) == len(set(device_ids))


def test_fleet_rejects_invalid_device_count():
    with pytest.raises(ValueError):
        DeviceFleet.create(
            number_of_devices=0
        )