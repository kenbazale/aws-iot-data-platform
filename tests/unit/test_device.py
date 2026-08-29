from iot_platform.simulator.device import IoTDevice


def create_test_device(battery_level: float = 100.0) -> IoTDevice:
    return IoTDevice(
        device_id="device-000001",
        device_type="industrial_cooler",
        firmware_version="1.0.0",
        base_temperature=-17.0,
        base_humidity=60.0,
        base_pressure=1.8,
        latitude=-15.786,
        longitude=35.005,  
        battery_level=battery_level,
    )

def test_event_contains_required_fields():
    device = create_test_device()
    
    event = device.generate_event()
    
    required_fields = {
        "event_id",
        "device_id",
        "device_type",
        "event_timestamp",
        "temperature",
        "humidity",
        "pressure",
        "battery_level",
        "latitude",
        "longitude",
        "firmware_version",
    }
    assert required_fields.issubset(event.keys())

def test_event_has_correct_device_id():
    device = create_test_device()

    event = device.generate_event()

    assert event["device_id"] == "device-000001"


def test_battery_never_becomes_negative():
    device = create_test_device(battery_level=0.001)

    event = device.generate_event()

    assert event["battery_level"] >= 0
    

def test_event_ids_are_unique():
    device = create_test_device()

    events = [
        device.generate_event()
        for _ in range(1000)
    ]

    event_ids = [
        event["event_id"]
        for event in events
    ]

    assert len(event_ids) == len(set(event_ids))