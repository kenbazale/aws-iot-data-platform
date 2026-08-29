from dataclasses import dataclass
import random

from iot_platform.simulator.device import IoTDevice


@dataclass
class DeviceFleet:
    """
    Collection of simulated IoT devices.

    The fleet is responsible for creating devices with different
    operating characteristics. Individual devices remain responsible
    for generating their own telemetry.
    """

    devices: list[IoTDevice]

    @classmethod
    def create(
        cls,
        number_of_devices: int,
        seed: int | None = None,
    ) -> "DeviceFleet":
        """
        Create a fleet of simulated IoT devices.

        Args:
            number_of_devices: Number of devices to create.
            seed: Optional random seed for reproducible tests.

        Returns:
            DeviceFleet containing the requested number of devices.
        """

        if number_of_devices <= 0:
            raise ValueError("number_of_devices must be greater than zero")

        if seed is not None:
            random.seed(seed)

        devices = []

        for i in range(1, number_of_devices + 1):
            device = IoTDevice(
                device_id=f"device-{i:06d}",
                device_type="industrial_cooler",
                firmware_version="1.0.0",
                base_temperature=random.uniform(-20.0, -15.0),
                base_humidity=random.uniform(50.0, 70.0),
                base_pressure=random.uniform(1.7, 2.0),
                latitude=random.uniform(-17.0, -14.0),
                longitude=random.uniform(33.0, 36.0),
            )
            devices.append(device)

        return cls(devices=devices)

    def generate_events(self) -> list[dict]:
        """Generate one telemetry event from every device."""

        return [device.generate_event() for device in self.devices]
