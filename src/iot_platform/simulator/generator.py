from collections.abc import Iterator

from iot_platform.simulator.fleet import DeviceFleet


class EventGenerator:
    """
    Generates telemetry events from a device fleet.

    The generator is intentionally independent of the destination.
    Events can later be written to:

        - local JSON
        - S3
        - Kinesis
        - Kafka

    without changing the device simulation logic.
    """

    def __init__(self, fleet: DeviceFleet):
        self.fleet = fleet

    def generate_batch(self) -> list[dict]:
        """
        Generate one event from every device.
        """

        return self.fleet.generate_events()

    def generate(
        self,
        batches: int,
    ) -> Iterator[dict]:
        """
        Generate events for multiple batches.

        Each batch represents one telemetry interval.
        """

        if batches <= 0:
            raise ValueError(
                "batches must be greater than zero"
            )

        for _ in range(batches):
            yield from self.generate_batch()