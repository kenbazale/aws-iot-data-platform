import argparse
import json
from pathlib import Path

from iot_platform.config.settings import load_config
from iot_platform.simulator.fleet import DeviceFleet
from iot_platform.simulator.generator import EventGenerator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate simulated IoT telemetry."
    )

    parser.add_argument(
        "--output",
        default="telemetry.jsonl",
        help="Output filename inside the configured raw directory.",
    )

    parser.add_argument(
        "--batches",
        type=int,
        default=5,
        help="Number of telemetry batches to generate.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_config("configs/dev.yaml")

    number_of_devices = config["simulation"]["devices"]

    output_path = (
        Path(config["storage"]["raw_path"])
        / args.output
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fleet = DeviceFleet.create(
        number_of_devices=number_of_devices,
        seed=42,
    )

    generator = EventGenerator(fleet)

    events = list(
        generator.generate(
            batches=args.batches,
        )
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:

        for event in events:
            file.write(
                json.dumps(event)
                + "\n"
            )

    print(
        f"Generated {len(events)} telemetry events: "
        f"{output_path}"
    )


if __name__ == "__main__":
    main()
