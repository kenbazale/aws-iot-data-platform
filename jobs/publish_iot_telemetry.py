import argparse
import json
import logging
import ssl
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from iot_platform.simulator.fleet import DeviceFleet
from iot_platform.simulator.generator import EventGenerator


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish simulated IoT telemetry to AWS IoT Core."
    )

    parser.add_argument(
        "--endpoint",
        required=True,
        help="AWS IoT Core ATS endpoint.",
    )

    parser.add_argument(
        "--cert-dir",
        default="aws/iot/certs",
        help="Directory containing device.pem.crt and private.pem.key.",
    )

    parser.add_argument(
        "--ca-file",
        default="aws/iot/certs/AmazonRootCA1.pem",
        help="AWS IoT Root CA certificate.",
    )

    parser.add_argument(
        "--device-id",
        default="iot-platform-device-001",
        help="Authorized IoT device identity/topic suffix.",
    )

    parser.add_argument(
        "--batches",
        type=int,
        default=2,
        help="Number of telemetry batches to publish.",
    )

    parser.add_argument(
        "--devices",
        type=int,
        default=1,
        help="Number of simulated devices to generate.",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds to wait between published events.",
    )

    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def build_client(
    endpoint: str,
    cert_dir: Path,
    ca_file: Path,
    client_id: str,
) -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
    )

    client.tls_set(
        ca_certs=str(ca_file),
        certfile=str(cert_dir / "device.pem.crt"),
        keyfile=str(cert_dir / "private.pem.key"),
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )

    client.tls_insecure_set(False)

    def on_connect(
        client: mqtt.Client,
        userdata,
        flags,
        reason_code,
        properties,
    ) -> None:
        logger.info("Connected to AWS IoT Core: reason_code=%s", reason_code)

    def on_disconnect(
        client: mqtt.Client,
        userdata,
        disconnect_flags,
        reason_code,
        properties,
    ) -> None:
        logger.info(
            "Disconnected from AWS IoT Core: reason_code=%s",
            reason_code,
        )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    return client


def main() -> None:
    args = parse_args()

    if args.batches <= 0:
        raise ValueError("--batches must be greater than zero")

    if args.devices <= 0:
        raise ValueError("--devices must be greater than zero")

    cert_dir = Path(args.cert_dir)
    ca_file = Path(args.ca_file)

    client = build_client(
        endpoint=args.endpoint,
        cert_dir=cert_dir,
        ca_file=ca_file,
        client_id=args.device_id,
    )

    topic = f"iot/devices/{args.device_id}/telemetry"

    fleet = DeviceFleet.create(
        number_of_devices=args.devices,
        seed=42,
    )

    generator = EventGenerator(fleet)

    logger.info("Connecting to %s", args.endpoint)
    client.connect(args.endpoint, port=8883, keepalive=60)
    client.loop_start()

    try:
        # Give the network loop a moment to establish the connection.
        time.sleep(1)

        published = 0

        for event in generator.generate(args.batches):
            # The current certificate is authorized as
            # iot-platform-device-001. Preserve the generated telemetry
            # payload, but publish it through the authorized MQTT topic.
            payload = json.dumps(event)

            info = client.publish(
                topic,
                payload=payload,
                qos=1,
            )

            info.wait_for_publish()

            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(
                    f"MQTT publish failed: rc={info.rc}"
                )

            published += 1

            logger.info(
                "Published event=%s simulated_device=%s topic=%s",
                event["event_id"],
                event["device_id"],
                topic,
            )

            time.sleep(args.interval)

        logger.info(
            "Publishing complete: published=%d topic=%s",
            published,
            topic,
        )

    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    configure_logging()
    main()
