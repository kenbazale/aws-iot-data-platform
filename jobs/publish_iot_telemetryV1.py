import json
import os
import time
from pathlib import Path

import paho.mqtt.client as mqtt

from iot_platform.simulator.fleet import DeviceFleet
from iot_platform.simulator.generator import EventGenerator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

IOT_ENDPOINT = os.environ["IOT_ENDPOINT"]
IOT_PORT = int(os.getenv("IOT_PORT", "8883"))

IOT_TOPIC_TEMPLATE = "iot/devices/{device_id}/telemetry"

CLIENT_ID = os.getenv(
    "IOT_CLIENT_ID",
    "iot-platform-device-001",
)

BATCHES = int(os.getenv("IOT_BATCHES", "2"))
INTERVAL_SECONDS = float(
    os.getenv("IOT_INTERVAL_SECONDS", "5")
)

CERT_DIR = Path(
    os.getenv(
        "IOT_CERT_DIR",
        "aws/iot/certs",
    )
)

CA_FILE = os.getenv(
    "IOT_CA_FILE",
    "/etc/ssl/certs/ca-certificates.crt",
)

CERT_FILE = CERT_DIR / "device.pem.crt"
KEY_FILE = CERT_DIR / "private.pem.key"


# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties,
):
    if reason_code == 0:
        print("Connected to AWS IoT Core")
    else:
        print(
            f"MQTT connection failed | "
            f"reason_code={reason_code}"
        )


def on_publish(
    client,
    userdata,
    mid,
    reason_code,
    properties,
):
    print(f"Published MQTT message | mid={mid}")


# ---------------------------------------------------------------------------
# MQTT client
# ---------------------------------------------------------------------------

def create_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        protocol=mqtt.MQTTv5,
    )

    client.on_connect = on_connect
    client.on_publish = on_publish

    client.tls_set(
        ca_certs=CA_FILE,
        certfile=str(CERT_FILE),
        keyfile=str(KEY_FILE),
    )

    return client


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Starting IoT telemetry publisher")
    print(f"Endpoint: {IOT_ENDPOINT}")
    print(f"Client ID: {CLIENT_ID}")
    print(f"Batches: {BATCHES}")
    print(f"Interval: {INTERVAL_SECONDS}s")

    fleet = DeviceFleet.create(
        number_of_devices=10,
        seed=42,
    )

    generator = EventGenerator(fleet)

    client = create_mqtt_client()

    client.connect(
        IOT_ENDPOINT,
        IOT_PORT,
        keepalive=60,
    )

    client.loop_start()

    try:
        for batch_number in range(1, BATCHES + 1):
            events = list(generator.generate_batch())

            print(
                f"Generating batch {batch_number} "
                f"with {len(events)} events"
            )

            for event in events:
                device_id = event["device_id"]

                topic = IOT_TOPIC_TEMPLATE.format(
                    device_id=device_id,
                )

                payload = json.dumps(event)

                result = client.publish(
                    topic,
                    payload,
                    qos=1,
                )

                result.wait_for_publish()

                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError(
                        f"MQTT publish failed | "
                        f"device={device_id} | "
                        f"rc={result.rc}"
                    )

                print(
                    f"Published event | "
                    f"event_id={event['event_id']} | "
                    f"device_id={device_id}"
                )

            if batch_number < BATCHES:
                time.sleep(INTERVAL_SECONDS)

    finally:
        client.loop_stop()
        client.disconnect()

    print("IoT telemetry publisher completed successfully")


if __name__ == "__main__":
    main()