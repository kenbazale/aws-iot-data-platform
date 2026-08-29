from dataclasses import dataclass
from datetime import datetime, timezone
import random
import uuid

@dataclass
class IoTDevice:
    """
    Represents a simulated IoT device.

    Each device has a normal operating baseline. Individual
    readings fluctuate around that baseline to create realistic
    telemetry rather than completely random data.
    """
    device_id: str
    device_type: str
    firmware_version: str
    
    base_temperature: float
    base_humidity: float
    base_pressure: float
    
    latitude: float
    longitude: float
    
    battery_level: float = 100.0
    
    def generate_event(self) -> dict:
        """
        Generate a single telemetry event.
        """
        event_timestamp = datetime.now(timezone.utc)
        
        temperature = (
            self.base_temperature +
            random.gauss(0, 0.5)
        )
        
        

        humidity = (
            self.base_humidity +
            random.gauss(0, 2.0)
        )

        pressure = (
            self.base_pressure +
            random.gauss(0, 0.03)
        )
        # Simulate gradual battery drain.
        self.battery_level = max(
            0.0,
            self.battery_level - random.uniform(0.001, 0.01)
        )
        
        return {
            "event_id": str(uuid.uuid4()),
            "device_id": self.device_id,
            "device_type": self.device_type,
            "event_timestamp": event_timestamp.isoformat(),
            "temperature": round(temperature, 3),
            "humidity": round(humidity, 3),
            "pressure": round(pressure, 4),
            "battery_level": round(self.battery_level, 3),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "firmware_version": self.firmware_version,
        }