
import time
import paho.mqtt.client as mqtt
import getpass
import json
import logging

logger = logging.getLogger(__name__)


class MQTTConnector:
    def __init__(self, host=None, port=None, topic=None, username=None, password=None, debug=False):
        self.host = host or input("Enter MQTT Host: ")
        self.port = int(port) if port else 1883
        self.topic = topic or "victron/data"

        if debug:
            self._setup_logging()

        # Use MQTTv311 for maximum compatibility with Mosquitto
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="Pi_Victron_Gateway",
            protocol=mqtt.MQTTv311,
        )

        self.client.enable_logger(logger)

        self.username = username or input("Enter MQTT Username: ")
        self.password = password or getpass.getpass("Enter MQTT Password: ")
        self.client.username_pw_set(self.username, self.password)

        self.client.on_connect = self.on_connect


    def _setup_logging(self):
        handler = logging.StreamHandler()
        formatter = logging.Formatter(f'%(asctime)s - %(name)s - {self.host}  %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        if not logger.handlers:
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        print("[INFO] Logging configured")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[SUCCESS] Connected to {self.host}")
            self.client.subscribe(self.topic)
        else:
            print(f"[FAILED] Connection refused with code {rc}")

    def start(self):
        try:
            self.client.connect(self.host, self.port, keepalive=60)
            self.client.loop_start()
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    def publish(self, topic, data, qos=1):
        """Restored: Handles both dicts and raw strings."""
        try:
            payload = json.dumps(data) if isinstance(data, dict) else data
            result = self.client.publish(topic, payload, qos=qos)
            return result
        except Exception as e:
            print(f"Publish error: {e}")
            return None

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()


if __name__ == "__main__":
    # Test block to verify everything is working
    connector = MQTTConnector(debug=True)
    if connector.start():
        try:
            while True:
                connector.publish(
                    connector.topic, {"test": "heartbeat", "ts": time.time()}
                )
                time.sleep(5)
        except KeyboardInterrupt:
            connector.stop()
