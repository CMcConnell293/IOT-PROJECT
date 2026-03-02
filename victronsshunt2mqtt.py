import time
import serial
from serial.tools import list_ports
import json

from mqtt_utils import MQTTConnector


class VEDirectReader:
    def __init__(self, port=None, device_name=None, baud=19200, timeout=2):
        self.port = port or self.get_valid_port()
        self.device_name = device_name or self.get_valid_device()
        self.baud = baud if baud is not None else 19200
        self.ser = None
        self.timeout = timeout or 2

    def connect(self):
        try:
            if self.ser and self.ser.is_open:
                self.ser.close()

            self.ser = serial.Serial(self.port, self.baud, timeout=self.timeout)
            self.ser.reset_input_buffer()
            return True
        except Exception as e:
            print(f"Serial connection error on {self.port}: {e}")
            return False

    def get_data_block(self):
        packet = {}
        start_time = time.time()

        while True:
            # Timeout if no full block received within 5 seconds
            if time.time() - start_time > 5:
                return None

            try:
                line = self.ser.readline().decode("utf-8", errors="ignore").strip()

                if not line:
                    time.sleep(0.1)  # CPU safety for empty buffer
                    continue

                if "\t" in line:
                    parts = line.split("\t")
                    if len(parts) == 2:
                        key, value = parts
                        packet[key] = value

                        if key == "Checksum":
                            return packet
            except Exception as e:
                print(f"Read Error: {e}")
                return None

    def create_json_payload(self, data_dict):

        def safe_int(val, default=0):
            try:
                if val is None:
                    return default
                return int(val)
            except (ValueError, TypeError):
                return default
        output = {
            "ts": safe_int(time.time()),
            "voltage": safe_int(data_dict.get("V", 0)) / 1000,
            "current": safe_int(data_dict.get("I", 0)) / 1000,
            "power": safe_int(data_dict.get("P", 0)),
            "soc": safe_int(data_dict.get("SOC", 0)) / 10,
            "ttg": safe_int(data_dict.get("TTG", -1)),
            "CE": safe_int(data_dict.get("CE", 0)) / 1000,
            "device": self.device_name,
        }
        return json.dumps(output)

    @staticmethod
    def get_valid_port():
        ports = list_ports.comports()
        if not ports:
            return input("No ports found. Enter port manually: ")
        for i, p in enumerate(ports):
            print(f"{i}: {p.device} [{p.description}]")
        selection = input("\nSelect port number: ")
        try:
            return ports[int(selection)].device
        except (ValueError, IndexError):
            return selection

    @staticmethod
    def get_valid_device():
        name = input("Enter nickname (default: SmartShunt): ")
        return name if name else "SmartShunt"


def main():
    mq = MQTTConnector(
        host="localhost",
        port=1883,
        topic="victron/SS300/data",
        username="callum",
        password="root",
        debug=False
    )

    shunt = VEDirectReader(
        port="/dev/ttyUSB0",
        device_name="SmartShunt",
        baud=19200,
        timeout=2,
    )

    mq.start()

    # Shunt Connect Retries

    max_connect_retries = 5
    connected = False

    for i in range(1, max_connect_retries + 1):
        if shunt.connect():
            connected = True
            print(f'Connection Successful on attempt: {i} of {max_connect_retries}')
            break
        else:
            print(f'Initial connection attempt: {i}/{max_connect_retries} failed. Retrying in 5 seconds...')
            time.sleep(5)

    if not connected:
        print('Could not connect, Exiting...')
        return

    print(f"--- Gateway Active: {shunt.device_name} -> {mq.host} ---")

    retry_count = 0
    max_retries = 5

    try:
        while True:
            data_dict = shunt.get_data_block()

            if data_dict:
                retry_count = 0
                json_payload = shunt.create_json_payload(data_dict)
                mq.publish(mq.topic, json_payload, qos=1)
                print(f"Published: {json_payload}")
            else:
                retry_count += 1
                print(f"Retry {retry_count}/{max_retries}: No data.")

                if retry_count >= max_retries:
                    print("Max retries exceeded. Exiting.")
                    break

                time.sleep(5)
                shunt.connect()

            time.sleep(0.01)

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        mq.stop()
        if shunt.ser and shunt.ser.is_open:
            shunt.ser.close()


if __name__ == "__main__":
    main()
