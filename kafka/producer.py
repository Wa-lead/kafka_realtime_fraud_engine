import socket
import threading
from protocol import (
    ByteBuffer, ByteWriter,
    recv_framed, send_framed,
    API_PRODUCE, API_CREATE_TOPIC,
    ERR_NONE,
)


class Producer:
    def __init__(self, host='localhost', port=9092, client_id='producer-1'):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.correlation_id = 0
        self.lock = threading.Lock()

        # Connect to broker
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def _next_correlation_id(self) -> int:
        with self.lock:
            self.correlation_id += 1
            return self.correlation_id

    def _build_header(self, api_key: int) -> ByteWriter:
        """Build the standard request header."""
        writer = ByteWriter()
        writer.write_int16(api_key)
        writer.write_int16(1)  # api_version
        writer.write_int32(self._next_correlation_id())
        writer.write_string(self.client_id)
        return writer

    def create_topic(self, topic: str, num_partitions: int):
        """Ask the broker to create a topic."""
        writer = self._build_header(API_CREATE_TOPIC)
        writer.write_string(topic)
        writer.write_int32(num_partitions)

        send_framed(self.sock, writer.to_bytes())

        # Read response
        response = recv_framed(self.sock)
        buf = ByteBuffer(response)
        correlation_id = buf.read_int32()
        error_code = buf.read_int16()

        if error_code == ERR_NONE:
            pass
        else:
            print(f"Failed to create topic: error {error_code}")

    def send(self, topic: str, key: str, value: str):
        """Send a message to the broker."""
        writer = self._build_header(API_PRODUCE)
        writer.write_string(topic)
        writer.write_string(key)
        writer.write_bytes(value.encode('utf-8'))

        send_framed(self.sock, writer.to_bytes())

        # Read response
        response = recv_framed(self.sock)
        buf = ByteBuffer(response)
        correlation_id = buf.read_int32()
        error_code = buf.read_int16()
        partition = buf.read_int32()
        offset = buf.read_int64()

        if error_code == ERR_NONE:
            pass
        else:
            print(f"Failed to send: error {error_code}")

    def close(self):
        self.sock.close()


if __name__ == '__main__':
    producer = Producer()

    # Create a topic
    producer.create_topic('transactions', 3)

    # Send some messages
    producer.send('transactions', 'customer_100', '{"amount": 5000, "merchant": "store_a"}')
    producer.send('transactions', 'customer_200', '{"amount": 1200, "merchant": "store_b"}')
    producer.send('transactions', 'customer_100', '{"amount": 800, "merchant": "store_c"}')
    producer.send('transactions', 'customer_300', '{"amount": 15000, "merchant": "store_d"}')
    producer.send('transactions', 'customer_100', '{"amount": 200, "merchant": "store_e"}')

    producer.close()
