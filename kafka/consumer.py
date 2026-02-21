import socket
import time
import threading
from protocol import (
    ByteBuffer, ByteWriter,
    recv_framed, send_framed,
    API_FETCH, API_JOIN_GROUP,
    ERR_NONE,
)


class Consumer:
    def __init__(self, host='localhost', port=9092, client_id='consumer-1'):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.correlation_id = 0
        self.lock = threading.Lock()

        # Connect to broker
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

        # Will be set after joining a group
        self.assigned_partition = None
        self.current_offset = 0

    def _next_correlation_id(self) -> int:
        with self.lock:
            self.correlation_id += 1
            return self.correlation_id

    def _build_header(self, api_key: int) -> ByteWriter:
        writer = ByteWriter()
        writer.write_int16(api_key)
        writer.write_int16(1)
        writer.write_int32(self._next_correlation_id())
        writer.write_string(self.client_id)
        return writer

    def join_group(self, group: str, topic: str):
        """Join a consumer group and get a partition assignment."""
        writer = self._build_header(API_JOIN_GROUP)
        writer.write_string(group)
        writer.write_string(self.client_id)
        writer.write_string(topic)

        send_framed(self.sock, writer.to_bytes())

        response = recv_framed(self.sock)
        buf = ByteBuffer(response)
        correlation_id = buf.read_int32()
        error_code = buf.read_int16()
        partition = buf.read_int32()

        if error_code == ERR_NONE and partition >= 0:
            self.assigned_partition = partition
            pass
        else:
            print(f"Failed to join group: error {error_code}")

    def fetch(self, topic: str, max_records: int = 10) -> list:
        """Fetch messages from the assigned partition."""
        if self.assigned_partition is None:
            print("Not assigned to any partition. Join a group first.")
            return []

        writer = self._build_header(API_FETCH)
        writer.write_string(topic)
        writer.write_int32(self.assigned_partition)
        writer.write_int64(self.current_offset)
        writer.write_int32(max_records)

        send_framed(self.sock, writer.to_bytes())

        response = recv_framed(self.sock)
        buf = ByteBuffer(response)
        correlation_id = buf.read_int32()
        error_code = buf.read_int16()
        num_records = buf.read_int32()

        records = []
        for _ in range(num_records):
            offset = buf.read_int64()
            key = buf.read_string()
            value = buf.read_bytes().decode('utf-8')
            records.append((offset, key, value))

            # Advance our offset past what we've read
            self.current_offset = offset + 1

        return records

    def poll(self, topic: str, interval: float = 1.0):
        """Continuously poll for new messages."""
        print(f"Polling topic '{topic}' partition {self.assigned_partition}...")
        print("-" * 50)

        while True:
            records = self.fetch(topic)
            for offset, key, value in records:
                print(f"  offset={offset} key={key} value={value}")

            time.sleep(interval)

    def close(self):
        self.sock.close()


if __name__ == '__main__':
    import sys

    # Allow passing consumer id as argument so we can run multiple
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else 'consumer-1'

    consumer = Consumer(client_id=consumer_id)
    consumer.join_group('fraud-engine', 'transactions')

    try:
        consumer.poll('transactions')
    except KeyboardInterrupt:
        print("\nShutting down consumer")
        consumer.close()
