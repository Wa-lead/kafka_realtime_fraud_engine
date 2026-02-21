## NEXT STEPS: add interfances because currently the payload details are only known to these functions

import socket
import threading
from protocol import (
    ByteBuffer, ByteWriter,
    recv_framed, send_framed,
    API_PRODUCE, API_FETCH, API_JOIN_GROUP, API_CREATE_TOPIC,
    ERR_NONE, ERR_UNKNOWN_TOPIC, ERR_UNKNOWN_PARTITION,
)
from partition import Partition


class Broker:
    def __init__(self, host='localhost', port=9092, log_dir='./data', topics=None):
        self.host = host
        self.port = port
        self.log_dir = log_dir
        self.topics = {}
        self.consumer_groups = {}

        self.lock = threading.Lock()
        if topics:
            for name, num_partitions in topics.items():
                self.create_topic(name, num_partitions)



    def create_topic(self, name: str, num_partitions: int):
        """Create a new topic with the given number of partitions."""
        with self.lock:
            if name in self.topics:
                return

            partitions = []
            for i in range(num_partitions):
                partitions.append(Partition(self.log_dir, name, i))

            self.topics[name] = partitions
            print(f"  Topic created: '{name}' ({num_partitions} partitions)")


    def join_group(self, group: str, consumer_id: str, topic: str) -> int:
        """
        Add a consumer to a group and assign it a partition.
        Returns the assigned partition index.
        """
        with self.lock:
            if topic not in self.topics:
                return -1

            num_partitions = len(self.topics[topic])

            if group not in self.consumer_groups:
                self.consumer_groups[group] = {}

            group_members = self.consumer_groups[group]

            # Simple assignment: give the next available partition
            assigned_partitions = set(group_members.values())
            for i in range(num_partitions):
                if i not in assigned_partitions:
                    group_members[consumer_id] = i
                    return i

            return -1


    def handle_produce(self, buf: ByteBuffer) -> bytes:
        """
        PRODUCE request payload:
            [topic: string][key: string][value: bytes]
        Response:
            [error_code: 2][partition: 4][offset: 8]
        """
        topic = buf.read_string()
        key = buf.read_string()
        value = buf.read_bytes()

        if topic not in self.topics:
            return ByteWriter().write_int16(ERR_UNKNOWN_TOPIC).write_int32(0).write_int64(0).to_bytes()

        partitions = self.topics[topic]
        partition_index = hash(key) % len(partitions)
        partition = partitions[partition_index]

        offset = partition.append(key, value)

        return (ByteWriter()
                .write_int16(ERR_NONE)
                .write_int32(partition_index)
                .write_int64(offset)
                .to_bytes())

    def handle_fetch(self, buf: ByteBuffer) -> bytes:
        """
        FETCH request payload:
            [topic: string][partition: 4][offset: 8][max_records: 4]
        Response:
            [error_code: 2][num_records: 4]
            then for each record: [offset: 8][key: string][value: bytes]
        """
        topic = buf.read_string()
        partition_index = buf.read_int32()
        start_offset = buf.read_int64()
        max_records = buf.read_int32()

        if topic not in self.topics:
            return ByteWriter().write_int16(ERR_UNKNOWN_TOPIC).write_int32(0).to_bytes()

        partitions = self.topics[topic]
        if partition_index >= len(partitions):
            return ByteWriter().write_int16(ERR_UNKNOWN_PARTITION).write_int32(0).to_bytes()

        partition = partitions[partition_index]
        records = partition.read(start_offset, max_records)

        writer = ByteWriter()
        writer.write_int16(ERR_NONE)
        writer.write_int32(len(records))

        for offset, key, value in records:
            writer.write_int64(offset)
            writer.write_string(key)
            writer.write_bytes(value)

        return writer.to_bytes()

    def handle_join_group(self, buf: ByteBuffer) -> bytes:
        """
        JOIN_GROUP request payload:
            [group: string][consumer_id: string][topic: string]
        Response:
            [error_code: 2][assigned_partition: 4]
        """
        group = buf.read_string()
        consumer_id = buf.read_string()
        topic = buf.read_string()

        partition = self.join_group(group, consumer_id, topic)

        if partition == -1:
            return ByteWriter().write_int16(ERR_UNKNOWN_TOPIC).write_int32(-1).to_bytes()

        return ByteWriter().write_int16(ERR_NONE).write_int32(partition).to_bytes()

    def handle_create_topic(self, buf: ByteBuffer) -> bytes:
        """
        CREATE_TOPIC request payload:
            [topic: string][num_partitions: 4]
        Response:
            [error_code: 2]
        """
        topic = buf.read_string()
        num_partitions = buf.read_int32()

        self.create_topic(topic, num_partitions)

        return ByteWriter().write_int16(ERR_NONE).to_bytes()

    def handle_request(self, data: bytes) -> bytes:
        """Parse request header and route to the right handler."""
        buf = ByteBuffer(data)

        # Parse header
        api_key = buf.read_int16()
        api_version = buf.read_int16()
        correlation_id = buf.read_int32()
        client_id = buf.read_string()

        # Route to handler
        if api_key == API_PRODUCE:
            response_body = self.handle_produce(buf)
        elif api_key == API_FETCH:
            response_body = self.handle_fetch(buf)
        elif api_key == API_JOIN_GROUP:
            response_body = self.handle_join_group(buf)
        elif api_key == API_CREATE_TOPIC:
            response_body = self.handle_create_topic(buf)
        else:
            response_body = ByteWriter().write_int16(99).to_bytes()  # unknown api

        # Build response: [correlation_id][body]
        response = ByteWriter()
        response.write_int32(correlation_id)
        response.data.extend(response_body)
        return response.to_bytes()

    # ──────────────────────────────────────────────
    # Network layer
    # ──────────────────────────────────────────────

    def handle_connection(self, client_socket, address):
        """Runs in a separate thread for each connected client."""
        try:
            while True:
                # Read one length-prefixed request
                data = recv_framed(client_socket)

                # Handle it and send response
                response = self.handle_request(data)
                send_framed(client_socket, response)

        except ConnectionError:
            pass
        finally:
            client_socket.close()

    def start(self):
        """Start the broker TCP server."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen()

        print(f"Broker listening on {self.host}:{self.port}")
        print("=" * 50)

        while True:
            client_socket, address = server.accept()
            thread = threading.Thread(
                target=self.handle_connection,
                args=(client_socket, address),
                daemon=True,
            )
            thread.start()


if __name__ == '__main__':
    broker = Broker()
    broker.start()
