import os
import threading
from protocol import ByteWriter, ByteBuffer


class Partition:
    """
    An append-only log file on disk.

    Each record on disk is stored as:
        [record_size: 4 bytes][offset: 8 bytes][key_size: 2+N bytes][value_size: 4+M bytes]

    We also maintain an in-memory index: offset -> file position
    so we can quickly seek to any offset without scanning the whole file.
    """

    def __init__(self, log_dir: str, topic: str, partition_id: int):
        self.topic = topic
        self.partition_id = partition_id
        self.next_offset = 0
        self.lock = threading.Lock()

        self.path = os.path.join(log_dir, f"{topic}-{partition_id}")
        os.makedirs(self.path, exist_ok=True)

        self.log_file = os.path.join(self.path, "log.bin")

        self.index = {}

        self._recover()

    def _recover(self):
        """Read existing log file to rebuild index and find next offset."""
        if not os.path.exists(self.log_file):
            return

        with open(self.log_file, 'rb') as f:
            while True:
                file_position = f.tell()

                # Try to read record size
                size_bytes = f.read(4)
                if len(size_bytes) < 4:
                    break

                record_size = int.from_bytes(size_bytes, byteorder='big')
                record_data = f.read(record_size)
                if len(record_data) < record_size:
                    break

                # Parse just the offset from the record
                buf = ByteBuffer(record_data)
                offset = buf.read_int64()

                self.index[offset] = file_position
                self.next_offset = offset + 1

        print(f"  Recovered {self.topic}-{self.partition_id}: {self.next_offset} records")

    def append(self, key: str, value: bytes) -> int:
        """
        Append a record to the log. Returns the assigned offset.
        Thread-safe via lock since multiple connections might produce
        to the same partition.
        """
        with self.lock:
            offset = self.next_offset

            # Build the record bytes
            writer = ByteWriter()
            writer.write_int64(offset)
            writer.write_string(key)
            writer.write_bytes(value)
            record_bytes = writer.to_bytes()

            with open(self.log_file, 'ab') as f:
                file_position = f.tell()
                f.write(len(record_bytes).to_bytes(4, byteorder='big'))
                f.write(record_bytes)

            # Update in-memory index
            self.index[offset] = file_position
            self.next_offset = offset + 1

            return offset

    def read(self, start_offset: int, max_records: int = 10) -> list:
        """
        Read records starting from start_offset.
        Returns list of (offset, key, value) tuples.
        """
        records = []

        if not os.path.exists(self.log_file):
            return records

        with open(self.log_file, 'rb') as f:
            # Use index to jump directly to the right position
            if start_offset in self.index:
                f.seek(self.index[start_offset])
            elif start_offset >= self.next_offset:
                return records  # Nothing to read
            else:
                return records  # Offset not found (maybe compacted)

            for _ in range(max_records):
                size_bytes = f.read(4)
                if len(size_bytes) < 4:
                    break

                record_size = int.from_bytes(size_bytes, byteorder='big')
                record_data = f.read(record_size)
                if len(record_data) < record_size:
                    break

                buf = ByteBuffer(record_data)
                offset = buf.read_int64()
                key = buf.read_string()
                value = buf.read_bytes()

                records.append((offset, key, value))

        return records
