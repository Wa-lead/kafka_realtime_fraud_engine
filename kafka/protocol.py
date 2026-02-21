# API Keys - what type of request is this
API_PRODUCE = 0
API_FETCH = 1
API_JOIN_GROUP = 2
API_CREATE_TOPIC = 3

# Error codes
ERR_NONE = 0
ERR_UNKNOWN_TOPIC = 1
ERR_UNKNOWN_PARTITION = 2
ERR_NO_GROUP = 3


class ByteBuffer:
    """Reads fields from a byte buffer, tracking position automatically."""

    def __init__(self, data: bytes):
        self.data = data
        self.position = 0

    def read_int8(self) -> int:
        value = self.data[self.position]
        self.position += 1
        return value

    def read_int16(self) -> int:
        value = int.from_bytes(self.data[self.position:self.position + 2], byteorder='big')
        self.position += 2
        return value

    def read_int32(self) -> int:
        value = int.from_bytes(self.data[self.position:self.position + 4], byteorder='big')
        self.position += 4
        return value

    def read_int64(self) -> int:
        value = int.from_bytes(self.data[self.position:self.position + 8], byteorder='big')
        self.position += 8
        return value

    def read_string(self) -> str:
        length = self.read_int16()
        value = self.data[self.position:self.position + length].decode('utf-8')
        self.position += length
        return value

    def read_bytes(self) -> bytes:
        length = self.read_int32()
        value = self.data[self.position:self.position + length]
        self.position += length
        return value

    def remaining(self) -> bytes:
        return self.data[self.position:]


class ByteWriter:
    """Builds a byte buffer by appending fields."""

    def __init__(self):
        self.data = bytearray()

    def write_int8(self, value: int):
        self.data.append(value)
        return self

    def write_int16(self, value: int):
        self.data.extend(value.to_bytes(2, byteorder='big'))
        return self

    def write_int32(self, value: int):
        self.data.extend(value.to_bytes(4, byteorder='big'))
        return self

    def write_int64(self, value: int):
        self.data.extend(value.to_bytes(8, byteorder='big'))
        return self

    def write_string(self, value: str):
        encoded = value.encode('utf-8')
        self.write_int16(len(encoded))
        self.data.extend(encoded)
        return self

    def write_bytes(self, value: bytes):
        self.write_int32(len(value))
        self.data.extend(value)
        return self

    def to_bytes(self) -> bytes:
        return bytes(self.data)


def frame_message(data: bytes) -> bytes:
    """Wrap data with a 4-byte length prefix for TCP framing."""
    return len(data).to_bytes(4, byteorder='big') + data


def recv_exact(sock, num_bytes: int) -> bytes:
    """Read exactly num_bytes from socket, looping if TCP splits the data."""
    data = b''
    while len(data) < num_bytes:
        chunk = sock.recv(num_bytes - len(data))
        if not chunk:
            raise ConnectionError("Connection closed")
        data += chunk
    return data


def recv_framed(sock) -> bytes:
    """Read one length-prefixed message from socket."""
    size_bytes = recv_exact(sock, 4)
    message_size = int.from_bytes(size_bytes, byteorder='big')
    return recv_exact(sock, message_size)


def send_framed(sock, data: bytes):
    """Send a length-prefixed message over socket."""
    sock.sendall(frame_message(data))
